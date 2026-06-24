use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::{Duration, Instant, sleep, timeout},
};

use crate::{
    approval::{
        ApprovalBroker, ApprovalCancellation, ApprovalOption, ApprovalRequest, ApprovalSelection,
        SharedApprovalBroker,
    },
    config::{ExecutorConfig, ExecutorProtocol},
    executor::{
        ExecutorBackend, ExecutorChannelEvent, ExecutorDescriptor, ExecutorEventSink,
        ExecutorPrepareRequest, ExecutorPromptRequest, ExecutorResponse, ExecutorUpdate,
        PreparedExecutor, summarize_json_rpc_error,
    },
};

type SessionKey = (String, String);
type SharedCodexSession = Arc<Mutex<CodexAppServerSession>>;
type SessionMap = HashMap<SessionKey, SharedCodexSession>;
type SharedJsonRpcState = Arc<Mutex<JsonRpcState>>;
type SharedStdin = Arc<Mutex<ChildStdin>>;
const MAX_READY_NOTIFICATIONS_BEFORE_REQUEST: usize = 1024;

struct CodexServerRequest {
    message: Value,
    pending_file_change: Option<String>,
}

struct CodexTurnScope {
    cancelled: ApprovalCancellation,
}

impl CodexTurnScope {
    fn new() -> Self {
        Self {
            cancelled: ApprovalCancellation::new(),
        }
    }

    fn subscribe(&self) -> ApprovalCancellation {
        self.cancelled.clone()
    }

    fn cancel(&self) {
        self.cancelled.cancel();
    }
}

impl Drop for CodexTurnScope {
    fn drop(&mut self) {
        self.cancel();
    }
}

async fn wait_turn_cancelled(cancelled: ApprovalCancellation) {
    cancelled.cancelled().await;
}

#[derive(Debug, Clone, Copy)]
struct CodexRuntimeLimits {
    rpc_timeout: Duration,
    idle_timeout: Duration,
}

impl Default for CodexRuntimeLimits {
    fn default() -> Self {
        Self {
            rpc_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(600),
        }
    }
}

#[derive(Debug)]
pub struct CodexAppServerManager {
    executors: BTreeMap<String, ExecutorConfig>,
    approvals: SharedApprovalBroker,
    limits: CodexRuntimeLimits,
    sessions: Mutex<SessionMap>,
}

impl CodexAppServerManager {
    pub fn new(executors: BTreeMap<String, ExecutorConfig>) -> Self {
        Self::with_approvals(executors, Arc::new(ApprovalBroker::default()))
    }

    pub fn with_approvals(
        executors: BTreeMap<String, ExecutorConfig>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self::with_limits(executors, approvals, CodexRuntimeLimits::default())
    }

    fn with_limits(
        executors: BTreeMap<String, ExecutorConfig>,
        approvals: SharedApprovalBroker,
        limits: CodexRuntimeLimits,
    ) -> Self {
        let executors = executors
            .into_iter()
            .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::AppServer)
            .collect();
        Self {
            executors,
            approvals,
            limits,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    async fn get_or_create_session(
        &self,
        session_key: &str,
        executor: &str,
        cfg: &ExecutorConfig,
        session_cwd: Option<&Path>,
    ) -> anyhow::Result<SharedCodexSession> {
        let key = (session_key.to_string(), executor.to_string());
        let cwd = resolve_cwd(session_cwd.or(cfg.cwd.as_deref()))?;
        let existing = self.sessions.lock().await.get(&key).cloned();
        if let Some(existing) = existing {
            let matches = existing.lock().await.matches(cfg, &cwd);
            if matches {
                return Ok(existing);
            }
        }
        let session = Arc::new(Mutex::new(
            CodexAppServerSession::start(
                cfg.clone(),
                cwd,
                session_key.to_string(),
                executor.to_string(),
                self.approvals.clone(),
                self.limits,
            )
            .await?,
        ));
        self.sessions.lock().await.insert(key, session.clone());
        Ok(session)
    }

    async fn existing_session(
        &self,
        session_key: &str,
        executor: &str,
    ) -> anyhow::Result<SharedCodexSession> {
        self.sessions
            .lock()
            .await
            .get(&(session_key.to_string(), executor.to_string()))
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "executor `{executor}` has not prepared Codex app-server session for `{session_key}`"
                )
            })
    }
}

#[async_trait]
impl ExecutorBackend for CodexAppServerManager {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.executors.get(name).map(|cfg| ExecutorDescriptor {
            name: cfg.name.clone(),
            protocol: "app_server".to_string(),
        })
    }

    fn list(&self) -> Vec<ExecutorDescriptor> {
        self.executors
            .values()
            .map(|cfg| ExecutorDescriptor {
                name: cfg.name.clone(),
                protocol: "app_server".to_string(),
            })
            .collect()
    }

    async fn prepare(&self, request: ExecutorPrepareRequest) -> anyhow::Result<PreparedExecutor> {
        let cfg = self
            .executors
            .get(&request.executor)
            .ok_or_else(|| anyhow::anyhow!("executor `{}` is not configured", request.executor))?;
        tracing::info!(
            executor = %request.executor,
            session_key = %request.session_key,
            "preparing Codex app-server executor session"
        );
        let session = self
            .get_or_create_session(
                &request.session_key,
                &request.executor,
                cfg,
                request.cwd.as_deref(),
            )
            .await?;
        let mut session = session.lock().await;
        let (thread_id, started_new_session) = session.ensure_thread().await?;
        tracing::info!(
            executor = %request.executor,
            session_key = %request.session_key,
            thread_id = %thread_id,
            started_new_session,
            "prepared Codex app-server executor session"
        );
        Ok(PreparedExecutor {
            external_session_id: Some(thread_id),
            started_new_session,
        })
    }

    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
    ) -> anyhow::Result<ExecutorResponse> {
        let session = self
            .existing_session(&request.session_key, &request.executor)
            .await?;
        let mut session = session.lock().await;
        let session_key = request.session_key.clone();
        let executor = request.executor.clone();
        let prompt_len = request.prompt.len();
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            prompt_len,
            "starting Codex app-server turn"
        );
        let result = session
            .run_turn(&request.prompt, request.user_id, events)
            .await;
        match &result {
            Ok(response) => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                final_text_len = response.final_text.len(),
                "completed Codex app-server turn"
            ),
            Err(err) => tracing::warn!(
                error = %err,
                executor = %executor,
                session_key = %session_key,
                "failed Codex app-server turn"
            ),
        }
        result
    }
}

#[derive(Debug)]
struct CodexAppServerSession {
    cfg: ExecutorConfig,
    cwd: PathBuf,
    client: CodexJsonRpcClient,
    session_key: String,
    executor: String,
    approvals: SharedApprovalBroker,
    pending_file_changes: HashMap<String, String>,
    limits: CodexRuntimeLimits,
    thread_id: Option<String>,
    initialized: bool,
}

impl CodexAppServerSession {
    async fn start(
        cfg: ExecutorConfig,
        cwd: PathBuf,
        session_key: String,
        executor: String,
        approvals: SharedApprovalBroker,
        limits: CodexRuntimeLimits,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            command = %cfg.command,
            cwd = %cwd.display(),
            "starting Codex app-server process"
        );
        let client = CodexJsonRpcClient::spawn(
            &cfg.command,
            &cfg.args,
            &cwd,
            &cfg.env,
            session_key.clone(),
            executor.clone(),
        )
        .await?;
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            "started Codex app-server process"
        );
        Ok(Self {
            cfg,
            cwd,
            client,
            session_key,
            executor,
            approvals,
            pending_file_changes: HashMap::new(),
            limits,
            thread_id: None,
            initialized: false,
        })
    }

    fn matches(&self, cfg: &ExecutorConfig, cwd: &Path) -> bool {
        self.cfg.command == cfg.command
            && self.cfg.args == cfg.args
            && self.cfg.env == cfg.env
            && self.cwd == cwd
            && self.client.is_alive()
    }

    async fn initialize(&mut self) -> anyhow::Result<()> {
        if self.initialized {
            return Ok(());
        }
        let initialized = self
            .client
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "agent-router",
                        "title": "Agent Router",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": {},
                }),
                self.limits.rpc_timeout,
            )
            .await;
        if let Err(err) = initialized {
            self.client
                .close("codex app-server initialize failed")
                .await;
            return Err(err);
        }
        self.client.notify("initialized", json!({})).await?;
        self.initialized = true;
        Ok(())
    }

    async fn ensure_thread(&mut self) -> anyhow::Result<(String, bool)> {
        self.initialize().await?;
        if let Some(thread_id) = &self.thread_id {
            return Ok((thread_id.clone(), false));
        }
        let result = self
            .client
            .request(
                "thread/start",
                json!({ "cwd": self.cwd }),
                self.limits.rpc_timeout,
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(err) => {
                self.client
                    .close("codex app-server thread/start failed")
                    .await;
                return Err(err);
            }
        };
        let thread_id = thread_id_from_result(&result)
            .ok_or_else(|| anyhow::anyhow!("codex thread/start did not return thread id"))?;
        self.thread_id = Some(thread_id.clone());
        Ok((thread_id, true))
    }

    async fn run_turn(
        &mut self,
        prompt: &str,
        user_id: Option<String>,
        events: &mut dyn ExecutorEventSink,
    ) -> anyhow::Result<ExecutorResponse> {
        let thread_id = self
            .thread_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("codex app-server thread has not been created"))?;
        let mut notifications = self.client.subscribe_notifications();
        let mut server_requests = self.client.subscribe_server_requests();
        let mut closed = self.client.subscribe_closed();
        let turn_scope = CodexTurnScope::new();
        let (server_response_tx, mut server_responses) = mpsc::channel(64);
        let (server_request_tx, server_request_rx) = mpsc::unbounded_channel();
        let server_request_handler = self.spawn_server_request_worker(
            server_request_rx,
            user_id.clone(),
            server_response_tx.clone(),
            turn_scope.subscribe(),
        );
        let mut turn_start = self
            .client
            .request_started(
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": prompt}],
                }),
            )
            .await?;

        let result: anyhow::Result<ExecutorResponse> = async {
            let mut final_text = String::new();
            let mut turn_start_acknowledged = false;
            let mut turn_completed = false;
            let turn_start_timeout = sleep(self.limits.rpc_timeout);
            let idle_timeout = sleep(self.limits.idle_timeout);
            tokio::pin!(turn_start_timeout);
            tokio::pin!(idle_timeout);
            loop {
                tokio::select! {
                response = &mut turn_start.response, if !turn_start_acknowledged => {
                    idle_timeout
                        .as_mut()
                        .reset(Instant::now() + self.limits.idle_timeout);
                    turn_start_acknowledged = true;
                    json_rpc_result(&turn_start.method, response??)?;
                    if turn_completed {
                        break;
                    }
                }
                request = server_requests.recv() => {
                    match request {
                        Ok(request) => {
                            idle_timeout
                                .as_mut()
                                .reset(Instant::now() + self.limits.idle_timeout);
                            self.drain_ready_notifications(
                                &mut notifications,
                                &mut final_text,
                                events,
                                &mut turn_completed,
                                &mut idle_timeout,
                            ).await?;
                            if turn_completed {
                                if turn_start_acknowledged {
                                    break;
                                }
                                continue;
                            }
                            server_request_tx
                                .send(CodexServerRequest {
                                    pending_file_change: self.pending_file_change_summary(
                                        request.get("params").unwrap_or(&Value::Null),
                                    ),
                                    message: request,
                                })
                                .map_err(|_| {
                                    anyhow::anyhow!("codex app-server request worker closed")
                                })?;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => {
                            anyhow::bail!("codex app-server request stream closed")
                        }
                    }
                }
                response = server_responses.recv() => {
                    if let Some(response) = response {
                        self.drain_ready_notifications(
                            &mut notifications,
                            &mut final_text,
                            events,
                            &mut turn_completed,
                            &mut idle_timeout,
                        ).await?;
                        if turn_completed {
                            if turn_start_acknowledged {
                                break;
                            }
                            continue;
                        }
                        if !turn_start_acknowledged && turn_start_timeout.as_ref().is_elapsed() {
                            self.client.cancel_pending(turn_start.id).await;
                            self.client.close("codex app-server turn/start timed out").await;
                            anyhow::bail!(
                                "codex app-server `turn/start` timed out after {}s",
                                self.limits.rpc_timeout.as_secs()
                            );
                        }
                        if idle_timeout.as_ref().is_elapsed() {
                            self.client.cancel_pending(turn_start.id).await;
                            self.client.close("codex app-server idle timed out").await;
                            anyhow::bail!(
                                "codex app-server idle timed out after {}s without activity",
                                self.limits.idle_timeout.as_secs()
                            );
                        }
                        write_json(&self.client.stdin, response).await?;
                    }
                }
                notification = notifications.recv() => {
                    match notification {
                        Ok(notification) => {
                            idle_timeout
                                .as_mut()
                                .reset(Instant::now() + self.limits.idle_timeout);
                            self.handle_notification(
                                notification,
                                &mut final_text,
                                events,
                                &mut turn_completed,
                            ).await?;
                            if turn_completed && turn_start_acknowledged {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => {
                            anyhow::bail!("codex app-server notification stream closed")
                        }
                    }
                }
                reason = closed.recv() => {
                    let reason = reason.unwrap_or_else(|_| "codex app-server closed".to_string());
                    anyhow::bail!("{reason}");
                }
                _ = &mut turn_start_timeout, if !turn_start_acknowledged => {
                    self.client.cancel_pending(turn_start.id).await;
                    self.client.close("codex app-server turn/start timed out").await;
                    anyhow::bail!(
                        "codex app-server `turn/start` timed out after {}s",
                        self.limits.rpc_timeout.as_secs()
                    );
                }
                _ = &mut idle_timeout => {
                    self.client.cancel_pending(turn_start.id).await;
                    self.client.close("codex app-server idle timed out").await;
                    anyhow::bail!(
                        "codex app-server idle timed out after {}s without activity",
                        self.limits.idle_timeout.as_secs()
                    );
                }
                }
            }

            Ok(ExecutorResponse { final_text })
        }
        .await;

        drop(server_request_tx);
        turn_scope.cancel();
        let _ = server_request_handler.await;
        result
    }

    async fn drain_ready_notifications(
        &mut self,
        notifications: &mut broadcast::Receiver<Value>,
        final_text: &mut String,
        events: &mut dyn ExecutorEventSink,
        turn_completed: &mut bool,
        idle_timeout: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    ) -> anyhow::Result<()> {
        for _ in 0..MAX_READY_NOTIFICATIONS_BEFORE_REQUEST {
            match notifications.try_recv() {
                Ok(notification) => {
                    idle_timeout
                        .as_mut()
                        .reset(Instant::now() + self.limits.idle_timeout);
                    self.handle_notification(notification, final_text, events, turn_completed)
                        .await?;
                }
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => {
                    anyhow::bail!("codex app-server notification stream closed")
                }
            }
        }
        Ok(())
    }

    async fn handle_notification(
        &mut self,
        notification: Value,
        final_text: &mut String,
        events: &mut dyn ExecutorEventSink,
        turn_completed: &mut bool,
    ) -> anyhow::Result<()> {
        self.track_pending_file_change(&notification);
        let collected = collect_codex_notification(notification, final_text)?;
        for update in collected.updates {
            events.send(update).await?;
        }
        match collected.outcome {
            CodexNotificationOutcome::Pending => {}
            CodexNotificationOutcome::TurnCompleted => *turn_completed = true,
        }
        Ok(())
    }

    fn spawn_server_request_worker(
        &self,
        mut requests: mpsc::UnboundedReceiver<CodexServerRequest>,
        user_id: Option<String>,
        responses: mpsc::Sender<Value>,
        turn_cancelled: ApprovalCancellation,
    ) -> JoinHandle<()> {
        let approvals = self.approvals.clone();
        let session_key = self.session_key.clone();
        let executor = self.executor.clone();

        tokio::spawn(async move {
            loop {
                let request = tokio::select! {
                    biased;
                    _ = wait_turn_cancelled(turn_cancelled.clone()) => break,
                    request = requests.recv() => {
                        let Some(request) = request else {
                            break;
                        };
                        request
                    }
                };
                match Self::handle_server_request(
                    approvals.clone(),
                    session_key.clone(),
                    executor.clone(),
                    user_id.clone(),
                    request.message,
                    request.pending_file_change,
                    turn_cancelled.clone(),
                )
                .await
                {
                    Ok(Some(response)) => {
                        tokio::select! {
                            biased;
                            _ = wait_turn_cancelled(turn_cancelled.clone()) => {}
                            result = responses.send(response) => {
                                if result.is_err() {
                                    tracing::debug!("dropping Codex app-server response for closed turn");
                                }
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to handle Codex app-server request");
                    }
                }
            }
        })
    }

    async fn handle_server_request(
        approvals: SharedApprovalBroker,
        session_key: String,
        executor: String,
        user_id: Option<String>,
        message: Value,
        pending_file_change: Option<String>,
        turn_cancelled: ApprovalCancellation,
    ) -> anyhow::Result<Option<Value>> {
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let params = message.get("params").unwrap_or(&Value::Null);
        match method {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                let request = codex_approval_request(
                    &session_key,
                    &executor,
                    user_id,
                    method,
                    params,
                    pending_file_change,
                );
                let Some(selection) = approvals
                    .request_until_cancelled(request, turn_cancelled)
                    .await
                else {
                    return Ok(None);
                };
                let decision = match selection {
                    ApprovalSelection::Selected(option_id) if option_id == "accept" => "accept",
                    _ => "decline",
                };
                Ok(Some(codex_result_response(
                    id,
                    json!({ "decision": decision }),
                )))
            }
            "item/permissions/requestApproval" => Ok(Some(codex_result_response(
                id,
                json!({ "decision": "decline" }),
            ))),
            "mcpServer/elicitation/request" => Ok(Some(codex_result_response(
                id,
                json!({ "action": "decline" }),
            ))),
            _ => Ok(Some(codex_error_response(
                id,
                -32601,
                format!("agent-router does not support codex client method `{method}`"),
            ))),
        }
    }

    fn track_pending_file_change(&mut self, notification: &Value) {
        let method = notification
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("");
        if method != "item/started" && method != "item/completed" {
            return;
        }
        let Some(item) = notification
            .get("params")
            .and_then(|params| params.get("item"))
        else {
            return;
        };
        if item.get("type").and_then(Value::as_str) != Some("fileChange") {
            return;
        }
        let item_id = item.get("id").and_then(Value::as_str).unwrap_or("");
        if item_id.is_empty() {
            return;
        }
        if method == "item/completed" {
            self.pending_file_changes.remove(item_id);
            return;
        }
        let summary = summarize_file_change(item);
        self.pending_file_changes
            .insert(item_id.to_string(), summary);
    }

    fn pending_file_change_summary(&self, params: &Value) -> Option<String> {
        let item_id = params.get("itemId").and_then(Value::as_str)?;
        self.pending_file_changes.get(item_id).cloned()
    }
}

fn codex_approval_request(
    session_key: &str,
    executor: &str,
    requester_user_id: Option<String>,
    method: &str,
    params: &Value,
    pending_file_change: Option<String>,
) -> ApprovalRequest {
    let (title, body) = match method {
        "item/commandExecution/requestApproval" => {
            let command = params
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let cwd = params.get("cwd").and_then(Value::as_str).unwrap_or("");
            let reason = params.get("reason").and_then(Value::as_str).unwrap_or("");
            let mut lines = Vec::new();
            if !cwd.is_empty() {
                lines.push(format!("cwd: {cwd}"));
            }
            if !reason.is_empty() {
                lines.push(format!("reason: {reason}"));
            }
            lines.push(format!("$ {command}"));
            ("Codex command approval".to_string(), lines.join("\n"))
        }
        "item/fileChange/requestApproval" => {
            let reason = params.get("reason").and_then(Value::as_str).unwrap_or("");
            let grant_root = params
                .get("grantRoot")
                .and_then(Value::as_str)
                .unwrap_or("");
            let item_id = params.get("itemId").and_then(Value::as_str).unwrap_or("");
            let mut lines = Vec::new();
            if !reason.is_empty() {
                lines.push(format!("reason: {reason}"));
            }
            if !grant_root.is_empty() {
                lines.push(format!("grant root: {grant_root}"));
            }
            if !item_id.is_empty() {
                lines.push(format!("item: {item_id}"));
            }
            if let Some(summary) = pending_file_change.filter(|summary| !summary.is_empty()) {
                lines.push(format!("changes: {summary}"));
            }
            ("Codex file change approval".to_string(), lines.join("\n"))
        }
        _ => ("Codex approval".to_string(), String::new()),
    };
    ApprovalRequest {
        session_key: session_key.to_string(),
        executor: executor.to_string(),
        requester_user_id,
        title,
        body: truncate_text(body, 2_000),
        options: vec![
            ApprovalOption {
                id: "accept".to_string(),
                kind: "allow_once".to_string(),
                name: "Approve".to_string(),
                auto_approvable: true,
            },
            ApprovalOption {
                id: "decline".to_string(),
                kind: "reject_once".to_string(),
                name: "Deny".to_string(),
                auto_approvable: false,
            },
        ],
    }
}

#[derive(Debug)]
struct CodexJsonRpcClient {
    stdin: SharedStdin,
    state: SharedJsonRpcState,
    next_id: AtomicU64,
    notifications: broadcast::Sender<Value>,
    server_requests: broadcast::Sender<Value>,
    closed: broadcast::Sender<String>,
    child: Arc<Mutex<Child>>,
    session_key: String,
    executor: String,
}

#[derive(Debug)]
struct PendingJsonRpcRequest {
    id: u64,
    method: String,
    response: oneshot::Receiver<anyhow::Result<Value>>,
}

#[derive(Debug, Default)]
struct JsonRpcState {
    closed: bool,
    pending: HashMap<u64, oneshot::Sender<anyhow::Result<Value>>>,
}

impl CodexJsonRpcClient {
    async fn spawn(
        command: &str,
        args: &[String],
        cwd: &Path,
        env: &BTreeMap<String, String>,
        session_key: String,
        executor: String,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            target: "agent_router::codex_app_server",
            executor = %executor,
            session_key = %session_key,
            command = %command,
            arg_count = args.len(),
            cwd = %cwd.display(),
            "spawning Codex app-server process"
        );
        let mut cmd = Command::new(command);
        cmd.args(args).current_dir(cwd);
        for (key, value) in env {
            cmd.env(key, value);
        }
        cmd.kill_on_drop(true);
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = cmd.spawn().map_err(|err| {
            anyhow::anyhow!("could not start codex app-server command `{command}`: {err}")
        })?;
        let pid = child.id();
        tracing::info!(
            target: "agent_router::codex_app_server",
            executor = %executor,
            session_key = %session_key,
            command = %command,
            pid = ?pid,
            "spawned Codex app-server process"
        );
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("codex app-server process did not expose stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("codex app-server process did not expose stdout"))?;
        let stderr = child.stderr.take();

        let stdin = Arc::new(Mutex::new(stdin));
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let (notifications, _) = broadcast::channel(512);
        let (server_requests, _) = broadcast::channel(64);
        let (closed, _) = broadcast::channel(8);
        let child = Arc::new(Mutex::new(child));

        tokio::spawn(read_codex_stdout(
            BufReader::new(stdout),
            state.clone(),
            notifications.clone(),
            server_requests.clone(),
            closed.clone(),
            session_key.clone(),
            executor.clone(),
        ));
        if let Some(stderr) = stderr {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(
                        target: "agent_router::codex_app_server",
                        bytes = line.len(),
                        "codex app-server emitted stderr"
                    );
                }
            });
        }

        Ok(Self {
            stdin,
            state,
            next_id: AtomicU64::new(1),
            notifications,
            server_requests,
            closed,
            child,
            session_key,
            executor,
        })
    }

    fn subscribe_notifications(&self) -> broadcast::Receiver<Value> {
        self.notifications.subscribe()
    }

    fn subscribe_server_requests(&self) -> broadcast::Receiver<Value> {
        self.server_requests.subscribe()
    }

    fn subscribe_closed(&self) -> broadcast::Receiver<String> {
        self.closed.subscribe()
    }

    fn is_alive(&self) -> bool {
        if self
            .state
            .try_lock()
            .map(|state| state.closed)
            .unwrap_or(true)
        {
            return false;
        }
        self.child
            .try_lock()
            .map(|mut child| {
                child
                    .try_wait()
                    .map(|status| status.is_none())
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    }

    async fn request(
        &self,
        method: &str,
        params: Value,
        timeout_duration: Duration,
    ) -> anyhow::Result<Value> {
        let request = self.request_started(method, params).await?;
        let id = request.id;
        let method = request.method;
        let response = match timeout(timeout_duration, request.response).await {
            Ok(Ok(Ok(response))) => response,
            Ok(Ok(Err(err))) => return Err(err),
            Ok(Err(_)) => anyhow::bail!("codex app-server response channel closed"),
            Err(_) => {
                self.cancel_pending(id).await;
                anyhow::bail!(
                    "codex app-server `{method}` timed out after {}s",
                    timeout_duration.as_secs()
                );
            }
        };
        json_rpc_result(&method, response)
    }

    async fn request_started(
        &self,
        method: &str,
        params: Value,
    ) -> anyhow::Result<PendingJsonRpcRequest> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            anyhow::ensure!(!state.closed, "codex app-server stdout is closed");
            state.pending.insert(id, tx);
        }
        if let Err(err) = write_json(
            &self.stdin,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }),
        )
        .await
        {
            self.state.lock().await.pending.remove(&id);
            return Err(err);
        }
        Ok(PendingJsonRpcRequest {
            id,
            method: method.to_string(),
            response: rx,
        })
    }

    async fn cancel_pending(&self, id: u64) {
        self.state.lock().await.pending.remove(&id);
    }

    async fn close(&self, reason: &str) {
        fail_all_pending(&self.state, reason).await;
        let _ = self.closed.send(reason.to_string());
        let mut child = self.child.lock().await;
        let pid = child.id();
        tracing::warn!(
            target: "agent_router::codex_app_server",
            executor = %self.executor,
            session_key = %self.session_key,
            pid = ?pid,
            reason,
            "closing Codex app-server process"
        );
        if let Err(err) = child.start_kill() {
            tracing::warn!(
                target: "agent_router::codex_app_server",
                executor = %self.executor,
                session_key = %self.session_key,
                pid = ?pid,
                reason,
                error = %err,
                "failed to signal Codex app-server process"
            );
        }
    }

    async fn notify(&self, method: &str, params: Value) -> anyhow::Result<()> {
        write_json(
            &self.stdin,
            json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }),
        )
        .await
    }
}

fn json_rpc_result(method: &str, response: Value) -> anyhow::Result<Value> {
    if let Some(error) = response.get("error") {
        anyhow::bail!(
            "codex app-server `{method}` failed: {}",
            summarize_json_rpc_error(error)
        );
    }
    Ok(response.get("result").cloned().unwrap_or(Value::Null))
}

fn codex_result_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn codex_error_response(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

async fn read_codex_stdout<R>(
    reader: BufReader<R>,
    state: SharedJsonRpcState,
    notifications: broadcast::Sender<Value>,
    server_requests: broadcast::Sender<Value>,
    closed: broadcast::Sender<String>,
    session_key: String,
    executor: String,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            tracing::debug!(
                target: "agent_router::codex_app_server",
                bytes = line.len(),
                "ignoring non-json codex stdout"
            );
            continue;
        };
        dispatch_codex_message(message, &state, &notifications, &server_requests).await;
    }
    tracing::warn!(
        target: "agent_router::codex_app_server",
        executor = %executor,
        session_key = %session_key,
        "Codex app-server stdout closed"
    );
    fail_all_pending(&state, "codex app-server closed stdout").await;
    let _ = closed.send("codex app-server closed stdout".to_string());
}

async fn dispatch_codex_message(
    message: Value,
    state: &SharedJsonRpcState,
    notifications: &broadcast::Sender<Value>,
    server_requests: &broadcast::Sender<Value>,
) {
    let has_method = message.get("method").is_some();
    let has_id = message.get("id").is_some();
    if has_id && !has_method {
        let sender = match message.get("id").and_then(Value::as_u64) {
            Some(id) => state.lock().await.pending.remove(&id),
            None => None,
        };
        if let Some(tx) = sender {
            let _ = tx.send(Ok(message));
        }
        return;
    }
    if has_id && has_method {
        let _ = server_requests.send(message);
        return;
    }
    if has_method {
        let _ = notifications.send(message);
    }
}

async fn fail_all_pending(state: &SharedJsonRpcState, message: &str) {
    let drained = {
        let mut guard = state.lock().await;
        guard.closed = true;
        guard.pending.drain().collect::<Vec<_>>()
    };
    for (_, tx) in drained {
        let _ = tx.send(Err(anyhow::anyhow!("{message}")));
    }
}

async fn write_json(stdin: &SharedStdin, value: Value) -> anyhow::Result<()> {
    let mut guard = stdin.lock().await;
    let line = serde_json::to_string(&value)?;
    guard.write_all(line.as_bytes()).await?;
    guard.write_all(b"\n").await?;
    guard.flush().await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexNotificationOutcome {
    Pending,
    TurnCompleted,
}

#[derive(Debug)]
struct CollectedCodexNotification {
    outcome: CodexNotificationOutcome,
    updates: Vec<ExecutorUpdate>,
}

fn collect_codex_notification(
    notification: Value,
    final_text: &mut String,
) -> anyhow::Result<CollectedCodexNotification> {
    let method = notification
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("");
    if method == "turn/completed" {
        validate_turn_completed(&notification)?;
        return Ok(CollectedCodexNotification {
            outcome: CodexNotificationOutcome::TurnCompleted,
            updates: Vec::new(),
        });
    }
    if method != "item/completed" {
        return Ok(CollectedCodexNotification {
            outcome: CodexNotificationOutcome::Pending,
            updates: Vec::new(),
        });
    }
    let Some(item) = notification
        .get("params")
        .and_then(|params| params.get("item"))
    else {
        return Ok(CollectedCodexNotification {
            outcome: CodexNotificationOutcome::Pending,
            updates: Vec::new(),
        });
    };
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
    let mut updates = Vec::new();
    match item_type {
        "agentMessage" => {
            let text = item
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let phase = item.get("phase").and_then(Value::as_str).unwrap_or("");
            let mut update = ExecutorUpdate::new(
                "agent_message_chunk",
                String::new(),
                text.clone(),
                String::new(),
            );
            if phase == "commentary" {
                update = update.with_channel_event(ExecutorChannelEvent::agent_progress(text));
            } else {
                *final_text = text;
            }
            updates.push(update);
        }
        "reasoning" => {
            let summary = extract_text(item.get("summary"));
            let text = summary
                .clone()
                .or_else(|| extract_text(item.get("content")))
                .unwrap_or_default();
            let mut update =
                ExecutorUpdate::new("agent_thought_chunk", "Reasoning", text, String::new());
            if let Some(summary) = summary.filter(|summary| !summary.trim().is_empty()) {
                let summary = truncate_text(summary, 1_000);
                update = update
                    .with_channel_event(ExecutorChannelEvent::reasoning_summary(summary.clone()));
                update = update.with_transcript_summary(format!("Reasoning: {summary}"));
            }
            updates.push(update);
        }
        "commandExecution" => {
            let summary = command_execution_summary(item);
            let channel_summary = command_execution_channel_summary(item);
            updates.push(
                ExecutorUpdate::new(
                    "tool_call",
                    "Bash",
                    summary.clone(),
                    item.get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                )
                .with_transcript_summary(format!("Bash: {channel_summary}"))
                .with_channel_event(ExecutorChannelEvent::tool_call("Bash", channel_summary)),
            );
        }
        "fileChange" => {
            updates.push(ExecutorUpdate::new(
                "diff",
                "File change",
                file_change_summary(item),
                item.get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            ));
        }
        "mcpToolCall" | "dynamicToolCall" => {
            let summary = tool_call_item_summary(item);
            updates.push(
                ExecutorUpdate::new(
                    "tool_call",
                    item_type.to_string(),
                    summary.clone(),
                    item.get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                )
                .with_transcript_summary(format!("{item_type}: {summary}"))
                .with_channel_event(ExecutorChannelEvent::tool_call(item_type, summary)),
            );
        }
        _ => {}
    }
    Ok(CollectedCodexNotification {
        outcome: CodexNotificationOutcome::Pending,
        updates,
    })
}

fn validate_turn_completed(notification: &Value) -> anyhow::Result<()> {
    let turn = notification
        .get("params")
        .and_then(|params| params.get("turn"))
        .unwrap_or(&Value::Null);
    let status = turn.get("status").and_then(Value::as_str).unwrap_or("");
    if status.is_empty() || status == "completed" {
        return Ok(());
    }
    let error = turn
        .get("error")
        .map(summarize_json_rpc_error)
        .unwrap_or_else(|| "no error details".to_string());
    anyhow::bail!("codex app-server turn ended with status `{status}`: {error}")
}

fn command_execution_summary(item: &Value) -> String {
    let command = item
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let output = item
        .get("aggregatedOutput")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let exit_code = item.get("exitCode").and_then(Value::as_i64);
    let mut lines = Vec::new();
    if !command.is_empty() {
        lines.push(format!("$ {command}"));
    }
    if let Some(exit_code) = exit_code {
        lines.push(format!("exit: {exit_code}"));
    }
    if !output.is_empty() {
        lines.push(output.to_string());
    }
    truncate_text(lines.join("\n"), 2_000)
}

fn command_execution_channel_summary(item: &Value) -> String {
    let command = item
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let exit_code = item.get("exitCode").and_then(Value::as_i64);
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut lines = Vec::new();
    if !command.trim().is_empty() {
        lines.push(format!("$ {}", truncate_text(one_line(command), 500)));
    }
    if let Some(exit_code) = exit_code {
        lines.push(format!("exit: {exit_code}"));
    }
    if !status.is_empty() {
        lines.push(format!("status: {status}"));
    }
    if lines.is_empty() {
        "command execution completed".to_string()
    } else {
        truncate_text(lines.join("\n"), 1_000)
    }
}

fn tool_call_item_summary(item: &Value) -> String {
    let name = item
        .get("name")
        .or_else(|| item.get("toolName"))
        .or_else(|| item.get("tool_name"))
        .or_else(|| item.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("tool call");
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if status.is_empty() {
        name.to_string()
    } else {
        format!("{name}\nstatus: {status}")
    }
}

fn file_change_summary(item: &Value) -> String {
    summarize_file_change(item)
}

fn summarize_file_change(item: &Value) -> String {
    let changes = item
        .get("changes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let status = item.get("status").and_then(Value::as_str).unwrap_or("");
    if changes.is_empty() {
        let summary = if status.is_empty() {
            "1 change pending".to_string()
        } else {
            format!("status={status}; 1 change pending")
        };
        return truncate_text(summary, 2_000);
    }
    let mut kinds = BTreeMap::<String, usize>::new();
    let paths = changes
        .iter()
        .filter_map(|change| {
            let kind = change
                .get("kind")
                .and_then(|kind| kind.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("update");
            *kinds.entry(kind.to_string()).or_default() += 1;
            change.get("path").and_then(Value::as_str)
        })
        .take(8)
        .collect::<Vec<_>>();
    let counts = kinds
        .into_iter()
        .map(|(kind, count)| format!("{count} {kind}"))
        .collect::<Vec<_>>()
        .join(", ");
    let paths = paths.join(", ");
    let mut parts = Vec::new();
    if !status.is_empty() {
        parts.push(format!("status={status}"));
    }
    if !counts.is_empty() {
        parts.push(counts);
    }
    if !paths.is_empty() {
        parts.push(paths);
    }
    truncate_text(parts.join("; "), 2_000)
}

fn extract_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Object(map) => map
            .get("text")
            .or_else(|| map.get("content"))
            .and_then(|value| extract_text(Some(value))),
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(|item| extract_text(Some(item)))
                .collect::<Vec<_>>()
                .join("");
            (!text.is_empty()).then_some(text)
        }
        other => Some(other.to_string()),
    }
}

fn thread_id_from_result(result: &Value) -> Option<String> {
    result
        .get("thread")
        .and_then(|thread| {
            thread
                .get("id")
                .or_else(|| thread.get("sessionId"))
                .and_then(Value::as_str)
        })
        .or_else(|| result.get("sessionId").and_then(Value::as_str))
        .or_else(|| result.get("threadId").and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

fn truncate_text(text: String, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text;
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn resolve_cwd(cwd: Option<&Path>) -> anyhow::Result<PathBuf> {
    let path = match cwd {
        Some(cwd) => cwd.to_path_buf(),
        None => std::env::current_dir()?,
    };
    Ok(path.canonicalize().unwrap_or(path))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, sync::Arc, time::Duration};

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use crate::approval::ApprovalBroker;
    use crate::executor::{
        ExecutorBackend, ExecutorChannelEventKind, ExecutorPrepareRequest, ExecutorPromptRequest,
        test_support::CollectingExecutorEventSink,
    };

    use super::*;

    fn executor_config(script: &Path, cwd: &Path) -> BTreeMap<String, ExecutorConfig> {
        let mut executors = BTreeMap::new();
        executors.insert(
            "codex".to_string(),
            ExecutorConfig {
                name: "codex".to_string(),
                protocol: ExecutorProtocol::AppServer,
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(cwd.to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        executors
    }

    fn make_executable(path: &Path) {
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).unwrap();
        }
    }

    #[test]
    fn codex_reasoning_channel_event_requires_summary() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {"item": {"type": "reasoning", "content": "raw thinking"}}
            }),
            &mut final_text,
        )
        .unwrap();

        assert_eq!(collected.updates.len(), 1);
        assert!(collected.updates[0].channel_event.is_none());
        assert!(collected.updates[0].summary(700).is_none());

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {"item": {"type": "reasoning", "summary": [{"text": "safe summary"}]}}
            }),
            &mut final_text,
        )
        .unwrap();

        let event = collected.updates[0].channel_event.as_ref().unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ReasoningSummary);
        assert_eq!(event.text, "safe summary");
        assert_eq!(
            collected.updates[0].summary(700).as_deref(),
            Some("Reasoning: safe summary")
        );
    }

    #[test]
    fn codex_commentary_agent_message_emits_progress_event() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "phase": "commentary",
                        "text": "I will inspect the config first."
                    }
                }
            }),
            &mut final_text,
        )
        .unwrap();

        assert!(final_text.is_empty());
        assert_eq!(collected.updates.len(), 1);
        assert_eq!(collected.updates[0].kind, "agent_message_chunk");
        assert_eq!(
            collected.updates[0].text,
            "I will inspect the config first."
        );
        let event = collected.updates[0].channel_event.as_ref().unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::AgentProgress);
        assert_eq!(event.text, "I will inspect the config first.");
    }

    #[test]
    fn codex_final_agent_message_does_not_emit_progress_event() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "phase": "final_answer",
                        "text": "done"
                    }
                }
            }),
            &mut final_text,
        )
        .unwrap();

        assert_eq!(final_text, "done");
        assert_eq!(collected.updates.len(), 1);
        assert!(collected.updates[0].channel_event.is_none());
    }

    #[test]
    fn codex_legacy_agent_message_remains_final_reply_only() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {"item": {"type": "agentMessage", "text": "legacy reply"}}
            }),
            &mut final_text,
        )
        .unwrap();

        assert_eq!(final_text, "legacy reply");
        assert_eq!(collected.updates.len(), 1);
        assert!(collected.updates[0].channel_event.is_none());
    }

    #[test]
    fn codex_command_channel_event_includes_command_without_aggregated_output() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "commandExecution",
                        "command": "printenv SECRET_TOKEN",
                        "aggregatedOutput": "SECRET_TOKEN=super-secret",
                        "exitCode": 0,
                        "status": "completed"
                    }
                }
            }),
            &mut final_text,
        )
        .unwrap();

        let event = collected.updates[0].channel_event.as_ref().unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ToolCall);
        assert!(event.text.contains("$ printenv SECRET_TOKEN"));
        assert!(event.text.contains("exit: 0"));
        assert!(!event.text.contains("super-secret"));
        let summary = collected.updates[0].summary(700).unwrap();
        assert!(summary.contains("$ printenv SECRET_TOKEN"));
        assert!(summary.contains("exit: 0"));
        assert!(!summary.contains("super-secret"));
    }

    fn write_fake_codex_script(path: &Path, behavior: &str) {
        fs::write(
            path,
            format!(
                r#"#!/usr/bin/env python3
import json
import sys
import time

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

turn_request_id = None

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"userAgent": "fake"}}}})
    elif method == "initialized":
        pass
    elif method == "thread/start":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"thread": {{"id": "thread-1"}}}}}})
{behavior}
"#
            ),
        )
        .unwrap();
        make_executable(path);
    }

    #[tokio::test]
    async fn codex_prepare_prefers_session_cwd_over_executor_cwd() {
        let tmp = tempfile::tempdir().unwrap();
        let executor_cwd = tmp.path().join("executor-cwd");
        let session_cwd = tmp.path().join("session-cwd");
        fs::create_dir_all(&executor_cwd).unwrap();
        fs::create_dir_all(&session_cwd).unwrap();
        let script = tmp.path().join("fake_codex.py");
        write_fake_codex_script(&script, "");
        let manager = CodexAppServerManager::new(executor_config(&script, &executor_cwd));

        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: Some(session_cwd.clone()),
                previous_session_id: None,
            })
            .await
            .unwrap();

        let session = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();
        assert_eq!(
            session.lock().await.cwd,
            session_cwd.canonicalize().unwrap()
        );
    }

    #[tokio::test]
    async fn codex_manager_prompts_fake_app_server() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fake_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        send({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"item": {"type": "agentMessage", "text": "codex reply"}},
        })
        send({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {"turn": {"id": "turn-1", "status": "completed"}},
        })
"#,
        );
        let manager = CodexAppServerManager::new(executor_config(&script, tmp.path()));

        let prepared = manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();
        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "hello".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
            )
            .await
            .unwrap();

        assert_eq!(prepared.external_session_id.as_deref(), Some("thread-1"));
        assert!(prepared.started_new_session);
        assert_eq!(response.final_text, "codex reply");
    }

    #[tokio::test]
    async fn codex_command_approval_waits_for_text_approval() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("approval_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        turn_request_id = request_id
        send({
            "jsonrpc": "2.0",
            "id": 900,
            "method": "item/commandExecution/requestApproval",
            "params": {"command": "pwd", "cwd": "/tmp", "reason": "test"},
        })
    elif request_id == 900:
        if msg.get("result", {}).get("decision") == "accept":
            send({"jsonrpc": "2.0", "id": turn_request_id, "result": {"turn": {"id": "turn-1"}}})
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "agentMessage", "text": "approved"}},
            })
            send({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {"turn": {"id": "turn-1", "status": "completed"}},
            })
        else:
            send({"jsonrpc": "2.0", "id": turn_request_id, "error": {"code": -32000, "message": "not approved"}})
"#,
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_approvals(
            executor_config(&script, tmp.path()),
            approvals.clone(),
        ));
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        session_key: "session-1".to_string(),
                        executor: "codex".to_string(),
                        prompt: "run pwd".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                )
                .await
        });
        let approval_prompt = prompts.recv().await.unwrap();
        assert!(approval_prompt.body.contains("$ pwd"));
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        let response = prompt_task.await.unwrap().unwrap();
        assert_eq!(response.final_text, "approved");
    }

    #[tokio::test]
    async fn codex_pending_approval_is_cancelled_when_turn_completes() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("completed_with_pending_approval.py");
        let stale_response_marker = tmp.path().join("stale_response.json");
        let marker_literal = serde_json::to_string(&stale_response_marker.display().to_string())
            .expect("marker path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"turn": {{"id": "turn-1"}}}}}})
        send({{
            "jsonrpc": "2.0",
            "id": 903,
            "method": "item/commandExecution/requestApproval",
            "params": {{"command": "sleep 10", "cwd": "/tmp", "reason": "test"}},
        }})
        time.sleep(0.05)
        send({{
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {{"item": {{"type": "agentMessage", "text": "done"}}}},
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"turn": {{"id": "turn-1", "status": "completed"}}}},
        }})
    elif request_id == 903:
        with open({marker_literal}, "w") as f:
            f.write(json.dumps(msg))
"#
            ),
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_approvals(
            executor_config(&script, tmp.path()),
            approvals.clone(),
        ));
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        session_key: "session-1".to_string(),
                        executor: "codex".to_string(),
                        prompt: "finish with pending approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                )
                .await
        });

        let approval_prompt = timeout(Duration::from_secs(5), prompts.recv())
            .await
            .expect("approval prompt timed out")
            .unwrap();
        let response = timeout(Duration::from_secs(5), prompt_task)
            .await
            .expect("prompt task timed out")
            .unwrap()
            .unwrap();
        assert_eq!(response.final_text, "done");

        assert!(!approvals.has_pending_for_session("session-1").await);
        let reply = approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();
        assert_eq!(
            reply.text,
            format!("Approval {} is not pending.", approval_prompt.id)
        );

        sleep(Duration::from_millis(50)).await;
        assert!(!stale_response_marker.exists());
    }

    #[tokio::test]
    async fn codex_failed_turn_status_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("failed_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        send({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "turn": {
                    "id": "turn-1",
                    "status": "failed",
                    "error": {"code": 123, "message": "secret error detail"},
                },
            },
        })
"#,
        );
        let manager = CodexAppServerManager::new(executor_config(&script, tmp.path()));
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "fail".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
            )
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("status `failed`"));
        assert!(message.contains("code=123"));
        assert!(!message.contains("secret error detail"));
    }

    #[tokio::test]
    async fn codex_file_change_approval_includes_cached_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("file_change_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        turn_request_id = request_id
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        for i in range(12):
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "reasoning", "summary": ["noise " + str(i)]}},
            })
        send({
            "jsonrpc": "2.0",
            "method": "item/started",
            "params": {
                "item": {
                    "id": "patch-1",
                    "type": "fileChange",
                    "changes": [
                        {"kind": {"type": "modify"}, "path": "src/lib.rs"},
                        {"kind": {"type": "add"}, "path": "src/new.rs"},
                    ],
                },
            },
        })
        send({
            "jsonrpc": "2.0",
            "id": 901,
            "method": "item/fileChange/requestApproval",
            "params": {"itemId": "patch-1", "grantRoot": "/repo", "reason": "apply patch"},
        })
    elif request_id == 901:
        if msg.get("result", {}).get("decision") == "accept":
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "agentMessage", "text": "patched"}},
            })
            send({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {"turn": {"id": "turn-1", "status": "completed"}},
            })
"#,
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_approvals(
            executor_config(&script, tmp.path()),
            approvals.clone(),
        ));
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        session_key: "session-1".to_string(),
                        executor: "codex".to_string(),
                        prompt: "patch".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                )
                .await
        });
        let approval_prompt = prompts.recv().await.unwrap();
        assert!(approval_prompt.body.contains("src/lib.rs"));
        assert!(approval_prompt.body.contains("src/new.rs"));
        assert!(approval_prompt.body.contains("1 add"));
        assert!(approval_prompt.body.contains("1 modify"));
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        let response = prompt_task.await.unwrap().unwrap();
        assert_eq!(response.final_text, "patched");
    }

    #[tokio::test]
    async fn codex_mcp_elicitation_declines_with_action_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("elicitation_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        turn_request_id = request_id
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        send({
            "jsonrpc": "2.0",
            "id": 902,
            "method": "mcpServer/elicitation/request",
            "params": {"serverName": "external"},
        })
    elif request_id == 902:
        result = msg.get("result", {})
        if result == {"action": "decline"}:
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "agentMessage", "text": "declined"}},
            })
            send({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {"turn": {"id": "turn-1", "status": "completed"}},
            })
        else:
            send({"jsonrpc": "2.0", "id": turn_request_id, "error": {"code": -32000, "message": "wrong shape"}})
"#,
        );
        let manager = CodexAppServerManager::new(executor_config(&script, tmp.path()));
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "elicit".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
            )
            .await
            .unwrap();

        assert_eq!(response.final_text, "declined");
    }

    #[tokio::test]
    async fn codex_turn_start_timeout_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("stuck_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        pass
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            Arc::new(ApprovalBroker::default()),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_millis(50),
                idle_timeout: Duration::from_secs(1),
            },
        );
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "hang".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("turn/start"));
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn codex_turn_start_timeout_is_not_blocked_by_early_server_request() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("early_request_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "item/commandExecution/requestApproval",
            "params": {"command": "sleep 10", "cwd": "/tmp", "reason": "test"},
        })
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            Arc::new(ApprovalBroker::new(Duration::from_secs(5))),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_millis(100),
                idle_timeout: Duration::from_secs(1),
            },
        );
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = tokio::time::timeout(
            Duration::from_millis(500),
            manager.prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "hang".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
            ),
        )
        .await
        .expect("turn/start timeout waited behind approval")
        .unwrap_err();

        assert!(err.to_string().contains("turn/start"));
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn codex_turn_start_timeout_survives_many_early_server_requests() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("many_early_requests_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        for index in range(80):
            send({
                "jsonrpc": "2.0",
                "id": 1000 + index,
                "method": "item/commandExecution/requestApproval",
                "params": {"command": "sleep 10", "cwd": "/tmp", "reason": "test"},
            })
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            Arc::new(ApprovalBroker::new(Duration::from_secs(5))),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_millis(100),
                idle_timeout: Duration::from_secs(1),
            },
        );
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = timeout(
            Duration::from_millis(500),
            manager.prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "hang".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
            ),
        )
        .await
        .expect("turn/start timeout waited behind saturated request queue")
        .unwrap_err();

        assert!(err.to_string().contains("turn/start"));
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn codex_turn_idle_timeout_returns_error_without_activity() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("idle_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            Arc::new(ApprovalBroker::default()),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_secs(1),
                idle_timeout: Duration::from_millis(50),
            },
        );
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "idle".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("idle timed out"));
        assert!(err.to_string().contains("without activity"));
    }

    #[tokio::test]
    async fn codex_turn_idle_timeout_resets_on_notifications() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("active_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        for index in range(4):
            time.sleep(0.08)
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "reasoning", "summary": [{"text": f"still working {index}"}]}},
            })
        send({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"item": {"type": "agentMessage", "text": "done"}},
        })
        send({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {"turn": {"id": "turn-1", "status": "completed"}},
        })
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            Arc::new(ApprovalBroker::default()),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_secs(1),
                idle_timeout: Duration::from_millis(200),
            },
        );
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "work slowly".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
            )
            .await
            .unwrap();

        assert_eq!(response.final_text, "done");
    }

    #[tokio::test]
    async fn codex_drain_ready_notifications_resets_idle_timeout() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fake_codex.py");
        write_fake_codex_script(&script, "");
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            Arc::new(ApprovalBroker::default()),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_secs(1),
                idle_timeout: Duration::from_secs(5),
            },
        );
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                cwd: None,
                previous_session_id: None,
            })
            .await
            .unwrap();

        let session = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();
        let mut session = session.lock().await;
        let (tx, mut notifications) = broadcast::channel(4);
        let mut events = CollectingExecutorEventSink::default();
        let mut final_text = String::new();
        let mut turn_completed = false;
        let idle_timeout = sleep(Duration::from_millis(1));
        tokio::pin!(idle_timeout);

        tx.send(json!({
            "method": "item/completed",
            "params": {"item": {"type": "reasoning", "summary": [{"text": "still working"}]}}
        }))
        .unwrap();

        session
            .drain_ready_notifications(
                &mut notifications,
                &mut final_text,
                &mut events,
                &mut turn_completed,
                &mut idle_timeout,
            )
            .await
            .unwrap();

        assert!(!turn_completed);
        assert!(idle_timeout.deadline() > Instant::now() + Duration::from_secs(4));
    }
}
