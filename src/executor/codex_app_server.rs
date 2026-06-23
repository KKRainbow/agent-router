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
    sync::{Mutex, broadcast, oneshot},
    time::{Duration, sleep, timeout},
};

use crate::{
    approval::{
        ApprovalBroker, ApprovalOption, ApprovalRequest, ApprovalSelection, SharedApprovalBroker,
    },
    config::{ExecutorConfig, ExecutorProtocol},
    executor::{
        ExecutorBackend, ExecutorDescriptor, ExecutorPrepareRequest, ExecutorPromptRequest,
        ExecutorResponse, ExecutorUpdate, PreparedExecutor,
    },
};

type SessionKey = (String, String);
type SharedCodexSession = Arc<Mutex<CodexAppServerSession>>;
type SessionMap = HashMap<SessionKey, SharedCodexSession>;
type SharedJsonRpcState = Arc<Mutex<JsonRpcState>>;
type SharedStdin = Arc<Mutex<ChildStdin>>;

#[derive(Debug, Clone, Copy)]
struct CodexRuntimeLimits {
    rpc_timeout: Duration,
    turn_timeout: Duration,
}

impl Default for CodexRuntimeLimits {
    fn default() -> Self {
        Self {
            rpc_timeout: Duration::from_secs(30),
            turn_timeout: Duration::from_secs(600),
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
    ) -> anyhow::Result<SharedCodexSession> {
        let key = (session_key.to_string(), executor.to_string());
        let cwd = resolve_cwd(cfg.cwd.as_deref())?;
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
        let session = self
            .get_or_create_session(&request.session_key, &request.executor, cfg)
            .await?;
        let mut session = session.lock().await;
        let (thread_id, started_new_session) = session.ensure_thread().await?;
        Ok(PreparedExecutor {
            external_session_id: Some(thread_id),
            started_new_session,
        })
    }

    async fn prompt(&self, request: ExecutorPromptRequest) -> anyhow::Result<ExecutorResponse> {
        let session = self
            .existing_session(&request.session_key, &request.executor)
            .await?;
        let mut session = session.lock().await;
        session.run_turn(&request.prompt, request.user_id).await
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
        let client = CodexJsonRpcClient::spawn(&cfg.command, &cfg.args, &cwd, &cfg.env).await?;
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
    ) -> anyhow::Result<ExecutorResponse> {
        let thread_id = self
            .thread_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("codex app-server thread has not been created"))?;
        let mut notifications = self.client.subscribe_notifications();
        let mut server_requests = self.client.subscribe_server_requests();
        let mut closed = self.client.subscribe_closed();
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

        let mut final_text = String::new();
        let mut updates = Vec::new();
        let mut turn_start_acknowledged = false;
        let mut turn_completed = false;
        let turn_start_timeout = sleep(self.limits.rpc_timeout);
        let turn_timeout = sleep(self.limits.turn_timeout);
        tokio::pin!(turn_start_timeout);
        tokio::pin!(turn_timeout);
        loop {
            tokio::select! {
                response = &mut turn_start.response, if !turn_start_acknowledged => {
                    turn_start_acknowledged = true;
                    json_rpc_result(&turn_start.method, response?)?;
                    if turn_completed {
                        break;
                    }
                }
                request = server_requests.recv() => {
                    match request {
                        Ok(request) => {
                            self.drain_ready_notifications(
                                &mut notifications,
                                &mut final_text,
                                &mut updates,
                                &mut turn_completed,
                            )?;
                            self.handle_server_request(request, user_id.clone()).await?;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => {
                            anyhow::bail!("codex app-server request stream closed")
                        }
                    }
                }
                notification = notifications.recv() => {
                    match notification {
                        Ok(notification) => {
                            self.handle_notification(
                                notification,
                                &mut final_text,
                                &mut updates,
                                &mut turn_completed,
                            )?;
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
                _ = &mut turn_timeout => {
                    self.client.cancel_pending(turn_start.id).await;
                    self.client.close("codex app-server turn timed out").await;
                    anyhow::bail!(
                        "codex app-server turn timed out after {}s",
                        self.limits.turn_timeout.as_secs()
                    );
                }
            }
        }

        Ok(ExecutorResponse {
            final_text,
            updates,
        })
    }

    fn drain_ready_notifications(
        &mut self,
        notifications: &mut broadcast::Receiver<Value>,
        final_text: &mut String,
        updates: &mut Vec<ExecutorUpdate>,
        turn_completed: &mut bool,
    ) -> anyhow::Result<()> {
        loop {
            match notifications.try_recv() {
                Ok(notification) => {
                    self.handle_notification(notification, final_text, updates, turn_completed)?;
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

    fn handle_notification(
        &mut self,
        notification: Value,
        final_text: &mut String,
        updates: &mut Vec<ExecutorUpdate>,
        turn_completed: &mut bool,
    ) -> anyhow::Result<()> {
        self.track_pending_file_change(&notification);
        match collect_codex_notification(notification, final_text, updates)? {
            CodexNotificationOutcome::Pending => {}
            CodexNotificationOutcome::TurnCompleted => *turn_completed = true,
        }
        Ok(())
    }

    async fn handle_server_request(
        &self,
        message: Value,
        user_id: Option<String>,
    ) -> anyhow::Result<()> {
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let params = message.get("params").unwrap_or(&Value::Null);
        match method {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                let request = codex_approval_request(
                    &self.session_key,
                    &self.executor,
                    user_id,
                    method,
                    params,
                    self.pending_file_change_summary(params),
                );
                let decision = match self.approvals.request(request).await {
                    ApprovalSelection::Selected(option_id) if option_id == "accept" => "accept",
                    _ => "decline",
                };
                self.client
                    .respond(id, json!({ "decision": decision }))
                    .await?;
            }
            "item/permissions/requestApproval" => {
                self.client
                    .respond(id, json!({ "decision": "decline" }))
                    .await?;
            }
            "mcpServer/elicitation/request" => {
                self.client
                    .respond(id, json!({ "action": "decline" }))
                    .await?;
            }
            _ => {
                self.client
                    .respond_error(
                        id,
                        -32601,
                        format!("agent-router does not support codex client method `{method}`"),
                    )
                    .await?;
            }
        }
        Ok(())
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
            },
            ApprovalOption {
                id: "decline".to_string(),
                kind: "reject_once".to_string(),
                name: "Deny".to_string(),
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
}

#[derive(Debug)]
struct PendingJsonRpcRequest {
    id: u64,
    method: String,
    response: oneshot::Receiver<Value>,
}

#[derive(Debug, Default)]
struct JsonRpcState {
    closed: bool,
    pending: HashMap<u64, oneshot::Sender<Value>>,
}

impl CodexJsonRpcClient {
    async fn spawn(
        command: &str,
        args: &[String],
        cwd: &Path,
        env: &BTreeMap<String, String>,
    ) -> anyhow::Result<Self> {
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
            Ok(Ok(response)) => response,
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
        let _ = child.start_kill();
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

    async fn respond(&self, id: Value, result: Value) -> anyhow::Result<()> {
        write_json(
            &self.stdin,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result,
            }),
        )
        .await
    }

    async fn respond_error(&self, id: Value, code: i64, message: String) -> anyhow::Result<()> {
        write_json(
            &self.stdin,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": code,
                    "message": message,
                },
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

fn summarize_json_rpc_error(error: &Value) -> String {
    let code = error
        .get("code")
        .map(Value::to_string)
        .unwrap_or_else(|| "unknown".to_string());
    let message_state = if error
        .get("message")
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .is_some()
    {
        "omitted"
    } else {
        "absent"
    };
    format!("code={code}, message={message_state}")
}

async fn read_codex_stdout<R>(
    reader: BufReader<R>,
    state: SharedJsonRpcState,
    notifications: broadcast::Sender<Value>,
    server_requests: broadcast::Sender<Value>,
    closed: broadcast::Sender<String>,
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
            let _ = tx.send(message);
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
    for (id, tx) in drained {
        let _ = tx.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32000,
                "message": message,
            }
        }));
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

fn collect_codex_notification(
    notification: Value,
    final_text: &mut String,
    updates: &mut Vec<ExecutorUpdate>,
) -> anyhow::Result<CodexNotificationOutcome> {
    let method = notification
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("");
    if method == "turn/completed" {
        validate_turn_completed(&notification)?;
        return Ok(CodexNotificationOutcome::TurnCompleted);
    }
    if method != "item/completed" {
        return Ok(CodexNotificationOutcome::Pending);
    }
    let Some(item) = notification
        .get("params")
        .and_then(|params| params.get("item"))
    else {
        return Ok(CodexNotificationOutcome::Pending);
    };
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
    match item_type {
        "agentMessage" => {
            let text = item
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            *final_text = text.clone();
            updates.push(ExecutorUpdate {
                kind: "agent_message_chunk".to_string(),
                title: String::new(),
                text,
                status: String::new(),
            });
        }
        "reasoning" => {
            let text =
                extract_text(item.get("summary")).or_else(|| extract_text(item.get("content")));
            updates.push(ExecutorUpdate {
                kind: "agent_thought_chunk".to_string(),
                title: "Reasoning".to_string(),
                text: text.unwrap_or_default(),
                status: String::new(),
            });
        }
        "commandExecution" => {
            updates.push(ExecutorUpdate {
                kind: "tool_call".to_string(),
                title: "Bash".to_string(),
                text: command_execution_summary(item),
                status: item
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            });
        }
        "fileChange" => {
            updates.push(ExecutorUpdate {
                kind: "diff".to_string(),
                title: "File change".to_string(),
                text: file_change_summary(item),
                status: item
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            });
        }
        "mcpToolCall" | "dynamicToolCall" => {
            updates.push(ExecutorUpdate {
                kind: "tool_call".to_string(),
                title: item_type.to_string(),
                text: truncate_text(item.to_string(), 1_000),
                status: item
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            });
        }
        _ => {}
    }
    Ok(CodexNotificationOutcome::Pending)
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
    use crate::executor::{ExecutorBackend, ExecutorPrepareRequest, ExecutorPromptRequest};

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

    fn write_fake_codex_script(path: &Path, behavior: &str) {
        fs::write(
            path,
            format!(
                r#"#!/usr/bin/env python3
import json
import sys

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
                previous_session_id: None,
            })
            .await
            .unwrap();
        let response = manager
            .prompt(ExecutorPromptRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                prompt: "hello".to_string(),
                user_id: Some("U1".to_string()),
            })
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
                previous_session_id: None,
            })
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            prompt_manager
                .prompt(ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "run pwd".to_string(),
                    user_id: Some("U1".to_string()),
                })
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
                previous_session_id: None,
            })
            .await
            .unwrap();

        let err = manager
            .prompt(ExecutorPromptRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                prompt: "fail".to_string(),
                user_id: Some("U1".to_string()),
            })
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
                previous_session_id: None,
            })
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            prompt_manager
                .prompt(ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "codex".to_string(),
                    prompt: "patch".to_string(),
                    user_id: Some("U1".to_string()),
                })
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
                previous_session_id: None,
            })
            .await
            .unwrap();

        let response = manager
            .prompt(ExecutorPromptRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                prompt: "elicit".to_string(),
                user_id: Some("U1".to_string()),
            })
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
                turn_timeout: Duration::from_secs(1),
            },
        );
        manager
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                previous_session_id: None,
            })
            .await
            .unwrap();

        let err = manager
            .prompt(ExecutorPromptRequest {
                session_key: "session-1".to_string(),
                executor: "codex".to_string(),
                prompt: "hang".to_string(),
                user_id: Some("U1".to_string()),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("turn/start"));
        assert!(err.to_string().contains("timed out"));
    }
}
