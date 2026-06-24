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
};

use crate::{
    approval::{
        ApprovalBroker, ApprovalCancellation, ApprovalOption, ApprovalRequest, ApprovalSelection,
        SharedApprovalBroker,
    },
    config::{ExecutorConfig, ExecutorProtocol},
    executor::{
        ExecutorBackend, ExecutorChannelEvent, ExecutorDescriptor, ExecutorEventSink,
        ExecutorInterruptRequest, ExecutorPrepareRequest, ExecutorPromptOutcome,
        ExecutorPromptRequest, ExecutorResponse, ExecutorUpdate, PreparedExecutor,
        TurnCancellation, summarize_json_rpc_error,
    },
};

type SharedJsonRpcState = Arc<Mutex<JsonRpcState>>;
type SharedAcpToolCalls = Arc<Mutex<HashMap<String, AcpToolCallState>>>;
type SharedStdin = Arc<Mutex<ChildStdin>>;
type SessionKey = (String, String);
type SharedAcpSession = Arc<Mutex<AcpSession>>;
type SessionMap = HashMap<SessionKey, SharedAcpSession>;

#[derive(Debug, Default)]
struct AcpToolCallState {
    title: String,
    text: String,
    status: String,
}

#[derive(Debug)]
pub struct AcpExecutorManager {
    executors: BTreeMap<String, ExecutorConfig>,
    approvals: SharedApprovalBroker,
    sessions: Mutex<SessionMap>,
}

impl AcpExecutorManager {
    pub fn new(executors: BTreeMap<String, ExecutorConfig>) -> Self {
        Self::with_approvals(executors, Arc::new(ApprovalBroker::default()))
    }

    pub fn with_approvals(
        executors: BTreeMap<String, ExecutorConfig>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        let executors = executors
            .into_iter()
            .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::Acp)
            .collect();
        Self {
            executors,
            approvals,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    async fn get_or_create_session(
        &self,
        session_key: &str,
        executor: &str,
        cfg: &ExecutorConfig,
        session_cwd: Option<&Path>,
    ) -> anyhow::Result<Arc<Mutex<AcpSession>>> {
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
            AcpSession::start(
                cfg.clone(),
                cwd,
                session_key.to_string(),
                executor.to_string(),
                self.approvals.clone(),
            )
            .await?,
        ));
        let mut sessions = self.sessions.lock().await;
        sessions.insert(key, session.clone());
        Ok(session)
    }

    async fn discard_session_if_same(
        &self,
        session_key: &str,
        executor: &str,
        session: &Arc<Mutex<AcpSession>>,
    ) {
        let key = (session_key.to_string(), executor.to_string());
        let mut sessions = self.sessions.lock().await;
        if sessions
            .get(&key)
            .is_some_and(|existing| Arc::ptr_eq(existing, session))
        {
            sessions.remove(&key);
        }
    }

    async fn existing_session(
        &self,
        session_key: &str,
        executor: &str,
    ) -> anyhow::Result<Arc<Mutex<AcpSession>>> {
        self.sessions
            .lock()
            .await
            .get(&(session_key.to_string(), executor.to_string()))
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "executor `{executor}` has not prepared ACP session for `{session_key}`"
                )
            })
    }
}

#[async_trait]
impl ExecutorBackend for AcpExecutorManager {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.executors.get(name).map(|cfg| ExecutorDescriptor {
            name: cfg.name.clone(),
            protocol: "acp".to_string(),
        })
    }

    fn list(&self) -> Vec<ExecutorDescriptor> {
        self.executors
            .values()
            .map(|cfg| ExecutorDescriptor {
                name: cfg.name.clone(),
                protocol: "acp".to_string(),
            })
            .collect()
    }

    async fn prepare(
        &self,
        request: ExecutorPrepareRequest,
        cancel: TurnCancellation,
    ) -> anyhow::Result<PreparedExecutor> {
        if cancel.is_cancelled().await {
            anyhow::bail!("ACP prepare cancelled");
        }
        let cfg = self
            .executors
            .get(&request.executor)
            .ok_or_else(|| anyhow::anyhow!("executor `{}` is not configured", request.executor))?;
        tracing::info!(
            executor = %request.executor,
            session_key = %request.session_key,
            previous_session_id = ?request.previous_session_id,
            "preparing ACP executor session"
        );
        let session = self
            .get_or_create_session(
                &request.session_key,
                &request.executor,
                cfg,
                request.cwd.as_deref(),
            )
            .await?;
        if cancel.is_cancelled().await {
            self.discard_session_if_same(&request.session_key, &request.executor, &session)
                .await;
            let mut session = session.lock().await;
            session.client.close("ACP prepare cancelled").await;
            session.session_id = None;
            anyhow::bail!("ACP prepare cancelled");
        }
        let mut session = session.lock().await;
        let (external_session_id, started_new_session) = session
            .ensure_session(request.previous_session_id.as_deref(), cancel)
            .await?;
        tracing::info!(
            executor = %request.executor,
            session_key = %request.session_key,
            external_session_id = %external_session_id,
            started_new_session,
            "prepared ACP executor session"
        );
        Ok(PreparedExecutor {
            external_session_id: Some(external_session_id),
            started_new_session,
        })
    }

    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome {
        let session = match self
            .existing_session(&request.session_key, &request.executor)
            .await
        {
            Ok(session) => session,
            Err(err) => return ExecutorPromptOutcome::Failed(err),
        };
        let mut session = session.lock().await;
        let session_key = request.session_key.clone();
        let executor = request.executor.clone();
        let prompt_len = request.prompt.len();
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            prompt_len,
            "starting ACP executor turn"
        );
        let result = session
            .prompt(&request.prompt, request.user_id, events, cancel)
            .await;
        match &result {
            ExecutorPromptOutcome::Completed(result) => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                final_text_len = result.final_text.len(),
                "completed ACP executor turn"
            ),
            ExecutorPromptOutcome::Cancelled => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                "cancelled ACP executor turn"
            ),
            ExecutorPromptOutcome::Failed(err) => tracing::warn!(
                error = %err,
                executor = %executor,
                session_key = %session_key,
                "failed ACP executor turn"
            ),
        }
        result
    }

    async fn interrupt(&self, request: ExecutorInterruptRequest) -> anyhow::Result<()> {
        let session = self
            .sessions
            .lock()
            .await
            .get(&(request.session_key.clone(), request.executor.clone()))
            .cloned();
        let Some(session) = session else {
            return Ok(());
        };
        if let Ok(mut session) = session.try_lock() {
            session.client.close("ACP turn interrupted").await;
            session.session_id = None;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct AcpSession {
    cfg: ExecutorConfig,
    cwd: PathBuf,
    client: JsonRpcClient,
    session_id: Option<String>,
    initialized: bool,
}

impl AcpSession {
    async fn start(
        cfg: ExecutorConfig,
        cwd: PathBuf,
        session_key: String,
        executor: String,
        approvals: SharedApprovalBroker,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            command = %cfg.command,
            cwd = %cwd.display(),
            "starting ACP executor process"
        );
        let client = JsonRpcClient::spawn(
            &cfg.command,
            &cfg.args,
            &cwd,
            &cfg.env,
            session_key.clone(),
            executor.clone(),
            approvals,
        )
        .await?;
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            "started ACP executor process"
        );
        Ok(Self {
            cfg,
            cwd,
            client,
            session_id: None,
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

    async fn initialize(&mut self, cancel: TurnCancellation) -> anyhow::Result<()> {
        if self.initialized {
            return Ok(());
        }
        self.client
            .request_until_cancelled(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "clientInfo": {
                        "name": "agent-router",
                        "title": "Agent Router",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                }),
                cancel,
                "ACP initialize cancelled",
            )
            .await?;
        self.initialized = true;
        Ok(())
    }

    async fn ensure_session(
        &mut self,
        preferred_session_id: Option<&str>,
        cancel: TurnCancellation,
    ) -> anyhow::Result<(String, bool)> {
        self.initialize(cancel.clone()).await?;
        if let Some(session_id) = &self.session_id {
            return Ok((session_id.clone(), false));
        }

        if let Some(preferred) = preferred_session_id.filter(|value| !value.is_empty()) {
            for method in ["session/load", "session/resume"] {
                let result = self
                    .client
                    .request_until_cancelled(
                        method,
                        json!({
                            "cwd": self.cwd,
                            "sessionId": preferred,
                            "mcpServers": [],
                        }),
                        cancel.clone(),
                        "ACP session resume cancelled",
                    )
                    .await;
                if let Ok(result) = result {
                    let session_id =
                        session_id_from_result(&result).unwrap_or_else(|| preferred.to_string());
                    self.session_id = Some(session_id.clone());
                    return Ok((session_id, false));
                }
                if cancel.is_cancelled().await {
                    anyhow::bail!("ACP session resume cancelled");
                }
            }
        }

        let result = self
            .client
            .request_until_cancelled(
                "session/new",
                json!({
                    "cwd": self.cwd,
                    "mcpServers": [],
                }),
                cancel,
                "ACP session/new cancelled",
            )
            .await?;
        let session_id = session_id_from_result(&result)
            .ok_or_else(|| anyhow::anyhow!("ACP session/new did not return sessionId"))?;
        self.session_id = Some(session_id.clone());
        Ok((session_id, true))
    }

    async fn prompt(
        &mut self,
        prompt: &str,
        user_id: Option<String>,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome {
        self.client.set_active_user(user_id).await;
        self.client.set_active_turn_cancel(cancel.clone()).await;
        let result = self.prompt_with_active_user(prompt, events, cancel).await;
        self.client.clear_active_turn_cancel().await;
        self.client.clear_active_user().await;
        result
    }

    async fn prompt_with_active_user(
        &mut self,
        prompt: &str,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome {
        let session_id = match self.session_id.clone() {
            Some(session_id) => session_id,
            None => {
                return ExecutorPromptOutcome::Failed(anyhow::anyhow!(
                    "ACP session has not been created"
                ));
            }
        };
        if cancel.is_cancelled().await {
            return ExecutorPromptOutcome::Cancelled;
        }
        let mut updates_rx = self.client.subscribe();
        let request = match self
            .client
            .request_started(
                "session/prompt",
                json!({
                    "sessionId": session_id.clone(),
                    "prompt": [{"type": "text", "text": prompt}],
                }),
            )
            .await
        {
            Ok(request) => request,
            Err(err) => return ExecutorPromptOutcome::Failed(err),
        };
        let response_fut = request.response;
        tokio::pin!(response_fut);

        let mut text_parts = Vec::new();
        let result = loop {
            tokio::select! {
                result = &mut response_fut => {
                    let response = match result {
                        Ok(Ok(response)) => response,
                        Ok(Err(err)) => {
                            return self
                                .failed_prompt_or_cancelled(&cancel, &session_id, request.id, err)
                                .await;
                        }
                        Err(_) => {
                            return self
                                .failed_prompt_or_cancelled(
                                    &cancel,
                                    &session_id,
                                    request.id,
                                    anyhow::anyhow!("ACP response channel closed"),
                                )
                                .await;
                        }
                    };
                    if let Some(error) = response.get("error") {
                        return self
                            .failed_prompt_or_cancelled(
                                &cancel,
                                &session_id,
                                request.id,
                                anyhow::anyhow!(
                                    "ACP `{}` failed: {}",
                                    request.method,
                                    summarize_json_rpc_error(error)
                                ),
                            )
                            .await;
                    }
                    break response.get("result").cloned().unwrap_or(Value::Null);
                }
                _ = cancel.cancelled() => {
                    self.cancel_prompt_session(&session_id, request.id).await;
                    return ExecutorPromptOutcome::Cancelled;
                }
                received = updates_rx.recv() => {
                    match received {
                        Ok(update) => {
                            if let Err(err) = collect_update(update, events, &mut text_parts).await {
                                return self
                                    .failed_prompt_or_cancelled(
                                        &cancel,
                                        &session_id,
                                        request.id,
                                        err,
                                    )
                                    .await;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => continue,
                    }
                }
            }
        };
        if cancel.is_cancelled().await {
            self.cancel_prompt_session(&session_id, request.id).await;
            return ExecutorPromptOutcome::Cancelled;
        }
        while let Ok(update) = updates_rx.try_recv() {
            if let Err(err) = collect_update(update, events, &mut text_parts).await {
                return self
                    .failed_prompt_or_cancelled(&cancel, &session_id, request.id, err)
                    .await;
            }
        }
        if acp_result_cancelled(&result) {
            self.client.close("ACP turn returned cancelled").await;
            self.session_id = None;
            return ExecutorPromptOutcome::Cancelled;
        }
        let final_text = if text_parts.is_empty() {
            extract_text_result(&result)
        } else {
            text_parts.join("")
        };
        ExecutorPromptOutcome::Completed(ExecutorResponse { final_text })
    }

    async fn failed_prompt_or_cancelled(
        &mut self,
        cancel: &TurnCancellation,
        session_id: &str,
        request_id: u64,
        err: anyhow::Error,
    ) -> ExecutorPromptOutcome {
        if cancel.is_cancelled().await {
            self.cancel_prompt_session(session_id, request_id).await;
            ExecutorPromptOutcome::Cancelled
        } else {
            ExecutorPromptOutcome::Failed(err)
        }
    }

    async fn cancel_prompt_session(&mut self, session_id: &str, request_id: u64) {
        let notify_result = self
            .client
            .notify("session/cancel", json!({ "sessionId": session_id }))
            .await;
        self.client.cancel_pending(request_id).await;
        self.client.close("ACP turn cancelled").await;
        self.session_id = None;
        if let Err(err) = notify_result {
            tracing::debug!(
                target: "agent_router::acp",
                error = %err,
                "ignored failed ACP session/cancel notification after local cancellation"
            );
        }
    }
}

async fn collect_update(
    update: ExecutorUpdate,
    events: &mut dyn ExecutorEventSink,
    text_parts: &mut Vec<String>,
) -> anyhow::Result<()> {
    if update.kind == "agent_message_chunk" {
        text_parts.push(update.text.clone());
    }
    events.send(update).await
}

#[derive(Debug)]
struct JsonRpcClient {
    stdin: SharedStdin,
    state: SharedJsonRpcState,
    next_id: AtomicU64,
    updates: broadcast::Sender<ExecutorUpdate>,
    child: Arc<Mutex<Child>>,
    active_user_id: Arc<Mutex<Option<String>>>,
    active_turn_cancel: Arc<Mutex<Option<TurnCancellation>>>,
}

#[derive(Debug, Default)]
struct JsonRpcState {
    closed: bool,
    pending: HashMap<u64, oneshot::Sender<anyhow::Result<Value>>>,
}

#[derive(Debug)]
struct PendingJsonRpcRequest {
    id: u64,
    method: String,
    response: oneshot::Receiver<anyhow::Result<Value>>,
}

#[derive(Debug, Clone)]
struct JsonRpcServerContext {
    state: SharedJsonRpcState,
    tool_calls: SharedAcpToolCalls,
    stdin: SharedStdin,
    updates: broadcast::Sender<ExecutorUpdate>,
    approvals: SharedApprovalBroker,
    session_key: String,
    executor: String,
    active_user_id: Arc<Mutex<Option<String>>>,
    active_turn_cancel: Arc<Mutex<Option<TurnCancellation>>>,
}

impl JsonRpcClient {
    async fn spawn(
        command: &str,
        args: &[String],
        cwd: &Path,
        env: &BTreeMap<String, String>,
        session_key: String,
        executor: String,
        approvals: SharedApprovalBroker,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            target: "agent_router::acp",
            executor = %executor,
            session_key = %session_key,
            command = %command,
            arg_count = args.len(),
            cwd = %cwd.display(),
            "spawning ACP process"
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
        let mut child = cmd
            .spawn()
            .map_err(|err| anyhow::anyhow!("could not start ACP command `{command}`: {err}"))?;
        let pid = child.id();
        tracing::info!(
            target: "agent_router::acp",
            executor = %executor,
            session_key = %session_key,
            command = %command,
            pid = ?pid,
            "spawned ACP process"
        );
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("ACP process did not expose stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("ACP process did not expose stdout"))?;
        let stderr = child.stderr.take();

        let stdin = Arc::new(Mutex::new(stdin));
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let tool_calls = Arc::new(Mutex::new(HashMap::new()));
        let (updates, _) = broadcast::channel(256);
        let child = Arc::new(Mutex::new(child));
        let active_user_id = Arc::new(Mutex::new(None));
        let active_turn_cancel = Arc::new(Mutex::new(None));
        let server_context = JsonRpcServerContext {
            state: state.clone(),
            tool_calls,
            stdin: stdin.clone(),
            updates: updates.clone(),
            approvals,
            session_key,
            executor,
            active_user_id: active_user_id.clone(),
            active_turn_cancel: active_turn_cancel.clone(),
        };

        tokio::spawn(read_stdout(BufReader::new(stdout), server_context));
        if let Some(stderr) = stderr {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "agent_router::acp", stderr = %line);
                }
            });
        }

        Ok(Self {
            stdin,
            state,
            next_id: AtomicU64::new(1),
            updates,
            child,
            active_user_id,
            active_turn_cancel,
        })
    }

    async fn set_active_user(&self, user_id: Option<String>) {
        *self.active_user_id.lock().await = user_id;
    }

    async fn clear_active_user(&self) {
        *self.active_user_id.lock().await = None;
    }

    async fn set_active_turn_cancel(&self, cancel: TurnCancellation) {
        *self.active_turn_cancel.lock().await = Some(cancel);
    }

    async fn clear_active_turn_cancel(&self) {
        *self.active_turn_cancel.lock().await = None;
    }

    fn subscribe(&self) -> broadcast::Receiver<ExecutorUpdate> {
        self.updates.subscribe()
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

    async fn request_started(
        &self,
        method: &str,
        params: Value,
    ) -> anyhow::Result<PendingJsonRpcRequest> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            anyhow::ensure!(!state.closed, "ACP client stdout is closed");
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

    async fn request_until_cancelled(
        &self,
        method: &str,
        params: Value,
        cancel: TurnCancellation,
        close_reason: &str,
    ) -> anyhow::Result<Value> {
        let request = self.request_started(method, params).await?;
        tokio::select! {
            response = request.response => {
                let response = response
                    .map_err(|_| anyhow::anyhow!("ACP response channel closed"))??;
                if let Some(error) = response.get("error") {
                    anyhow::bail!(
                        "ACP `{}` failed: {}",
                        request.method,
                        summarize_json_rpc_error(error)
                    );
                }
                Ok(response.get("result").cloned().unwrap_or(Value::Null))
            }
            _ = cancel.cancelled() => {
                self.cancel_pending(request.id).await;
                self.close(close_reason).await;
                anyhow::bail!("{close_reason}")
            }
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

    async fn cancel_pending(&self, id: u64) {
        self.state.lock().await.pending.remove(&id);
    }

    async fn close(&self, reason: &str) {
        fail_all_pending(&self.state, reason).await;
        let mut child = self.child.lock().await;
        let pid = child.id();
        tracing::warn!(
            target: "agent_router::acp",
            pid = ?pid,
            reason,
            "closing ACP process"
        );
        if let Err(err) = child.start_kill() {
            tracing::debug!(
                target: "agent_router::acp",
                error = %err,
                reason,
                "failed to kill ACP process"
            );
        }
    }
}

async fn read_stdout<R>(reader: BufReader<R>, context: JsonRpcServerContext)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            tracing::debug!(target: "agent_router::acp", raw_stdout = %line, "ignoring non-json ACP stdout");
            continue;
        };
        dispatch_message(message, &context).await;
    }
    tracing::warn!(
        target: "agent_router::acp",
        executor = %context.executor,
        session_key = %context.session_key,
        "ACP process stdout closed"
    );
    fail_all_pending(&context.state, "ACP process closed stdout").await;
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

async fn dispatch_message(message: Value, context: &JsonRpcServerContext) {
    if message.get("method").is_none() {
        let sender = match message.get("id").and_then(Value::as_u64) {
            Some(id) => context.state.lock().await.pending.remove(&id),
            None => None,
        };
        if let Some(tx) = sender {
            let _ = tx.send(Ok(message));
        }
        return;
    }

    if message.get("id").is_some() {
        respond_to_server_request(context, &message).await;
        return;
    }

    if message.get("method").and_then(Value::as_str) == Some("session/update") {
        let update = {
            let mut tool_calls = context.tool_calls.lock().await;
            project_acp_update_with_state(&message, &mut tool_calls)
        };
        if let Some(update) = update {
            let _ = context.updates.send(update);
        }
    }
}

async fn respond_to_server_request(context: &JsonRpcServerContext, message: &Value) {
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
    let payload = if method == "session/request_permission" {
        let requester_user_id = context.active_user_id.lock().await.clone();
        let request = approval_request_from_permission_message(
            message,
            &context.session_key,
            &context.executor,
            requester_user_id,
        );
        let turn_cancel = context.active_turn_cancel.lock().await.clone();
        let selection = request_permission_until_turn_cancelled(
            context.approvals.clone(),
            request,
            turn_cancel,
        )
        .await;
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": permission_result(selection),
        })
    } else {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32601,
                "message": format!("agent-router MVP does not support ACP client method `{method}`")
            }
        })
    };
    let _ = write_json(&context.stdin, payload).await;
}

async fn request_permission_until_turn_cancelled(
    approvals: SharedApprovalBroker,
    request: ApprovalRequest,
    turn_cancel: Option<TurnCancellation>,
) -> ApprovalSelection {
    let Some(turn_cancel) = turn_cancel else {
        return approvals.request(request).await;
    };
    let approval_cancel = ApprovalCancellation::new();
    let approval_cancel_for_turn = approval_cancel.clone();
    let watcher = tokio::spawn(async move {
        let _ = turn_cancel.cancelled().await;
        approval_cancel_for_turn.cancel();
    });
    let selection = approvals
        .request_until_cancelled(request, approval_cancel)
        .await
        .unwrap_or(ApprovalSelection::Cancelled);
    watcher.abort();
    selection
}

fn approval_request_from_permission_message(
    message: &Value,
    session_key: &str,
    executor: &str,
    requester_user_id: Option<String>,
) -> ApprovalRequest {
    let params = message.get("params").unwrap_or(&Value::Null);
    let tool_call = params
        .get("toolCall")
        .or_else(|| params.get("tool_call"))
        .unwrap_or(params);
    let title = tool_call
        .get("title")
        .or_else(|| tool_call.get("name"))
        .or_else(|| tool_call.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("Tool permission")
        .to_string();
    ApprovalRequest {
        session_key: session_key.to_string(),
        executor: executor.to_string(),
        requester_user_id,
        title,
        body: permission_body(params, tool_call),
        options: permission_options(params),
    }
}

fn permission_body(params: &Value, tool_call: &Value) -> String {
    let content = extract_text(tool_call.get("content"))
        .or_else(|| extract_text(tool_call.get("rawInput")))
        .or_else(|| extract_text(tool_call.get("raw_input")))
        .or_else(|| extract_text(params.get("toolCall")))
        .or_else(|| extract_text(params.get("tool_call")))
        .unwrap_or_default();
    truncate_text(content, 2_000)
}

fn permission_options(params: &Value) -> Vec<ApprovalOption> {
    params
        .get("options")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let id = item
                        .get("optionId")
                        .or_else(|| item.get("option_id"))
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)?;
                    let kind = item.get("kind").and_then(Value::as_str).unwrap_or_default();
                    let name = item
                        .get("name")
                        .or_else(|| item.get("label"))
                        .and_then(Value::as_str)
                        .unwrap_or(id);
                    Some(ApprovalOption {
                        id: id.to_string(),
                        kind: kind.to_string(),
                        name: name.to_string(),
                        auto_approvable: true,
                    })
                })
                .collect::<Vec<_>>()
        })
        .filter(|options| !options.is_empty())
        .unwrap_or_else(|| {
            vec![
                ApprovalOption {
                    id: "allow_once".to_string(),
                    kind: "allow_once".to_string(),
                    name: "Allow once".to_string(),
                    auto_approvable: false,
                },
                ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject_once".to_string(),
                    name: "Deny".to_string(),
                    auto_approvable: false,
                },
            ]
        })
}

fn permission_result(selection: ApprovalSelection) -> Value {
    match selection {
        ApprovalSelection::Selected(option_id) => json!({
            "outcome": {
                "outcome": "selected",
                "optionId": option_id,
            }
        }),
        ApprovalSelection::Cancelled => json!({
            "outcome": {
                "outcome": "cancelled",
            }
        }),
    }
}

fn truncate_text(text: String, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text;
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

async fn write_json(stdin: &SharedStdin, value: Value) -> anyhow::Result<()> {
    let mut guard = stdin.lock().await;
    let line = serde_json::to_string(&value)?;
    guard.write_all(line.as_bytes()).await?;
    guard.write_all(b"\n").await?;
    guard.flush().await?;
    Ok(())
}

#[cfg(test)]
fn project_acp_update(message: &Value) -> Option<ExecutorUpdate> {
    let mut tool_calls = HashMap::new();
    project_acp_update_with_state(message, &mut tool_calls)
}

fn project_acp_update_with_state(
    message: &Value,
    tool_calls: &mut HashMap<String, AcpToolCallState>,
) -> Option<ExecutorUpdate> {
    let params = message.get("params")?;
    let update = params
        .get("update")
        .or_else(|| params.get("sessionUpdate"))
        .unwrap_or(params);
    let tool_call_id = tool_call_id(update);
    let kind = update
        .get("sessionUpdate")
        .or_else(|| update.get("kind"))
        .or_else(|| update.get("type"))
        .or_else(|| update.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("update");
    let lower_kind = kind.to_ascii_lowercase();
    let is_tool_update = lower_kind.contains("tool");
    let tool = if is_tool_update {
        update
            .get("toolCall")
            .or_else(|| update.get("tool_call"))
            .unwrap_or(update)
    } else {
        update
    };
    let text = extract_text(update.get("content"))
        .or_else(|| extract_text(update.get("text")))
        .or_else(|| extract_text(tool.get("content")))
        .or_else(|| extract_tool_raw_input(tool.get("rawInput")))
        .or_else(|| extract_tool_raw_input(tool.get("raw_input")));
    let title = update
        .get("title")
        .or_else(|| update.get("name"))
        .or_else(|| tool.get("title"))
        .or_else(|| tool.get("name"))
        .or_else(|| tool.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let status = update
        .get("status")
        .or_else(|| tool.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let normalized_kind = if matches!(kind, "agent_message_chunk" | "agent_message") {
        "agent_message_chunk".to_string()
    } else if matches!(kind, "agent_thought_chunk" | "agent_thought") {
        "agent_thought_chunk".to_string()
    } else if is_tool_update {
        format!("tool_{kind}")
    } else if kind.contains("plan") {
        "plan".to_string()
    } else if kind.contains("diff") || kind.contains("file") {
        "diff".to_string()
    } else if kind.contains("error") {
        "error".to_string()
    } else {
        kind.to_string()
    };
    let text = text.unwrap_or_default();
    let (title, text, status) = if is_tool_update {
        merge_acp_tool_update(tool_calls, tool_call_id, title, text, status)
    } else {
        (title, text, status)
    };
    let mut update =
        ExecutorUpdate::new(normalized_kind, title.clone(), text.clone(), status.clone());
    if is_tool_update {
        update = update.with_channel_event(ExecutorChannelEvent::tool_call(
            acp_tool_title(&title),
            acp_tool_channel_summary(&title, &text, &status),
        ));
    }
    Some(update)
}

fn tool_call_id(update: &Value) -> Option<&str> {
    update
        .get("toolCallId")
        .or_else(|| update.get("tool_call_id"))
        .or_else(|| {
            update
                .get("toolCall")
                .and_then(|tool| tool.get("toolCallId"))
        })
        .or_else(|| {
            update
                .get("tool_call")
                .and_then(|tool| tool.get("tool_call_id"))
        })
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
}

fn merge_acp_tool_update(
    tool_calls: &mut HashMap<String, AcpToolCallState>,
    tool_call_id: Option<&str>,
    title: String,
    text: String,
    status: String,
) -> (String, String, String) {
    let Some(tool_call_id) = tool_call_id else {
        return (title, text, status);
    };

    let state = tool_calls.entry(tool_call_id.to_string()).or_default();
    if !title.trim().is_empty() {
        state.title = title;
    }
    if !text.trim().is_empty() {
        state.text = text;
    }
    if !status.trim().is_empty() {
        state.status = status;
    }

    let merged = (
        state.title.clone(),
        state.text.clone(),
        state.status.clone(),
    );
    if is_terminal_tool_status(&merged.2) {
        tool_calls.remove(tool_call_id);
    }
    merged
}

fn is_terminal_tool_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "completed" | "failed" | "cancelled" | "canceled"
    )
}

fn acp_tool_title(title: &str) -> String {
    let title = title.trim();
    if title.is_empty() {
        "External agent tool".to_string()
    } else {
        title.to_string()
    }
}

fn acp_tool_channel_summary(title: &str, text: &str, status: &str) -> String {
    let mut lines = Vec::new();
    let text = text.trim();
    if !text.is_empty() {
        lines.push(text.to_string());
    }
    let status = status.trim();
    if !status.is_empty() {
        lines.push(format!("status: {status}"));
    }
    if lines.is_empty() {
        lines.push(acp_tool_title(title));
    }
    truncate_text(lines.join("\n"), 1_000)
}

fn extract_tool_raw_input(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => nonempty_one_line(text),
        Value::Object(map) => {
            for key in ["command", "cmd", "script"] {
                if let Some(command) = map.get(key).and_then(Value::as_str) {
                    return nonempty_one_line(command).map(|command| format!("$ {command}"));
                }
            }
            for key in ["path", "file", "query", "pattern"] {
                if let Some(value) = map.get(key).and_then(Value::as_str)
                    && let Some(value) = nonempty_one_line(value)
                {
                    return Some(value);
                }
            }
            None
        }
        _ => extract_text(value),
    }
}

fn nonempty_one_line(text: &str) -> Option<String> {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    (!line.is_empty()).then_some(line)
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

fn session_id_from_result(result: &Value) -> Option<String> {
    result
        .get("sessionId")
        .or_else(|| result.get("session_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn extract_text_result(result: &Value) -> String {
    ["text", "message", "content"]
        .iter()
        .find_map(|key| extract_text(result.get(*key)))
        .unwrap_or_default()
}

fn acp_result_cancelled(result: &Value) -> bool {
    ["stopReason", "stop_reason", "reason"]
        .iter()
        .filter_map(|key| result.get(*key).and_then(Value::as_str))
        .any(|reason| {
            let reason = reason.to_ascii_lowercase();
            reason == "cancelled" || reason == "canceled"
        })
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
    use std::{
        collections::{BTreeMap, HashMap},
        fs,
        sync::Arc,
        time::Duration,
    };

    use crate::approval::ApprovalBroker;
    use crate::executor::{
        ExecutorBackend, ExecutorChannelEventKind, ExecutorPrepareRequest, ExecutorPromptRequest,
        InterruptReason, test_support::CollectingExecutorEventSink,
    };

    use super::*;

    #[test]
    fn synthetic_acp_permission_options_are_not_auto_approvable() {
        let message = serde_json::json!({
            "params": {
                "toolCall": {
                    "title": "Run shell command",
                    "content": [{"type": "text", "text": "$ cargo test"}]
                }
            }
        });

        let request = approval_request_from_permission_message(
            &message,
            "session-1",
            "kimi",
            Some("U1".to_string()),
        );

        assert!(
            request
                .options
                .iter()
                .any(|option| option.id == "allow_once")
        );
        assert_eq!(request.allow_once_option_id(), None);
    }

    #[test]
    fn acp_tool_update_projects_channel_event() {
        let update = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "tool_call",
                    "title": "Bash",
                    "content": {"text": "$ cargo test"},
                    "status": "completed"
                }
            }
        }))
        .unwrap();

        assert_eq!(update.kind, "tool_tool_call");
        assert_eq!(update.title, "Bash");
        assert_eq!(update.text, "$ cargo test");
        assert_eq!(update.status, "completed");
        let event = update.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ToolCall);
        assert_eq!(event.title, "Bash");
        assert_eq!(event.text, "$ cargo test\nstatus: completed");

        let nested = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCall": {
                        "name": "read_file",
                        "content": {"text": "src/main.rs"},
                        "status": "failed"
                    }
                }
            }
        }))
        .unwrap();
        let event = nested.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ToolCall);
        assert_eq!(event.title, "read_file");
        assert_eq!(event.text, "src/main.rs\nstatus: failed");
    }

    #[test]
    fn acp_tool_update_merges_partial_updates_by_id() {
        let mut tool_calls = HashMap::new();
        let started = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "call-1",
                        "title": "Run tests",
                        "rawInput": {"command": "cargo test -q"},
                        "status": "pending"
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        let event = started.channel_event.unwrap();
        assert_eq!(event.title, "Run tests");
        assert_eq!(event.text, "$ cargo test -q\nstatus: pending");

        let failed = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "call-1",
                        "status": "failed"
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        let event = failed.channel_event.unwrap();
        assert_eq!(event.title, "Run tests");
        assert_eq!(event.text, "$ cargo test -q\nstatus: failed");
        assert!(tool_calls.is_empty());
    }

    #[test]
    fn acp_non_tool_updates_do_not_project_channel_events() {
        let agent_message = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "final answer"}
                }
            }
        }))
        .unwrap();
        assert_eq!(agent_message.kind, "agent_message_chunk");
        assert!(agent_message.channel_event.is_none());

        let thought = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_thought_chunk",
                    "content": {"text": "raw thought"}
                }
            }
        }))
        .unwrap();
        assert_eq!(thought.kind, "agent_thought_chunk");
        assert!(thought.channel_event.is_none());
    }

    #[tokio::test]
    async fn acp_prepare_prefers_session_cwd_over_executor_cwd() {
        let tmp = tempfile::tempdir().unwrap();
        let executor_cwd = tmp.path().join("executor-cwd");
        let session_cwd = tmp.path().join("session-cwd");
        fs::create_dir_all(&executor_cwd).unwrap();
        fs::create_dir_all(&session_cwd).unwrap();
        let script = tmp.path().join("fake_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(executor_cwd),
                env: BTreeMap::new(),
            },
        );
        let manager = AcpExecutorManager::new(executors);

        manager
            .prepare(
                ExecutorPrepareRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    cwd: Some(session_cwd.clone()),
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let session = manager.existing_session("session-1", "kimi").await.unwrap();
        assert_eq!(
            session.lock().await.cwd,
            session_cwd.canonicalize().unwrap()
        );
    }

    #[tokio::test]
    async fn acp_manager_prompts_fake_stdio_server() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fake_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        prompt = "".join(item.get("text", "") for item in msg.get("params", {}).get("prompt", []))
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"update": {"sessionUpdate": "plan", "title": "Plan", "content": {"text": "working"}}},
        })
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "reply:" + prompt}}},
        })
        send({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": "end_turn"}})
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = AcpExecutorManager::new(executors);

        let prepared = manager
            .prepare(
                ExecutorPrepareRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();
        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    prompt: "hello".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            prepared.external_session_id.as_deref(),
            Some("fake-session")
        );
        assert_eq!(response.final_text, "reply:hello");
        assert!(prepared.started_new_session);
        assert_eq!(events.updates[0].kind, "plan");
    }

    #[tokio::test]
    async fn acp_prompt_cancellation_closes_session_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("cancellable_acp.py");
        let prompt_marker = tmp.path().join("prompt_started");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "fake-session"}}}})
    elif method == "session/prompt":
        with open({prompt_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
    elif method == "session/cancel":
        pass
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(AcpExecutorManager::new(executors));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_manager = manager.clone();
        let prompt_cancel = cancel.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        session_key: "session-1".to_string(),
                        executor: "kimi".to_string(),
                        prompt: "wait".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if prompt_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(prompt_marker.exists());
        assert!(cancel.cancel(InterruptReason::UserStop).await);

        assert!(matches!(
            prompt_task.await.unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));
        let session = manager.existing_session("session-1", "kimi").await.unwrap();
        assert!(!session.lock().await.client.is_alive());
    }

    #[tokio::test]
    async fn acp_cancelled_stop_reason_returns_cancelled_outcome() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("cancelled_result_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": "cancelled"}})
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = AcpExecutorManager::new(executors);
        manager
            .prepare(
                ExecutorPrepareRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let outcome = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    prompt: "cancel".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await;
        assert!(matches!(outcome, ExecutorPromptOutcome::Cancelled));
    }

    #[tokio::test]
    async fn acp_prompt_cancellation_removes_pending_permission() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("approval_wait_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        send({
            "jsonrpc": "2.0",
            "id": 900,
            "method": "session/request_permission",
            "params": {
                "sessionId": "fake-session",
                "toolCall": {
                    "title": "Run shell command",
                    "content": [{"type": "text", "text": "$ cargo test"}]
                },
                "options": [
                    {"optionId": "allow_once", "kind": "allow_once", "name": "Allow once"},
                    {"optionId": "deny", "kind": "reject_once", "name": "Deny"}
                ]
            }
        })
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(AcpExecutorManager::with_approvals(
            executors,
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        session_key: "session-1".to_string(),
                        executor: "kimi".to_string(),
                        prompt: "run tests".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        let approval_prompt = prompts.recv().await.unwrap();
        assert!(approvals.has_pending_for_session("session-1").await);
        assert!(cancel.cancel(InterruptReason::UserStop).await);
        assert!(matches!(
            prompt_task.await.unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));
        for _ in 0..50 {
            if !approvals.has_pending_for_session("session-1").await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    }

    #[tokio::test]
    async fn acp_permission_request_waits_for_text_approval() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("approval_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        send({
            "jsonrpc": "2.0",
            "id": 900,
            "method": "session/request_permission",
            "params": {
                "sessionId": "fake-session",
                "toolCall": {
                    "title": "Run shell command",
                    "content": [{"type": "text", "text": "$ cargo test"}]
                },
                "options": [
                    {"optionId": "allow_once", "kind": "allow_once", "name": "Allow once"},
                    {"optionId": "deny", "kind": "reject_once", "name": "Deny"}
                ]
            }
        })
    elif request_id == 900:
        outcome = msg.get("result", {}).get("outcome", {})
        if outcome.get("outcome") == "selected" and outcome.get("optionId") == "allow_once":
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "approved"}}},
            })
            send({"jsonrpc": "2.0", "id": 3, "result": {"stopReason": "end_turn"}})
        else:
            send({"jsonrpc": "2.0", "id": 3, "error": {"code": -32000, "message": "not approved"}})
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(AcpExecutorManager::with_approvals(
            executors,
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        session_key: "session-1".to_string(),
                        executor: "kimi".to_string(),
                        prompt: "run tests".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });
        let approval_prompt = prompts.recv().await.unwrap();
        assert!(approval_prompt.body.contains("cargo test"));
        let reply = approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();
        assert!(reply.text.contains("Approved"));

        let response = prompt_task.await.unwrap().unwrap();
        assert_eq!(response.final_text, "approved");
    }

    #[tokio::test]
    async fn acp_prompt_returns_error_when_child_exits_without_response() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("exiting_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        sys.exit(0)
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = AcpExecutorManager::new(executors);
        manager
            .prepare(
                ExecutorPrepareRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    prompt: "hello".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("closed stdout"));
    }
}
