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

#[derive(Debug)]
pub struct CodexAppServerManager {
    executors: BTreeMap<String, ExecutorConfig>,
    approvals: SharedApprovalBroker,
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
        let executors = executors
            .into_iter()
            .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::AppServer)
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
    ) -> anyhow::Result<Self> {
        let client = CodexJsonRpcClient::spawn(&cfg.command, &cfg.args, &cwd, &cfg.env).await?;
        Ok(Self {
            cfg,
            cwd,
            client,
            session_key,
            executor,
            approvals,
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
        self.client
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
            )
            .await?;
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
            .request("thread/start", json!({ "cwd": self.cwd }))
            .await?;
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
                        Ok(request) => self.handle_server_request(request, user_id.clone()).await?,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => {
                            anyhow::bail!("codex app-server request stream closed")
                        }
                    }
                }
                notification = notifications.recv() => {
                    match notification {
                        Ok(notification) => {
                            if collect_codex_notification(notification, &mut final_text, &mut updates) {
                                turn_completed = true;
                                if turn_start_acknowledged {
                                    break;
                                }
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
            }
        }

        Ok(ExecutorResponse {
            final_text,
            updates,
        })
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
                );
                let decision = match self.approvals.request(request).await {
                    ApprovalSelection::Selected(option_id) if option_id == "accept" => "accept",
                    _ => "decline",
                };
                self.client
                    .respond(id, json!({ "decision": decision }))
                    .await?;
            }
            "item/permissions/requestApproval" | "mcpServer/elicitation/request" => {
                self.client
                    .respond(id, json!({ "decision": "decline" }))
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
}

fn codex_approval_request(
    session_key: &str,
    executor: &str,
    requester_user_id: Option<String>,
    method: &str,
    params: &Value,
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
                    tracing::debug!(target: "agent_router::codex_app_server", stderr = %line);
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

    async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let request = self.request_started(method, params).await?;
        let method = request.method;
        let response = request
            .response
            .await
            .map_err(|_| anyhow::anyhow!("codex app-server response channel closed"))?;
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
            method: method.to_string(),
            response: rx,
        })
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
        anyhow::bail!("codex app-server `{method}` failed: {error}");
    }
    Ok(response.get("result").cloned().unwrap_or(Value::Null))
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
            tracing::debug!(target: "agent_router::codex_app_server", raw_stdout = %line, "ignoring non-json codex stdout");
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

fn collect_codex_notification(
    notification: Value,
    final_text: &mut String,
    updates: &mut Vec<ExecutorUpdate>,
) -> bool {
    let method = notification
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("");
    if method == "turn/completed" {
        return true;
    }
    if method != "item/completed" {
        return false;
    }
    let Some(item) = notification
        .get("params")
        .and_then(|params| params.get("item"))
    else {
        return false;
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
    false
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
    let changes = item
        .get("changes")
        .and_then(Value::as_array)
        .map(|changes| {
            changes
                .iter()
                .filter_map(|change| change.get("path").and_then(Value::as_str))
                .take(8)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    let status = item.get("status").and_then(Value::as_str).unwrap_or("");
    truncate_text(format!("status={status}; changes={changes}"), 2_000)
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

    #[tokio::test]
    async fn codex_manager_prompts_fake_app_server() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fake_codex.py");
        fs::write(
            &script,
            r#"#!/usr/bin/env python3
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
        send({"jsonrpc": "2.0", "id": request_id, "result": {"userAgent": "fake"}})
    elif method == "initialized":
        pass
    elif method == "thread/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"thread": {"id": "thread-1"}}})
    elif method == "turn/start":
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
        )
        .unwrap();
        make_executable(&script);
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
        fs::write(
            &script,
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
        send({"jsonrpc": "2.0", "id": request_id, "result": {"userAgent": "fake"}})
    elif method == "initialized":
        pass
    elif method == "thread/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"thread": {"id": "thread-1"}}})
    elif method == "turn/start":
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
        )
        .unwrap();
        make_executable(&script);
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
}
