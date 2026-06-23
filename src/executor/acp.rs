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

type SharedJsonRpcState = Arc<Mutex<JsonRpcState>>;
type SharedStdin = Arc<Mutex<ChildStdin>>;
type SessionKey = (String, String);
type SharedAcpSession = Arc<Mutex<AcpSession>>;
type SessionMap = HashMap<SessionKey, SharedAcpSession>;

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
    ) -> anyhow::Result<Arc<Mutex<AcpSession>>> {
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

    async fn prepare(&self, request: ExecutorPrepareRequest) -> anyhow::Result<PreparedExecutor> {
        let cfg = self
            .executors
            .get(&request.executor)
            .ok_or_else(|| anyhow::anyhow!("executor `{}` is not configured", request.executor))?;
        let session = self
            .get_or_create_session(&request.session_key, &request.executor, cfg)
            .await?;
        let mut session = session.lock().await;
        let (external_session_id, started_new_session) = session
            .ensure_session(request.previous_session_id.as_deref())
            .await?;
        Ok(PreparedExecutor {
            external_session_id: Some(external_session_id),
            started_new_session,
        })
    }

    async fn prompt(&self, request: ExecutorPromptRequest) -> anyhow::Result<ExecutorResponse> {
        let session = self
            .existing_session(&request.session_key, &request.executor)
            .await?;
        let mut session = session.lock().await;
        let result = session.prompt(&request.prompt, request.user_id).await?;
        Ok(ExecutorResponse {
            final_text: result.final_text,
            updates: result.updates,
        })
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
        let client = JsonRpcClient::spawn(
            &cfg.command,
            &cfg.args,
            &cwd,
            &cfg.env,
            session_key,
            executor,
            approvals,
        )
        .await?;
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

    async fn initialize(&mut self) -> anyhow::Result<()> {
        if self.initialized {
            return Ok(());
        }
        self.client
            .request(
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
            )
            .await?;
        self.initialized = true;
        Ok(())
    }

    async fn ensure_session(
        &mut self,
        preferred_session_id: Option<&str>,
    ) -> anyhow::Result<(String, bool)> {
        self.initialize().await?;
        if let Some(session_id) = &self.session_id {
            return Ok((session_id.clone(), false));
        }

        if let Some(preferred) = preferred_session_id.filter(|value| !value.is_empty()) {
            for method in ["session/load", "session/resume"] {
                let result = self
                    .client
                    .request(
                        method,
                        json!({
                            "cwd": self.cwd,
                            "sessionId": preferred,
                            "mcpServers": [],
                        }),
                    )
                    .await;
                if let Ok(result) = result {
                    let session_id =
                        session_id_from_result(&result).unwrap_or_else(|| preferred.to_string());
                    self.session_id = Some(session_id.clone());
                    return Ok((session_id, false));
                }
            }
        }

        let result = self
            .client
            .request(
                "session/new",
                json!({
                    "cwd": self.cwd,
                    "mcpServers": [],
                }),
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
    ) -> anyhow::Result<AcpPromptResult> {
        self.client.set_active_user(user_id).await;
        let result = self.prompt_with_active_user(prompt).await;
        self.client.clear_active_user().await;
        result
    }

    async fn prompt_with_active_user(&mut self, prompt: &str) -> anyhow::Result<AcpPromptResult> {
        let session_id = self
            .session_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("ACP session has not been created"))?;
        let mut updates_rx = self.client.subscribe();
        let response_fut = self.client.request(
            "session/prompt",
            json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": prompt}],
            }),
        );
        tokio::pin!(response_fut);

        let mut updates = Vec::new();
        let mut text_parts = Vec::new();
        let result = loop {
            tokio::select! {
                result = &mut response_fut => break result?,
                received = updates_rx.recv() => {
                    match received {
                        Ok(update) => collect_update(update, &mut updates, &mut text_parts),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => continue,
                    }
                }
            }
        };
        while let Ok(update) = updates_rx.try_recv() {
            collect_update(update, &mut updates, &mut text_parts);
        }
        let final_text = if text_parts.is_empty() {
            extract_text_result(&result)
        } else {
            text_parts.join("")
        };
        Ok(AcpPromptResult {
            final_text,
            updates,
        })
    }
}

fn collect_update(
    update: ExecutorUpdate,
    updates: &mut Vec<ExecutorUpdate>,
    text_parts: &mut Vec<String>,
) {
    if update.kind == "agent_message_chunk" {
        text_parts.push(update.text.clone());
    }
    updates.push(update);
}

#[derive(Debug)]
struct AcpPromptResult {
    final_text: String,
    updates: Vec<ExecutorUpdate>,
}

#[derive(Debug)]
struct JsonRpcClient {
    stdin: SharedStdin,
    state: SharedJsonRpcState,
    next_id: AtomicU64,
    updates: broadcast::Sender<ExecutorUpdate>,
    child: Arc<Mutex<Child>>,
    active_user_id: Arc<Mutex<Option<String>>>,
}

#[derive(Debug, Default)]
struct JsonRpcState {
    closed: bool,
    pending: HashMap<u64, oneshot::Sender<Value>>,
}

#[derive(Debug, Clone)]
struct JsonRpcServerContext {
    state: SharedJsonRpcState,
    stdin: SharedStdin,
    updates: broadcast::Sender<ExecutorUpdate>,
    approvals: SharedApprovalBroker,
    session_key: String,
    executor: String,
    active_user_id: Arc<Mutex<Option<String>>>,
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
        let (updates, _) = broadcast::channel(256);
        let child = Arc::new(Mutex::new(child));
        let active_user_id = Arc::new(Mutex::new(None));
        let server_context = JsonRpcServerContext {
            state: state.clone(),
            stdin: stdin.clone(),
            updates: updates.clone(),
            approvals,
            session_key,
            executor,
            active_user_id: active_user_id.clone(),
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
        })
    }

    async fn set_active_user(&self, user_id: Option<String>) {
        *self.active_user_id.lock().await = user_id;
    }

    async fn clear_active_user(&self) {
        *self.active_user_id.lock().await = None;
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

    async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value> {
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
        let response = rx
            .await
            .map_err(|_| anyhow::anyhow!("ACP response channel closed"))?;
        if let Some(error) = response.get("error") {
            anyhow::bail!("ACP `{method}` failed: {error}");
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
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
    fail_all_pending(&context.state, "ACP process closed stdout").await;
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

async fn dispatch_message(message: Value, context: &JsonRpcServerContext) {
    if message.get("method").is_none() {
        let sender = match message.get("id").and_then(Value::as_u64) {
            Some(id) => context.state.lock().await.pending.remove(&id),
            None => None,
        };
        if let Some(tx) = sender {
            let _ = tx.send(message);
        }
        return;
    }

    if message.get("id").is_some() {
        respond_to_server_request(context, &message).await;
        return;
    }

    if message.get("method").and_then(Value::as_str) == Some("session/update")
        && let Some(update) = project_acp_update(&message)
    {
        let _ = context.updates.send(update);
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
        let selection = context.approvals.request(request).await;
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
                },
                ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject_once".to_string(),
                    name: "Deny".to_string(),
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

fn project_acp_update(message: &Value) -> Option<ExecutorUpdate> {
    let params = message.get("params")?;
    let update = params
        .get("update")
        .or_else(|| params.get("sessionUpdate"))
        .unwrap_or(params);
    let kind = update
        .get("sessionUpdate")
        .or_else(|| update.get("kind"))
        .or_else(|| update.get("type"))
        .or_else(|| update.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("update");
    let text = extract_text(update.get("content")).or_else(|| extract_text(update.get("text")));
    let title = update
        .get("title")
        .or_else(|| update.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let status = update
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let normalized_kind = if matches!(kind, "agent_message_chunk" | "agent_message") {
        "agent_message_chunk".to_string()
    } else if matches!(kind, "agent_thought_chunk" | "agent_thought") {
        "agent_thought_chunk".to_string()
    } else if kind.contains("tool") {
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
    Some(ExecutorUpdate {
        kind: normalized_kind,
        title,
        text: text.unwrap_or_default(),
        status,
    })
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

    use crate::approval::ApprovalBroker;
    use crate::executor::{ExecutorBackend, ExecutorPrepareRequest, ExecutorPromptRequest};

    use super::*;

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
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "kimi".to_string(),
                previous_session_id: None,
            })
            .await
            .unwrap();
        let response = manager
            .prompt(ExecutorPromptRequest {
                session_key: "session-1".to_string(),
                executor: "kimi".to_string(),
                prompt: "hello".to_string(),
                user_id: Some("U1".to_string()),
            })
            .await
            .unwrap();

        assert_eq!(
            prepared.external_session_id.as_deref(),
            Some("fake-session")
        );
        assert_eq!(response.final_text, "reply:hello");
        assert!(prepared.started_new_session);
        assert_eq!(response.updates[0].kind, "plan");
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
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "kimi".to_string(),
                previous_session_id: None,
            })
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            prompt_manager
                .prompt(ExecutorPromptRequest {
                    session_key: "session-1".to_string(),
                    executor: "kimi".to_string(),
                    prompt: "run tests".to_string(),
                    user_id: Some("U1".to_string()),
                })
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
            .prepare(ExecutorPrepareRequest {
                session_key: "session-1".to_string(),
                executor: "kimi".to_string(),
                previous_session_id: None,
            })
            .await
            .unwrap();

        let err = manager
            .prompt(ExecutorPromptRequest {
                session_key: "session-1".to_string(),
                executor: "kimi".to_string(),
                prompt: "hello".to_string(),
                user_id: Some("U1".to_string()),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("closed stdout"));
    }
}
