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
    sessions: Mutex<SessionMap>,
}

impl AcpExecutorManager {
    pub fn new(executors: BTreeMap<String, ExecutorConfig>) -> Self {
        let executors = executors
            .into_iter()
            .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::Acp)
            .collect();
        Self {
            executors,
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
        let session = Arc::new(Mutex::new(AcpSession::start(cfg.clone(), cwd).await?));
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
        let result = session.prompt(&request.prompt).await?;
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
    async fn start(cfg: ExecutorConfig, cwd: PathBuf) -> anyhow::Result<Self> {
        let client = JsonRpcClient::spawn(&cfg.command, &cfg.args, &cwd, &cfg.env).await?;
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

    async fn prompt(&mut self, prompt: &str) -> anyhow::Result<AcpPromptResult> {
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
}

#[derive(Debug, Default)]
struct JsonRpcState {
    closed: bool,
    pending: HashMap<u64, oneshot::Sender<Value>>,
}

impl JsonRpcClient {
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

        tokio::spawn(read_stdout(
            BufReader::new(stdout),
            state.clone(),
            stdin.clone(),
            updates.clone(),
        ));
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
        })
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

async fn read_stdout<R>(
    reader: BufReader<R>,
    state: SharedJsonRpcState,
    stdin: SharedStdin,
    updates: broadcast::Sender<ExecutorUpdate>,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            tracing::debug!(target: "agent_router::acp", raw_stdout = %line, "ignoring non-json ACP stdout");
            continue;
        };
        dispatch_message(message, &state, &stdin, &updates).await;
    }
    fail_all_pending(&state, "ACP process closed stdout").await;
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

async fn dispatch_message(
    message: Value,
    state: &SharedJsonRpcState,
    stdin: &SharedStdin,
    updates: &broadcast::Sender<ExecutorUpdate>,
) {
    if message.get("method").is_none() {
        let sender = match message.get("id").and_then(Value::as_u64) {
            Some(id) => state.lock().await.pending.remove(&id),
            None => None,
        };
        if let Some(tx) = sender {
            let _ = tx.send(message);
        }
        return;
    }

    if message.get("id").is_some() {
        respond_to_server_request(stdin, &message).await;
        return;
    }

    if message.get("method").and_then(Value::as_str) == Some("session/update")
        && let Some(update) = project_acp_update(&message)
    {
        let _ = updates.send(update);
    }
}

async fn respond_to_server_request(stdin: &SharedStdin, message: &Value) {
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
    let payload = if method == "session/request_permission" {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "outcome": {"outcome": "cancelled"}
            }
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
    let _ = write_json(stdin, payload).await;
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
    use std::{collections::BTreeMap, fs};

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
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("closed stdout"));
    }
}
