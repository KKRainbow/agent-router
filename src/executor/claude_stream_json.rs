use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::{Instant, sleep};

use crate::approval::{
    ApprovalBroker, ApprovalCancellation, ApprovalOption, ApprovalRequest, ApprovalSelection,
    SharedApprovalBroker,
};
use crate::config::{ExecutorConfig, ExecutorProtocol};
use crate::executor::{
    ExecutorBackend, ExecutorChannelEvent, ExecutorDescriptor, ExecutorEventSink,
    ExecutorInterruptRequest, ExecutorPrepareRequest, ExecutorPromptOutcome, ExecutorPromptRequest,
    ExecutorResponse, ExecutorUpdate, PreparedExecutor, TurnCancellation,
};

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClaudeEvent {
    System {
        session_id: Option<String>,
        model: Option<String>,
    },
    Assistant {
        message: AssistantMessage,
    },
    User {
        message: UserMessage,
    },
    Result {
        result: Option<String>,
        subtype: Option<String>,
        session_id: Option<String>,
        usage: Option<serde_json::Value>,
    },
    ControlRequest {
        request_id: String,
        request: Option<serde_json::Value>,
    },
    ControlCancelRequest {
        request_id: String,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct AssistantMessage {
    #[serde(default)]
    pub content: Vec<AssistantContent>,
    pub usage: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantContent {
    Text {
        text: String,
    },
    Thinking {
        thinking: String,
    },
    ToolUse {
        name: String,
        input: serde_json::Value,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct UserMessage {
    pub content: Vec<UserContent>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UserContent {
    ToolResult {
        content: serde_json::Value,
        is_error: Option<bool>,
    },
    Text {
        text: String,
    },
}

pub fn parse_event_line(line: &str) -> Option<ClaudeEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    match serde_json::from_str(trimmed) {
        Ok(event) => Some(event),
        Err(err) => {
            tracing::debug!(
                target: "agent_router::claude",
                line,
                error = %err,
                "ignoring non-event JSON line"
            );
            None
        }
    }
}

pub fn is_compaction_result(subtype: Option<&str>) -> bool {
    matches!(subtype, Some("compact") | Some("compaction"))
}

#[derive(Debug, Clone, Serialize)]
pub struct UserEvent {
    r#type: &'static str,
    message: UserEventMessage,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserEventMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlResponse {
    r#type: &'static str,
    response: ControlResponseInner,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlResponseInner {
    subtype: &'static str,
    request_id: String,
    response: PermissionDecision,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "behavior", rename_all = "lowercase")]
pub enum PermissionDecision {
    Allow {
        #[serde(rename = "updatedInput")]
        updated_input: Map<String, Value>,
    },
    Deny {
        message: String,
    },
}

impl UserEvent {
    pub fn new(prompt: String) -> Self {
        Self {
            r#type: "user",
            message: UserEventMessage {
                role: "user",
                content: prompt,
            },
        }
    }
}

impl ControlResponse {
    pub fn allow(request_id: String) -> Self {
        Self {
            r#type: "control_response",
            response: ControlResponseInner {
                subtype: "success",
                request_id,
                response: PermissionDecision::Allow {
                    updated_input: Map::new(),
                },
            },
        }
    }

    pub fn deny(request_id: String, message: impl Into<String>) -> Self {
        Self {
            r#type: "control_response",
            response: ControlResponseInner {
                subtype: "success",
                request_id,
                response: PermissionDecision::Deny {
                    message: message.into(),
                },
            },
        }
    }
}

#[cfg(test)]
mod event_tests {
    use super::*;

    #[test]
    fn parses_system_event_with_session_id() {
        let line = r#"{"type":"system","session_id":"sess-123","model":"claude-sonnet-4"}"#;
        let event = parse_event_line(line).expect("valid system event");
        match event {
            ClaudeEvent::System { session_id, model } => {
                assert_eq!(session_id, Some("sess-123".to_string()));
                assert_eq!(model, Some("claude-sonnet-4".to_string()));
            }
            _ => panic!("expected System event"),
        }
    }

    #[test]
    fn parses_system_event_without_optional_fields() {
        let line = r#"{"type":"system"}"#;
        let event = parse_event_line(line).expect("valid system event");
        match event {
            ClaudeEvent::System { session_id, model } => {
                assert_eq!(session_id, None);
                assert_eq!(model, None);
            }
            _ => panic!("expected System event"),
        }
    }

    #[test]
    fn parses_assistant_text_and_thinking() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"},{"type":"thinking","thinking":"pondering"}]}}"#;
        let event = parse_event_line(line).expect("valid assistant event");
        match event {
            ClaudeEvent::Assistant { message } => {
                assert_eq!(message.content.len(), 2);
                assert_eq!(
                    message.content[0],
                    AssistantContent::Text {
                        text: "hello".to_string()
                    }
                );
                assert_eq!(
                    message.content[1],
                    AssistantContent::Thinking {
                        thinking: "pondering".to_string()
                    }
                );
            }
            _ => panic!("expected Assistant event"),
        }
    }

    #[test]
    fn parses_assistant_message_without_content() {
        let line = r#"{"type":"assistant","message":{}}"#;
        let event = parse_event_line(line).expect("valid assistant event");
        match event {
            ClaudeEvent::Assistant { message } => {
                assert!(message.content.is_empty());
                assert_eq!(message.usage, None);
            }
            _ => panic!("expected Assistant event"),
        }
    }

    #[test]
    fn recognizes_compaction_result() {
        let line = r#"{"type":"result","result":"compact summary","subtype":"compact","session_id":"sess-123","usage":null}"#;
        let event = parse_event_line(line).expect("valid result event");
        match &event {
            ClaudeEvent::Result { subtype, .. } => {
                assert!(is_compaction_result(subtype.as_deref()));
            }
            _ => panic!("expected Result event"),
        }
        assert!(!is_compaction_result(Some("final")));
        assert!(!is_compaction_result(None));
    }

    #[test]
    fn parses_result_event_with_minimal_fields() {
        let line = r#"{"type":"result"}"#;
        let event = parse_event_line(line).expect("valid result event");
        match event {
            ClaudeEvent::Result {
                result,
                subtype,
                session_id,
                usage,
            } => {
                assert_eq!(result, None);
                assert_eq!(subtype, None);
                assert_eq!(session_id, None);
                assert_eq!(usage, None);
            }
            _ => panic!("expected Result event"),
        }
    }

    #[test]
    fn parses_user_tool_result_with_error() {
        let line = r#"{"type":"user","message":{"content":[{"type":"tool_result","content":"ok","is_error":true}]}}"#;
        let event = parse_event_line(line).expect("valid user event");
        match event {
            ClaudeEvent::User { message } => {
                assert_eq!(message.content.len(), 1);
                assert_eq!(
                    message.content[0],
                    UserContent::ToolResult {
                        content: serde_json::Value::String("ok".to_string()),
                        is_error: Some(true),
                    }
                );
            }
            _ => panic!("expected User event"),
        }
    }

    #[test]
    fn parses_control_request_without_request_body() {
        let line = r#"{"type":"control_request","request_id":"req-1"}"#;
        let event = parse_event_line(line).expect("valid control request event");
        match event {
            ClaudeEvent::ControlRequest {
                request_id,
                request,
            } => {
                assert_eq!(request_id, "req-1");
                assert_eq!(request, None);
            }
            _ => panic!("expected ControlRequest event"),
        }
    }

    #[test]
    fn ignores_non_json_line() {
        assert!(parse_event_line("not a json line").is_none());
        assert!(parse_event_line("").is_none());
    }
}

#[cfg(test)]
mod outgoing_tests {
    use super::*;

    #[test]
    fn user_event_serializes() {
        let ev = UserEvent::new("hi".to_string());
        let value = serde_json::to_value(&ev).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": "hi"
                }
            })
        );
    }

    #[test]
    fn control_response_allow_serializes() {
        let resp = ControlResponse::allow("req-1".to_string());
        let value = serde_json::to_value(&resp).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "type": "control_response",
                "response": {
                    "subtype": "success",
                    "request_id": "req-1",
                    "response": {
                        "behavior": "allow",
                        "updatedInput": {}
                    }
                }
            })
        );
    }

    #[test]
    fn control_response_deny_serializes() {
        let resp = ControlResponse::deny("req-1".to_string(), "not allowed");
        let value = serde_json::to_value(&resp).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "type": "control_response",
                "response": {
                    "subtype": "success",
                    "request_id": "req-1",
                    "response": {
                        "behavior": "deny",
                        "message": "not allowed"
                    }
                }
            })
        );
    }
}

type SessionKey = (String, String);
type SharedClaudeSession = Arc<Mutex<ClaudeSession>>;

#[derive(Debug)]
pub struct ClaudeStreamJsonManager {
    executors: BTreeMap<String, ExecutorConfig>,
    approvals: SharedApprovalBroker,
    sessions: Mutex<HashMap<SessionKey, SharedClaudeSession>>,
}

impl ClaudeStreamJsonManager {
    pub fn new(executors: BTreeMap<String, ExecutorConfig>) -> Self {
        Self::with_approvals(executors, Arc::new(ApprovalBroker::default()))
    }

    pub fn with_approvals(
        executors: BTreeMap<String, ExecutorConfig>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        let executors = executors
            .into_iter()
            .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::ClaudeStreamJson)
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
        cwd: PathBuf,
        previous_session_id: Option<String>,
        cancel: &TurnCancellation,
    ) -> anyhow::Result<SharedClaudeSession> {
        let key = (session_key.to_string(), executor.to_string());
        loop {
            if cancel.is_cancelled().await {
                anyhow::bail!("Claude stream-json prepare cancelled");
            }
            let existing = self.sessions.lock().await.get(&key).cloned();
            if let Some(existing) = existing.as_ref() {
                let guard = existing.lock().await;
                let matches = guard.is_alive() && guard.matches(cfg, &cwd);
                drop(guard);
                if matches {
                    return Ok(existing.clone());
                }
                if cancel.is_cancelled().await {
                    anyhow::bail!("Claude stream-json prepare cancelled");
                }
                if !self.remove_session_if_same(&key, existing, cancel).await {
                    continue;
                }
            }
            let session = Arc::new(Mutex::new(
                ClaudeSession::start(
                    cfg.clone(),
                    cwd.clone(),
                    session_key.to_string(),
                    executor.to_string(),
                    self.approvals.clone(),
                    previous_session_id.clone(),
                )
                .await?,
            ));
            if cancel.is_cancelled().await {
                session.lock().await.close().await;
                anyhow::bail!("Claude stream-json prepare cancelled");
            }
            if !self.publish_session_if_absent(&key, &session).await {
                session.lock().await.close().await;
                continue;
            }
            return Ok(session);
        }
    }

    async fn publish_session_if_absent(
        &self,
        key: &SessionKey,
        session: &SharedClaudeSession,
    ) -> bool {
        let mut sessions = self.sessions.lock().await;
        if sessions.contains_key(key) {
            return false;
        }
        sessions.insert(key.clone(), session.clone());
        true
    }

    async fn remove_session_if_same(
        &self,
        key: &SessionKey,
        session: &SharedClaudeSession,
        cancel: &TurnCancellation,
    ) -> bool {
        let removed = {
            let mut sessions = self.sessions.lock().await;
            if !cancel.is_cancelled_now()
                && sessions
                    .get(key)
                    .is_some_and(|existing| Arc::ptr_eq(existing, session))
            {
                sessions.remove(key);
                true
            } else {
                false
            }
        };
        if removed {
            session.lock().await.close().await;
        }
        removed
    }

    async fn existing_session(
        &self,
        session_key: &str,
        executor: &str,
    ) -> anyhow::Result<SharedClaudeSession> {
        self.sessions
            .lock()
            .await
            .get(&(session_key.to_string(), executor.to_string()))
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "executor `{executor}` has not prepared Claude stream-json session for `{session_key}`"
                )
            })
    }
}

#[async_trait]
impl ExecutorBackend for ClaudeStreamJsonManager {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.executors.get(name).map(|cfg| ExecutorDescriptor {
            name: cfg.name.clone(),
            protocol: "claude_stream_json".to_string(),
        })
    }

    fn list(&self) -> Vec<ExecutorDescriptor> {
        self.executors
            .values()
            .map(|cfg| ExecutorDescriptor {
                name: cfg.name.clone(),
                protocol: "claude_stream_json".to_string(),
            })
            .collect()
    }

    async fn prepare(
        &self,
        request: ExecutorPrepareRequest,
        cancel: TurnCancellation,
    ) -> anyhow::Result<PreparedExecutor> {
        if cancel.is_cancelled().await {
            anyhow::bail!("Claude stream-json prepare cancelled");
        }
        let cfg = self.executors.get(&request.turn.executor).ok_or_else(|| {
            anyhow::anyhow!("executor `{}` is not configured", request.turn.executor)
        })?;
        tracing::info!(
            executor = %request.turn.executor,
            session_key = %request.turn.session_key,
            generation = request.turn.generation,
            previous_session_id = ?request.previous_session_id,
            "preparing Claude stream-json executor session"
        );
        let cwd = request
            .cwd
            .clone()
            .or_else(|| cfg.cwd.clone())
            .unwrap_or_else(|| PathBuf::from("."));
        let previous_session_id = request.previous_session_id.clone();
        let session = self
            .get_or_create_session(
                &request.turn.session_key,
                &request.turn.executor,
                cfg,
                cwd,
                previous_session_id.clone(),
                &cancel,
            )
            .await?;
        if cancel.is_cancelled().await {
            anyhow::bail!("Claude stream-json prepare cancelled");
        }
        let external_session_id = {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                if cancel.is_cancelled().await {
                    anyhow::bail!("Claude stream-json prepare cancelled");
                }
                let id = session.lock().await.session_id().await;
                if id.is_some() || Instant::now() >= deadline {
                    break id;
                }
                sleep(Duration::from_millis(10)).await;
            }
        };
        let started_new_session = previous_session_id.is_none()
            || external_session_id.as_ref() != previous_session_id.as_ref();
        tracing::info!(
            executor = %request.turn.executor,
            session_key = %request.turn.session_key,
            generation = request.turn.generation,
            external_session_id = ?external_session_id,
            started_new_session,
            "prepared Claude stream-json executor session"
        );
        Ok(PreparedExecutor {
            external_session_id,
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
            .existing_session(&request.turn.session_key, &request.turn.executor)
            .await
        {
            Ok(session) => session,
            Err(err) => return ExecutorPromptOutcome::Failed(err),
        };
        let session = session.lock().await;
        let session_key = request.turn.session_key.clone();
        let executor = request.turn.executor.clone();
        let generation = request.turn.generation;
        let prompt_len = request.prompt.len();
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            generation,
            prompt_len,
            "starting Claude stream-json turn"
        );
        let result = session
            .run_turn(&request.prompt, request.user_id, events, cancel)
            .await;
        match &result {
            ExecutorPromptOutcome::Completed(response) => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                generation,
                final_text_len = response.final_text.len(),
                "completed Claude stream-json turn"
            ),
            ExecutorPromptOutcome::Cancelled => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                generation,
                "cancelled Claude stream-json turn"
            ),
            ExecutorPromptOutcome::Failed(err) => tracing::warn!(
                error = %err,
                executor = %executor,
                session_key = %session_key,
                generation,
                "failed Claude stream-json turn"
            ),
        }
        result
    }

    async fn interrupt(&self, request: ExecutorInterruptRequest) -> anyhow::Result<()> {
        let session = self
            .sessions
            .lock()
            .await
            .get(&(
                request.turn.session_key.clone(),
                request.turn.executor.clone(),
            ))
            .cloned();
        if let Some(session) = session {
            session.lock().await.close().await;
        }
        Ok(())
    }
}

type SharedStdin = Arc<Mutex<ChildStdin>>;

#[allow(dead_code)]
#[derive(Debug)]
pub struct ClaudeSession {
    cfg: ExecutorConfig,
    cwd: PathBuf,
    stdin: SharedStdin,
    child: Arc<Mutex<Child>>,
    updates: broadcast::Sender<ExecutorUpdate>,
    session_id: Arc<Mutex<Option<String>>>,
    active_user_id: Arc<Mutex<Option<String>>>,
    approvals: SharedApprovalBroker,
    session_key: String,
    executor: String,
    alive: Arc<AtomicBool>,
    pending_approvals: Arc<Mutex<HashMap<String, ApprovalCancellation>>>,
    _shutdown_tx: oneshot::Sender<()>,
}

impl ClaudeSession {
    pub async fn start(
        cfg: ExecutorConfig,
        cwd: PathBuf,
        session_key: String,
        executor: String,
        approvals: SharedApprovalBroker,
        previous_session_id: Option<String>,
    ) -> anyhow::Result<Self> {
        let mut cmd = Command::new(&cfg.command);
        cmd.args(&cfg.args)
            .arg("--output-format")
            .arg("stream-json")
            .arg("--input-format")
            .arg("stream-json")
            .arg("--permission-prompt-tool")
            .arg("stdio")
            .arg("--replay-user-messages")
            // stream-json in non-TTY mode is treated as --print and requires
            // --verbose to emit the JSON event stream.
            .arg("--verbose");
        if let Some(id) = previous_session_id.filter(|id| !id.is_empty()) {
            cmd.arg("--resume").arg(id);
        }
        cmd.kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(&cwd);
        // Prevent Claude Code from detecting a nested session when this agent is
        // spawned from within another Claude Code process.
        cmd.env_remove("CLAUDECODE");
        for (key, value) in &cfg.env {
            cmd.env(key, value);
        }

        let mut child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn claude executor: {}", cfg.command))?;
        let stdin = Arc::new(Mutex::new(
            child.stdin.take().context("missing child stdin")?,
        ));
        let stdout = child.stdout.take().context("missing child stdout")?;
        let stderr = child.stderr.take().context("missing child stderr")?;
        let child = Arc::new(Mutex::new(child));
        let (updates, _) = broadcast::channel(256);
        let session_id = Arc::new(Mutex::new(None));
        let active_user_id = Arc::new(Mutex::new(None));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let alive = Arc::new(AtomicBool::new(true));
        let pending_approvals = Arc::new(Mutex::new(HashMap::new()));

        tokio::spawn(read_stdout(
            BufReader::new(stdout),
            BufReader::new(stderr),
            updates.clone(),
            session_id.clone(),
            active_user_id.clone(),
            approvals.clone(),
            session_key.clone(),
            executor.clone(),
            stdin.clone(),
            child.clone(),
            alive.clone(),
            pending_approvals.clone(),
            shutdown_rx,
        ));

        Ok(Self {
            cfg,
            cwd,
            stdin,
            child,
            updates,
            session_id,
            active_user_id,
            approvals,
            session_key,
            executor,
            alive,
            pending_approvals,
            _shutdown_tx: shutdown_tx,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ExecutorUpdate> {
        self.updates.subscribe()
    }

    pub async fn session_id(&self) -> Option<String> {
        self.session_id.lock().await.clone()
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    pub fn matches(&self, cfg: &ExecutorConfig, cwd: &Path) -> bool {
        self.cfg.command == cfg.command
            && self.cfg.args == cfg.args
            && self.cfg.env == cfg.env
            && self.cwd == cwd
    }

    pub async fn send_prompt(&self, prompt: &str) -> anyhow::Result<()> {
        let event = UserEvent::new(prompt.to_string());
        let mut json = serde_json::to_string(&event).context("serialize user event")?;
        json.push('\n');
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(json.as_bytes())
            .await
            .context("write to claude stdin")?;
        stdin.flush().await.context("flush claude stdin")?;
        Ok(())
    }

    pub async fn run_turn(
        &self,
        prompt: &str,
        user_id: Option<String>,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome {
        *self.active_user_id.lock().await = user_id;

        if let Err(err) = self.send_prompt(prompt).await {
            *self.active_user_id.lock().await = None;
            return ExecutorPromptOutcome::Failed(err);
        }

        let mut rx = self.subscribe();
        let mut final_text;

        loop {
            tokio::select! {
                _reason = cancel.cancelled() => {
                    *self.active_user_id.lock().await = None;
                    self.close().await;
                    return ExecutorPromptOutcome::Cancelled;
                }
                update = rx.recv() => {
                    match update {
                        Ok(update) => {
                            if update.kind == "result" {
                                final_text = update.text;
                                break;
                            }
                            if let Err(err) = events.send(update).await {
                                *self.active_user_id.lock().await = None;
                                return ExecutorPromptOutcome::Failed(err);
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            *self.active_user_id.lock().await = None;
                            return ExecutorPromptOutcome::Failed(anyhow::anyhow!(
                                "claude update channel closed"
                            ));
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }
            }
        }

        loop {
            match rx.try_recv() {
                Ok(update) => {
                    if update.kind == "result" {
                        final_text = update.text;
                        continue;
                    }
                    if let Err(err) = events.send(update).await {
                        *self.active_user_id.lock().await = None;
                        return ExecutorPromptOutcome::Failed(err);
                    }
                }
                Err(broadcast::error::TryRecvError::Empty)
                | Err(broadcast::error::TryRecvError::Closed) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            }
        }

        *self.active_user_id.lock().await = None;
        ExecutorPromptOutcome::Completed(ExecutorResponse { final_text })
    }

    pub async fn close(&self) {
        self.alive.store(false, Ordering::Relaxed);

        let mut pending = self.pending_approvals.lock().await;
        for cancellation in pending.values() {
            cancellation.cancel();
        }
        pending.clear();
        drop(pending);

        let mut stdin = self.stdin.lock().await;
        if let Err(err) = stdin.shutdown().await {
            tracing::warn!(target: "agent_router::claude", error = %err, "failed to shutdown claude stdin");
        }
        drop(stdin);

        let mut child = self.child.lock().await;
        if let Err(err) = child.start_kill() {
            tracing::warn!(target: "agent_router::claude", error = %err, "failed to kill claude child");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_stdout<R, E>(
    mut reader: R,
    mut stderr: E,
    updates: broadcast::Sender<ExecutorUpdate>,
    session_id: Arc<Mutex<Option<String>>>,
    active_user_id: Arc<Mutex<Option<String>>>,
    approvals: SharedApprovalBroker,
    session_key: String,
    executor: String,
    stdin: SharedStdin,
    child: Arc<Mutex<Child>>,
    alive: Arc<AtomicBool>,
    pending_approvals: Arc<Mutex<HashMap<String, ApprovalCancellation>>>,
    mut shutdown: oneshot::Receiver<()>,
) where
    R: AsyncBufReadExt + Unpin,
    E: AsyncBufReadExt + Unpin,
{
    let mut stdout_line = String::new();
    let mut stderr_line = String::new();
    loop {
        stdout_line.clear();
        stderr_line.clear();
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::debug!(target: "agent_router::claude", "read_stdout shutting down");
                break;
            }
            res = reader.read_line(&mut stdout_line) => {
                match res {
                    Ok(0) => break,
                    Ok(_) => {
                        let user_id = active_user_id.lock().await.clone();
                        handle_event_line(
                            &stdout_line,
                            updates.clone(),
                            &session_id,
                            approvals.clone(),
                            &session_key,
                            &executor,
                            user_id,
                            &stdin,
                            &pending_approvals,
                        )
                        .await;
                    }
                    Err(err) => {
                        tracing::warn!(target: "agent_router::claude", error = %err, "stdout read error");
                        break;
                    }
                }
            }
            res = stderr.read_line(&mut stderr_line) => {
                match res {
                    Ok(0) => break,
                    Ok(_) => {
                        let trimmed = stderr_line.trim();
                        if !trimmed.is_empty() {
                            tracing::debug!(target: "agent_router::claude", line = %trimmed, "claude stderr");
                        }
                    }
                    Err(err) => {
                        tracing::warn!(target: "agent_router::claude", error = %err, "stderr read error");
                        break;
                    }
                }
            }
        }
    }

    alive.store(false, Ordering::Relaxed);
    let mut child = child.lock().await;
    if let Err(err) = child.start_kill() {
        tracing::warn!(target: "agent_router::claude", error = %err, "failed to kill claude child");
    }
    let _ = child.wait().await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_event_line(
    line: &str,
    updates: broadcast::Sender<ExecutorUpdate>,
    session_id: &Arc<Mutex<Option<String>>>,
    approvals: SharedApprovalBroker,
    session_key: &str,
    executor: &str,
    user_id: Option<String>,
    stdin: &SharedStdin,
    pending_approvals: &Arc<Mutex<HashMap<String, ApprovalCancellation>>>,
) {
    let Some(event) = parse_event_line(line) else {
        return;
    };

    if let ClaudeEvent::System { session_id: id, .. } | ClaudeEvent::Result { session_id: id, .. } =
        &event
        && let Some(id) = id.clone()
    {
        *session_id.lock().await = Some(id);
    }

    match event {
        ClaudeEvent::ControlRequest {
            request_id,
            request,
        } => {
            let subtype = request
                .as_ref()
                .and_then(|r| r.get("subtype"))
                .and_then(Value::as_str);
            if subtype != Some("can_use_tool") {
                tracing::debug!(
                    target: "agent_router::claude",
                    request_id,
                    subtype = ?subtype,
                    "ignoring non-tool control request"
                );
                return;
            }
            let approval_req = build_approval_request(
                session_key,
                executor,
                user_id,
                request_id.clone(),
                request.clone(),
            );
            let cancellation = ApprovalCancellation::new();
            pending_approvals
                .lock()
                .await
                .insert(request_id.clone(), cancellation.clone());

            let approvals = approvals.clone();
            let stdin = stdin.clone();
            let pending_approvals = pending_approvals.clone();
            tokio::spawn(async move {
                let selection = approvals
                    .request_until_cancelled(approval_req, cancellation)
                    .await;
                pending_approvals.lock().await.remove(&request_id);

                let response = match selection {
                    Some(ApprovalSelection::Selected(option_id)) if option_id == "allow" => {
                        ControlResponse::allow(request_id)
                    }
                    _ => ControlResponse::deny(
                        request_id,
                        "The user denied this tool use. Stop and wait for instructions.",
                    ),
                };
                let mut json = match serde_json::to_string(&response) {
                    Ok(json) => json,
                    Err(err) => {
                        tracing::warn!(
                            target: "agent_router::claude",
                            error = %err,
                            "failed to serialize control response"
                        );
                        return;
                    }
                };
                json.push('\n');
                let mut guard = stdin.lock().await;
                if let Err(err) = guard.write_all(json.as_bytes()).await {
                    tracing::warn!(
                        target: "agent_router::claude",
                        error = %err,
                        "failed to write control response"
                    );
                    return;
                }
                if let Err(err) = guard.flush().await {
                    tracing::warn!(
                        target: "agent_router::claude",
                        error = %err,
                        "failed to flush control response"
                    );
                }
            });
        }
        ClaudeEvent::ControlCancelRequest { request_id } => {
            if let Some(cancellation) = pending_approvals.lock().await.remove(&request_id) {
                cancellation.cancel();
            }
        }
        other => {
            for update in event_to_updates(other) {
                let _ = updates.send(update);
            }
        }
    }
}

fn build_approval_request(
    session_key: &str,
    executor: &str,
    requester_user_id: Option<String>,
    request_id: String,
    request: Option<Value>,
) -> ApprovalRequest {
    let tool_name = request
        .as_ref()
        .and_then(|r| {
            r.get("tool")
                .or_else(|| r.get("tool_name"))
                .and_then(Value::as_str)
        })
        .unwrap_or("tool");
    let description = request
        .as_ref()
        .and_then(|r| {
            r.get("description")
                .or_else(|| r.get("message"))
                .and_then(Value::as_str)
                .map(String::from)
        })
        .unwrap_or_else(|| {
            format!(
                "Claude requests permission to run tool {} (request_id: {}).",
                tool_name, request_id
            )
        });
    ApprovalRequest {
        session_key: session_key.to_string(),
        executor: executor.to_string(),
        requester_user_id,
        title: format!("Claude: {tool_name}"),
        body: description,
        options: vec![
            ApprovalOption {
                id: "allow".to_string(),
                kind: "allow".to_string(),
                name: "Allow".to_string(),
                auto_approvable: false,
            },
            ApprovalOption {
                id: "deny".to_string(),
                kind: "reject".to_string(),
                name: "Deny".to_string(),
                auto_approvable: false,
            },
        ],
    }
}

fn event_to_updates(event: ClaudeEvent) -> Vec<ExecutorUpdate> {
    let mut updates = Vec::new();
    match event {
        ClaudeEvent::System { .. } => {}
        ClaudeEvent::Assistant { message } => {
            for content in message.content {
                match content {
                    AssistantContent::Text { text } => {
                        updates.push(ExecutorUpdate::new(
                            "agent_message_chunk",
                            "Assistant",
                            text,
                            "",
                        ));
                    }
                    AssistantContent::Thinking { thinking } => {
                        updates.push(
                            ExecutorUpdate::new("reasoning_summary", "Reasoning", &thinking, "")
                                .with_channel_event(ExecutorChannelEvent::reasoning_summary(
                                    thinking,
                                )),
                        );
                    }
                    AssistantContent::ToolUse { name, input } => {
                        let text = serde_json::to_string(&input).unwrap_or_default();
                        updates.push(
                            ExecutorUpdate::new("tool_call", &name, &text, "").with_channel_event(
                                ExecutorChannelEvent::tool_call(name, text.clone()),
                            ),
                        );
                    }
                }
            }
        }
        ClaudeEvent::User { message } => {
            for content in message.content {
                if let UserContent::ToolResult { content, is_error } = content {
                    let text = content.to_string();
                    let title = if is_error == Some(true) {
                        "Tool result (error)"
                    } else {
                        "Tool result"
                    };
                    updates.push(ExecutorUpdate::new("tool_result", title, text, ""));
                }
            }
        }
        ClaudeEvent::Result {
            result, subtype, ..
        } => {
            if !is_compaction_result(subtype.as_deref()) {
                updates.push(ExecutorUpdate::new(
                    "result",
                    "Result",
                    result.as_deref().unwrap_or(""),
                    "",
                ));
            }
        }
        ClaudeEvent::ControlRequest { .. } | ClaudeEvent::ControlCancelRequest { .. } => {}
    }
    updates
}

#[cfg(test)]
mod session_tests {
    use super::*;
    use crate::executor::{ExecutorTurnRef, InterruptReason};
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::Duration;

    fn claude_executor_config(script: &Path, cwd: &Path) -> ExecutorConfig {
        ExecutorConfig {
            name: "claude".to_string(),
            protocol: crate::config::ExecutorProtocol::ClaudeStreamJson,
            command: "sh".to_string(),
            args: vec![script.display().to_string()],
            cwd: Some(cwd.to_path_buf()),
            env: BTreeMap::new(),
        }
    }

    fn write_fake_claude_script(path: &Path, session_id: &str) {
        std::fs::write(
            path,
            format!(
                r#"printf '{{"type":"system","session_id":"{session_id}"}}\n'
while IFS= read -r line; do
  printf '{{"type":"result","result":"ok"}}\n'
done
"#
            ),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn session_tracks_external_id_from_system_event() {
        let system_line = r#"{"type":"system","session_id":"sess-ext-123"}"#;
        let cfg = ExecutorConfig {
            name: "claude-test".to_string(),
            protocol: crate::config::ExecutorProtocol::ClaudeStreamJson,
            command: "printf".to_string(),
            args: vec!["%s\n".to_string(), system_line.to_string()],
            cwd: None,
            env: BTreeMap::new(),
        };
        let approvals = Arc::new(crate::approval::ApprovalBroker::default());
        let session = ClaudeSession::start(
            cfg,
            PathBuf::from("."),
            "test-key".to_string(),
            "claude".to_string(),
            approvals,
            None,
        )
        .await
        .expect("start session");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while session.session_id().await.is_none() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(session.session_id().await, Some("sess-ext-123".to_string()));
    }

    #[tokio::test]
    async fn fake_claude_turn_completes() {
        use crate::executor::test_support::CollectingExecutorEventSink;

        let cfg = ExecutorConfig {
            name: "fake-claude".to_string(),
            protocol: crate::config::ExecutorProtocol::ClaudeStreamJson,
            command: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fake_claude.sh")
                .to_string_lossy()
                .to_string(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
        };
        let approvals = Arc::new(crate::approval::ApprovalBroker::default());
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let session = ClaudeSession::start(
            cfg,
            temp_dir.path().to_path_buf(),
            "fake-session".to_string(),
            "claude".to_string(),
            approvals,
            None,
        )
        .await
        .expect("start session");

        let mut sink = CollectingExecutorEventSink::default();
        let outcome = session
            .run_turn("hi", None, &mut sink, TurnCancellation::new())
            .await;

        match outcome {
            ExecutorPromptOutcome::Completed(response) => {
                assert_eq!(response.final_text, "Hello from fake Claude");
            }
            other => panic!("expected Completed outcome, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn claude_session_publication_lost_race_keeps_existing_session() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let winner_script = temp_dir.path().join("winner_claude.sh");
        let loser_script = temp_dir.path().join("loser_claude.sh");
        write_fake_claude_script(&winner_script, "winner-session");
        write_fake_claude_script(&loser_script, "loser-session");
        let manager = ClaudeStreamJsonManager::new(BTreeMap::new());
        let key = ("session-1".to_string(), "claude".to_string());
        let approvals = Arc::new(crate::approval::ApprovalBroker::default());
        let winner = Arc::new(Mutex::new(
            ClaudeSession::start(
                claude_executor_config(&winner_script, temp_dir.path()),
                temp_dir.path().to_path_buf(),
                "session-1".to_string(),
                "claude".to_string(),
                approvals.clone(),
                None,
            )
            .await
            .unwrap(),
        ));
        let loser = Arc::new(Mutex::new(
            ClaudeSession::start(
                claude_executor_config(&loser_script, temp_dir.path()),
                temp_dir.path().to_path_buf(),
                "session-1".to_string(),
                "claude".to_string(),
                approvals,
                None,
            )
            .await
            .unwrap(),
        ));

        assert!(manager.publish_session_if_absent(&key, &winner).await);
        assert!(!manager.publish_session_if_absent(&key, &loser).await);
        loser.lock().await.close().await;

        let current = manager
            .existing_session("session-1", "claude")
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&current, &winner));
        assert!(winner.lock().await.is_alive());
        assert!(!loser.lock().await.is_alive());
        winner.lock().await.close().await;
    }

    #[tokio::test]
    async fn claude_cancelled_removal_does_not_remove_matching_session() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let script = temp_dir.path().join("current_claude.sh");
        write_fake_claude_script(&script, "current-session");
        let manager = ClaudeStreamJsonManager::new(BTreeMap::new());
        let key = ("session-1".to_string(), "claude".to_string());
        let cancel = TurnCancellation::new();
        let session = manager
            .get_or_create_session(
                "session-1",
                "claude",
                &claude_executor_config(&script, temp_dir.path()),
                temp_dir.path().to_path_buf(),
                None,
                &cancel,
            )
            .await
            .unwrap();
        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);

        assert!(
            !manager
                .remove_session_if_same(&key, &session, &cancel)
                .await
        );

        let current = manager
            .existing_session("session-1", "claude")
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&current, &session));
        assert!(session.lock().await.is_alive());
        session.lock().await.close().await;
    }

    #[tokio::test]
    async fn claude_cancelled_mismatched_prepare_does_not_remove_newer_session() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let old_script = temp_dir.path().join("old_claude.sh");
        let new_script = temp_dir.path().join("new_claude.sh");
        write_fake_claude_script(&old_script, "old-session");
        write_fake_claude_script(&new_script, "new-session");
        let manager = ClaudeStreamJsonManager::new(BTreeMap::new());
        let new_cancel = TurnCancellation::new();
        let newer = manager
            .get_or_create_session(
                "session-1",
                "claude",
                &claude_executor_config(&new_script, temp_dir.path()),
                temp_dir.path().to_path_buf(),
                None,
                &new_cancel,
            )
            .await
            .unwrap();
        let stale_cancel = TurnCancellation::new();
        assert!(
            stale_cancel
                .cancel(InterruptReason::ReplacedByNewMessage)
                .await
        );

        let err = manager
            .get_or_create_session(
                "session-1",
                "claude",
                &claude_executor_config(&old_script, temp_dir.path()),
                temp_dir.path().to_path_buf(),
                None,
                &stale_cancel,
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Claude stream-json prepare cancelled")
        );
        let current = manager
            .existing_session("session-1", "claude")
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&current, &newer));
        newer.lock().await.close().await;
    }

    #[tokio::test]
    async fn cancelled_prepare_after_publication_keeps_session_reusable() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let script = temp_dir.path().join("delayed_session.sh");
        let started_marker = temp_dir.path().join("started");
        let gate = temp_dir.path().join("allow_session_id");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
touch "{}"
while [ ! -f "{}" ]; do
  sleep 0.01
done
printf '{{"type":"system","session_id":"claude-reused"}}\n'
while IFS= read -r line; do
  printf '{{"type":"result","result":"ok"}}\n'
done
"#,
                started_marker.display(),
                gate.display()
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "claude".to_string(),
            ExecutorConfig {
                name: "claude".to_string(),
                protocol: crate::config::ExecutorProtocol::ClaudeStreamJson,
                command: "sh".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(temp_dir.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(ClaudeStreamJsonManager::new(executors));
        let cancel = TurnCancellation::new();
        let prepare_manager = manager.clone();
        let prepare_cancel = cancel.clone();
        let prepare_task = tokio::spawn(async move {
            prepare_manager
                .prepare(
                    ExecutorPrepareRequest {
                        turn: ExecutorTurnRef {
                            session_key: "session-1".to_string(),
                            executor: "claude".to_string(),
                            generation: 1,
                        },
                        cwd: None,
                        previous_session_id: None,
                    },
                    prepare_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if manager
                .existing_session("session-1", "claude")
                .await
                .is_ok()
                && started_marker.exists()
            {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        assert!(started_marker.exists());
        let session = manager
            .existing_session("session-1", "claude")
            .await
            .unwrap();
        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);

        let err = tokio::time::timeout(Duration::from_secs(2), prepare_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Claude stream-json prepare cancelled")
        );
        assert!(session.lock().await.is_alive());

        std::fs::write(&gate, "go").unwrap();
        let prepared = manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: ExecutorTurnRef {
                        session_key: "session-1".to_string(),
                        executor: "claude".to_string(),
                        generation: 2,
                    },
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();
        let reused = manager
            .existing_session("session-1", "claude")
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&session, &reused));
        assert_eq!(
            prepared.external_session_id.as_deref(),
            Some("claude-reused")
        );
        assert!(prepared.started_new_session);
    }
}

#[cfg(test)]
mod approval_bridge_tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn control_request_builds_approval_request() {
        let request = serde_json::json!({
            "subtype": "can_use_tool",
            "tool": "bash",
            "description": "Run `cargo test` in /workspace"
        });
        let approval_req = build_approval_request(
            "session-1",
            "claude-local",
            Some("U123".to_string()),
            "req-42".to_string(),
            Some(request),
        );
        assert_eq!(approval_req.session_key, "session-1");
        assert_eq!(approval_req.executor, "claude-local");
        assert_eq!(approval_req.requester_user_id, Some("U123".to_string()));
        assert_eq!(approval_req.title, "Claude: bash");
        assert_eq!(approval_req.body, "Run `cargo test` in /workspace");
        assert_eq!(approval_req.options.len(), 2);
        assert_eq!(approval_req.options[0].id, "allow");
        assert_eq!(approval_req.options[0].name, "Allow");
        assert_eq!(approval_req.options[1].id, "deny");
        assert_eq!(approval_req.options[1].name, "Deny");
    }

    #[tokio::test]
    async fn approval_request_is_cancelled_on_close() {
        let cfg = ExecutorConfig {
            name: "claude-test".to_string(),
            protocol: crate::config::ExecutorProtocol::ClaudeStreamJson,
            command: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                r#"read line; printf '{"type":"control_request","request_id":"req-1","request":{"subtype":"can_use_tool","tool":"bash"}}\n'; cat"#
                    .to_string(),
            ],
            cwd: None,
            env: BTreeMap::new(),
        };
        let approvals = Arc::new(crate::approval::ApprovalBroker::default());
        let mut prompts = approvals.subscribe();
        let session = ClaudeSession::start(
            cfg,
            PathBuf::from("."),
            "test-key".to_string(),
            "claude".to_string(),
            approvals.clone(),
            None,
        )
        .await
        .expect("start session");

        session
            .send_prompt("trigger")
            .await
            .expect("send trigger prompt");

        let prompt = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .expect("receive prompt within timeout")
            .expect("prompt channel open");
        assert_eq!(prompt.session_key, "test-key");
        assert!(approvals.has_pending_for_session("test-key").await);

        session.close().await;

        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while approvals.has_pending_for_session("test-key").await
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!approvals.has_pending_for_session("test-key").await);
    }
}
