# Claude Code Executor Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a native `claude_stream_json` executor backend to agent-router so users can configure Claude Code CLI as an agent and switch to it with `/agent claude`.

**Architecture:** Implement a new `ExecutorBackend` protocol adapter (`src/executor/claude_stream_json.rs`) that spawns a long-running `claude` child process, speaks its stream-json stdio protocol, and maps events/permissions into agent-router's existing executor abstractions. The adapter is registered alongside ACP and Codex app-server in `src/executor/registry.rs`.

**Tech Stack:** Rust, Tokio process/stdio, serde_json, Claude Code CLI stream-json protocol.

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/config.rs` | Add `ClaudeStreamJson` to `ExecutorProtocol`; parse `protocol: claude_stream_json`; default `command` to `"claude"`. |
| `src/executor/claude_stream_json.rs` | New module: `ClaudeStreamJsonManager`, `ClaudeSession`, stream-json event types/parser, permission bridge. |
| `src/executor/mod.rs` | Re-export the new module. |
| `src/executor/registry.rs` | Instantiate `ClaudeStreamJsonManager` and route `ExecutorProtocol::ClaudeStreamJson` to it. |
| `src/executor/claude_stream_json_test.rs` or inline `#[cfg(test)]` | Unit tests for event parsing and a fake Claude process test. |
| `config/agent-router.example.yaml` | Add a commented `claude` executor example. |

---

## Task 1: Extend Configuration for `claude_stream_json`

**Files:**
- Modify: `src/config.rs`
- Test: `src/config.rs` (existing tests)

- [ ] **Step 1: Add the new protocol variant**

  Locate `pub enum ExecutorProtocol` and add the variant:

  ```rust
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub enum ExecutorProtocol {
      Acp,
      AppServer,
      ClaudeStreamJson,
  }
  ```

- [ ] **Step 2: Parse `claude_stream_json` in config loader**

  In `parse_executor_config`, update the protocol match:

  ```rust
  let protocol = match raw.protocol.as_deref().unwrap_or("acp") {
      "acp" => ExecutorProtocol::Acp,
      "app_server" | "codex_app_server" => ExecutorProtocol::AppServer,
      "claude_stream_json" => ExecutorProtocol::ClaudeStreamJson,
      other => anyhow::bail!("executors.{name}.protocol `{other}` is not supported in MVP"),
  };
  ```

- [ ] **Step 3: Default the command to `claude`**

  Update the command defaulting logic so `claude_stream_json` executors do not require an explicit `command`:

  ```rust
  let command = raw
      .command
      .filter(|value| !value.trim().is_empty())
      .or_else(|| (protocol == ExecutorProtocol::AppServer).then(|| "codex".to_string()))
      .or_else(|| (protocol == ExecutorProtocol::ClaudeStreamJson).then(|| "claude".to_string()))
      .ok_or_else(|| anyhow::anyhow!("executors.{name}.command is required"))?;
  ```

  Leave `args` defaulting to `Vec::new()` for this protocol.

- [ ] **Step 4: Add a config parsing test**

  Append to the existing `#[cfg(test)] mod tests`:

  ```rust
  #[test]
  fn parses_claude_stream_json_executor_config() {
      let raw = r#"
router:
  default_executor: claude
executors:
  claude:
    protocol: claude_stream_json
    env:
      ANTHROPIC_API_KEY: secret
"#;
      let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
      let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();
      let claude = cfg.executors.get("claude").unwrap();

      assert_eq!(cfg.router.default_executor, "claude");
      assert_eq!(claude.protocol, ExecutorProtocol::ClaudeStreamJson);
      assert_eq!(claude.command, "claude");
      assert!(claude.args.is_empty());
      assert_eq!(claude.env.get("ANTHROPIC_API_KEY").unwrap(), "secret");
  }
  ```

- [ ] **Step 5: Run config tests**

  ```bash
  cargo test --lib config::tests
  ```

  Expected: all existing + new tests pass.

- [ ] **Step 6: Commit**

  ```bash
  git add src/config.rs
  git commit -m "config: add claude_stream_json executor protocol"
  ```

---

## Task 2: Create Stream-JSON Event Types and Parser

**Files:**
- Create: `src/executor/claude_stream_json.rs` (start with event types)
- Test: inline `#[cfg(test)]` in the same file

- [ ] **Step 1: Define event structs/enums**

  At the top of `src/executor/claude_stream_json.rs`:

  ```rust
  use serde::{Deserialize, Serialize};
  use serde_json::Value;

  #[derive(Debug, Clone, Deserialize)]
  #[serde(rename_all = "snake_case", tag = "type")]
  pub enum ClaudeEvent {
      System {
          #[serde(default)]
          session_id: Option<String>,
          #[serde(default)]
          model: Option<String>,
      },
      Assistant {
          message: AssistantMessage,
      },
      User {
          message: UserMessage,
      },
      Result {
          #[serde(default)]
          result: Option<String>,
          #[serde(default)]
          subtype: Option<String>,
          #[serde(default)]
          session_id: Option<String>,
          #[serde(default)]
          usage: Option<Value>,
      },
      ControlRequest {
          request_id: String,
          request: Option<Value>,
      },
      ControlCancelRequest {
          request_id: String,
      },
  }

  #[derive(Debug, Clone, Deserialize)]
  pub struct AssistantMessage {
      #[serde(default)]
      pub content: Vec<AssistantContent>,
      #[serde(default)]
      pub usage: Option<Value>,
  }

  #[derive(Debug, Clone, Deserialize)]
  #[serde(rename_all = "snake_case", tag = "type")]
  pub enum AssistantContent {
      Text { text: String },
      Thinking { thinking: String },
      ToolUse { name: String, input: Value },
  }

  #[derive(Debug, Clone, Deserialize)]
  pub struct UserMessage {
      #[serde(default)]
      pub content: Vec<UserContent>,
  }

  #[derive(Debug, Clone, Deserialize)]
  #[serde(rename_all = "snake_case", tag = "type")]
  pub enum UserContent {
      ToolResult { content: Value, is_error: Option<bool> },
      Text { text: String },
  }
  ```

- [ ] **Step 2: Add line parsing helper**

  ```rust
  pub fn parse_event_line(line: &str) -> Option<ClaudeEvent> {
      if line.trim().is_empty() {
          return None;
      }
      match serde_json::from_str::<ClaudeEvent>(line) {
          Ok(event) => Some(event),
          Err(err) => {
              tracing::debug!(target: "agent_router::claude", line, error = %err, "ignoring non-event JSON line");
              None
          }
      }
  }

  pub fn is_compaction_result(subtype: Option<&str>) -> bool {
      matches!(subtype, Some("compact" | "compaction"))
  }
  ```

- [ ] **Step 3: Write parser unit tests**

  ```rust
  #[cfg(test)]
  mod event_tests {
      use super::*;

      #[test]
      fn parses_system_event_with_session_id() {
          let line = r#"{"type":"system","session_id":"sess-123","model":"claude-opus-4-7"}"#;
          let event = parse_event_line(line).unwrap();
          match event {
              ClaudeEvent::System { session_id, model } => {
                  assert_eq!(session_id.as_deref(), Some("sess-123"));
                  assert_eq!(model.as_deref(), Some("claude-opus-4-7"));
              }
              other => panic!("unexpected event: {other:?}"),
          }
      }

      #[test]
      fn parses_assistant_text_and_thinking() {
          let line = r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"let me see"},{ "type":"text","text":"hello"}]}}"#;
          let event = parse_event_line(line).unwrap();
          match event {
              ClaudeEvent::Assistant { message } => {
                  assert_eq!(message.content.len(), 2);
              }
              other => panic!("unexpected event: {other:?}"),
          }
      }

      #[test]
      fn recognizes_compaction_result() {
          let line = r#"{"type":"result","subtype":"compact","result":""}"#;
          let event = parse_event_line(line).unwrap();
          match event {
              ClaudeEvent::Result { subtype, .. } => {
                  assert!(is_compaction_result(subtype.as_deref()));
              }
              other => panic!("unexpected event: {other:?}"),
          }
      }

      #[test]
      fn ignores_non_json_line() {
          assert!(parse_event_line("some verbose log").is_none());
      }
  }
  ```

- [ ] **Step 4: Run event parser tests**

  ```bash
  cargo test --lib executor::claude_stream_json::event_tests
  ```

  Expected: tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add src/executor/claude_stream_json.rs
  git commit -m "executor: add Claude stream-json event types and parser"
  ```

---

## Task 3: Implement Permission Request/Response Serialization

**Files:**
- Modify: `src/executor/claude_stream_json.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Define outgoing message types**

  Append to the module:

  ```rust
  #[derive(Debug, Clone, Serialize)]
  struct UserEvent {
      r#type: &'static str,
      message: UserEventMessage,
  }

  #[derive(Debug, Clone, Serialize)]
  struct UserEventMessage {
      role: &'static str,
      content: String,
  }

  #[derive(Debug, Clone, Serialize)]
  struct ControlResponse {
      r#type: &'static str,
      response: ControlResponseInner,
  }

  #[derive(Debug, Clone, Serialize)]
  struct ControlResponseInner {
      subtype: &'static str,
      request_id: String,
      response: PermissionDecision,
  }

  #[derive(Debug, Clone, Serialize)]
  #[serde(rename_all = "lowercase")]
  enum PermissionDecision {
      Allow {
          #[serde(rename = "updatedInput")]
          updated_input: serde_json::Map<String, Value>,
      },
      Deny {
          message: String,
      },
  }
  ```

- [ ] **Step 2: Add builder functions**

  ```rust
  impl UserEvent {
      fn new(prompt: String) -> Self {
          Self {
              r#type: "user",
              message: UserEventMessage { role: "user", content: prompt },
          }
      }
  }

  impl ControlResponse {
      fn allow(request_id: String) -> Self {
          Self {
              r#type: "control_response",
              response: ControlResponseInner {
                  subtype: "success",
                  request_id,
                  response: PermissionDecision::Allow {
                      updated_input: serde_json::Map::new(),
                  },
              },
          }
      }

      fn deny(request_id: String, message: impl Into<String>) -> Self {
          Self {
              r#type: "control_response",
              response: ControlResponseInner {
                  subtype: "success",
                  request_id,
                  response: PermissionDecision::Deny { message: message.into() },
              },
          }
      }
  }
  ```

- [ ] **Step 3: Add serialization tests**

  ```rust
  #[test]
  fn user_event_serializes() {
      let ev = UserEvent::new("hi".to_string());
      let json = serde_json::to_string(&ev).unwrap();
      assert!(json.contains("\"type\":\"user\""));
      assert!(json.contains("\"content\":\"hi\""));
  }

  #[test]
  fn control_response_allow_serializes() {
      let resp = ControlResponse::allow("req-1".to_string());
      let json = serde_json::to_string(&resp).unwrap();
      assert!(json.contains("\"type\":\"control_response\""));
      assert!(json.contains("\"behavior\":\"allow\""));
  }
  ```

- [ ] **Step 4: Run tests**

  ```bash
  cargo test --lib executor::claude_stream_json
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add src/executor/claude_stream_json.rs
  git commit -m "executor: add outgoing Claude stream-json messages"
  ```

---

## Task 4: Implement `ClaudeSession` Process Lifecycle

**Files:**
- Modify: `src/executor/claude_stream_json.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Add required imports and session struct**

  At the top of the file (combine with existing imports):

  ```rust
  use std::{
      collections::HashMap,
      path::{Path, PathBuf},
      process::Stdio,
      sync::{
          atomic::{AtomicBool, Ordering},
          Arc,
      },
  };

  use anyhow::Context as _;
  use serde_json::Value;
  use tokio::{
      io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
      process::{Child, ChildStdin, Command},
      sync::{Mutex, broadcast, mpsc, oneshot},
  };
  ```

  Define the session struct:

  ```rust
  type SharedStdin = Arc<Mutex<ChildStdin>>;

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
      alive: AtomicBool,
      _shutdown_tx: oneshot::Sender<()>,
  }
  ```

- [ ] **Step 2: Implement spawn logic**

  ```rust
  impl ClaudeSession {
      pub async fn start(
          cfg: ExecutorConfig,
          cwd: PathBuf,
          session_key: String,
          executor: String,
          approvals: SharedApprovalBroker,
          previous_session_id: Option<String>,
      ) -> anyhow::Result<Self> {
          let mut args = cfg.args.clone();
          args.extend_from_slice(&[
              "--output-format".to_string(),
              "stream-json".to_string(),
              "--input-format".to_string(),
              "stream-json".to_string(),
              "--permission-prompt-tool".to_string(),
              "stdio".to_string(),
              "--replay-user-messages".to_string(),
          ]);

          if let Some(id) = previous_session_id.filter(|id| !id.is_empty()) {
              args.push("--resume".to_string());
              args.push(id);
          }

          let mut cmd = Command::new(&cfg.command);
          cmd.args(&args)
              .current_dir(&cwd)
              .kill_on_drop(true)
              .stdin(Stdio::piped())
              .stdout(Stdio::piped())
              .stderr(Stdio::piped());

          for (key, value) in &cfg.env {
              cmd.env(key, value);
          }

          let mut child = cmd
              .spawn()
              .with_context(|| format!("could not start Claude command `{}`", cfg.command))?;

          let stdin = child.stdin.take().context("Claude process did not expose stdin")?;
          let stdout = child.stdout.take().context("Claude process did not expose stdout")?;
          let stderr = child.stderr.take();

          let stdin = Arc::new(Mutex::new(stdin));
          let child = Arc::new(Mutex::new(child));
          let (updates, _) = broadcast::channel(256);
          let session_id = Arc::new(Mutex::new(None::<String>));
          let active_user_id = Arc::new(Mutex::new(None::<String>));
          let (shutdown_tx, shutdown_rx) = oneshot::channel();

          let session = Self {
              cfg: cfg.clone(),
              cwd,
              stdin: stdin.clone(),
              child: child.clone(),
              updates: updates.clone(),
              session_id: session_id.clone(),
              active_user_id: active_user_id.clone(),
              approvals: approvals.clone(),
              session_key: session_key.clone(),
              executor: executor.clone(),
              alive: AtomicBool::new(true),
              _shutdown_tx: shutdown_tx,
          };

          tokio::spawn(read_stdout(
              BufReader::new(stdout),
              stderr,
              updates,
              session_id,
              active_user_id,
              approvals,
              session_key,
              executor,
              stdin,
              child,
              shutdown_rx,
          ));

          Ok(session)
      }

      pub fn subscribe(&self) -> broadcast::Receiver<ExecutorUpdate> {
          self.updates.subscribe()
      }

      pub async fn session_id(&self) -> Option<String> {
          self.session_id.lock().await.clone()
      }

      pub fn is_alive(&self) -> bool {
          self.alive.load(Ordering::SeqCst)
      }
  }
  ```

- [ ] **Step 3: Implement stdout reader task**

  ```rust
  async fn read_stdout<R>(
      reader: BufReader<R>,
      stderr: Option<impl AsyncBufReadExt + Unpin>,
      updates: broadcast::Sender<ExecutorUpdate>,
      session_id: Arc<Mutex<Option<String>>>,
      active_user_id: Arc<Mutex<Option<String>>>,
      approvals: SharedApprovalBroker,
      session_key: String,
      executor: String,
      stdin: SharedStdin,
      child: Arc<Mutex<Child>>,
      mut shutdown: oneshot::Receiver<()>,
  ) where
      R: tokio::io::AsyncRead + Unpin,
  {
      if let Some(stderr) = stderr {
          tokio::spawn(async move {
              let mut lines = stderr.lines();
              while let Ok(Some(line)) = lines.next_line().await {
                  tracing::debug!(target: "agent_router::claude", stderr = %line);
              }
          });
      }

      let mut lines = reader.lines();
      loop {
          tokio::select! {
              line = lines.next_line() => {
                  match line {
                      Ok(Some(line)) => {
                          let user_id = active_user_id.lock().await.clone();
                          handle_event_line(
                              &line,
                              &updates,
                              &session_id,
                              &active_user_id,
                              &approvals,
                              &session_key,
                              &executor,
                              user_id,
                              &stdin,
                          )
                          .await;
                      }
                      Ok(None) => break,
                      Err(err) => {
                          tracing::warn!(target: "agent_router::claude", error = %err, "stdout read error");
                          break;
                      }
                  }
              }
              _ = &mut shutdown => break,
          }
      }

      let _ = child.lock().await.start_kill();
      tracing::info!(target: "agent_router::claude", "Claude stdout reader exited");
  }

  async fn handle_event_line(
      line: &str,
      updates: &broadcast::Sender<ExecutorUpdate>,
      session_id: &Arc<Mutex<Option<String>>>,
      _active_user_id: &Arc<Mutex<Option<String>>>,
      approvals: &SharedApprovalBroker,
      session_key: &str,
      executor: &str,
      user_id: Option<String>,
      stdin: &SharedStdin,
  ) {
      let Some(event) = parse_event_line(line) else {
          return;
      };

      match &event {
          ClaudeEvent::System { session_id: sid, .. } | ClaudeEvent::Result { session_id: sid, .. } => {
              if let Some(sid) = sid.clone() {
                  *session_id.lock().await = Some(sid);
              }
          }
          _ => {}
      }

      for update in event_to_updates(event) {
          let _ = updates.send(update);
      }
  }
  ```

- [ ] **Step 4: Add `event_to_updates` mapper (stub)**

  ```rust
  fn event_to_updates(event: ClaudeEvent) -> Vec<ExecutorUpdate> {
      match event {
          ClaudeEvent::Assistant { message } => message
              .content
              .into_iter()
              .filter_map(|content| match content {
                  AssistantContent::Text { text } if !text.is_empty() => {
                      Some(ExecutorUpdate::new("agent_message_chunk", "Assistant", text, ""))
                  }
                  AssistantContent::Thinking { thinking } if !thinking.is_empty() => {
                      Some(ExecutorUpdate::new("reasoning_summary", "Reasoning", thinking, ""))
                  }
                  AssistantContent::ToolUse { name, input } => Some(
                      ExecutorUpdate::new("tool_call", name.clone(), format_tool_input(name, input), "")
                          .with_channel_event(ExecutorChannelEvent::tool_call(name, "")),
                  ),
                  _ => None,
              })
              .collect(),
          ClaudeEvent::User { message } => message
              .content
              .into_iter()
              .filter_map(|content| match content {
                  UserContent::ToolResult { content, is_error } => Some(ExecutorUpdate::new(
                      "tool_result",
                      "Tool result",
                      format!("{}\n{}", if is_error.unwrap_or(false) { "status: error" } else { "status: ok" }, truncate_text(&json_to_text(content), 500)),
                      "",
                  )),
                  _ => None,
              })
              .collect(),
          ClaudeEvent::Result { result, .. } => {
              if let Some(text) = result {
                  vec![ExecutorUpdate::new("result", "Result", text, "")]
              } else {
                  vec![]
              }
          }
          _ => vec![],
      }
  }
  ```

  Add helper stubs for `format_tool_input`, `json_to_text`, and `truncate_text` as private functions in the same file.

- [ ] **Step 5: Add method to send a user prompt**

  ```rust
  impl ClaudeSession {
      pub async fn send_prompt(&self, prompt: &str) -> anyhow::Result<()> {
          let event = UserEvent::new(prompt.to_string());
          let mut data = serde_json::to_vec(&event)?;
          data.push(b'\n');
          let mut stdin = self.stdin.lock().await;
          stdin.write_all(&data).await?;
          stdin.flush().await?;
          Ok(())
      }
  }
  ```

- [ ] **Step 6: Add a fake process test**

  Create a helper script `tests/fake_claude.py` (or use an inline Rust fake). For the plan, use an inline shell script in a test helper:

  ```rust
  #[cfg(test)]
  mod session_tests {
      use super::*;

      #[tokio::test]
      async fn session_tracks_external_id_from_system_event() {
          let cfg = ExecutorConfig {
              name: "claude".to_string(),
              protocol: ExecutorProtocol::ClaudeStreamJson,
              command: "echo".to_string(),
              args: vec![r#"{"type":"system","session_id":"sess-abc"}"#.to_string()],
              cwd: None,
              env: Default::default(),
          };
          let session = ClaudeSession::start(
              cfg,
              std::env::temp_dir().into(),
              "test-session".to_string(),
              "claude".to_string(),
              Arc::new(crate::approval::ApprovalBroker::default()),
              None,
          )
          .await
          .expect("spawn");
          tokio::time::sleep(std::time::Duration::from_millis(50)).await;
          assert_eq!(session.session_id().await.as_deref(), Some("sess-abc"));
      }
  }
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add src/executor/claude_stream_json.rs
  git commit -m "executor: add ClaudeSession spawn and stdout reader"
  ```

---

## Task 5: Implement Turn Prompt Loop with Cancellation

**Files:**
- Modify: `src/executor/claude_stream_json.rs`

- [ ] **Step 1: Add prompt method that consumes until result**

  Extend `ClaudeSession` with:

  ```rust
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

      let mut updates = self.subscribe();
      let mut final_text = String::new();

      loop {
          tokio::select! {
              _ = cancel.cancelled() => {
                  self.close().await;
                  return ExecutorPromptOutcome::Cancelled;
              }
              update = updates.recv() => {
                  match update {
                      Ok(update) => {
                          if update.kind == "result" {
                              final_text = update.text;
                              break;
                          }
                          if let Err(err) = events.send(update).await {
                              return ExecutorPromptOutcome::Failed(err);
                          }
                      }
                      Err(broadcast::error::RecvError::Closed) => {
                          return ExecutorPromptOutcome::Failed(anyhow::anyhow!("Claude stdout closed unexpectedly"));
                      }
                      Err(broadcast::error::RecvError::Lagged(_)) => continue,
                  }
              }
          }
      }

      // Drain any trailing updates
      while let Ok(update) = updates.try_recv() {
          if let Err(err) = events.send(update).await {
              return ExecutorPromptOutcome::Failed(err);
          }
      }

      *self.active_user_id.lock().await = None;
      ExecutorPromptOutcome::Completed(ExecutorResponse { final_text })
  }

  pub async fn close(&self) {
      self.alive.store(false, Ordering::SeqCst);
      let mut stdin = self.stdin.lock().await;
      let _ = stdin.shutdown().await;
      let mut child = self.child.lock().await;
      let _ = child.start_kill();
  }
  ```

- [ ] **Step 2: Commit**

  ```bash
  git add src/executor/claude_stream_json.rs
  git commit -m "executor: add Claude turn loop with cancellation"
  ```

---

## Task 6: Implement `ClaudeStreamJsonManager`

**Files:**
- Modify: `src/executor/claude_stream_json.rs`
- Modify: `src/executor/mod.rs`

- [ ] **Step 1: Add the manager struct and constructor**

  In `src/executor/claude_stream_json.rs`:

  ```rust
  use crate::approval::{ApprovalBroker, SharedApprovalBroker};
  use crate::config::{ExecutorConfig, ExecutorProtocol};
  use crate::executor::{
      ExecutorBackend, ExecutorDescriptor, ExecutorEventSink, ExecutorInterruptRequest,
      ExecutorPrepareRequest, ExecutorPromptOutcome, ExecutorPromptRequest, ExecutorResponse,
      PreparedExecutor, TurnCancellation,
  };

  #[derive(Debug)]
  pub struct ClaudeStreamJsonManager {
      executors: BTreeMap<String, ExecutorConfig>,
      approvals: SharedApprovalBroker,
      sessions: Mutex<HashMap<(String, String), Arc<Mutex<ClaudeSession>>>>,
  }

  impl ClaudeStreamJsonManager {
      pub fn new(executors: BTreeMap<String, ExecutorConfig>) -> Self {
          Self::with_approvals(executors, Arc::new(ApprovalBroker::default()))
      }

      pub fn with_approvals(
          executors: BTreeMap<String, ExecutorConfig>,
          approvals: SharedApprovalBroker,
      ) -> Self {
          Self {
              executors: executors
                  .into_iter()
                  .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::ClaudeStreamJson)
                  .collect(),
              approvals,
              sessions: Mutex::new(HashMap::new()),
          }
      }
  }
  ```

- [ ] **Step 2: Implement `ExecutorBackend`**

  ```rust
  #[async_trait::async_trait]
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
              anyhow::bail!("Claude prepare cancelled");
          }
          let cfg = self.executors.get(&request.turn.executor).ok_or_else(|| {
              anyhow::anyhow!("executor `{}` is not configured", request.turn.executor)
          })?;

          let cwd = request.cwd.clone().or_else(|| cfg.cwd.clone()).unwrap_or_else(|| PathBuf::from("."));
          let key = (request.turn.session_key.clone(), request.turn.executor.clone());

          let mut sessions = self.sessions.lock().await;
          let session = if let Some(existing) = sessions.get(&key).cloned() {
              if existing.lock().await.is_alive() {
                  existing
              } else {
                  let new_session = Arc::new(Mutex::new(
                      ClaudeSession::start(
                          cfg.clone(),
                          cwd,
                          request.turn.session_key.clone(),
                          request.turn.executor.clone(),
                          self.approvals.clone(),
                          request.previous_session_id.clone(),
                      )
                      .await?,
                  ));
                  sessions.insert(key, new_session.clone());
                  new_session
              }
          } else {
              let new_session = Arc::new(Mutex::new(
                  ClaudeSession::start(
                      cfg.clone(),
                      cwd,
                      request.turn.session_key.clone(),
                      request.turn.executor.clone(),
                      self.approvals.clone(),
                      request.previous_session_id.clone(),
                  )
                  .await?,
              ));
              sessions.insert(key, new_session.clone());
              new_session
          };

          // Give the session a moment to report its id; if it already has one from resume, use it.
          let external_session_id = session.lock().await.session_id().await;
          let started_new_session = request.previous_session_id.is_none() || external_session_id.as_ref() != request.previous_session_id.as_ref();

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
          let key = (request.turn.session_key.clone(), request.turn.executor.clone());
          let session = match self.sessions.lock().await.get(&key).cloned() {
              Some(session) => session,
              None => return ExecutorPromptOutcome::Failed(anyhow::anyhow!("Claude session not prepared")),
          };

          let mut session = session.lock().await;
          session
              .run_turn(&request.prompt, request.user_id, events, cancel)
              .await
      }

      async fn interrupt(&self, request: ExecutorInterruptRequest) -> anyhow::Result<()> {
          let key = (request.turn.session_key.clone(), request.turn.executor.clone());
          if let Some(session) = self.sessions.lock().await.get(&key).cloned() {
              session.lock().await.close().await;
          }
          Ok(())
      }
  }
  ```

- [ ] **Step 3: Re-export the module**

  In `src/executor/mod.rs`, add:

  ```rust
  pub mod claude_stream_json;
  ```

  at the top alongside `pub mod acp;` and `pub mod codex_app_server;`.

- [ ] **Step 4: Commit**

  ```bash
  git add src/executor/mod.rs src/executor/claude_stream_json.rs
  git commit -m "executor: add ClaudeStreamJsonManager backend"
  ```

---

## Task 7: Wire the New Backend into the Registry

**Files:**
- Modify: `src/executor/registry.rs`

- [ ] **Step 1: Import the new manager**

  Update the `use` block:

  ```rust
  use crate::executor::{
      ExecutorBackend, ExecutorDescriptor, ExecutorEventSink, ExecutorInterruptRequest,
      ExecutorPrepareRequest, ExecutorPromptOutcome, ExecutorPromptRequest, PreparedExecutor,
      TurnCancellation, acp::AcpExecutorManager, claude_stream_json::ClaudeStreamJsonManager,
      codex_app_server::CodexAppServerManager,
  };
  ```

- [ ] **Step 2: Add manager field and construction**

  ```rust
  #[derive(Debug)]
  pub struct ExecutorRegistry {
      executors: BTreeMap<String, ExecutorConfig>,
      acp: AcpExecutorManager,
      codex_app_server: CodexAppServerManager,
      claude_stream_json: ClaudeStreamJsonManager,
  }

  impl ExecutorRegistry {
      pub fn new(
          executors: BTreeMap<String, ExecutorConfig>,
          approvals: SharedApprovalBroker,
      ) -> Self {
          Self {
              acp: AcpExecutorManager::with_approvals(executors.clone(), approvals.clone()),
              codex_app_server: CodexAppServerManager::with_approvals(executors.clone(), approvals.clone()),
              claude_stream_json: ClaudeStreamJsonManager::new(executors.clone()),
              executors,
          }
      }
  }
  ```

- [ ] **Step 3: Route to the new backend**

  In `backend_for`:

  ```rust
  fn backend_for(&self, executor: &str) -> anyhow::Result<&dyn ExecutorBackend> {
      let cfg = self
          .executors
          .get(executor)
          .ok_or_else(|| anyhow::anyhow!("executor `{executor}` is not configured"))?;
      match cfg.protocol {
          ExecutorProtocol::Acp => Ok(&self.acp),
          ExecutorProtocol::AppServer => Ok(&self.codex_app_server),
          ExecutorProtocol::ClaudeStreamJson => Ok(&self.claude_stream_json),
      }
  }
  ```

- [ ] **Step 4: Include in `list()`**

  Update `list` to also collect from `claude_stream_json`:

  ```rust
  fn list(&self) -> Vec<ExecutorDescriptor> {
      let mut executors = self.acp.list();
      executors.extend(self.codex_app_server.list());
      executors.extend(self.claude_stream_json.list());
      executors.sort_by(|left, right| left.name.cmp(&right.name));
      executors
  }
  ```

- [ ] **Step 5: Add a registry test**

  In `src/executor/registry.rs` tests, add a third executor to `mixed_executor_config`:

  ```rust
  (
      "claude".to_string(),
      ExecutorConfig {
          name: "claude".to_string(),
          protocol: ExecutorProtocol::ClaudeStreamJson,
          command: "claude".to_string(),
          args: Vec::new(),
          cwd: None,
          env: BTreeMap::new(),
      },
  ),
  ```

  Update `lists_mixed_executor_protocols` assertion:

  ```rust
  #[test]
  fn lists_mixed_executor_protocols() {
      let registry =
          ExecutorRegistry::new(mixed_executor_config(), Arc::new(ApprovalBroker::default()));

      let executors = registry
          .list()
          .into_iter()
          .map(|executor| (executor.name, executor.protocol))
          .collect::<Vec<_>>();

      assert_eq!(
          executors,
          [
              ("claude".to_string(), "claude_stream_json".to_string()),
              ("codex".to_string(), "app_server".to_string()),
              ("kimi".to_string(), "acp".to_string()),
          ]
      );
  }
  ```

- [ ] **Step 6: Build and run registry tests**

  ```bash
  cargo test --lib executor::registry::tests
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add src/executor/registry.rs
  git commit -m "executor: wire ClaudeStreamJsonManager into registry"
  ```

---

## Task 8: Implement Permission Bridge

**Files:**
- Modify: `src/executor/claude_stream_json.rs`

- [ ] **Step 1: Import approval types**

  Ensure `src/executor/claude_stream_json.rs` imports:

  ```rust
  use crate::approval::{
      ApprovalBroker, ApprovalOption, ApprovalRequest, ApprovalSelection, SharedApprovalBroker,
  };
  ```

  `ClaudeStreamJsonManager` and `ClaudeSession` already carry `SharedApprovalBroker`, `session_key`, `executor`, and `active_user_id` from Tasks 4 and 6.

- [ ] **Step 2: Handle `control_request` events**

  In `handle_event_line`, when a `ControlRequest` is seen, spawn an async task (or use a bounded channel) to request approval and write the response:

  ```rust
  async fn handle_event_line(
      line: &str,
      updates: &broadcast::Sender<ExecutorUpdate>,
      session_id: &Arc<Mutex<Option<String>>>,
      approvals: &SharedApprovalBroker,
      session_key: &str,
      executor: &str,
      user_id: Option<String>,
      stdin: &SharedStdin,
  ) {
      // ... existing session_id update ...

      match event {
          ClaudeEvent::ControlRequest { request_id, request } => {
              let approval_req = build_approval_request(session_key, executor, user_id, request_id.clone(), request);
              let approvals = approvals.clone();
              let stdin = stdin.clone();
              let request_id = request_id.clone();
              tokio::spawn(async move {
                  let selection = approvals.request(approval_req).await;
                  let response = match selection {
                      ApprovalSelection::Selected(option_id) if option_id == "allow" => {
                          ControlResponse::allow(request_id)
                      }
                      _ => ControlResponse::deny(
                          request_id,
                          "The user denied this tool use. Stop and wait for instructions.",
                      ),
                  };
                  if let Ok(mut data) = serde_json::to_vec(&response) {
                      data.push(b'\n');
                      let mut stdin = stdin.lock().await;
                      let _ = stdin.write_all(&data).await;
                  }
              });
          }
          _ => {
              for update in event_to_updates(event) {
                  let _ = updates.send(update);
               }
          }
      }
  }
  ```

  Add `build_approval_request` helper that constructs `ApprovalRequest` with options `["allow", "deny"]` and a title/body derived from the Claude request payload:

  ```rust
  fn build_approval_request(
      session_key: &str,
      executor: &str,
      user_id: Option<String>,
      request_id: String,
      request: Option<Value>,
  ) -> ApprovalRequest {
      let tool_name = request
          .as_ref()
          .and_then(|r| r.get("tool"))
          .and_then(|v| v.as_str())
          .unwrap_or("tool");
      let body = request
          .and_then(|r| r.get("description"))
          .and_then(|v| v.as_str())
          .map(|s| s.to_string())
          .unwrap_or_else(|| format!("Claude requested approval for tool {tool_name}"));
      ApprovalRequest {
          session_key: session_key.to_string(),
          executor: executor.to_string(),
          requester_user_id: user_id,
          title: format!("Claude: {tool_name}"),
          body,
          options: vec![
              ApprovalOption { id: "allow".to_string(), label: "Allow".to_string() },
              ApprovalOption { id: "deny".to_string(), label: "Deny".to_string() },
          ],
      }
  }
  ```

  **MVP limitation:** Permission requests are not wired to the turn cancellation token. If the user cancels the turn, the Claude process is killed; any in-flight permission request is dropped.

  `active_user_id` is already set/cleared in `run_turn` (Task 5) and passed into `handle_event_line` via `read_stdout`.

- [ ] **Step 4: Commit**

  ```bash
  git add src/executor/claude_stream_json.rs
  git commit -m "executor: bridge Claude permission requests to approval broker"
  ```

---

## Task 9: Update Example Config and Documentation

**Files:**
- Modify: `config/agent-router.example.yaml`
- Modify: `docs/superpowers/specs/2026-06-24-claude-code-executor-design.md` (mark decisions if any)

- [ ] **Step 1: Add commented example executor**

  In `config/agent-router.example.yaml`, append to the `executors` block:

  ```yaml
  # Claude Code CLI via stream-json stdio protocol.
  # Requires `claude` installed and ANTHROPIC_API_KEY available.
  # claude:
  #   protocol: claude_stream_json
  #   command: claude
  #   args: []
  #   cwd: /data/project/hermes
  ```

- [ ] **Step 2: Update README mention (optional)**

  In `README.md`, under "Agent integrations", change:

  ```markdown
  - Kimi through ACP, for example `kimi acp`
  - Codex through app-server
  ```

  to:

  ```markdown
  - Kimi through ACP, for example `kimi acp`
  - Codex through app-server
  - Claude Code through stream-json stdio
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add config/agent-router.example.yaml README.md
  git commit -m "docs: add Claude Code executor example and README mention"
  ```

---

## Task 10: Integration Smoke Test

**Files:**
- Create: `tests/fake_claude.sh` (helper script)
- Modify: `src/executor/claude_stream_json.rs` (integration test)

- [ ] **Step 1: Create a fake Claude script**

  ```bash
  #!/usr/bin/env bash
  # tests/fake_claude.sh — emits a deterministic stream-json conversation.
  # Reads the first user line, ignores it, then prints canned events.
  read -r _first_line
  cat <<'JSON'
  {"type":"system","session_id":"fake-sid-123","model":"claude-fake"}
  {"type":"assistant","message":{"content":[{"type":"text","text":"Hello from fake Claude"}]}}
  {"type":"result","result":"Hello from fake Claude"}
  JSON
  ```

  Make it executable:

  ```bash
  chmod +x tests/fake_claude.sh
  ```

- [ ] **Step 2: Add an integration test**

  In `src/executor/claude_stream_json.rs` `#[cfg(test)]`:

  ```rust
  #[tokio::test]
  async fn fake_claude_turn_completes() {
      let script = std::env::current_dir()
          .unwrap()
          .join("tests/fake_claude.sh");
      let cfg = ExecutorConfig {
          name: "claude".to_string(),
          protocol: ExecutorProtocol::ClaudeStreamJson,
          command: script.to_string_lossy().to_string(),
          args: vec![],
          cwd: None,
          env: Default::default(),
      };
      let session = ClaudeSession::start(
          cfg,
          std::env::temp_dir(),
          "fake-session".to_string(),
          "claude".to_string(),
          Arc::new(crate::approval::ApprovalBroker::default()),
          None,
      )
      .await
      .expect("start fake claude");

      let mut sink = CollectingExecutorEventSink::default();
      let outcome = session
          .run_turn("hi", None, &mut sink, TurnCancellation::new())
          .await;

      assert!(matches!(outcome, ExecutorPromptOutcome::Completed(_)));
      if let ExecutorPromptOutcome::Completed(response) = outcome {
          assert_eq!(response.final_text, "Hello from fake Claude");
      }
  }
  ```

  Ensure `CollectingExecutorEventSink` from `executor::test_support` is used.

- [ ] **Step 3: Run integration test**

  ```bash
  cargo test --lib executor::claude_stream_json::session_tests::fake_claude_turn_completes
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add tests/fake_claude.sh src/executor/claude_stream_json.rs
  git commit -m "test: add fake Claude integration smoke test"
  ```

---

## Task 11: Final Build, Clippy, and Test Run

**Files:**
- All modified files

- [ ] **Step 1: Full test suite**

  ```bash
  cargo test
  ```

  Expected: all tests pass.

- [ ] **Step 2: Clippy**

  ```bash
  cargo clippy --all-targets -- -D warnings
  ```

  Fix any warnings.

- [ ] **Step 3: Format**

  ```bash
  cargo fmt
  ```

- [ ] **Step 4: Commit formatting fixes**

  ```bash
  git add -A
  git commit -m "style: cargo fmt and clippy fixes for Claude executor"
  ```

---

## Self-Review Checklist

- [ ] **Spec coverage:** Every section of `2026-06-24-claude-code-executor-design.md` has a corresponding task.
  - Config protocol: Task 1
  - Manager + session: Tasks 4–6
  - Event mapping: Tasks 2–3
  - Permission bridge: Task 8
  - Registry wiring: Task 7
  - Testing: Tasks 2, 4, 7, 10
  - Docs/config: Task 9
- [ ] **Placeholder scan:** No "TBD", "TODO", "implement later", or vague "handle edge cases" steps remain.
- [ ] **Type consistency:** `ExecutorProtocol::ClaudeStreamJson` is used consistently across config, manager, and registry.
- [ ] **Test coverage:** Unit tests for parser/serialization, registry list test, fake process integration test.
- [ ] **Known limitation documented:** Permission cancellation integration (TurnCancellation bridging) is simplified in MVP; refine in follow-up if needed.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-24-claude-code-executor.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using `executing-plans`, batch execution with checkpoints.

**Which approach?**
