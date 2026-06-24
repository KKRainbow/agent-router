use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    approval::{
        ApprovalBroker, ApprovalPolicy, ApprovalRequest, ApprovalSelection, SharedApprovalBroker,
    },
    executor::{
        ExecutorBackend, ExecutorChannelEventKind, ExecutorEventSink, ExecutorPrepareRequest,
        ExecutorPromptRequest, ExecutorUpdate,
    },
    session::{
        ApprovalMode, ExecutorBinding, ExecutorHealth, SessionState, TranscriptMessage,
        projection::{
            ProjectionInput, build_context_projection, merge_seen_context,
            projected_assistant_content, visible_message_fingerprints,
        },
        store::SessionStore,
    },
    text::truncate_chars,
};

#[derive(Debug, Clone)]
pub struct RouterInput {
    pub session_key: String,
    pub text: String,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterOutputEvent {
    Channel(RouterChannelEvent),
    FinalReply(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterChannelEvent {
    pub kind: RouterChannelEventKind,
    pub executor: String,
    pub title: String,
    pub text: String,
}

impl RouterChannelEvent {
    pub fn render_text(&self) -> String {
        let heading = match self.kind {
            RouterChannelEventKind::ReasoningSummary => "Reasoning summary".to_string(),
            RouterChannelEventKind::ToolCall if self.title.trim().is_empty() => {
                "Tool call".to_string()
            }
            RouterChannelEventKind::ToolCall => format!("Tool call: {}", self.title.trim()),
        };
        format!("[{}] {heading}\n{}", self.executor, self.text)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterChannelEventKind {
    ReasoningSummary,
    ToolCall,
}

#[async_trait]
pub trait RouterOutputSink: Send {
    fn send_channel_event(&mut self, event: RouterChannelEvent);
    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()>;
}

#[async_trait]
pub trait RouterService: Send + Sync + 'static {
    async fn handle(
        &self,
        input: RouterInput,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()>;
}

pub struct SessionApprovalPolicy<S>
where
    S: SessionStore,
{
    default_executor: String,
    default_mode: ApprovalMode,
    store: Arc<S>,
}

impl<S> SessionApprovalPolicy<S>
where
    S: SessionStore,
{
    pub fn new(
        default_executor: impl Into<String>,
        default_mode: ApprovalMode,
        store: Arc<S>,
    ) -> Self {
        Self {
            default_executor: default_executor.into(),
            default_mode,
            store,
        }
    }
}

#[async_trait]
impl<S> ApprovalPolicy for SessionApprovalPolicy<S>
where
    S: SessionStore,
{
    async fn auto_selection(&self, request: &ApprovalRequest) -> Option<ApprovalSelection> {
        let state = self
            .store
            .load_or_create(&request.session_key, &self.default_executor)
            .await;
        let effective_mode = state.approval_mode_override.unwrap_or(self.default_mode);
        if effective_mode != ApprovalMode::Yolo {
            return None;
        }
        let option_id = request.allow_once_option_id()?;
        tracing::info!(
            session_key = %request.session_key,
            executor = %request.executor,
            option_id = %option_id,
            "auto-approving request in YOLO mode"
        );
        Some(ApprovalSelection::Selected(option_id))
    }
}

pub struct AgentRouter<S, E>
where
    S: SessionStore,
    E: ExecutorBackend,
{
    default_executor: String,
    default_approval_mode: ApprovalMode,
    store: Arc<S>,
    executor: Arc<E>,
    approvals: SharedApprovalBroker,
    workspace_root: Option<PathBuf>,
    session_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl<S, E> AgentRouter<S, E>
where
    S: SessionStore,
    E: ExecutorBackend,
{
    pub fn new(default_executor: impl Into<String>, store: Arc<S>, executor: Arc<E>) -> Self {
        Self::with_approvals(
            default_executor,
            store,
            executor,
            Arc::new(ApprovalBroker::default()),
        )
    }

    pub fn with_approvals(
        default_executor: impl Into<String>,
        store: Arc<S>,
        executor: Arc<E>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self::with_approval_mode(
            default_executor,
            ApprovalMode::Normal,
            store,
            executor,
            approvals,
        )
    }

    pub fn with_approval_mode(
        default_executor: impl Into<String>,
        default_approval_mode: ApprovalMode,
        store: Arc<S>,
        executor: Arc<E>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self {
            default_executor: default_executor.into(),
            default_approval_mode,
            store,
            executor,
            approvals,
            workspace_root: None,
            session_locks: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_workspace_root(mut self, workspace_root: Option<PathBuf>) -> Self {
        self.workspace_root = workspace_root;
        self
    }

    async fn session_lock(&self, session_key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.session_locks.lock().await;
        locks
            .entry(session_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn handle_locked(
        &self,
        input: RouterInput,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let text = input.text.trim();
        let command = text.split_whitespace().next().unwrap_or("");
        if command == "/agent" {
            return self
                .handle_agent_command(&input.session_key, text, output)
                .await;
        }
        if command == "/yolo" {
            return self
                .handle_yolo_command(&input.session_key, text, output)
                .await;
        }
        self.route_to_active_executor(input, output).await
    }

    async fn handle_agent_command(
        &self,
        session_key: &str,
        text: &str,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let args = text.trim_start_matches("/agent").trim();
        let mut state = self
            .store
            .load_or_create(session_key, &self.default_executor)
            .await;
        if args.is_empty() || args == "status" {
            return output.send_final_reply(self.render_status(&state)).await;
        }
        if args.split_whitespace().count() != 1 {
            return output
                .send_final_reply("Usage: /agent [status|done|<executor>]".to_string())
                .await;
        }

        let target = args;
        if target == "done" {
            state.active_executor = state.default_executor.clone();
            self.store.save(state.clone()).await;
            return output
                .send_final_reply(format!(
                    "Agent handoff ended. Active executor: {}",
                    state.active_executor
                ))
                .await;
        }

        if self.executor.get(target).is_none() {
            return output
                .send_final_reply(format!("Executor `{target}` is not configured."))
                .await;
        }
        state.active_executor = target.to_string();
        self.store.save(state).await;
        output
            .send_final_reply(format!("Active executor: {target}"))
            .await
    }

    async fn handle_yolo_command(
        &self,
        session_key: &str,
        text: &str,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let args = text.trim_start_matches("/yolo").trim();
        let mut state = self
            .store
            .load_or_create(session_key, &self.default_executor)
            .await;
        if args.is_empty() || args == "status" {
            return output
                .send_final_reply(self.render_yolo_status(&state))
                .await;
        }
        if args.split_whitespace().count() != 1 {
            return output
                .send_final_reply("Usage: /yolo [status|on|off|inherit]".to_string())
                .await;
        }

        match args {
            "on" => {
                state.approval_mode_override = Some(ApprovalMode::Yolo);
                self.store.save(state.clone()).await;
                output
                    .send_final_reply(self.render_yolo_status_with_prefix(
                        "YOLO mode enabled for this session.",
                        &state,
                    ))
                    .await
            }
            "off" => {
                state.approval_mode_override = Some(ApprovalMode::Normal);
                self.store.save(state.clone()).await;
                output
                    .send_final_reply(self.render_yolo_status_with_prefix(
                        "YOLO mode disabled for this session.",
                        &state,
                    ))
                    .await
            }
            "inherit" => {
                state.approval_mode_override = None;
                self.store.save(state.clone()).await;
                output
                    .send_final_reply(self.render_yolo_status_with_prefix(
                        "YOLO mode now inherits the global default.",
                        &state,
                    ))
                    .await
            }
            _ => {
                output
                    .send_final_reply("Usage: /yolo [status|on|off|inherit]".to_string())
                    .await
            }
        }
    }

    async fn route_to_active_executor(
        &self,
        input: RouterInput,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let mut state = self
            .store
            .load_or_create(&input.session_key, &self.default_executor)
            .await;
        let executor_name = state.active_executor.clone();
        let descriptor = self.executor.get(&executor_name).ok_or_else(|| {
            anyhow::anyhow!("active executor `{executor_name}` is not configured")
        })?;
        let session_cwd = self.ensure_session_cwd(&mut state)?;
        if session_cwd.is_some() {
            self.store.save(state.clone()).await;
        }
        tracing::info!(
            session_key = %input.session_key,
            executor = %executor_name,
            cwd = session_cwd
                .as_deref()
                .map(|cwd| cwd.display().to_string())
                .unwrap_or_else(|| "executor-default".to_string()),
            text_len = input.text.len(),
            "routing turn to active executor"
        );

        let binding = state.binding_for(&executor_name);
        let prepared = self
            .executor
            .prepare(ExecutorPrepareRequest {
                session_key: input.session_key.clone(),
                executor: executor_name.clone(),
                cwd: session_cwd.clone(),
                previous_session_id: binding.external_session_id.clone(),
            })
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                state.executor_bindings.insert(
                    executor_name.clone(),
                    ExecutorBinding {
                        protocol: descriptor.protocol.clone(),
                        health: ExecutorHealth::Unhealthy,
                        ..binding_with_session_cwd(binding, session_cwd.as_deref())
                    },
                );
                self.store.save(state).await;
                return Err(err);
            }
        };
        let projection = build_context_projection(ProjectionInput {
            transcript: &state.transcript,
            seen_context: &binding.seen_context,
            current_message: &input.text,
            started_new_session: prepared.started_new_session,
            max_messages: 40,
        });

        let (response, updates) = {
            let mut executor_events = RouterExecutorEventSink::new(&executor_name, output);
            let response = self
                .executor
                .prompt(
                    ExecutorPromptRequest {
                        session_key: input.session_key.clone(),
                        executor: executor_name.clone(),
                        prompt: projection.prompt,
                        user_id: input.user_id.clone(),
                    },
                    &mut executor_events,
                )
                .await;
            (response, executor_events.into_updates())
        };

        match response {
            Ok(response) => {
                let activity_summaries = updates
                    .iter()
                    .filter_map(|update| update.summary(700))
                    .collect::<Vec<_>>();
                let assistant_content = projected_assistant_content(
                    &executor_name,
                    &response.final_text,
                    &activity_summaries,
                );
                let user_entry = TranscriptMessage::user(input.text);
                let assistant_entry = TranscriptMessage::assistant(
                    assistant_content,
                    executor_name.clone(),
                    prepared.external_session_id.clone(),
                );
                let new_fingerprints =
                    visible_message_fingerprints(&[user_entry.clone(), assistant_entry.clone()])
                        .into_iter()
                        .map(|(_, fingerprint)| fingerprint)
                        .collect::<Vec<_>>();

                state.transcript.push(user_entry);
                state.transcript.push(assistant_entry);
                state.executor_bindings.insert(
                    executor_name.clone(),
                    update_binding_after_success(
                        binding,
                        prepared.external_session_id,
                        descriptor.protocol,
                        session_cwd.as_deref(),
                        projection.acknowledged_fingerprints,
                        new_fingerprints,
                    ),
                );
                self.store.save(state).await;
                tracing::info!(
                    session_key = %input.session_key,
                    executor = %executor_name,
                    final_text_len = response.final_text.len(),
                    "committed successful router turn"
                );
                output.send_final_reply(response.final_text).await
            }
            Err(err) => {
                state.executor_bindings.insert(
                    executor_name.clone(),
                    update_binding_after_prompt_failure(
                        binding,
                        prepared.external_session_id,
                        descriptor.protocol,
                        session_cwd.as_deref(),
                    ),
                );
                self.store.save(state).await;
                tracing::warn!(
                    error = %err,
                    session_key = %input.session_key,
                    executor = %executor_name,
                    "router turn failed"
                );
                Err(err)
            }
        }
    }

    fn render_status(&self, state: &SessionState) -> String {
        let mut lines = vec![
            format!("Default executor: {}", state.default_executor),
            format!("Active executor: {}", state.active_executor),
        ];
        if let Some(cwd) = &state.cwd {
            lines.push(format!("Session cwd: {}", cwd.display()));
        }
        lines.push("Executors:".to_string());
        for descriptor in self.executor.list() {
            let binding = state.executor_bindings.get(&descriptor.name);
            let suffix = binding
                .and_then(|binding| binding.external_session_id.as_ref())
                .map(|session_id| format!(", session {session_id}"))
                .unwrap_or_default();
            lines.push(format!(
                "- {}: {}{}",
                descriptor.name, descriptor.protocol, suffix
            ));
        }
        lines.join("\n")
    }

    fn ensure_session_cwd(&self, state: &mut SessionState) -> anyhow::Result<Option<PathBuf>> {
        let Some(root) = &self.workspace_root else {
            return Ok(None);
        };
        let cwd = match &state.cwd {
            Some(cwd) => cwd.clone(),
            None => {
                let cwd = root.join(session_workspace_dir_name(&state.session_key));
                state.cwd = Some(cwd.clone());
                cwd
            }
        };
        std::fs::create_dir_all(&cwd)?;
        let cwd = cwd.canonicalize().unwrap_or(cwd);
        state.cwd = Some(cwd.clone());
        Ok(Some(cwd))
    }

    fn render_yolo_status_with_prefix(&self, prefix: &str, state: &SessionState) -> String {
        format!("{prefix}\n{}", self.render_yolo_status(state))
    }

    fn render_yolo_status(&self, state: &SessionState) -> String {
        let override_label = state
            .approval_mode_override
            .map(ApprovalMode::as_str)
            .unwrap_or("inherit");
        let effective_mode = state
            .approval_mode_override
            .unwrap_or(self.default_approval_mode);
        [
            format!(
                "Global default approval mode: {}",
                self.default_approval_mode.as_str()
            ),
            format!("Session override: {override_label}"),
            format!("Effective approval mode: {}", effective_mode.as_str()),
        ]
        .join("\n")
    }
}

#[async_trait]
impl<S, E> RouterService for AgentRouter<S, E>
where
    S: SessionStore,
    E: ExecutorBackend,
{
    async fn handle(
        &self,
        input: RouterInput,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        if let Some(reply) = self
            .approvals
            .resolve_command(&input.session_key, &input.text, input.user_id.as_deref())
            .await
        {
            return output.send_final_reply(reply.text).await;
        }
        let lock = self.session_lock(&input.session_key).await;
        let _guard = lock.lock().await;
        self.handle_locked(input, output).await
    }
}

struct RouterExecutorEventSink<'a> {
    executor: &'a str,
    output: &'a mut dyn RouterOutputSink,
    updates: Vec<ExecutorUpdate>,
}

impl<'a> RouterExecutorEventSink<'a> {
    fn new(executor: &'a str, output: &'a mut dyn RouterOutputSink) -> Self {
        Self {
            executor,
            output,
            updates: Vec::new(),
        }
    }

    fn into_updates(self) -> Vec<ExecutorUpdate> {
        self.updates
    }
}

#[async_trait]
impl ExecutorEventSink for RouterExecutorEventSink<'_> {
    async fn send(&mut self, update: ExecutorUpdate) -> anyhow::Result<()> {
        if let Some(event) = channel_event_from_executor_update(self.executor, &update) {
            self.output.send_channel_event(event);
        }
        self.updates.push(update);
        Ok(())
    }
}

fn channel_event_from_executor_update(
    executor: &str,
    update: &ExecutorUpdate,
) -> Option<RouterChannelEvent> {
    let event = update.channel_event.as_ref()?;
    if event.text.trim().is_empty() {
        return None;
    }
    Some(RouterChannelEvent {
        kind: router_channel_event_kind(event.kind),
        executor: executor.to_string(),
        title: event.title.clone(),
        text: truncate_chars(event.text.trim(), 1_500),
    })
}

fn router_channel_event_kind(kind: ExecutorChannelEventKind) -> RouterChannelEventKind {
    match kind {
        ExecutorChannelEventKind::ReasoningSummary => RouterChannelEventKind::ReasoningSummary,
        ExecutorChannelEventKind::ToolCall => RouterChannelEventKind::ToolCall,
    }
}

fn update_binding_after_success(
    mut binding: ExecutorBinding,
    external_session_id: Option<String>,
    protocol: String,
    cwd: Option<&Path>,
    handoff_fingerprints: Vec<String>,
    new_message_fingerprints: Vec<String>,
) -> ExecutorBinding {
    binding.protocol = protocol;
    binding.external_session_id = external_session_id;
    binding.health = ExecutorHealth::Healthy;
    binding = binding_with_session_cwd(binding, cwd);
    binding.seen_context = merge_seen_context(
        &binding.seen_context,
        &[handoff_fingerprints, new_message_fingerprints].concat(),
    );
    binding
}

fn update_binding_after_prompt_failure(
    mut binding: ExecutorBinding,
    prepared_session_id: Option<String>,
    protocol: String,
    cwd: Option<&Path>,
) -> ExecutorBinding {
    binding.protocol = protocol;
    binding.health = ExecutorHealth::Unhealthy;
    binding = binding_with_session_cwd(binding, cwd);
    if prepared_session_id != binding.external_session_id {
        binding.external_session_id = prepared_session_id;
        binding.seen_context.clear();
    }
    binding
}

fn binding_with_session_cwd(mut binding: ExecutorBinding, cwd: Option<&Path>) -> ExecutorBinding {
    if let Some(cwd) = cwd {
        binding.cwd = Some(cwd.display().to_string());
    }
    binding
}

fn session_workspace_dir_name(session_key: &str) -> String {
    let raw_prefix = session_key
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let mut prefix = raw_prefix
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if prefix.is_empty() {
        prefix = "session".to_string();
    }
    if prefix.len() > 48 {
        prefix.truncate(48);
        prefix = prefix.trim_end_matches('-').to_string();
        if prefix.is_empty() {
            prefix = "session".to_string();
        }
    }
    let digest = Sha256::digest(session_key.as_bytes());
    let hash = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}-{hash}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        approval::{
            ApprovalBroker, ApprovalOption, ApprovalPolicy, ApprovalRequest, ApprovalSelection,
        },
        executor::{ExecutorChannelEvent, test_support::FakeExecutorBackend},
        session::{
            TranscriptMessage, projection::message_fingerprint, store::InMemorySessionStore,
        },
    };
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct CollectingRouterOutputSink {
        events: Vec<RouterOutputEvent>,
    }

    #[async_trait::async_trait]
    impl RouterOutputSink for CollectingRouterOutputSink {
        fn send_channel_event(&mut self, event: RouterChannelEvent) {
            self.events.push(RouterOutputEvent::Channel(event));
        }

        async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
            self.events.push(RouterOutputEvent::FinalReply(text));
            Ok(())
        }
    }

    impl CollectingRouterOutputSink {
        fn final_reply(&self) -> &str {
            match self.events.last().expect("router emitted no events") {
                RouterOutputEvent::FinalReply(text) => text,
                RouterOutputEvent::Channel(_) => panic!("last router event was not final reply"),
            }
        }
    }

    #[tokio::test]
    async fn agent_status_shows_default_and_active_executor() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store, executor);

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "/agent status".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        assert!(output.final_reply().contains("Default executor: kimi"));
        assert!(output.final_reply().contains("Active executor: kimi"));
    }

    #[tokio::test]
    async fn yolo_commands_update_current_session_override() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::with_approval_mode(
            "kimi",
            ApprovalMode::Normal,
            store.clone(),
            executor,
            Arc::new(ApprovalBroker::default()),
        );

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yolo on".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();
        assert!(output.final_reply().contains("Session override: yolo"));
        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        assert_eq!(saved.approval_mode_override, Some(ApprovalMode::Yolo));

        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yolo inherit".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();
        assert!(output.final_reply().contains("Session override: inherit"));
        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        assert_eq!(saved.approval_mode_override, None);

        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yolo off".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();
        assert!(output.final_reply().contains("Session override: normal"));
        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        assert_eq!(saved.approval_mode_override, Some(ApprovalMode::Normal));
    }

    #[tokio::test]
    async fn concatenated_yolo_command_name_is_not_a_router_command() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yoloon".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        assert_eq!(output.final_reply(), "fake response");
        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        assert_eq!(saved.approval_mode_override, None);
        assert!(
            executor.prompts.lock().await[0]
                .prompt
                .contains("Current user message:\n/yoloon")
        );
    }

    #[tokio::test]
    async fn yolo_command_enables_broker_auto_approval_for_session() {
        let store = Arc::new(InMemorySessionStore::default());
        let approvals = Arc::new(ApprovalBroker::with_policy(
            Duration::from_secs(5),
            Arc::new(SessionApprovalPolicy::new(
                "kimi",
                ApprovalMode::Normal,
                store.clone(),
            )),
        ));
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::with_approval_mode(
            "kimi",
            ApprovalMode::Normal,
            store,
            executor,
            approvals.clone(),
        );
        let mut prompts = approvals.subscribe();
        let mut notices = approvals.subscribe_auto_selections();

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yolo on".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        let selection = approvals
            .request(ApprovalRequest {
                session_key: "slack:dm:D1:111.000".to_string(),
                executor: "kimi".to_string(),
                requester_user_id: Some("U1".to_string()),
                title: "Run command".to_string(),
                body: "$ cargo test".to_string(),
                options: vec![ApprovalOption {
                    id: "allow_once".to_string(),
                    kind: "allow_once".to_string(),
                    name: "Allow once".to_string(),
                    auto_approvable: true,
                }],
            })
            .await;

        assert_eq!(
            selection,
            ApprovalSelection::Selected("allow_once".to_string())
        );
        assert!(prompts.try_recv().is_err());
        let notice = notices.try_recv().unwrap();
        assert_eq!(notice.session_key, "slack:dm:D1:111.000");
    }

    #[tokio::test]
    async fn session_approval_policy_isolates_slack_dm_sessions() {
        let store = Arc::new(InMemorySessionStore::default());
        let policy = SessionApprovalPolicy::new("kimi", ApprovalMode::Normal, store.clone());
        let mut yolo_session = SessionState::new("slack:dm:D1:111.000", "kimi");
        yolo_session.approval_mode_override = Some(ApprovalMode::Yolo);
        store.save(yolo_session).await;

        let yolo_request = ApprovalRequest {
            session_key: "slack:dm:D1:111.000".to_string(),
            executor: "kimi".to_string(),
            requester_user_id: Some("U1".to_string()),
            title: "Run command".to_string(),
            body: "$ cargo test".to_string(),
            options: vec![
                ApprovalOption {
                    id: "allow_once".to_string(),
                    kind: "allow_once".to_string(),
                    name: "Allow once".to_string(),
                    auto_approvable: true,
                },
                ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject_once".to_string(),
                    name: "Deny".to_string(),
                    auto_approvable: false,
                },
            ],
        };
        let normal_request = ApprovalRequest {
            session_key: "slack:dm:D1:222.000".to_string(),
            ..yolo_request.clone()
        };

        assert_eq!(
            policy.auto_selection(&yolo_request).await,
            Some(ApprovalSelection::Selected("allow_once".to_string()))
        );
        assert_eq!(policy.auto_selection(&normal_request).await, None);
    }

    #[tokio::test]
    async fn session_approval_policy_normal_override_disables_global_yolo() {
        let store = Arc::new(InMemorySessionStore::default());
        let policy = SessionApprovalPolicy::new("kimi", ApprovalMode::Yolo, store.clone());
        let mut state = SessionState::new("slack:channel:C1:111.000", "kimi");
        state.approval_mode_override = Some(ApprovalMode::Normal);
        store.save(state).await;

        let request = ApprovalRequest {
            session_key: "slack:channel:C1:111.000".to_string(),
            executor: "kimi".to_string(),
            requester_user_id: Some("U1".to_string()),
            title: "Run command".to_string(),
            body: "$ cargo test".to_string(),
            options: vec![ApprovalOption {
                id: "allow_once".to_string(),
                kind: "allow_once".to_string(),
                name: "Allow once".to_string(),
                auto_approvable: true,
            }],
        };

        assert_eq!(policy.auto_selection(&request).await, None);

        let inherited_request = ApprovalRequest {
            session_key: "slack:channel:C1:222.000".to_string(),
            ..request
        };
        assert_eq!(
            policy.auto_selection(&inherited_request).await,
            Some(ApprovalSelection::Selected("allow_once".to_string()))
        );
    }

    #[tokio::test]
    async fn approval_command_resolves_pending_request() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let router =
            AgentRouter::with_approvals("kimi", store, executor.clone(), approvals.clone());
        let request_broker = approvals.clone();
        let pending = tokio::spawn(async move {
            request_broker
                .request(ApprovalRequest {
                    session_key: "slack:C1:T1".to_string(),
                    executor: "kimi".to_string(),
                    requester_user_id: Some("U1".to_string()),
                    title: "Run command".to_string(),
                    body: "$ cargo test".to_string(),
                    options: vec![
                        ApprovalOption {
                            id: "allow_once".to_string(),
                            kind: "allow_once".to_string(),
                            name: "Allow once".to_string(),
                            auto_approvable: true,
                        },
                        ApprovalOption {
                            id: "deny".to_string(),
                            kind: "reject_once".to_string(),
                            name: "Deny".to_string(),
                            auto_approvable: false,
                        },
                    ],
                })
                .await
        });

        let prompt = prompts.recv().await.unwrap();
        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: format!("/approve {}", prompt.id),
                    user_id: Some("U1".to_string()),
                },
                &mut output,
            )
            .await
            .unwrap();

        assert!(output.final_reply().contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
        assert!(executor.prompts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn normal_message_routes_with_projected_context() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        state.transcript.push(TranscriptMessage::user("prior"));
        store.save(state).await;
        let router = AgentRouter::new("kimi", store.clone(), executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "next".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        assert_eq!(output.final_reply(), "fake response");
        let prompts = executor.prompts.lock().await;
        assert!(prompts[0].prompt.contains("user: prior"));
        assert!(prompts[0].prompt.contains("Current user message:\nnext"));
        drop(prompts);
        let prepared = executor.prepared.lock().await;
        assert_eq!(prepared[0].previous_session_id, None);
        drop(prepared);
        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        assert_eq!(saved.transcript.len(), 3);
        assert!(
            saved.executor_bindings["kimi"]
                .external_session_id
                .is_some()
        );
        assert_eq!(saved.executor_bindings["kimi"].protocol, "fake");
    }

    #[tokio::test]
    async fn workspace_root_assigns_stable_distinct_session_cwds() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor.clone())
            .with_workspace_root(Some(workspace_root.clone()));
        let mut output = CollectingRouterOutputSink::default();

        for (session_key, text) in [
            ("slack:dm:D1:111.000", "first"),
            ("slack:dm:D1:222.000", "second"),
            ("slack:dm:D1:111.000", "third"),
        ] {
            router
                .handle(
                    RouterInput {
                        session_key: session_key.to_string(),
                        text: text.to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
        }

        let prepared = executor.prepared.lock().await;
        let first_cwd = prepared[0].cwd.as_ref().unwrap();
        let second_cwd = prepared[1].cwd.as_ref().unwrap();
        let third_cwd = prepared[2].cwd.as_ref().unwrap();

        assert!(first_cwd.starts_with(workspace_root.canonicalize().unwrap()));
        assert!(first_cwd.is_dir());
        assert!(second_cwd.is_dir());
        assert_ne!(first_cwd, second_cwd);
        assert_eq!(first_cwd, third_cwd);
        assert!(
            !first_cwd
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(':')
        );

        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        let first_cwd_text = first_cwd.display().to_string();
        assert_eq!(
            saved.cwd.as_ref().unwrap().canonicalize().unwrap(),
            first_cwd.clone()
        );
        assert_eq!(
            saved.executor_bindings["kimi"].cwd.as_deref(),
            Some(first_cwd_text.as_str())
        );
    }

    #[test]
    fn session_workspace_dir_name_is_safe_and_stable() {
        let name = session_workspace_dir_name("slack:dm:D1:111.000");

        assert!(name.starts_with("slack-dm-d1-111-000-"));
        assert!(
            name.chars()
                .all(|ch| { ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' })
        );
        assert_eq!(name, session_workspace_dir_name("slack:dm:D1:111.000"));
        assert_ne!(name, session_workspace_dir_name("slack:dm:D1:222.000"));
    }

    #[test]
    fn channel_events_include_reasoning_and_tool_updates_only() {
        let updates = [
            ExecutorUpdate::new("plan", "Plan", "working", ""),
            ExecutorUpdate::new(
                "agent_thought_chunk",
                "Reasoning",
                "raw thinking should not leak",
                "",
            ),
            ExecutorUpdate::new("tool_call", "Bash", "$ cargo test\nok", "completed")
                .with_channel_event(ExecutorChannelEvent::tool_call("Bash", "$ cargo test")),
            ExecutorUpdate::new("agent_thought_chunk", "Reasoning", "summary", "")
                .with_channel_event(ExecutorChannelEvent::reasoning_summary(
                    "I should inspect the config.",
                )),
        ];
        let events = updates
            .iter()
            .filter_map(|update| channel_event_from_executor_update("codex", update))
            .collect::<Vec<_>>();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, RouterChannelEventKind::ToolCall);
        assert_eq!(
            events[0].render_text(),
            "[codex] Tool call: Bash\n$ cargo test"
        );
        assert_eq!(events[1].kind, RouterChannelEventKind::ReasoningSummary);
        assert_eq!(
            events[1].render_text(),
            "[codex] Reasoning summary\nI should inspect the config."
        );
    }

    #[tokio::test]
    async fn executor_channel_events_are_emitted_before_final_reply() {
        #[derive(Debug)]
        struct StreamingExecutorBackend {
            release: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        }

        #[async_trait::async_trait]
        impl ExecutorBackend for StreamingExecutorBackend {
            fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
                (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                    name: "kimi".to_string(),
                    protocol: "fake".to_string(),
                })
            }

            fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
                self.get("kimi").into_iter().collect()
            }

            async fn prepare(
                &self,
                _request: ExecutorPrepareRequest,
            ) -> anyhow::Result<crate::executor::PreparedExecutor> {
                Ok(crate::executor::PreparedExecutor {
                    external_session_id: Some("stream-session".to_string()),
                    started_new_session: true,
                })
            }

            async fn prompt(
                &self,
                _request: ExecutorPromptRequest,
                events: &mut dyn ExecutorEventSink,
            ) -> anyhow::Result<crate::executor::ExecutorResponse> {
                events
                    .send(
                        ExecutorUpdate::new("tool_call", "Bash", "$ cargo test", "completed")
                            .with_transcript_summary("Bash: status: completed")
                            .with_channel_event(ExecutorChannelEvent::tool_call(
                                "Bash",
                                "status: completed",
                            )),
                    )
                    .await?;
                let release = self
                    .release
                    .lock()
                    .await
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("release gate already consumed"))?;
                release.await?;
                Ok(crate::executor::ExecutorResponse {
                    final_text: "done".to_string(),
                })
            }
        }

        struct ChannelRouterOutputSink {
            tx: tokio::sync::mpsc::UnboundedSender<RouterOutputEvent>,
        }

        #[async_trait::async_trait]
        impl RouterOutputSink for ChannelRouterOutputSink {
            fn send_channel_event(&mut self, event: RouterChannelEvent) {
                let _ = self.tx.send(RouterOutputEvent::Channel(event));
            }

            async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
                self.tx
                    .send(RouterOutputEvent::FinalReply(text))
                    .map_err(|_| anyhow::anyhow!("router output receiver dropped"))
            }
        }

        let store = Arc::new(InMemorySessionStore::default());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(StreamingExecutorBackend {
            release: tokio::sync::Mutex::new(Some(release_rx)),
        });
        let router = AgentRouter::new("kimi", store, executor);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let handle = tokio::spawn(async move {
            let mut output = ChannelRouterOutputSink { tx };
            router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "run tests".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
        });

        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first, RouterOutputEvent::Channel(_)));
        assert!(rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(second, RouterOutputEvent::FinalReply(text) if text == "done"));
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn best_effort_channel_event_delivery_does_not_fail_successful_turn() {
        #[derive(Debug)]
        struct ToolEventExecutorBackend;

        #[async_trait::async_trait]
        impl ExecutorBackend for ToolEventExecutorBackend {
            fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
                (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                    name: "kimi".to_string(),
                    protocol: "fake".to_string(),
                })
            }

            fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
                self.get("kimi").into_iter().collect()
            }

            async fn prepare(
                &self,
                _request: ExecutorPrepareRequest,
            ) -> anyhow::Result<crate::executor::PreparedExecutor> {
                Ok(crate::executor::PreparedExecutor {
                    external_session_id: Some("tool-session".to_string()),
                    started_new_session: true,
                })
            }

            async fn prompt(
                &self,
                _request: ExecutorPromptRequest,
                events: &mut dyn ExecutorEventSink,
            ) -> anyhow::Result<crate::executor::ExecutorResponse> {
                events
                    .send(
                        ExecutorUpdate::new("tool_call", "Bash", "$ cargo test", "completed")
                            .with_transcript_summary("Bash: status: completed")
                            .with_channel_event(ExecutorChannelEvent::tool_call(
                                "Bash",
                                "status: completed",
                            )),
                    )
                    .await?;
                Ok(crate::executor::ExecutorResponse {
                    final_text: "done".to_string(),
                })
            }
        }

        #[derive(Default)]
        struct BestEffortChannelEventSink {
            channel_event_count: usize,
            final_replies: Vec<String>,
        }

        #[async_trait::async_trait]
        impl RouterOutputSink for BestEffortChannelEventSink {
            fn send_channel_event(&mut self, _event: RouterChannelEvent) {
                self.channel_event_count += 1;
            }

            async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
                self.final_replies.push(text);
                Ok(())
            }
        }

        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(ToolEventExecutorBackend);
        let router = AgentRouter::new("kimi", store.clone(), executor);
        let mut output = BestEffortChannelEventSink::default();

        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "run tests".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        assert_eq!(output.channel_event_count, 1);
        assert_eq!(output.final_replies, ["done".to_string()]);
        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        assert_eq!(saved.transcript.len(), 2);
        assert_eq!(
            saved.executor_bindings["kimi"].health,
            ExecutorHealth::Healthy
        );
    }

    #[tokio::test]
    async fn seen_context_is_not_replayed_to_resumed_executor() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        let old = TranscriptMessage::user("old");
        let new = TranscriptMessage::user("new");
        state.transcript = vec![old.clone(), new];
        state.executor_bindings.insert(
            "kimi".to_string(),
            ExecutorBinding {
                protocol: "acp".to_string(),
                external_session_id: Some("ext-1".to_string()),
                seen_context: vec![message_fingerprint(&old)],
                ..ExecutorBinding::default()
            },
        );
        store.save(state).await;
        let router = AgentRouter::new("kimi", store, executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "continue".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        let prompts = executor.prompts.lock().await;
        assert!(!prompts[0].prompt.contains("user: old"));
        assert!(prompts[0].prompt.contains("user: new"));
        drop(prompts);
        let prepared = executor.prepared.lock().await;
        assert_eq!(prepared[0].previous_session_id.as_deref(), Some("ext-1"));
    }

    #[tokio::test]
    async fn fresh_executor_session_gets_full_context_even_with_previous_binding() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend {
            force_started_new_session: true,
            ..FakeExecutorBackend::default()
        });
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        let old = TranscriptMessage::user("old");
        state.transcript.push(old.clone());
        state.executor_bindings.insert(
            "kimi".to_string(),
            ExecutorBinding {
                protocol: "acp".to_string(),
                external_session_id: Some("stale-session".to_string()),
                seen_context: vec![message_fingerprint(&old)],
                ..ExecutorBinding::default()
            },
        );
        store.save(state).await;
        let router = AgentRouter::new("kimi", store, executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "recover".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        let prompts = executor.prompts.lock().await;
        assert!(prompts[0].prompt.contains("Recent router transcript"));
        assert!(prompts[0].prompt.contains("user: old"));
    }

    #[tokio::test]
    async fn prompt_failure_after_replacement_session_clears_stale_cursor() {
        #[derive(Debug, Default)]
        struct FailingAfterPrepare {
            prepared: tokio::sync::Mutex<Vec<ExecutorPrepareRequest>>,
        }

        #[async_trait::async_trait]
        impl ExecutorBackend for FailingAfterPrepare {
            fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
                (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                    name: "kimi".to_string(),
                    protocol: "fake".to_string(),
                })
            }

            fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
                self.get("kimi").into_iter().collect()
            }

            async fn prepare(
                &self,
                request: ExecutorPrepareRequest,
            ) -> anyhow::Result<crate::executor::PreparedExecutor> {
                self.prepared.lock().await.push(request);
                Ok(crate::executor::PreparedExecutor {
                    external_session_id: Some("replacement-session".to_string()),
                    started_new_session: true,
                })
            }

            async fn prompt(
                &self,
                _request: ExecutorPromptRequest,
                _events: &mut dyn ExecutorEventSink,
            ) -> anyhow::Result<crate::executor::ExecutorResponse> {
                anyhow::bail!("prompt failed")
            }
        }

        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FailingAfterPrepare::default());
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        let old = TranscriptMessage::user("old");
        state.transcript.push(old.clone());
        state.executor_bindings.insert(
            "kimi".to_string(),
            ExecutorBinding {
                protocol: "acp".to_string(),
                external_session_id: Some("stale-session".to_string()),
                seen_context: vec![message_fingerprint(&old)],
                ..ExecutorBinding::default()
            },
        );
        store.save(state).await;
        let router = AgentRouter::new("kimi", store.clone(), executor);

        let mut output = CollectingRouterOutputSink::default();
        let err = router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "recover".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("prompt failed"));
        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        let binding = saved.executor_bindings.get("kimi").unwrap();
        assert_eq!(
            binding.external_session_id.as_deref(),
            Some("replacement-session")
        );
        assert!(binding.seen_context.is_empty());
        assert_eq!(binding.health, ExecutorHealth::Unhealthy);
    }
}
