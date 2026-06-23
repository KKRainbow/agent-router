use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    approval::{ApprovalBroker, SharedApprovalBroker},
    executor::{ExecutorBackend, ExecutorPrepareRequest, ExecutorPromptRequest, ExecutorUpdate},
    session::{
        ExecutorBinding, ExecutorHealth, SessionState, TranscriptMessage,
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
pub struct RouterReply {
    pub text: String,
    pub channel_events: Vec<RouterChannelEvent>,
}

impl RouterReply {
    fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            channel_events: Vec::new(),
        }
    }

    fn with_channel_events(
        text: impl Into<String>,
        channel_events: Vec<RouterChannelEvent>,
    ) -> Self {
        Self {
            text: text.into(),
            channel_events,
        }
    }
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
pub trait RouterService: Send + Sync + 'static {
    async fn handle(&self, input: RouterInput) -> anyhow::Result<RouterReply>;
}

pub struct AgentRouter<S, E>
where
    S: SessionStore,
    E: ExecutorBackend,
{
    default_executor: String,
    store: Arc<S>,
    executor: Arc<E>,
    approvals: SharedApprovalBroker,
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
        Self {
            default_executor: default_executor.into(),
            store,
            executor,
            approvals,
            session_locks: Mutex::new(HashMap::new()),
        }
    }

    async fn session_lock(&self, session_key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.session_locks.lock().await;
        locks
            .entry(session_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn handle_locked(&self, input: RouterInput) -> anyhow::Result<RouterReply> {
        let text = input.text.trim();
        if text.starts_with("/agent") {
            return self.handle_agent_command(&input.session_key, text).await;
        }
        self.route_to_active_executor(input).await
    }

    async fn handle_agent_command(
        &self,
        session_key: &str,
        text: &str,
    ) -> anyhow::Result<RouterReply> {
        let args = text.trim_start_matches("/agent").trim();
        let mut state = self
            .store
            .load_or_create(session_key, &self.default_executor)
            .await;
        if args.is_empty() || args == "status" {
            return Ok(RouterReply::text(self.render_status(&state)));
        }
        if args.split_whitespace().count() != 1 {
            return Ok(RouterReply::text("Usage: /agent [status|done|<executor>]"));
        }

        let target = args;
        if target == "done" {
            state.active_executor = state.default_executor.clone();
            self.store.save(state.clone()).await;
            return Ok(RouterReply::text(format!(
                "Agent handoff ended. Active executor: {}",
                state.active_executor
            )));
        }

        if self.executor.get(target).is_none() {
            return Ok(RouterReply::text(format!(
                "Executor `{target}` is not configured."
            )));
        }
        state.active_executor = target.to_string();
        self.store.save(state).await;
        Ok(RouterReply::text(format!("Active executor: {target}")))
    }

    async fn route_to_active_executor(&self, input: RouterInput) -> anyhow::Result<RouterReply> {
        let mut state = self
            .store
            .load_or_create(&input.session_key, &self.default_executor)
            .await;
        let executor_name = state.active_executor.clone();
        let descriptor = self.executor.get(&executor_name).ok_or_else(|| {
            anyhow::anyhow!("active executor `{executor_name}` is not configured")
        })?;

        let binding = state.binding_for(&executor_name);
        let prepared = self
            .executor
            .prepare(ExecutorPrepareRequest {
                session_key: input.session_key.clone(),
                executor: executor_name.clone(),
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
                        ..binding
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

        let response = self
            .executor
            .prompt(ExecutorPromptRequest {
                session_key: input.session_key.clone(),
                executor: executor_name.clone(),
                prompt: projection.prompt,
                user_id: input.user_id.clone(),
            })
            .await;

        match response {
            Ok(response) => {
                let activity_summaries = response
                    .updates
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
                        projection.acknowledged_fingerprints,
                        new_fingerprints,
                    ),
                );
                self.store.save(state).await;
                Ok(RouterReply::with_channel_events(
                    response.final_text,
                    channel_events_from_executor_updates(&executor_name, &response.updates),
                ))
            }
            Err(err) => {
                state.executor_bindings.insert(
                    executor_name.clone(),
                    update_binding_after_prompt_failure(
                        binding,
                        prepared.external_session_id,
                        descriptor.protocol,
                    ),
                );
                self.store.save(state).await;
                Err(err)
            }
        }
    }

    fn render_status(&self, state: &SessionState) -> String {
        let mut lines = vec![
            format!("Default executor: {}", state.default_executor),
            format!("Active executor: {}", state.active_executor),
            "Executors:".to_string(),
        ];
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
}

#[async_trait]
impl<S, E> RouterService for AgentRouter<S, E>
where
    S: SessionStore,
    E: ExecutorBackend,
{
    async fn handle(&self, input: RouterInput) -> anyhow::Result<RouterReply> {
        if let Some(reply) = self
            .approvals
            .resolve_command(&input.session_key, &input.text, input.user_id.as_deref())
            .await
        {
            return Ok(RouterReply::text(reply.text));
        }
        let lock = self.session_lock(&input.session_key).await;
        let _guard = lock.lock().await;
        self.handle_locked(input).await
    }
}

fn channel_events_from_executor_updates(
    executor: &str,
    updates: &[ExecutorUpdate],
) -> Vec<RouterChannelEvent> {
    updates
        .iter()
        .filter_map(|update| channel_event_from_executor_update(executor, update))
        .collect()
}

fn channel_event_from_executor_update(
    executor: &str,
    update: &ExecutorUpdate,
) -> Option<RouterChannelEvent> {
    let kind = match update.kind.as_str() {
        "agent_thought_chunk" => RouterChannelEventKind::ReasoningSummary,
        "tool_call" => RouterChannelEventKind::ToolCall,
        _ => return None,
    };
    let text = if update.text.trim().is_empty() {
        update.status.trim()
    } else {
        update.text.trim()
    };
    if text.is_empty() {
        return None;
    }
    Some(RouterChannelEvent {
        kind,
        executor: executor.to_string(),
        title: update.title.clone(),
        text: truncate_chars(text, 1_500),
    })
}

fn update_binding_after_success(
    mut binding: ExecutorBinding,
    external_session_id: Option<String>,
    protocol: String,
    handoff_fingerprints: Vec<String>,
    new_message_fingerprints: Vec<String>,
) -> ExecutorBinding {
    binding.protocol = protocol;
    binding.external_session_id = external_session_id;
    binding.health = ExecutorHealth::Healthy;
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
) -> ExecutorBinding {
    binding.protocol = protocol;
    binding.health = ExecutorHealth::Unhealthy;
    if prepared_session_id != binding.external_session_id {
        binding.external_session_id = prepared_session_id;
        binding.seen_context.clear();
    }
    binding
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        approval::{ApprovalBroker, ApprovalOption, ApprovalRequest, ApprovalSelection},
        executor::test_support::FakeExecutorBackend,
        session::{
            TranscriptMessage, projection::message_fingerprint, store::InMemorySessionStore,
        },
    };
    use std::time::Duration;

    #[tokio::test]
    async fn agent_status_shows_default_and_active_executor() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store, executor);

        let reply = router
            .handle(RouterInput {
                session_key: "slack:C1:T1".to_string(),
                text: "/agent status".to_string(),
                user_id: None,
            })
            .await
            .unwrap();

        assert!(reply.text.contains("Default executor: kimi"));
        assert!(reply.text.contains("Active executor: kimi"));
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
                        },
                        ApprovalOption {
                            id: "deny".to_string(),
                            kind: "reject_once".to_string(),
                            name: "Deny".to_string(),
                        },
                    ],
                })
                .await
        });

        let prompt = prompts.recv().await.unwrap();
        let reply = router
            .handle(RouterInput {
                session_key: "slack:C1:T1".to_string(),
                text: format!("/approve {}", prompt.id),
                user_id: Some("U1".to_string()),
            })
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
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

        let reply = router
            .handle(RouterInput {
                session_key: "slack:C1:T1".to_string(),
                text: "next".to_string(),
                user_id: None,
            })
            .await
            .unwrap();

        assert_eq!(reply.text, "fake response");
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

    #[test]
    fn channel_events_include_reasoning_and_tool_updates_only() {
        let events = channel_events_from_executor_updates(
            "codex",
            &[
                ExecutorUpdate {
                    kind: "plan".to_string(),
                    title: "Plan".to_string(),
                    text: "working".to_string(),
                    status: String::new(),
                },
                ExecutorUpdate {
                    kind: "agent_thought_chunk".to_string(),
                    title: "Reasoning".to_string(),
                    text: "I should inspect the config.".to_string(),
                    status: String::new(),
                },
                ExecutorUpdate {
                    kind: "tool_call".to_string(),
                    title: "Bash".to_string(),
                    text: "$ cargo test\nok".to_string(),
                    status: "completed".to_string(),
                },
            ],
        );

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, RouterChannelEventKind::ReasoningSummary);
        assert_eq!(
            events[0].render_text(),
            "[codex] Reasoning summary\nI should inspect the config."
        );
        assert_eq!(events[1].kind, RouterChannelEventKind::ToolCall);
        assert_eq!(
            events[1].render_text(),
            "[codex] Tool call: Bash\n$ cargo test\nok"
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

        router
            .handle(RouterInput {
                session_key: "slack:C1:T1".to_string(),
                text: "continue".to_string(),
                user_id: None,
            })
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

        router
            .handle(RouterInput {
                session_key: "slack:C1:T1".to_string(),
                text: "recover".to_string(),
                user_id: None,
            })
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

        let err = router
            .handle(RouterInput {
                session_key: "slack:C1:T1".to_string(),
                text: "recover".to_string(),
                user_id: None,
            })
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
