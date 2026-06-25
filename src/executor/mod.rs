pub mod acp;
pub mod claude_stream_json;
pub mod codex_app_server;
pub mod registry;

use async_trait::async_trait;
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex as TokioMutex, watch};

use crate::text::truncate_chars;

#[derive(Debug, Clone)]
pub struct ExecutorDescriptor {
    pub name: String,
    pub protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTurnRef {
    pub session_key: String,
    pub executor: String,
    pub generation: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutorPrepareRequest {
    pub turn: ExecutorTurnRef,
    pub cwd: Option<PathBuf>,
    /// Last committed Backend Session id from the Executor Binding.
    pub previous_session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PreparedExecutor {
    /// Backend Session id adopted by this Turn, if the protocol has one.
    pub external_session_id: Option<String>,
    /// True when the adopted Backend Session is not the same identity as
    /// `ExecutorPrepareRequest::previous_session_id`.
    pub started_new_session: bool,
}

#[derive(Debug, Clone)]
pub struct ExecutorPromptRequest {
    pub turn: ExecutorTurnRef,
    pub prompt: String,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptReason {
    ReplacedByNewMessage,
    UserStop,
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct ExecutorInterruptRequest {
    pub turn: ExecutorTurnRef,
    pub reason: InterruptReason,
}

#[derive(Debug, Clone)]
pub struct TurnCancellation {
    inner: Arc<TurnCancellationInner>,
}

#[derive(Debug)]
struct TurnCancellationInner {
    cancelled: AtomicBool,
    reason: TokioMutex<Option<InterruptReason>>,
    changed: watch::Sender<Option<InterruptReason>>,
}

impl TurnCancellation {
    pub fn new() -> Self {
        let (changed, _) = watch::channel(None);
        Self {
            inner: Arc::new(TurnCancellationInner {
                cancelled: AtomicBool::new(false),
                reason: TokioMutex::new(None),
                changed,
            }),
        }
    }

    pub async fn cancel(&self, reason: InterruptReason) -> bool {
        let mut guard = self.inner.reason.lock().await;
        if guard.is_some() {
            return false;
        }
        *guard = Some(reason);
        self.inner.cancelled.store(true, Ordering::Release);
        let _ = self.inner.changed.send(Some(reason));
        true
    }

    pub async fn is_cancelled(&self) -> bool {
        self.inner.reason.lock().await.is_some()
    }

    pub fn is_cancelled_now(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) -> InterruptReason {
        let mut changed = self.inner.changed.subscribe();
        if let Some(reason) = *self.inner.reason.lock().await {
            return reason;
        }
        if let Some(reason) = *changed.borrow() {
            return reason;
        }
        loop {
            if changed.changed().await.is_err() {
                return InterruptReason::Shutdown;
            }
            if let Some(reason) = *changed.borrow() {
                return reason;
            }
        }
    }
}

impl Default for TurnCancellation {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorUpdate {
    pub kind: String,
    pub title: String,
    pub text: String,
    pub status: String,
    pub transcript_summary: Option<String>,
    pub channel_event: Option<ExecutorChannelEvent>,
}

impl ExecutorUpdate {
    pub fn new(
        kind: impl Into<String>,
        title: impl Into<String>,
        text: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            kind: kind.into(),
            title: title.into(),
            text: text.into(),
            status: status.into(),
            transcript_summary: None,
            channel_event: None,
        }
    }

    pub fn with_transcript_summary(mut self, summary: impl Into<String>) -> Self {
        self.transcript_summary = Some(summary.into());
        self
    }

    pub fn with_channel_event(mut self, channel_event: ExecutorChannelEvent) -> Self {
        self.channel_event = Some(channel_event);
        self
    }
}

impl ExecutorUpdate {
    pub fn summary(&self, limit: usize) -> Option<String> {
        let mut text = self.transcript_summary.clone()?;
        if text.trim().is_empty() {
            return None;
        }
        if limit > 3 && text.chars().count() > limit {
            text = truncate_chars(&text, limit);
        }
        Some(text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorChannelEvent {
    pub kind: ExecutorChannelEventKind,
    pub title: String,
    pub text: String,
}

impl ExecutorChannelEvent {
    pub fn agent_progress(text: impl Into<String>) -> Self {
        Self {
            kind: ExecutorChannelEventKind::AgentProgress,
            title: "Progress".to_string(),
            text: text.into(),
        }
    }

    pub fn reasoning_summary(text: impl Into<String>) -> Self {
        Self {
            kind: ExecutorChannelEventKind::ReasoningSummary,
            title: "Reasoning".to_string(),
            text: text.into(),
        }
    }

    pub fn tool_call(title: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            kind: ExecutorChannelEventKind::ToolCall,
            title: title.into(),
            text: text.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorChannelEventKind {
    AgentProgress,
    ReasoningSummary,
    ToolCall,
}

#[derive(Debug, Clone)]
pub struct ExecutorResponse {
    pub final_text: String,
}

#[derive(Debug)]
pub enum ExecutorPromptOutcome {
    Completed(ExecutorResponse),
    Cancelled,
    Failed(anyhow::Error),
}

impl ExecutorPromptOutcome {
    pub fn into_result(self) -> anyhow::Result<ExecutorResponse> {
        match self {
            Self::Completed(response) => Ok(response),
            Self::Cancelled => anyhow::bail!("executor turn cancelled"),
            Self::Failed(err) => Err(err),
        }
    }

    pub fn unwrap(self) -> ExecutorResponse {
        match self {
            Self::Completed(response) => response,
            Self::Cancelled => panic!("called `ExecutorPromptOutcome::unwrap()` on `Cancelled`"),
            Self::Failed(err) => {
                panic!("called `ExecutorPromptOutcome::unwrap()` on failed turn: {err}")
            }
        }
    }

    pub fn unwrap_err(self) -> anyhow::Error {
        match self {
            Self::Completed(_) => {
                panic!("called `ExecutorPromptOutcome::unwrap_err()` on completed turn")
            }
            Self::Cancelled => anyhow::anyhow!("executor turn cancelled"),
            Self::Failed(err) => err,
        }
    }
}

#[async_trait]
pub trait ExecutorEventSink: Send {
    async fn send(&mut self, update: ExecutorUpdate) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ExecutorBackend: Send + Sync + 'static {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor>;
    fn list(&self) -> Vec<ExecutorDescriptor>;

    /// Prepare the backend for one router Turn.
    ///
    /// Implementations may create, initialize, or reuse a Backend Session. If
    /// cancellation fires before a new Backend Session is published in shared
    /// adapter state, the adapter may drop that local work and return an error.
    /// Once a Backend Session has been published for reuse, cancellation of this
    /// prepare call must not close or remove that published session; it is shared
    /// adapter state and may already be visible to a replacement Turn.
    ///
    /// The router treats an error returned after `cancel` has fired as a
    /// cancelled Turn, not as backend failure.
    async fn prepare(
        &self,
        request: ExecutorPrepareRequest,
        cancel: TurnCancellation,
    ) -> anyhow::Result<PreparedExecutor>;

    /// Run one backend Turn.
    ///
    /// Prompt cancellation must use the backend protocol's interrupt or cancel
    /// primitive when available, then return `ExecutorPromptOutcome::Cancelled`.
    /// `Cancelled` is a Turn outcome and must not be reported as a generic
    /// backend failure.
    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome;

    /// Request cancellation of active backend work for a router Turn.
    ///
    /// This is a protocol-level Turn interrupt. It does not mean "destroy the
    /// Backend Session"; adapters should close or replace a Backend Session only
    /// from their explicit unhealthy-session recovery path.
    async fn interrupt(&self, _request: ExecutorInterruptRequest) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) fn summarize_json_rpc_error(error: &Value) -> String {
    let code = error
        .get("code")
        .and_then(Value::as_i64)
        .map(|code| code.to_string())
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

#[cfg(test)]
pub mod test_support {
    use std::{collections::BTreeMap, sync::Arc};

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::{
        ExecutorBackend, ExecutorDescriptor, ExecutorEventSink, ExecutorPrepareRequest,
        ExecutorPromptOutcome, ExecutorPromptRequest, ExecutorResponse, ExecutorUpdate,
        PreparedExecutor, TurnCancellation,
    };

    #[derive(Debug, Default)]
    pub struct FakeExecutorBackend {
        pub prompts: Arc<Mutex<Vec<ExecutorRequest>>>,
        pub prepared: Arc<Mutex<Vec<ExecutorPrepareRequest>>>,
        pub force_started_new_session: bool,
    }

    #[derive(Debug, Clone)]
    pub struct ExecutorRequest {
        pub session_key: String,
        pub executor: String,
        pub generation: u64,
        pub prompt: String,
    }

    #[derive(Debug, Default)]
    pub struct CollectingExecutorEventSink {
        pub updates: Vec<ExecutorUpdate>,
    }

    #[async_trait]
    impl ExecutorEventSink for CollectingExecutorEventSink {
        async fn send(&mut self, update: ExecutorUpdate) -> anyhow::Result<()> {
            self.updates.push(update);
            Ok(())
        }
    }

    #[async_trait]
    impl ExecutorBackend for FakeExecutorBackend {
        fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
            (name == "kimi").then(|| ExecutorDescriptor {
                name: "kimi".to_string(),
                protocol: "fake".to_string(),
            })
        }

        fn list(&self) -> Vec<ExecutorDescriptor> {
            vec![ExecutorDescriptor {
                name: "kimi".to_string(),
                protocol: "fake".to_string(),
            }]
        }

        async fn prepare(
            &self,
            request: ExecutorPrepareRequest,
            cancel: TurnCancellation,
        ) -> anyhow::Result<PreparedExecutor> {
            if cancel.is_cancelled().await {
                anyhow::bail!("executor prepare cancelled");
            }
            self.prepared.lock().await.push(request.clone());
            let started_new_session =
                self.force_started_new_session || request.previous_session_id.is_none();
            Ok(PreparedExecutor {
                external_session_id: Some(
                    request
                        .previous_session_id
                        .unwrap_or_else(|| "fake-session".to_string()),
                ),
                started_new_session,
            })
        }

        async fn prompt(
            &self,
            request: ExecutorPromptRequest,
            events: &mut dyn ExecutorEventSink,
            cancel: TurnCancellation,
        ) -> ExecutorPromptOutcome {
            if cancel.is_cancelled().await {
                return ExecutorPromptOutcome::Cancelled;
            }
            self.prompts.lock().await.push(ExecutorRequest {
                session_key: request.turn.session_key,
                executor: request.turn.executor,
                generation: request.turn.generation,
                prompt: request.prompt,
            });
            if let Err(err) = events
                .send(
                    ExecutorUpdate::new("plan", "Plan", "working", "")
                        .with_transcript_summary("Plan: working"),
                )
                .await
            {
                return ExecutorPromptOutcome::Failed(err);
            }
            ExecutorPromptOutcome::Completed(ExecutorResponse {
                final_text: "fake response".to_string(),
            })
        }
    }

    pub fn fake_executor_map() -> BTreeMap<String, crate::config::ExecutorConfig> {
        let mut map = BTreeMap::new();
        map.insert(
            "kimi".to_string(),
            crate::config::ExecutorConfig {
                name: "kimi".to_string(),
                protocol: crate::config::ExecutorProtocol::Acp,
                command: "kimi".to_string(),
                args: vec!["acp".to_string()],
                cwd: None,
                env: BTreeMap::new(),
            },
        );
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_wait_observes_reason_after_prior_cancel() {
        let cancel = TurnCancellation::new();

        assert!(cancel.cancel(InterruptReason::UserStop).await);

        assert_eq!(cancel.cancelled().await, InterruptReason::UserStop);
    }

    #[test]
    fn update_summary_truncates_on_char_boundary() {
        let update = ExecutorUpdate::new("tool_call", "Bash", "raw", "")
            .with_transcript_summary(format!("{}🙂z", "a".repeat(16)));

        let summary = update.summary(17).unwrap();

        assert!(summary.ends_with("..."));
    }

    #[test]
    fn json_rpc_error_summary_omits_sensitive_fields() {
        let summary = summarize_json_rpc_error(&serde_json::json!({
            "code": -32000,
            "message": "secret prompt text",
            "data": {"token": "secret-token"},
        }));

        assert_eq!(summary, "code=-32000, message=omitted");
        assert!(!summary.contains("secret"));
        assert!(!summary.contains("token"));
    }

    #[test]
    fn json_rpc_error_summary_ignores_malformed_code() {
        let summary = summarize_json_rpc_error(&serde_json::json!({
            "code": {"token": "secret-code"},
            "message": "",
        }));

        assert_eq!(summary, "code=unknown, message=absent");
        assert!(!summary.contains("secret"));
    }
}
