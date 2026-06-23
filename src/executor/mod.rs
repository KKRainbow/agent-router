pub mod acp;
pub mod codex_app_server;
pub mod registry;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::text::truncate_chars;

#[derive(Debug, Clone)]
pub struct ExecutorDescriptor {
    pub name: String,
    pub protocol: String,
}

#[derive(Debug, Clone)]
pub struct ExecutorPrepareRequest {
    pub session_key: String,
    pub executor: String,
    pub previous_session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PreparedExecutor {
    pub external_session_id: Option<String>,
    pub started_new_session: bool,
}

#[derive(Debug, Clone)]
pub struct ExecutorPromptRequest {
    pub session_key: String,
    pub executor: String,
    pub prompt: String,
    pub user_id: Option<String>,
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
    ReasoningSummary,
    ToolCall,
}

#[derive(Debug, Clone)]
pub struct ExecutorResponse {
    pub final_text: String,
}

#[async_trait]
pub trait ExecutorEventSink: Send {
    async fn send(&mut self, update: ExecutorUpdate) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ExecutorBackend: Send + Sync + 'static {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor>;
    fn list(&self) -> Vec<ExecutorDescriptor>;
    async fn prepare(&self, request: ExecutorPrepareRequest) -> anyhow::Result<PreparedExecutor>;
    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
    ) -> anyhow::Result<ExecutorResponse>;
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

pub(crate) fn internal_json_rpc_error(message: &str) -> Value {
    json!({
        "code": -32000,
        "message": message,
        "data": {"source": "agent-router"},
    })
}

pub(crate) fn internal_json_rpc_error_message(error: &Value) -> Option<&str> {
    let source = error
        .get("data")
        .and_then(|data| data.get("source"))
        .and_then(Value::as_str);
    if source != Some("agent-router") {
        return None;
    }
    error.get("message").and_then(Value::as_str)
}

#[cfg(test)]
pub mod test_support {
    use std::{collections::BTreeMap, sync::Arc};

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::{
        ExecutorBackend, ExecutorDescriptor, ExecutorEventSink, ExecutorPrepareRequest,
        ExecutorPromptRequest, ExecutorResponse, ExecutorUpdate, PreparedExecutor,
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
        ) -> anyhow::Result<PreparedExecutor> {
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
        ) -> anyhow::Result<ExecutorResponse> {
            self.prompts.lock().await.push(ExecutorRequest {
                session_key: request.session_key,
                executor: request.executor,
                prompt: request.prompt,
            });
            events
                .send(
                    ExecutorUpdate::new("plan", "Plan", "working", "")
                        .with_transcript_summary("Plan: working"),
                )
                .await?;
            Ok(ExecutorResponse {
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

    #[test]
    fn internal_json_rpc_error_message_requires_router_marker() {
        let internal = internal_json_rpc_error("ACP process closed stdout");
        assert_eq!(
            internal_json_rpc_error_message(&internal),
            Some("ACP process closed stdout")
        );

        let external = serde_json::json!({
            "code": -32000,
            "message": "secret prompt text",
        });
        assert_eq!(internal_json_rpc_error_message(&external), None);
    }
}
