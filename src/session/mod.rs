pub mod projection;
pub mod store;
pub(crate) mod work_queue;

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    #[default]
    Normal,
    Yolo,
}

impl ApprovalMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Yolo => "yolo",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    Tool,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptMessage {
    pub role: MessageRole,
    pub content: String,
    pub timestamp_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_session_id: Option<String>,
}

impl TranscriptMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: MessageRole::User,
            content: content.into(),
            timestamp_ms: now_ms(),
            executor: None,
            external_session_id: None,
        }
    }

    pub fn assistant(
        content: impl Into<String>,
        executor: impl Into<String>,
        external_session_id: Option<String>,
    ) -> Self {
        Self {
            role: MessageRole::Assistant,
            content: content.into(),
            timestamp_ms: now_ms(),
            executor: Some(executor.into()),
            external_session_id,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorHealth {
    #[default]
    Unknown,
    Healthy,
    Unhealthy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExecutorBinding {
    pub protocol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    pub health: ExecutorHealth,
    pub seen_context: Vec<String>,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionState {
    pub session_key: String,
    pub default_executor: String,
    pub active_executor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_mode_override: Option<ApprovalMode>,
    pub transcript: Vec<TranscriptMessage>,
    pub executor_bindings: BTreeMap<String, ExecutorBinding>,
}

impl SessionState {
    pub fn new(session_key: impl Into<String>, default_executor: impl Into<String>) -> Self {
        let default_executor = default_executor.into();
        Self {
            session_key: session_key.into(),
            active_executor: default_executor.clone(),
            default_executor,
            approval_mode_override: None,
            transcript: Vec::new(),
            executor_bindings: BTreeMap::new(),
        }
    }

    pub fn binding_for(&self, executor: &str) -> ExecutorBinding {
        self.executor_bindings
            .get(executor)
            .cloned()
            .unwrap_or_default()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
