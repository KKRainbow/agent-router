pub mod acp;

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ExecutorDescriptor {
    pub name: String,
    pub protocol: String,
}

#[derive(Debug, Clone)]
pub struct ExecutorRequest {
    pub session_key: String,
    pub executor: String,
    pub prompt: String,
    pub previous_session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorUpdate {
    pub kind: String,
    pub title: String,
    pub text: String,
    pub status: String,
}

impl ExecutorUpdate {
    pub fn summary(&self, limit: usize) -> Option<String> {
        let label = if self.title.is_empty() {
            self.kind.as_str()
        } else {
            self.title.as_str()
        };
        let detail = if self.text.is_empty() {
            self.status.as_str()
        } else {
            self.text.as_str()
        };
        let mut text = if detail.is_empty() {
            label.to_string()
        } else {
            format!("{label}: {detail}")
        };
        if text.trim().is_empty() {
            return None;
        }
        if limit > 3 && text.len() > limit {
            text.truncate(limit - 3);
            text.push_str("...");
        }
        Some(text)
    }
}

#[derive(Debug, Clone)]
pub struct ExecutorResponse {
    pub final_text: String,
    pub external_session_id: Option<String>,
    pub updates: Vec<ExecutorUpdate>,
    pub started_new_session: bool,
}

#[async_trait]
pub trait ExecutorBackend: Send + Sync + 'static {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor>;
    fn list(&self) -> Vec<ExecutorDescriptor>;
    async fn prompt(&self, request: ExecutorRequest) -> anyhow::Result<ExecutorResponse>;
}

#[cfg(test)]
pub mod test_support {
    use std::{collections::BTreeMap, sync::Arc};

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::{
        ExecutorBackend, ExecutorDescriptor, ExecutorRequest, ExecutorResponse, ExecutorUpdate,
    };

    #[derive(Debug, Default)]
    pub struct FakeExecutorBackend {
        pub prompts: Arc<Mutex<Vec<ExecutorRequest>>>,
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

        async fn prompt(&self, request: ExecutorRequest) -> anyhow::Result<ExecutorResponse> {
            self.prompts.lock().await.push(request);
            Ok(ExecutorResponse {
                final_text: "fake response".to_string(),
                external_session_id: Some("fake-session".to_string()),
                updates: vec![ExecutorUpdate {
                    kind: "plan".to_string(),
                    title: "Plan".to_string(),
                    text: "working".to_string(),
                    status: String::new(),
                }],
                started_new_session: false,
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
