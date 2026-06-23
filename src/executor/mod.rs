pub mod acp;

use async_trait::async_trait;

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
    pub updates: Vec<ExecutorUpdate>,
}

#[async_trait]
pub trait ExecutorBackend: Send + Sync + 'static {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor>;
    fn list(&self) -> Vec<ExecutorDescriptor>;
    async fn prepare(&self, request: ExecutorPrepareRequest) -> anyhow::Result<PreparedExecutor>;
    async fn prompt(&self, request: ExecutorPromptRequest) -> anyhow::Result<ExecutorResponse>;
}

#[cfg(test)]
pub mod test_support {
    use std::{collections::BTreeMap, sync::Arc};

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::{
        ExecutorBackend, ExecutorDescriptor, ExecutorPrepareRequest, ExecutorPromptRequest,
        ExecutorResponse, ExecutorUpdate, PreparedExecutor,
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

        async fn prompt(&self, request: ExecutorPromptRequest) -> anyhow::Result<ExecutorResponse> {
            self.prompts.lock().await.push(ExecutorRequest {
                session_key: request.session_key,
                executor: request.executor,
                prompt: request.prompt,
            });
            Ok(ExecutorResponse {
                final_text: "fake response".to_string(),
                updates: vec![ExecutorUpdate {
                    kind: "plan".to_string(),
                    title: "Plan".to_string(),
                    text: "working".to_string(),
                    status: String::new(),
                }],
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
