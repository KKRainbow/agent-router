use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::{
    approval::SharedApprovalBroker,
    config::{ExecutorConfig, ExecutorProtocol},
    executor::{
        ExecutorBackend, ExecutorDescriptor, ExecutorEventSink, ExecutorPrepareRequest,
        ExecutorPromptRequest, ExecutorResponse, PreparedExecutor, acp::AcpExecutorManager,
        codex_app_server::CodexAppServerManager,
    },
};

#[derive(Debug)]
pub struct ExecutorRegistry {
    executors: BTreeMap<String, ExecutorConfig>,
    acp: AcpExecutorManager,
    codex_app_server: CodexAppServerManager,
}

impl ExecutorRegistry {
    pub fn new(
        executors: BTreeMap<String, ExecutorConfig>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self {
            acp: AcpExecutorManager::with_approvals(executors.clone(), approvals.clone()),
            codex_app_server: CodexAppServerManager::with_approvals(executors.clone(), approvals),
            executors,
        }
    }

    fn backend_for(&self, executor: &str) -> anyhow::Result<&dyn ExecutorBackend> {
        let cfg = self
            .executors
            .get(executor)
            .ok_or_else(|| anyhow::anyhow!("executor `{executor}` is not configured"))?;
        match cfg.protocol {
            ExecutorProtocol::Acp => Ok(&self.acp),
            ExecutorProtocol::AppServer => Ok(&self.codex_app_server),
        }
    }
}

#[async_trait]
impl ExecutorBackend for ExecutorRegistry {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.backend_for(name).ok()?.get(name)
    }

    fn list(&self) -> Vec<ExecutorDescriptor> {
        let mut executors = self.acp.list();
        executors.extend(self.codex_app_server.list());
        executors.sort_by(|left, right| left.name.cmp(&right.name));
        executors
    }

    async fn prepare(&self, request: ExecutorPrepareRequest) -> anyhow::Result<PreparedExecutor> {
        self.backend_for(&request.executor)?.prepare(request).await
    }

    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
    ) -> anyhow::Result<ExecutorResponse> {
        self.backend_for(&request.executor)?
            .prompt(request, events)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use crate::approval::ApprovalBroker;

    use super::*;

    fn mixed_executor_config() -> BTreeMap<String, ExecutorConfig> {
        BTreeMap::from([
            (
                "codex".to_string(),
                ExecutorConfig {
                    name: "codex".to_string(),
                    protocol: ExecutorProtocol::AppServer,
                    command: "codex".to_string(),
                    args: Vec::new(),
                    cwd: Some(PathBuf::from(".")),
                    env: BTreeMap::new(),
                },
            ),
            (
                "kimi".to_string(),
                ExecutorConfig {
                    name: "kimi".to_string(),
                    protocol: ExecutorProtocol::Acp,
                    command: "kimi".to_string(),
                    args: vec!["acp".to_string()],
                    cwd: None,
                    env: BTreeMap::new(),
                },
            ),
        ])
    }

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
                ("codex".to_string(), "app_server".to_string()),
                ("kimi".to_string(), "acp".to_string()),
            ]
        );
    }

    #[test]
    fn gets_executor_from_matching_backend() {
        let registry =
            ExecutorRegistry::new(mixed_executor_config(), Arc::new(ApprovalBroker::default()));

        assert_eq!(registry.get("kimi").unwrap().protocol, "acp");
        assert_eq!(registry.get("codex").unwrap().protocol, "app_server");
        assert!(registry.get("missing").is_none());
    }
}
