use async_trait::async_trait;

use crate::session::{ContextArtifactRecord, ContextSyncRequest};

#[async_trait]
pub(crate) trait ChannelContextResolver {
    async fn resolve(
        &self,
        request: ChannelContextResolveRequest,
    ) -> anyhow::Result<ChannelContextResolveResult>;
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelContextResolveRequest {
    pub session_key: String,
    pub existing_artifacts: Vec<ContextArtifactRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelContextResolveResult {
    pub sync_request: ContextSyncRequest,
    pub succeeded_cache_keys: Vec<String>,
    pub failed_cache_keys: Vec<String>,
}
