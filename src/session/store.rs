use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::SessionState;

#[async_trait]
pub trait SessionStore: Send + Sync + 'static {
    async fn load(&self, session_key: &str) -> Option<SessionState>;
    async fn load_or_create(&self, session_key: &str, default_executor: &str) -> SessionState;
    async fn save(&self, state: SessionState);
}

#[derive(Debug, Default, Clone)]
pub struct InMemorySessionStore {
    inner: Arc<RwLock<HashMap<String, SessionState>>>,
}

#[async_trait]
impl SessionStore for InMemorySessionStore {
    async fn load(&self, session_key: &str) -> Option<SessionState> {
        let guard = self.inner.read().await;
        guard.get(session_key).cloned()
    }

    async fn load_or_create(&self, session_key: &str, default_executor: &str) -> SessionState {
        let mut guard = self.inner.write().await;
        guard
            .entry(session_key.to_string())
            .or_insert_with(|| SessionState::new(session_key, default_executor))
            .clone()
    }

    async fn save(&self, state: SessionState) {
        let mut guard = self.inner.write().await;
        guard.insert(state.session_key.clone(), state);
    }
}
