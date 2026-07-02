use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Write,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::machine::session_workspace_dir_name;

use super::{
    AgentRoutingMode, ExecutorBinding, ExecutorHealth, SessionState, TranscriptMessage, now_ms,
};

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const ROUTER_METADATA_DIR: &str = ".agent-router";
const SESSION_SNAPSHOT_FILE: &str = "session.json";

#[async_trait]
pub trait SessionStore: Send + Sync + 'static {
    async fn load(&self, session_key: &str) -> anyhow::Result<Option<SessionState>>;
    async fn load_or_create(
        &self,
        session_key: &str,
        default_executor: &str,
    ) -> anyhow::Result<SessionState>;
    async fn save(&self, state: SessionState) -> anyhow::Result<()>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemorySessionStore {
    inner: Arc<RwLock<HashMap<String, SessionState>>>,
}

#[async_trait]
impl SessionStore for InMemorySessionStore {
    async fn load(&self, session_key: &str) -> anyhow::Result<Option<SessionState>> {
        let guard = self.inner.read().await;
        Ok(guard.get(session_key).cloned())
    }

    async fn load_or_create(
        &self,
        session_key: &str,
        default_executor: &str,
    ) -> anyhow::Result<SessionState> {
        let mut guard = self.inner.write().await;
        Ok(guard
            .entry(session_key.to_string())
            .or_insert_with(|| SessionState::new(session_key, default_executor))
            .clone())
    }

    async fn save(&self, state: SessionState) -> anyhow::Result<()> {
        let mut guard = self.inner.write().await;
        guard.insert(state.session_key.clone(), state);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ProductionSessionStore {
    InMemory(InMemorySessionStore),
    Workspace(WorkspaceSessionStore),
}

impl ProductionSessionStore {
    pub fn new(
        workspace_root: Option<PathBuf>,
        default_executor: String,
        configured_executors: BTreeSet<String>,
    ) -> Self {
        match workspace_root {
            Some(root) => Self::Workspace(WorkspaceSessionStore::new(
                root,
                default_executor,
                configured_executors,
            )),
            None => Self::InMemory(InMemorySessionStore::default()),
        }
    }
}

#[async_trait]
impl SessionStore for ProductionSessionStore {
    async fn load(&self, session_key: &str) -> anyhow::Result<Option<SessionState>> {
        match self {
            Self::InMemory(store) => store.load(session_key).await,
            Self::Workspace(store) => store.load(session_key).await,
        }
    }

    async fn load_or_create(
        &self,
        session_key: &str,
        default_executor: &str,
    ) -> anyhow::Result<SessionState> {
        match self {
            Self::InMemory(store) => store.load_or_create(session_key, default_executor).await,
            Self::Workspace(store) => store.load_or_create(session_key, default_executor).await,
        }
    }

    async fn save(&self, state: SessionState) -> anyhow::Result<()> {
        match self {
            Self::InMemory(store) => store.save(state).await,
            Self::Workspace(store) => store.save(state).await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkspaceSessionStore {
    workspace_root: PathBuf,
    default_executor: String,
    configured_executors: BTreeSet<String>,
    inner: Arc<RwLock<HashMap<String, SessionState>>>,
}

impl WorkspaceSessionStore {
    pub fn new(
        workspace_root: impl Into<PathBuf>,
        default_executor: impl Into<String>,
        mut configured_executors: BTreeSet<String>,
    ) -> Self {
        let default_executor = default_executor.into();
        configured_executors.insert(default_executor.clone());
        Self {
            workspace_root: workspace_root.into(),
            default_executor,
            configured_executors,
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn session_dir(&self, session_key: &str) -> PathBuf {
        self.workspace_root
            .join(session_workspace_dir_name(session_key))
    }

    fn snapshot_path(&self, session_key: &str) -> PathBuf {
        self.session_dir(session_key)
            .join(ROUTER_METADATA_DIR)
            .join(SESSION_SNAPSHOT_FILE)
    }

    fn read_snapshot(&self, session_key: &str) -> anyhow::Result<Option<SessionSnapshot>> {
        let path = self.snapshot_path(session_key);
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                let snapshot: SessionSnapshot = serde_json::from_str(&text).map_err(|err| {
                    anyhow::anyhow!("parse session snapshot {}: {err}", path.display())
                })?;
                validate_snapshot(&snapshot, session_key, &path)?;
                Ok(Some(snapshot))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(anyhow::anyhow!(
                "read session snapshot {}: {}",
                path.display(),
                err
            )),
        }
    }

    fn state_from_snapshot(
        &self,
        session_key: &str,
        snapshot: SessionSnapshot,
    ) -> anyhow::Result<SessionState> {
        let session_dir = self.session_dir(session_key);
        ensure_dir_path_without_symlinks(&session_dir)?;
        let default_executor = if self
            .configured_executors
            .contains(&snapshot.default_executor)
        {
            snapshot.default_executor
        } else {
            tracing::warn!(
                session_key,
                persisted_default_executor = %snapshot.default_executor,
                configured_default_executor = %self.default_executor,
                "persisted default executor is no longer configured; using configured default"
            );
            self.default_executor.clone()
        };
        let active_executor = match snapshot.active_executor {
            Some(executor) if self.configured_executors.contains(&executor) => Some(executor),
            Some(executor) => {
                tracing::warn!(
                    session_key,
                    persisted_active_executor = %executor,
                    fallback_executor = %default_executor,
                    "persisted active executor is no longer configured; using resolved default"
                );
                Some(default_executor.clone())
            }
            None => None,
        };

        Ok(SessionState {
            session_key: snapshot.session_key,
            default_executor,
            active_executor,
            routing_mode: snapshot.routing_mode,
            active_executor_revision: 0,
            approval_mode_override: None,
            cwd: Some(session_dir),
            transcript: snapshot.transcript,
            context_artifacts: Vec::new(),
            machine_workspaces: BTreeMap::new(),
            executor_bindings: snapshot
                .executor_bindings
                .into_iter()
                .map(|(executor, binding)| {
                    (
                        executor,
                        ExecutorBinding {
                            protocol: binding.protocol,
                            machine_id: binding.machine_id,
                            external_session_id: binding.external_session_id,
                            cwd: binding.cwd,
                            health: ExecutorHealth::Unknown,
                            seen_context: binding.seen_context,
                            metadata: BTreeMap::new(),
                        },
                    )
                })
                .collect(),
        })
    }
}

#[async_trait]
impl SessionStore for WorkspaceSessionStore {
    async fn load(&self, session_key: &str) -> anyhow::Result<Option<SessionState>> {
        if let Some(state) = self.inner.read().await.get(session_key).cloned() {
            return Ok(Some(state));
        }
        let Some(state) = self
            .read_snapshot(session_key)?
            .map(|snapshot| self.state_from_snapshot(session_key, snapshot))
            .transpose()?
        else {
            return Ok(None);
        };
        self.inner
            .write()
            .await
            .insert(session_key.to_string(), state.clone());
        Ok(Some(state))
    }

    async fn load_or_create(
        &self,
        session_key: &str,
        default_executor: &str,
    ) -> anyhow::Result<SessionState> {
        if let Some(state) = self.load(session_key).await? {
            return Ok(state);
        }
        let state = SessionState::new(session_key, default_executor);
        self.save(state.clone()).await?;
        Ok(state)
    }

    async fn save(&self, state: SessionState) -> anyhow::Result<()> {
        let existing = self.read_snapshot(&state.session_key)?;
        let cache_state = state.clone();
        let now = now_ms();
        let snapshot = SessionSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            session_key: state.session_key.clone(),
            default_executor: state.default_executor,
            active_executor: state.active_executor,
            routing_mode: state.routing_mode,
            created_at_ms: existing
                .map(|snapshot| snapshot.created_at_ms)
                .unwrap_or(now),
            updated_at_ms: now,
            transcript: state.transcript,
            executor_bindings: state
                .executor_bindings
                .into_iter()
                .map(|(executor, binding)| {
                    (
                        executor,
                        PersistedExecutorBinding {
                            protocol: binding.protocol,
                            machine_id: binding.machine_id,
                            external_session_id: binding.external_session_id,
                            cwd: binding.cwd,
                            seen_context: binding.seen_context,
                        },
                    )
                })
                .collect(),
        };
        let metadata_dir = self
            .session_dir(&snapshot.session_key)
            .join(ROUTER_METADATA_DIR);
        ensure_dir_path_without_symlinks(&metadata_dir)?;
        write_snapshot_atomically(&metadata_dir.join(SESSION_SNAPSHOT_FILE), &snapshot)?;
        self.inner
            .write()
            .await
            .insert(cache_state.session_key.clone(), cache_state);
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionSnapshot {
    schema_version: u32,
    session_key: String,
    default_executor: String,
    active_executor: Option<String>,
    #[serde(default)]
    routing_mode: AgentRoutingMode,
    created_at_ms: u64,
    updated_at_ms: u64,
    transcript: Vec<TranscriptMessage>,
    executor_bindings: BTreeMap<String, PersistedExecutorBinding>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedExecutorBinding {
    protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    machine_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    external_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(default)]
    seen_context: Vec<String>,
}

fn validate_snapshot(
    snapshot: &SessionSnapshot,
    expected_session_key: &str,
    path: &Path,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        snapshot.schema_version == SNAPSHOT_SCHEMA_VERSION,
        "session snapshot {} has unsupported schema version {}",
        path.display(),
        snapshot.schema_version
    );
    anyhow::ensure!(
        snapshot.session_key == expected_session_key,
        "session snapshot {} belongs to `{}`, not `{}`",
        path.display(),
        snapshot.session_key,
        expected_session_key
    );
    anyhow::ensure!(
        !snapshot.default_executor.trim().is_empty(),
        "session snapshot {} has empty default_executor",
        path.display()
    );
    Ok(())
}

fn write_snapshot_atomically(path: &Path, snapshot: &SessionSnapshot) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    let temp_path = path.with_file_name(format!(
        ".{}.{}.tmp",
        SESSION_SNAPSHOT_FILE,
        std::process::id()
    ));
    {
        let mut file = std::fs::File::create(&temp_path).map_err(|err| {
            anyhow::anyhow!("create session snapshot {}: {err}", temp_path.display())
        })?;
        file.write_all(&bytes).map_err(|err| {
            anyhow::anyhow!("write session snapshot {}: {err}", temp_path.display())
        })?;
        file.write_all(b"\n").map_err(|err| {
            anyhow::anyhow!("write session snapshot {}: {err}", temp_path.display())
        })?;
        file.sync_all().map_err(|err| {
            anyhow::anyhow!("flush session snapshot {}: {err}", temp_path.display())
        })?;
    }
    match std::fs::rename(&temp_path, path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            std::fs::remove_file(path).map_err(|remove_err| {
                anyhow::anyhow!("replace session snapshot {}: {remove_err}", path.display())
            })?;
            std::fs::rename(&temp_path, path).map_err(|rename_err| {
                anyhow::anyhow!("replace session snapshot {}: {rename_err}", path.display())
            })
        }
        Err(err) => Err(anyhow::anyhow!(
            "replace session snapshot {}: {}",
            path.display(),
            err
        )),
    }
}

fn ensure_dir_path_without_symlinks(path: &Path) -> anyhow::Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir => continue,
            Component::ParentDir => {
                anyhow::bail!(
                    "session workspace must not contain parent components: {}",
                    path.display()
                );
            }
            Component::Normal(segment) => {
                current.push(segment);
                match std::fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        anyhow::bail!(
                            "session workspace component is a symlink: {}",
                            current.display()
                        );
                    }
                    Ok(metadata) if metadata.is_dir() => {}
                    Ok(_) => {
                        anyhow::bail!(
                            "session workspace component is not a directory: {}",
                            current.display()
                        );
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        if let Err(err) = std::fs::create_dir(&current)
                            && err.kind() != std::io::ErrorKind::AlreadyExists
                        {
                            return Err(anyhow::anyhow!(
                                "create session workspace directory {}: {}",
                                current.display(),
                                err
                            ));
                        }
                        let metadata = std::fs::symlink_metadata(&current).map_err(|err| {
                            anyhow::anyhow!(
                                "stat session workspace directory {}: {}",
                                current.display(),
                                err
                            )
                        })?;
                        anyhow::ensure!(
                            metadata.is_dir() && !metadata.file_type().is_symlink(),
                            "session workspace component is invalid after create: {}",
                            current.display()
                        );
                    }
                    Err(err) => {
                        return Err(anyhow::anyhow!(
                            "stat session workspace component {}: {}",
                            current.display(),
                            err
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::ContextArtifactRecord;
    use serde_json::json;

    fn make_store(root: &Path) -> WorkspaceSessionStore {
        WorkspaceSessionStore::new(
            root,
            "kimi",
            ["kimi".to_string(), "codex".to_string()]
                .into_iter()
                .collect(),
        )
    }

    #[tokio::test]
    async fn workspace_store_round_trips_transcript_and_binding() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        state.routing_mode = AgentRoutingMode::Manual;
        state.set_active_executor(Some("codex".to_string()));
        state.transcript.push(TranscriptMessage::user("hi"));
        state.transcript.push(TranscriptMessage::assistant(
            "done",
            "codex",
            Some("ext-1".to_string()),
        ));
        state.executor_bindings.insert(
            "codex".to_string(),
            ExecutorBinding {
                protocol: "app_server".to_string(),
                machine_id: Some("local".to_string()),
                external_session_id: Some("ext-1".to_string()),
                cwd: Some("/tmp/session".to_string()),
                health: ExecutorHealth::Healthy,
                seen_context: vec!["seen-1".to_string()],
                metadata: BTreeMap::from([("secret".to_string(), json!("not persisted"))]),
            },
        );

        store.save(state).await.unwrap();
        let restarted_store = make_store(tmp.path());
        let reloaded = restarted_store.load("slack:C1:T1").await.unwrap().unwrap();

        assert_eq!(reloaded.active_executor.as_deref(), Some("codex"));
        assert_eq!(reloaded.routing_mode, AgentRoutingMode::Manual);
        assert_eq!(reloaded.transcript.len(), 2);
        assert_eq!(reloaded.transcript[0].content, "hi");
        assert_eq!(reloaded.transcript[1].content, "done");
        let binding = reloaded.executor_bindings.get("codex").unwrap();
        assert_eq!(binding.external_session_id.as_deref(), Some("ext-1"));
        assert_eq!(binding.seen_context, ["seen-1"]);
        assert_eq!(binding.health, ExecutorHealth::Unknown);
        assert!(binding.metadata.is_empty());
        assert!(reloaded.cwd.unwrap().starts_with(tmp.path()));
    }

    #[tokio::test]
    async fn load_or_create_keeps_existing_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let mut state = SessionState::new("web:s1", "kimi");
        state.set_active_executor(Some("codex".to_string()));
        store.save(state).await.unwrap();

        let reloaded = store.load_or_create("web:s1", "kimi").await.unwrap();

        assert_eq!(reloaded.active_executor.as_deref(), Some("codex"));
    }

    #[tokio::test]
    async fn runtime_only_fields_stay_cached_but_not_persisted() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let mut state = SessionState::new("web:s1", "kimi");
        state.context_artifacts.push(ContextArtifactRecord {
            id: "file-1".to_string(),
            source: "slack".to_string(),
            kind: "file".to_string(),
            title: "file.txt".to_string(),
            source_locator: None,
            paths: vec!["slack/file.txt".to_string()],
            fingerprint: "fingerprint".to_string(),
            updated_at_ms: 10,
            metadata: BTreeMap::new(),
        });
        store.save(state).await.unwrap();

        let cached = store.load("web:s1").await.unwrap().unwrap();
        let restarted = make_store(tmp.path());
        let reloaded = restarted.load("web:s1").await.unwrap().unwrap();

        assert_eq!(cached.context_artifacts.len(), 1);
        assert!(reloaded.context_artifacts.is_empty());
    }

    #[tokio::test]
    async fn legacy_snapshot_without_routing_mode_loads_as_auto() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let path = store.snapshot_path("web:s1");
        ensure_dir_path_without_symlinks(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "session_key": "web:s1",
                "default_executor": "kimi",
                "active_executor": "kimi",
                "created_at_ms": 1,
                "updated_at_ms": 2,
                "transcript": [],
                "executor_bindings": {}
            }))
            .unwrap(),
        )
        .unwrap();

        let state = store.load("web:s1").await.unwrap().unwrap();

        assert_eq!(state.routing_mode, AgentRoutingMode::Auto);
    }

    #[tokio::test]
    async fn mismatched_session_key_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let path = store.snapshot_path("web:s1");
        ensure_dir_path_without_symlinks(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "session_key": "web:other",
                "default_executor": "kimi",
                "active_executor": "kimi",
                "created_at_ms": 1,
                "updated_at_ms": 2,
                "transcript": [],
                "executor_bindings": {}
            }))
            .unwrap(),
        )
        .unwrap();

        let err = store.load("web:s1").await.unwrap_err();

        assert!(err.to_string().contains("belongs to `web:other`"));
    }

    #[tokio::test]
    async fn unknown_executors_fall_back_on_load() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let path = store.snapshot_path("web:s1");
        ensure_dir_path_without_symlinks(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "session_key": "web:s1",
                "default_executor": "removed-default",
                "active_executor": "removed-active",
                "created_at_ms": 1,
                "updated_at_ms": 2,
                "transcript": [],
                "executor_bindings": {}
            }))
            .unwrap(),
        )
        .unwrap();

        let state = store.load("web:s1").await.unwrap().unwrap();

        assert_eq!(state.default_executor, "kimi");
        assert_eq!(state.active_executor.as_deref(), Some("kimi"));
    }
}
