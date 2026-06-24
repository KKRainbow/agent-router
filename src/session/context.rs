use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::now_ms;

#[derive(Debug, Clone)]
pub struct ContextSyncRequest {
    pub session_key: String,
    pub source: String,
    pub base_path: PathBuf,
    pub artifacts: Vec<ContextArtifactInput>,
    pub remove_artifacts: Vec<ContextArtifactRemovalInput>,
    pub unresolved: Vec<ContextSyncIssueInput>,
}

#[derive(Debug, Clone)]
pub struct ContextArtifactInput {
    pub id: String,
    pub kind: String,
    pub title: String,
    pub source_locator: Option<String>,
    pub files: Vec<ContextFileInput>,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct ContextFileInput {
    pub relative_path: PathBuf,
    pub content: ContextFileContent,
}

#[derive(Debug, Clone)]
pub enum ContextArtifactRemovalInput {
    Exact {
        kind: String,
        id: String,
    },
    ExceptKind {
        kind: String,
        retain_ids: BTreeSet<String>,
    },
    Kind {
        kind: String,
    },
}

#[derive(Debug, Clone)]
pub enum ContextFileContent {
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSyncIssueInput {
    pub kind: String,
    pub reference: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextArtifactRecord {
    pub id: String,
    pub source: String,
    pub kind: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_locator: Option<String>,
    pub paths: Vec<String>,
    pub fingerprint: String,
    pub updated_at_ms: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
struct ContextManifest<'a> {
    version: u32,
    source: &'a str,
    session_key: &'a str,
    synced_at_ms: u64,
    artifacts: &'a [ContextArtifactRecord],
    unresolved: &'a [ContextSyncIssueInput],
}

#[derive(Debug, Deserialize)]
struct ContextManifestSnapshot {
    source: String,
    session_key: String,
    synced_at_ms: u64,
    #[serde(default)]
    artifacts: Vec<ContextArtifactRecord>,
    #[serde(default)]
    unresolved: Vec<ContextSyncIssueInput>,
}

#[derive(Debug, Clone, Serialize)]
struct ContextFileManifest {
    path: String,
    size_bytes: usize,
    sha256: String,
}

pub fn write_context_sync(
    cwd: &Path,
    request: ContextSyncRequest,
    existing: &[ContextArtifactRecord],
) -> anyhow::Result<Vec<ContextArtifactRecord>> {
    let synced_at_ms = now_ms();
    validate_relative_path(&request.base_path)?;
    let resolved_issue_keys = request
        .artifacts
        .iter()
        .flat_map(context_resolved_issue_keys)
        .collect::<BTreeSet<_>>();
    let unresolved = merge_context_unresolved(
        read_existing_unresolved(
            cwd,
            &request.source,
            &request.session_key,
            &request.base_path,
            existing,
        )?,
        request.unresolved,
        &resolved_issue_keys,
    );
    let source_records = existing
        .iter()
        .filter(|record| record.source == request.source && record.kind != "manifest")
        .cloned()
        .collect::<Vec<_>>();
    let mut removed_artifacts = BTreeSet::new();
    let mut retained_artifacts_by_kind = BTreeMap::new();
    let mut removed_kinds = BTreeSet::new();
    for removal in request.remove_artifacts {
        match removal {
            ContextArtifactRemovalInput::Exact { kind, id } => {
                removed_artifacts.insert((kind, id));
            }
            ContextArtifactRemovalInput::ExceptKind { kind, retain_ids } => {
                retained_artifacts_by_kind.insert(kind, retain_ids);
            }
            ContextArtifactRemovalInput::Kind { kind } => {
                removed_kinds.insert(kind);
            }
        }
    }
    let (removed_source_records, retained_source_records): (Vec<_>, Vec<_>) =
        source_records.into_iter().partition(|record| {
            if removed_kinds.contains(&record.kind)
                || removed_artifacts.contains(&(record.kind.clone(), record.id.clone()))
            {
                return true;
            }
            if let Some(retain_ids) = retained_artifacts_by_kind.get(&record.kind) {
                return !retain_ids.contains(&record.id);
            }
            false
        });
    remove_context_artifact_files(cwd, &request.base_path, &removed_source_records)?;
    let mut source_records = retained_source_records;
    for record in &source_records {
        for path in &record.paths {
            validate_source_context_path(&request.base_path, Path::new(path))?;
        }
    }
    let mut updates = Vec::new();

    for artifact in request.artifacts {
        let mut path_records = Vec::new();
        let mut file_manifest = Vec::new();
        for file in artifact.files {
            validate_source_context_path(&request.base_path, &file.relative_path)?;
            let content = file.content.into_bytes();
            let hash = sha256_hex(&content);
            let path = cwd.join(&file.relative_path);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create context directory {}", parent.display()))?;
            }
            std::fs::write(&path, &content)
                .with_context(|| format!("write context file {}", path.display()))?;
            let relative_path = file.relative_path.display().to_string();
            path_records.push(relative_path.clone());
            file_manifest.push(ContextFileManifest {
                path: relative_path,
                size_bytes: content.len(),
                sha256: hash,
            });
        }

        let fingerprint = artifact_fingerprint(
            &request.source,
            &artifact.id,
            &artifact.kind,
            &file_manifest,
            &artifact.metadata,
        );
        updates.push(ContextArtifactRecord {
            id: artifact.id,
            source: request.source.clone(),
            kind: artifact.kind,
            title: artifact.title,
            source_locator: artifact.source_locator,
            paths: path_records,
            fingerprint,
            updated_at_ms: synced_at_ms,
            metadata: artifact.metadata,
        });
    }

    merge_context_artifacts(&mut source_records, updates);

    let manifest = ContextManifest {
        version: 1,
        source: &request.source,
        session_key: &request.session_key,
        synced_at_ms,
        artifacts: &source_records,
        unresolved: &unresolved,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let manifest_fingerprint = context_manifest_fingerprint(
        &request.source,
        &request.session_key,
        &source_records,
        &unresolved,
    );
    let manifest_rel = request.base_path.join("manifest.json");
    let manifest_path = cwd.join(&manifest_rel);
    if let Some(parent) = manifest_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create context directory {}", parent.display()))?;
    }
    std::fs::write(&manifest_path, &manifest_bytes)
        .with_context(|| format!("write context manifest {}", manifest_path.display()))?;

    let manifest_record = ContextArtifactRecord {
        id: format!("{}:manifest", request.source),
        source: request.source,
        kind: "manifest".to_string(),
        title: "Synced context manifest".to_string(),
        source_locator: None,
        paths: vec![manifest_rel.display().to_string()],
        fingerprint: manifest_fingerprint,
        updated_at_ms: synced_at_ms,
        metadata: context_manifest_metadata(&unresolved),
    };
    let mut records = existing
        .iter()
        .filter(|record| record.source != manifest_record.source)
        .cloned()
        .collect::<Vec<_>>();
    records.push(manifest_record);
    records.extend(source_records);
    records.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.id.cmp(&right.id))
    });

    Ok(records)
}

pub fn read_context_artifacts_from_manifest(
    cwd: &Path,
    source: &str,
    session_key: &str,
    base_path: &Path,
) -> anyhow::Result<Vec<ContextArtifactRecord>> {
    validate_relative_path(base_path)?;
    let manifest_rel = base_path.join("manifest.json");
    validate_relative_path(&manifest_rel)?;
    let manifest_path = cwd.join(&manifest_rel);
    let bytes = match std::fs::read(&manifest_path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("read context manifest {}", manifest_path.display()));
        }
    };
    let snapshot = serde_json::from_slice::<ContextManifestSnapshot>(&bytes)
        .with_context(|| format!("parse context manifest {}", manifest_path.display()))?;
    if snapshot.source != source || snapshot.session_key != session_key {
        return Ok(Vec::new());
    }
    for artifact in &snapshot.artifacts {
        anyhow::ensure!(
            artifact.source == snapshot.source,
            "context manifest artifact source mismatch: expected {}, got {}",
            snapshot.source,
            artifact.source
        );
        let mut file_manifest = Vec::new();
        for path in &artifact.paths {
            validate_source_context_path(base_path, Path::new(path))?;
            let artifact_path = cwd.join(path);
            let bytes = std::fs::read(&artifact_path)
                .with_context(|| format!("read context artifact {}", artifact_path.display()))?;
            file_manifest.push(ContextFileManifest {
                path: path.clone(),
                size_bytes: bytes.len(),
                sha256: sha256_hex(&bytes),
            });
        }
        let fingerprint = artifact_fingerprint(
            &artifact.source,
            &artifact.id,
            &artifact.kind,
            &file_manifest,
            &artifact.metadata,
        );
        anyhow::ensure!(
            fingerprint == artifact.fingerprint,
            "context manifest artifact fingerprint mismatch: {}",
            artifact.id
        );
    }
    let mut records = Vec::with_capacity(snapshot.artifacts.len() + 1);
    let manifest_fingerprint = context_manifest_fingerprint(
        &snapshot.source,
        &snapshot.session_key,
        &snapshot.artifacts,
        &snapshot.unresolved,
    );
    records.push(ContextArtifactRecord {
        id: format!("{}:manifest", snapshot.source),
        source: snapshot.source,
        kind: "manifest".to_string(),
        title: "Synced context manifest".to_string(),
        source_locator: None,
        paths: vec![manifest_rel.display().to_string()],
        fingerprint: manifest_fingerprint,
        updated_at_ms: snapshot.synced_at_ms,
        metadata: context_manifest_metadata(&snapshot.unresolved),
    });
    records.extend(snapshot.artifacts);
    records.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(records)
}

pub fn merge_context_artifacts(
    existing: &mut Vec<ContextArtifactRecord>,
    updates: Vec<ContextArtifactRecord>,
) {
    for update in updates {
        if let Some(existing) = existing.iter_mut().find(|record| {
            record.source == update.source && record.kind == update.kind && record.id == update.id
        }) {
            *existing = update;
        } else {
            existing.push(update);
        }
    }
}

fn remove_context_artifact_files(
    cwd: &Path,
    base_path: &Path,
    records: &[ContextArtifactRecord],
) -> anyhow::Result<()> {
    let base_abs = cwd.join(base_path);
    for record in records {
        for path in &record.paths {
            validate_source_context_path(base_path, Path::new(path))?;
            let path = cwd.join(path);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("remove context artifact {}", path.display()));
                }
            }
            prune_empty_context_dirs(path.parent(), &base_abs)?;
        }
    }
    Ok(())
}

fn prune_empty_context_dirs(mut dir: Option<&Path>, base_path: &Path) -> anyhow::Result<()> {
    while let Some(path) = dir {
        if path == base_path {
            break;
        }
        if !path.starts_with(base_path) {
            break;
        }
        match std::fs::remove_dir(path) {
            Ok(()) => dir = path.parent(),
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                break;
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("remove empty context directory {}", path.display()));
            }
        }
    }
    Ok(())
}

fn read_existing_unresolved(
    cwd: &Path,
    source: &str,
    session_key: &str,
    base_path: &Path,
    existing: &[ContextArtifactRecord],
) -> anyhow::Result<Vec<ContextSyncIssueInput>> {
    let Some(manifest) = existing
        .iter()
        .find(|record| record.source == source && record.kind == "manifest")
    else {
        return Ok(Vec::new());
    };
    let Some(manifest_path) = manifest.paths.first() else {
        return Ok(Vec::new());
    };
    let manifest_rel = PathBuf::from(manifest_path);
    if validate_source_context_path(base_path, &manifest_rel).is_err() {
        return Ok(Vec::new());
    }
    let path = cwd.join(&manifest_rel);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("read context manifest {}", path.display()));
        }
    };
    let snapshot = serde_json::from_slice::<ContextManifestSnapshot>(&bytes)
        .with_context(|| format!("parse context manifest {}", path.display()))?;
    if snapshot.source != source || snapshot.session_key != session_key {
        return Ok(Vec::new());
    }
    Ok(snapshot.unresolved)
}

fn merge_context_unresolved(
    existing: Vec<ContextSyncIssueInput>,
    current: Vec<ContextSyncIssueInput>,
    resolved_issue_keys: &BTreeSet<(String, String)>,
) -> Vec<ContextSyncIssueInput> {
    let mut by_key = BTreeMap::new();
    for issue in existing.into_iter().chain(current) {
        let key = context_issue_key(&issue);
        if resolved_issue_keys.contains(&key) {
            continue;
        }
        by_key.insert(key, issue);
    }
    by_key.into_values().collect()
}

fn context_issue_key(issue: &ContextSyncIssueInput) -> (String, String) {
    (issue.kind.clone(), issue.reference.clone())
}

fn context_manifest_metadata(unresolved: &[ContextSyncIssueInput]) -> BTreeMap<String, Value> {
    let mut metadata = BTreeMap::new();
    metadata.insert("unresolved_count".to_string(), json!(unresolved.len()));
    metadata.insert(
        "unresolved".to_string(),
        json!(
            unresolved
                .iter()
                .map(|issue| json!({
                    "kind": &issue.kind,
                    "reference": &issue.reference,
                }))
                .collect::<Vec<_>>()
        ),
    );
    metadata
}

fn context_resolved_issue_keys(artifact: &ContextArtifactInput) -> Vec<(String, String)> {
    artifact
        .metadata
        .get("resolves_unresolved")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            Some((
                entry.get("kind")?.as_str()?.to_string(),
                entry.get("reference")?.as_str()?.to_string(),
            ))
        })
        .collect()
}

pub fn sanitize_path_segment(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch);
        } else if !out.ends_with('-') {
            out.push('-');
        }
        if out.len() >= 96 {
            break;
        }
    }
    let out = out
        .trim_matches(|ch| matches!(ch, '.' | '-' | '_'))
        .to_string();
    if out.is_empty() {
        "item".to_string()
    } else {
        out
    }
}

fn validate_relative_path(path: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(!path.as_os_str().is_empty(), "context path is empty");
    anyhow::ensure!(
        !path.is_absolute(),
        "context path must be relative: {}",
        path.display()
    );
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => anyhow::bail!(
                "context path contains invalid component: {}",
                path.display()
            ),
        }
    }
    Ok(())
}

fn validate_source_context_path(base_path: &Path, path: &Path) -> anyhow::Result<()> {
    validate_relative_path(path)?;
    anyhow::ensure!(
        path.starts_with(base_path),
        "context path must be under base path {}: {}",
        base_path.display(),
        path.display()
    );
    Ok(())
}

fn artifact_fingerprint(
    source: &str,
    id: &str,
    kind: &str,
    files: &[ContextFileManifest],
    metadata: &BTreeMap<String, Value>,
) -> String {
    let payload = json!({
        "source": source,
        "id": id,
        "kind": kind,
        "files": files,
        "metadata": metadata,
    });
    format!(
        "artifact:{}",
        &sha256_hex(payload.to_string().as_bytes())[..24]
    )
}

fn context_manifest_fingerprint(
    source: &str,
    session_key: &str,
    records: &[ContextArtifactRecord],
    unresolved: &[ContextSyncIssueInput],
) -> String {
    let payload = json!({
        "source": source,
        "session_key": session_key,
        "artifacts": records.iter().map(|record| &record.fingerprint).collect::<Vec<_>>(),
        "unresolved": unresolved,
    });
    format!(
        "artifact:{}",
        &sha256_hex(payload.to_string().as_bytes())[..24]
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("{digest:x}")
}

impl ContextFileContent {
    fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Text(text) => text.into_bytes(),
            Self::Bytes(bytes) => bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_path_segment_removes_path_syntax() {
        assert_eq!(sanitize_path_segment("../hello world.md"), "hello-world.md");
        assert_eq!(sanitize_path_segment(""), "item");
    }

    #[test]
    fn write_context_sync_rejects_parent_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: vec![ContextArtifactInput {
                id: "bad".to_string(),
                kind: "file".to_string(),
                title: "bad".to_string(),
                source_locator: None,
                files: vec![ContextFileInput {
                    relative_path: PathBuf::from("../bad"),
                    content: ContextFileContent::Text("bad".to_string()),
                }],
                metadata: BTreeMap::new(),
            }],
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };

        assert!(write_context_sync(tmp.path(), request, &[]).is_err());
    }

    #[test]
    fn write_context_sync_rejects_paths_outside_base_path() {
        let tmp = tempfile::tempdir().unwrap();
        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: vec![ContextArtifactInput {
                id: "bad".to_string(),
                kind: "file".to_string(),
                title: "bad".to_string(),
                source_locator: None,
                files: vec![ContextFileInput {
                    relative_path: PathBuf::from("other/context.md"),
                    content: ContextFileContent::Text("bad".to_string()),
                }],
                metadata: BTreeMap::new(),
            }],
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };

        assert!(write_context_sync(tmp.path(), request, &[]).is_err());
    }

    #[test]
    fn write_context_sync_rejects_existing_paths_outside_base_path() {
        let tmp = tempfile::tempdir().unwrap();
        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };
        let existing = vec![ContextArtifactRecord {
            id: "slack:file:F1".to_string(),
            source: "slack".to_string(),
            kind: "slack_file".to_string(),
            title: "Slack file F1".to_string(),
            source_locator: None,
            paths: vec!["other/context.md".to_string()],
            fingerprint: "artifact:file".to_string(),
            updated_at_ms: 1,
            metadata: BTreeMap::new(),
        }];

        assert!(write_context_sync(tmp.path(), request, &existing).is_err());
    }

    #[test]
    fn write_context_sync_manifest_includes_existing_source_artifacts() {
        let tmp = tempfile::tempdir().unwrap();
        let first = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: vec![ContextArtifactInput {
                id: "slack:thread:C1:111.000".to_string(),
                kind: "slack_current_thread".to_string(),
                title: "Current Slack thread".to_string(),
                source_locator: None,
                files: vec![ContextFileInput {
                    relative_path: PathBuf::from("slack/current-thread.md"),
                    content: ContextFileContent::Text("thread".to_string()),
                }],
                metadata: BTreeMap::new(),
            }],
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };
        let records = write_context_sync(tmp.path(), first, &[]).unwrap();
        let second = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: Vec::new(),
            unresolved: vec![ContextSyncIssueInput {
                kind: "current_thread_cache".to_string(),
                reference: "C1:111.000".to_string(),
                reason: "rate_limited".to_string(),
            }],
        };

        let records = write_context_sync(tmp.path(), second, &records).unwrap();
        let manifest = std::fs::read_to_string(tmp.path().join("slack/manifest.json")).unwrap();
        let manifest = serde_json::from_str::<Value>(&manifest).unwrap();

        assert!(
            records
                .iter()
                .any(|record| record.kind == "slack_current_thread")
        );
        assert_eq!(manifest["artifacts"].as_array().unwrap().len(), 1);
        assert_eq!(
            manifest["artifacts"][0]["kind"].as_str(),
            Some("slack_current_thread")
        );
        assert_eq!(manifest["unresolved"].as_array().unwrap().len(), 1);
        let manifest_record = records
            .iter()
            .find(|record| record.kind == "manifest")
            .unwrap();
        let unresolved = manifest_record
            .metadata
            .get("unresolved")
            .and_then(Value::as_array)
            .unwrap();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0]["kind"].as_str(), Some("current_thread_cache"));
        assert_eq!(unresolved[0]["reference"].as_str(), Some("C1:111.000"));
    }

    #[test]
    fn write_context_sync_removes_requested_source_artifacts() {
        let tmp = tempfile::tempdir().unwrap();
        let first = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: vec![ContextArtifactInput {
                id: "slack:thread:C1:111.000".to_string(),
                kind: "slack_current_thread".to_string(),
                title: "Current Slack thread".to_string(),
                source_locator: None,
                files: vec![ContextFileInput {
                    relative_path: PathBuf::from("slack/current-thread.md"),
                    content: ContextFileContent::Text("thread".to_string()),
                }],
                metadata: BTreeMap::new(),
            }],
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };
        let records = write_context_sync(tmp.path(), first, &[]).unwrap();
        let removal = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: vec![ContextArtifactRemovalInput::Exact {
                id: "slack:thread:C1:111.000".to_string(),
                kind: "slack_current_thread".to_string(),
            }],
            unresolved: Vec::new(),
        };

        let records = write_context_sync(tmp.path(), removal, &records).unwrap();

        assert!(
            !records
                .iter()
                .any(|record| record.kind == "slack_current_thread")
        );
        assert!(!tmp.path().join("slack/current-thread.md").exists());
        let manifest = std::fs::read_to_string(tmp.path().join("slack/manifest.json")).unwrap();
        let manifest = serde_json::from_str::<Value>(&manifest).unwrap();
        assert_eq!(manifest["artifacts"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn write_context_sync_removes_all_requested_source_artifact_kinds() {
        let tmp = tempfile::tempdir().unwrap();
        let existing = vec![
            ContextArtifactRecord {
                id: "slack:linked-thread:C2:222.000".to_string(),
                source: "slack".to_string(),
                kind: "slack_linked_thread".to_string(),
                title: "Linked Slack thread".to_string(),
                source_locator: None,
                paths: vec!["slack/linked-threads/C2-222.000.md".to_string()],
                fingerprint: "artifact:linked".to_string(),
                updated_at_ms: 1,
                metadata: BTreeMap::new(),
            },
            ContextArtifactRecord {
                id: "slack:file:F1".to_string(),
                source: "slack".to_string(),
                kind: "slack_file".to_string(),
                title: "Slack file F1".to_string(),
                source_locator: None,
                paths: vec!["slack/files/F1/metadata.json".to_string()],
                fingerprint: "artifact:file".to_string(),
                updated_at_ms: 1,
                metadata: BTreeMap::new(),
            },
        ];
        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: vec![ContextArtifactRemovalInput::Kind {
                kind: "slack_linked_thread".to_string(),
            }],
            unresolved: Vec::new(),
        };

        let records = write_context_sync(tmp.path(), request, &existing).unwrap();

        assert!(
            !records
                .iter()
                .any(|record| record.kind == "slack_linked_thread")
        );
        assert!(records.iter().any(|record| record.kind == "slack_file"));
    }

    #[test]
    fn write_context_sync_removes_requested_kind_except_retained_artifacts() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("slack/linked-threads")).unwrap();
        std::fs::write(
            tmp.path().join("slack/linked-threads/C2-222.000.md"),
            "retained",
        )
        .unwrap();
        std::fs::write(
            tmp.path().join("slack/linked-threads/C3-333.000.md"),
            "stale",
        )
        .unwrap();
        let existing = vec![
            ContextArtifactRecord {
                id: "slack:linked-thread:C2:222.000".to_string(),
                source: "slack".to_string(),
                kind: "slack_linked_thread".to_string(),
                title: "Linked Slack thread".to_string(),
                source_locator: None,
                paths: vec!["slack/linked-threads/C2-222.000.md".to_string()],
                fingerprint: "artifact:linked-retained".to_string(),
                updated_at_ms: 1,
                metadata: BTreeMap::new(),
            },
            ContextArtifactRecord {
                id: "slack:linked-thread:C3:333.000".to_string(),
                source: "slack".to_string(),
                kind: "slack_linked_thread".to_string(),
                title: "Stale linked Slack thread".to_string(),
                source_locator: None,
                paths: vec!["slack/linked-threads/C3-333.000.md".to_string()],
                fingerprint: "artifact:linked-stale".to_string(),
                updated_at_ms: 1,
                metadata: BTreeMap::new(),
            },
            ContextArtifactRecord {
                id: "slack:file:F1".to_string(),
                source: "slack".to_string(),
                kind: "slack_file".to_string(),
                title: "Slack file F1".to_string(),
                source_locator: None,
                paths: vec!["slack/files/F1/metadata.json".to_string()],
                fingerprint: "artifact:file".to_string(),
                updated_at_ms: 1,
                metadata: BTreeMap::new(),
            },
        ];
        let retained = ["slack:linked-thread:C2:222.000".to_string()]
            .into_iter()
            .collect();
        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: vec![ContextArtifactRemovalInput::ExceptKind {
                kind: "slack_linked_thread".to_string(),
                retain_ids: retained,
            }],
            unresolved: Vec::new(),
        };

        let records = write_context_sync(tmp.path(), request, &existing).unwrap();

        assert!(
            records
                .iter()
                .any(|record| record.id == "slack:linked-thread:C2:222.000")
        );
        assert!(
            !records
                .iter()
                .any(|record| record.id == "slack:linked-thread:C3:333.000")
        );
        assert!(records.iter().any(|record| record.id == "slack:file:F1"));
        assert!(
            tmp.path()
                .join("slack/linked-threads/C2-222.000.md")
                .exists()
        );
        assert!(
            !tmp.path()
                .join("slack/linked-threads/C3-333.000.md")
                .exists()
        );
    }

    #[test]
    fn write_context_sync_removes_empty_artifact_directories() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("slack/files/F1/original")).unwrap();
        std::fs::write(tmp.path().join("slack/files/F1/metadata.json"), "{}").unwrap();
        std::fs::write(
            tmp.path().join("slack/files/F1/original/file.txt"),
            "content",
        )
        .unwrap();
        let existing = vec![ContextArtifactRecord {
            id: "slack:file:F1".to_string(),
            source: "slack".to_string(),
            kind: "slack_file".to_string(),
            title: "Slack file F1".to_string(),
            source_locator: None,
            paths: vec![
                "slack/files/F1/metadata.json".to_string(),
                "slack/files/F1/original/file.txt".to_string(),
            ],
            fingerprint: "artifact:file".to_string(),
            updated_at_ms: 1,
            metadata: BTreeMap::new(),
        }];
        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: vec![ContextArtifactRemovalInput::Exact {
                id: "slack:file:F1".to_string(),
                kind: "slack_file".to_string(),
            }],
            unresolved: Vec::new(),
        };

        let records = write_context_sync(tmp.path(), request, &existing).unwrap();

        assert!(!records.iter().any(|record| record.id == "slack:file:F1"));
        assert!(!tmp.path().join("slack/files/F1").exists());
        assert!(!tmp.path().join("slack/files").exists());
        assert!(tmp.path().join("slack").exists());
    }

    #[test]
    fn write_context_sync_preserves_and_resolves_unresolved_issues() {
        let tmp = tempfile::tempdir().unwrap();
        let failed = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: Vec::new(),
            unresolved: vec![ContextSyncIssueInput {
                kind: "file".to_string(),
                reference: "F1".to_string(),
                reason: "file has no private download URL".to_string(),
            }],
        };
        let records = write_context_sync(tmp.path(), failed, &[]).unwrap();

        let unchanged = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };
        let records = write_context_sync(tmp.path(), unchanged, &records).unwrap();
        let manifest = std::fs::read_to_string(tmp.path().join("slack/manifest.json")).unwrap();
        let manifest = serde_json::from_str::<Value>(&manifest).unwrap();
        assert_eq!(manifest["unresolved"].as_array().unwrap().len(), 1);

        let mut metadata = BTreeMap::new();
        metadata.insert(
            "resolves_unresolved".to_string(),
            json!([{"kind": "file", "reference": "F1"}]),
        );
        let resolved = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: vec![ContextArtifactInput {
                id: "slack:file:F1".to_string(),
                kind: "slack_file".to_string(),
                title: "Slack file F1".to_string(),
                source_locator: None,
                files: vec![ContextFileInput {
                    relative_path: PathBuf::from("slack/files/F1/metadata.json"),
                    content: ContextFileContent::Text("{}".to_string()),
                }],
                metadata,
            }],
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };

        write_context_sync(tmp.path(), resolved, &records).unwrap();
        let manifest = std::fs::read_to_string(tmp.path().join("slack/manifest.json")).unwrap();
        let manifest = serde_json::from_str::<Value>(&manifest).unwrap();

        assert_eq!(manifest["unresolved"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn write_context_sync_ignores_existing_manifest_outside_base_path() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("other")).unwrap();
        std::fs::write(
            tmp.path().join("other/manifest.json"),
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "source": "slack",
                "session_key": "s1",
                "synced_at_ms": 1,
                "artifacts": [],
                "unresolved": [{
                    "kind": "file",
                    "reference": "F1",
                    "reason": "wrong manifest"
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let existing = vec![ContextArtifactRecord {
            id: "slack:manifest".to_string(),
            source: "slack".to_string(),
            kind: "manifest".to_string(),
            title: "Bad manifest".to_string(),
            source_locator: None,
            paths: vec!["other/manifest.json".to_string()],
            fingerprint: "artifact:bad-manifest".to_string(),
            updated_at_ms: 1,
            metadata: BTreeMap::new(),
        }];

        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };

        let records = write_context_sync(tmp.path(), request, &existing).unwrap();
        let manifest = std::fs::read_to_string(tmp.path().join("slack/manifest.json")).unwrap();
        let manifest = serde_json::from_str::<Value>(&manifest).unwrap();

        assert_eq!(manifest["unresolved"].as_array().unwrap().len(), 0);
        assert_eq!(
            records
                .iter()
                .find(|record| record.kind == "manifest")
                .unwrap()
                .paths,
            vec!["slack/manifest.json".to_string()]
        );
    }

    #[test]
    fn write_context_sync_ignores_existing_manifest_for_other_source() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("slack")).unwrap();
        std::fs::write(
            tmp.path().join("slack/manifest.json"),
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "source": "other",
                "session_key": "s1",
                "synced_at_ms": 1,
                "artifacts": [],
                "unresolved": [{
                    "kind": "file",
                    "reference": "F1",
                    "reason": "wrong source"
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let existing = vec![ContextArtifactRecord {
            id: "slack:manifest".to_string(),
            source: "slack".to_string(),
            kind: "manifest".to_string(),
            title: "Wrong-source manifest".to_string(),
            source_locator: None,
            paths: vec!["slack/manifest.json".to_string()],
            fingerprint: "artifact:wrong-source-manifest".to_string(),
            updated_at_ms: 1,
            metadata: BTreeMap::new(),
        }];

        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };

        write_context_sync(tmp.path(), request, &existing).unwrap();
        let manifest = std::fs::read_to_string(tmp.path().join("slack/manifest.json")).unwrap();
        let manifest = serde_json::from_str::<Value>(&manifest).unwrap();

        assert_eq!(manifest["source"].as_str(), Some("slack"));
        assert_eq!(manifest["unresolved"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn read_context_artifacts_from_manifest_rejects_invalid_artifact_paths() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("slack")).unwrap();
        std::fs::write(
            tmp.path().join("slack/manifest.json"),
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "source": "slack",
                "session_key": "s1",
                "synced_at_ms": 1,
                "artifacts": [{
                    "id": "slack:file:F1",
                    "source": "slack",
                    "kind": "slack_file",
                    "title": "Slack file F1",
                    "paths": ["../secret.txt"],
                    "fingerprint": "artifact:file",
                    "updated_at_ms": 1
                }],
                "unresolved": []
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(
            read_context_artifacts_from_manifest(tmp.path(), "slack", "s1", Path::new("slack"))
                .is_err()
        );
    }

    #[test]
    fn read_context_artifacts_from_manifest_ignores_other_session_key() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("slack/files/F1")).unwrap();
        std::fs::write(tmp.path().join("slack/files/F1/metadata.json"), "{}").unwrap();
        std::fs::write(
            tmp.path().join("slack/manifest.json"),
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "source": "slack",
                "session_key": "other-session",
                "synced_at_ms": 1,
                "artifacts": [{
                    "id": "slack:file:F1",
                    "source": "slack",
                    "kind": "slack_file",
                    "title": "Slack file F1",
                    "paths": ["slack/files/F1/metadata.json"],
                    "fingerprint": "artifact:file",
                    "updated_at_ms": 1
                }],
                "unresolved": []
            }))
            .unwrap(),
        )
        .unwrap();

        let records =
            read_context_artifacts_from_manifest(tmp.path(), "slack", "s1", Path::new("slack"))
                .unwrap();

        assert!(records.is_empty());
    }

    #[test]
    fn read_context_artifacts_from_manifest_rejects_missing_artifact_files() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("slack")).unwrap();
        std::fs::write(
            tmp.path().join("slack/manifest.json"),
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "source": "slack",
                "session_key": "s1",
                "synced_at_ms": 1,
                "artifacts": [{
                    "id": "slack:file:F1",
                    "source": "slack",
                    "kind": "slack_file",
                    "title": "Slack file F1",
                    "paths": ["slack/files/F1/metadata.json"],
                    "fingerprint": "artifact:file",
                    "updated_at_ms": 1
                }],
                "unresolved": []
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(
            read_context_artifacts_from_manifest(tmp.path(), "slack", "s1", Path::new("slack"))
                .is_err()
        );
    }

    #[test]
    fn read_context_artifacts_from_manifest_rejects_modified_artifact_files() {
        let tmp = tempfile::tempdir().unwrap();
        let request = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: vec![ContextArtifactInput {
                id: "slack:file:F1".to_string(),
                kind: "slack_file".to_string(),
                title: "Slack file F1".to_string(),
                source_locator: None,
                files: vec![ContextFileInput {
                    relative_path: PathBuf::from("slack/files/F1/metadata.json"),
                    content: ContextFileContent::Text("{\"name\":\"one\"}".to_string()),
                }],
                metadata: BTreeMap::new(),
            }],
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        };
        write_context_sync(tmp.path(), request, &[]).unwrap();

        let records =
            read_context_artifacts_from_manifest(tmp.path(), "slack", "s1", Path::new("slack"))
                .unwrap();
        assert!(records.iter().any(|record| record.id == "slack:file:F1"));

        std::fs::write(
            tmp.path().join("slack/files/F1/metadata.json"),
            "{\"name\":\"two\"}",
        )
        .unwrap();

        assert!(
            read_context_artifacts_from_manifest(tmp.path(), "slack", "s1", Path::new("slack"))
                .is_err()
        );
    }

    #[test]
    fn read_context_artifacts_from_manifest_rejects_paths_outside_base_path() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("slack")).unwrap();
        std::fs::write(
            tmp.path().join("slack/manifest.json"),
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "source": "slack",
                "session_key": "s1",
                "synced_at_ms": 1,
                "artifacts": [{
                    "id": "slack:file:F1",
                    "source": "slack",
                    "kind": "slack_file",
                    "title": "Slack file F1",
                    "paths": ["other/context.md"],
                    "fingerprint": "artifact:file",
                    "updated_at_ms": 1
                }],
                "unresolved": []
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(
            read_context_artifacts_from_manifest(tmp.path(), "slack", "s1", Path::new("slack"))
                .is_err()
        );
    }
}
