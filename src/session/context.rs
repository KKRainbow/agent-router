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
        read_existing_unresolved(cwd, &request.source, existing)?,
        request.unresolved,
        &resolved_issue_keys,
    );
    let mut source_records = existing
        .iter()
        .filter(|record| record.source == request.source && record.kind != "manifest")
        .cloned()
        .collect::<Vec<_>>();
    let mut updates = Vec::new();

    for artifact in request.artifacts {
        let mut path_records = Vec::new();
        let mut file_manifest = Vec::new();
        for file in artifact.files {
            validate_relative_path(&file.relative_path)?;
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

    let mut manifest_metadata = BTreeMap::new();
    manifest_metadata.insert("unresolved_count".to_string(), json!(unresolved.len()));
    let manifest_record = ContextArtifactRecord {
        id: format!("{}:manifest", request.source),
        source: request.source,
        kind: "manifest".to_string(),
        title: "Synced context manifest".to_string(),
        source_locator: None,
        paths: vec![manifest_rel.display().to_string()],
        fingerprint: manifest_fingerprint,
        updated_at_ms: synced_at_ms,
        metadata: manifest_metadata,
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

fn read_existing_unresolved(
    cwd: &Path,
    source: &str,
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
    validate_relative_path(&manifest_rel)?;
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
            unresolved: Vec::new(),
        };

        assert!(write_context_sync(tmp.path(), request, &[]).is_err());
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
            unresolved: Vec::new(),
        };
        let records = write_context_sync(tmp.path(), first, &[]).unwrap();
        let second = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
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
    }

    #[test]
    fn write_context_sync_preserves_and_resolves_unresolved_issues() {
        let tmp = tempfile::tempdir().unwrap();
        let failed = ContextSyncRequest {
            session_key: "s1".to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: Vec::new(),
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
            unresolved: Vec::new(),
        };

        write_context_sync(tmp.path(), resolved, &records).unwrap();
        let manifest = std::fs::read_to_string(tmp.path().join("slack/manifest.json")).unwrap();
        let manifest = serde_json::from_str::<Value>(&manifest).unwrap();

        assert_eq!(manifest["unresolved"].as_array().unwrap().len(), 0);
    }
}
