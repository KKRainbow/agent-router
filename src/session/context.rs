use std::{
    collections::BTreeMap,
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

#[derive(Debug, Clone, Serialize)]
struct ContextFileManifest {
    path: String,
    size_bytes: usize,
    sha256: String,
}

pub fn write_context_sync(
    cwd: &Path,
    request: ContextSyncRequest,
) -> anyhow::Result<Vec<ContextArtifactRecord>> {
    let synced_at_ms = now_ms();
    validate_relative_path(&request.base_path)?;
    let mut records = Vec::new();

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
        records.push(ContextArtifactRecord {
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

    let manifest = ContextManifest {
        version: 1,
        source: &request.source,
        session_key: &request.session_key,
        synced_at_ms,
        artifacts: &records,
        unresolved: &request.unresolved,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let manifest_fingerprint = context_manifest_fingerprint(
        &request.source,
        &request.session_key,
        &records,
        &request.unresolved,
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
    manifest_metadata.insert(
        "unresolved_count".to_string(),
        json!(request.unresolved.len()),
    );
    records.insert(
        0,
        ContextArtifactRecord {
            id: format!("{}:manifest", request.source),
            source: request.source,
            kind: "manifest".to_string(),
            title: "Synced context manifest".to_string(),
            source_locator: None,
            paths: vec![manifest_rel.display().to_string()],
            fingerprint: manifest_fingerprint,
            updated_at_ms: synced_at_ms,
            metadata: manifest_metadata,
        },
    );

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

        assert!(write_context_sync(tmp.path(), request).is_err());
    }
}
