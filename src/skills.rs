//! Router-wide skills published by authenticated agents.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, ensure};
use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path as UrlPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::agent_api::AgentScope;

const MAX_FILES: usize = 64;
const MAX_BYTES: usize = 1_048_576;
const MAX_PATH_BYTES: usize = 240;
const MAX_DEPTH: usize = 16;
const METADATA_FILE: &str = ".agent-router-skill.json";

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct SkillSummary {
    pub name: String,
    pub description: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillFile {
    pub content_base64: String,
    #[serde(default)]
    pub executable: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct SharedSkill {
    pub name: String,
    pub description: String,
    pub files: BTreeMap<String, SkillFile>,
}

impl SharedSkill {
    fn summary(&self) -> SkillSummary {
        SkillSummary {
            name: self.name.clone(),
            description: self.description.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSkill {
    pub name: String,
    pub files: BTreeMap<String, SkillFile>,
}

struct DecodedFile {
    content: Vec<u8>,
    executable: bool,
}

struct ValidatedSkill {
    summary: SkillSummary,
    files: BTreeMap<String, DecodedFile>,
}

#[derive(Debug)]
pub struct SkillStore {
    root: PathBuf,
    publish_lock: Mutex<()>,
}

impl SkillStore {
    pub fn open(workspace_root: &Path) -> anyhow::Result<Self> {
        create_directory_tree(workspace_root)?;
        let workspace_root = workspace_root.canonicalize()?;
        require_directory(&workspace_root)?;
        let parent = workspace_root.join(".agent-router");
        create_directory(&parent)?;
        let root = parent.join("skills");
        create_directory(&root)?;
        Ok(Self {
            root,
            publish_lock: Mutex::new(()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn list(&self) -> anyhow::Result<Vec<SkillSummary>> {
        Ok(self.snapshot()?.iter().map(SharedSkill::summary).collect())
    }

    pub fn read(&self, name: &str) -> anyhow::Result<Option<SharedSkill>> {
        validate_name(name)?;
        self.check_root()?;
        let directory = self.root.join(name);
        match fs::symlink_metadata(&directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
            Ok(_) => require_directory(&directory)?,
        }
        let mut files = BTreeMap::new();
        let mut bytes = 0;
        let mut entries = 0;
        let executable_files = read_metadata(&directory)?;
        read_files(
            &directory,
            "",
            &executable_files,
            &mut files,
            &mut bytes,
            &mut entries,
        )?;
        ensure!(
            executable_files.iter().all(|path| files.contains_key(path)),
            "skill metadata references missing files"
        );
        let skill = validate_skill(CreateSkill {
            name: name.into(),
            files: files.clone(),
        })?;
        Ok(Some(SharedSkill {
            name: skill.summary.name,
            description: skill.summary.description,
            files,
        }))
    }

    pub fn snapshot(&self) -> anyhow::Result<Vec<SharedSkill>> {
        self.check_root()?;
        let mut skills = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("skill name is not UTF-8"))?;
            // Staging directories are never visible to readers. The built-in
            // skill is maintained separately and cannot be published by agents.
            if name.starts_with('.') || name == "agent-router" {
                continue;
            }
            if let Some(skill) = self.read(&name)? {
                skills.push(skill);
            }
        }
        skills.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(skills)
    }

    fn check_root(&self) -> anyhow::Result<()> {
        require_directory(self.root.parent().unwrap().parent().unwrap())?;
        require_directory(self.root.parent().unwrap())?;
        require_directory(&self.root)
    }

    #[cfg(test)]
    pub(crate) fn create(&self, request: CreateSkill) -> anyhow::Result<Option<SkillSummary>> {
        self.publish(validate_skill(request)?)
    }

    fn publish(&self, skill: ValidatedSkill) -> anyhow::Result<Option<SkillSummary>> {
        let _guard = self
            .publish_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("skill publication lock is poisoned"))?;
        self.check_root()?;
        let destination = self.root.join(&skill.summary.name);
        match fs::symlink_metadata(&destination) {
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let staging = tempfile::Builder::new()
            .prefix(".staging-")
            .tempdir_in(&self.root)?;
        write_decoded_files(staging.path(), &skill.files)?;
        let executable_files: BTreeSet<_> = skill
            .files
            .iter()
            .filter(|(_, file)| file.executable)
            .map(|(path, _)| path)
            .collect();
        fs::write(
            staging.path().join(METADATA_FILE),
            serde_json::to_vec(&executable_files)?,
        )?;
        fs::rename(staging.path(), destination)?;
        Ok(Some(skill.summary))
    }
}

pub fn routes(store: Arc<SkillStore>) -> Router {
    Router::new()
        .route(
            "/api/skills",
            get(list)
                .post(create)
                .layer(DefaultBodyLimit::max(2_097_152)),
        )
        .route("/api/skills/{name}", get(read))
        .with_state(store)
}

async fn list(
    State(store): State<Arc<SkillStore>>,
    Extension(_scope): Extension<AgentScope>,
) -> Response {
    match blocking(move || store.list()).await {
        Ok(skills) => Json(json!({"skills": skills})).into_response(),
        Err(error) => storage_failure(error),
    }
}

async fn read(
    State(store): State<Arc<SkillStore>>,
    Extension(_scope): Extension<AgentScope>,
    UrlPath(name): UrlPath<String>,
) -> Response {
    if let Err(error) = validate_name(&name) {
        return failure(StatusCode::BAD_REQUEST, &error.to_string());
    }
    match blocking(move || store.read(&name)).await {
        Ok(Some(skill)) => Json(skill).into_response(),
        Ok(None) => failure(StatusCode::NOT_FOUND, "skill not found"),
        Err(error) => storage_failure(error),
    }
}

async fn create(
    State(store): State<Arc<SkillStore>>,
    Extension(_scope): Extension<AgentScope>,
    Json(request): Json<CreateSkill>,
) -> Response {
    let skill = match validate_skill(request) {
        Ok(skill) => skill,
        Err(error) => return failure(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    match blocking(move || store.publish(skill)).await {
        Ok(Some(summary)) => (StatusCode::CREATED, Json(summary)).into_response(),
        Ok(None) => failure(StatusCode::CONFLICT, "skill already exists"),
        Err(error) => storage_failure(error),
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    tokio::task::spawn_blocking(work).await?
}

fn storage_failure(error: anyhow::Error) -> Response {
    tracing::error!(%error, "shared skill storage failed");
    failure(
        StatusCode::INTERNAL_SERVER_ERROR,
        "could not access shared skill storage",
    )
}

fn failure(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 64
            && name.split('-').all(|part| !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())),
        "skill name must be lowercase kebab-case, with at most 64 bytes"
    );
    ensure!(
        name != "agent-router",
        "agent-router is a reserved skill name"
    );
    validate_path(name)
}

fn validate_skill(request: CreateSkill) -> anyhow::Result<ValidatedSkill> {
    validate_name(&request.name)?;
    let files = decode_files(&request.files)?;
    let document = files.get("SKILL.md").context("SKILL.md is required")?;
    let document = std::str::from_utf8(&document.content).context("SKILL.md must be UTF-8")?;
    let mut lines = document.lines();
    ensure!(
        lines.next() == Some("---"),
        "SKILL.md must start with YAML frontmatter"
    );
    let mut frontmatter = String::new();
    let mut terminated = false;
    for line in lines {
        if line == "---" {
            terminated = true;
            break;
        }
        frontmatter.push_str(line);
        frontmatter.push('\n');
    }
    ensure!(terminated, "SKILL.md YAML frontmatter must end with ---");
    #[derive(Deserialize)]
    struct Metadata {
        name: String,
        description: String,
    }
    let metadata: Metadata =
        serde_yaml::from_str(&frontmatter).context("invalid SKILL.md YAML frontmatter")?;
    ensure!(
        metadata.name == request.name,
        "SKILL.md name must match the requested name"
    );
    let description = metadata.description.trim();
    ensure!(
        !description.is_empty() && description.chars().count() <= 1024,
        "skill description must contain 1 to 1024 characters"
    );
    Ok(ValidatedSkill {
        summary: SkillSummary {
            name: request.name,
            description: description.into(),
        },
        files,
    })
}

fn validate_path(path: &str) -> anyhow::Result<()> {
    // Reserve the top-level name for both files and directories so a package
    // accepted on a case-sensitive filesystem remains readable afterwards.
    let root_component = path.split('/').next().unwrap();
    ensure!(
        !root_component.eq_ignore_ascii_case(METADATA_FILE),
        ".agent-router-skill.json is reserved for Router metadata"
    );
    ensure!(
        !path.is_empty() && path.len() <= MAX_PATH_BYTES && path.split('/').count() <= MAX_DEPTH,
        "skill paths must contain 1 to 240 bytes and at most 16 components"
    );
    for part in path.split('/') {
        ensure!(
            !part.is_empty()
                && part != "."
                && part != ".."
                && !part.ends_with('.')
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte)),
            "skill paths must be relative portable paths using letters, digits, '.', '_' or '-'"
        );
        let stem = part.split('.').next().unwrap().to_ascii_lowercase();
        let numbered_device = stem.len() == 4
            && (stem.starts_with("com") || stem.starts_with("lpt"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9');
        ensure!(
            !matches!(stem.as_str(), "con" | "prn" | "aux" | "nul") && !numbered_device,
            "skill paths cannot contain reserved device names"
        );
    }
    Ok(())
}

fn decode_files(
    files: &BTreeMap<String, SkillFile>,
) -> anyhow::Result<BTreeMap<String, DecodedFile>> {
    ensure!(
        !files.is_empty() && files.len() <= MAX_FILES,
        "a skill must contain 1 to 64 files"
    );
    let file_names: BTreeSet<_> = files.keys().map(|path| path.to_ascii_lowercase()).collect();
    ensure!(
        file_names.len() == files.len(),
        "skill paths collide ignoring case"
    );
    let mut component_names = BTreeMap::new();
    let mut bytes = 0;
    let mut decoded = BTreeMap::new();
    for (path, file) in files {
        validate_path(path)?;
        let components: Vec<_> = path.split('/').collect();
        for index in 1..=components.len() {
            let prefix = components[..index].join("/");
            let normalized = prefix.to_ascii_lowercase();
            if index < components.len() {
                ensure!(
                    !file_names.contains(&normalized),
                    "a skill path cannot be both a file and a directory"
                );
            }
            if let Some(previous) = component_names.insert(normalized, prefix.clone()) {
                ensure!(previous == prefix, "skill paths collide ignoring case");
            }
        }
        let content = STANDARD
            .decode(&file.content_base64)
            .context("skill file content must be standard base64")?;
        bytes += content.len();
        ensure!(
            bytes <= MAX_BYTES,
            "decoded skill files must total at most 1 MiB"
        );
        decoded.insert(
            path.clone(),
            DecodedFile {
                content,
                executable: file.executable,
            },
        );
    }
    Ok(decoded)
}

pub(crate) fn write_files(
    directory: &Path,
    files: &BTreeMap<String, SkillFile>,
) -> anyhow::Result<()> {
    write_decoded_files(directory, &decode_files(files)?)
}

fn write_decoded_files(
    directory: &Path,
    files: &BTreeMap<String, DecodedFile>,
) -> anyhow::Result<()> {
    require_directory(directory)?;
    for (relative, file) in files {
        let mut path = directory.to_owned();
        let components: Vec<_> = relative.split('/').collect();
        for component in &components[..components.len() - 1] {
            path.push(component);
            create_directory(&path)?;
        }
        path.push(components.last().unwrap());
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        output.write_all(&file.content)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            output.set_permissions(fs::Permissions::from_mode(if file.executable {
                0o755
            } else {
                0o644
            }))?;
        }
        #[cfg(not(unix))]
        let _ = file.executable;
    }
    Ok(())
}

fn require_directory(path: &Path) -> anyhow::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && fs::read_link(path).is_err(),
        "skill storage requires plain directories"
    );
    Ok(())
}

fn create_directory(path: &Path) -> anyhow::Result<()> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => require_directory(path),
        Err(error) => Err(error.into()),
    }
}

fn create_directory_tree(path: &Path) -> anyhow::Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    match fs::symlink_metadata(path) {
        Ok(_) => require_directory(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                create_directory_tree(parent)?;
            }
            create_directory(path)
        }
        Err(error) => Err(error.into()),
    }
}

fn read_metadata(directory: &Path) -> anyhow::Result<BTreeSet<String>> {
    let path = directory.join(METADATA_FILE);
    let metadata = fs::symlink_metadata(&path)?;
    ensure!(
        metadata.is_file() && fs::read_link(&path).is_err(),
        "invalid skill metadata file"
    );
    let mut bytes = Vec::new();
    fs::File::open(path)?.take(32_769).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 32_768, "skill metadata is too large");
    let executable_files: BTreeSet<String> = serde_json::from_slice(&bytes)?;
    ensure!(
        executable_files.len() <= MAX_FILES,
        "skill metadata has too many files"
    );
    for path in &executable_files {
        validate_path(path)?;
    }
    Ok(executable_files)
}

fn read_files(
    directory: &Path,
    prefix: &str,
    executable_files: &BTreeSet<String>,
    files: &mut BTreeMap<String, SkillFile>,
    bytes: &mut usize,
    entries: &mut usize,
) -> anyhow::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        *entries += 1;
        ensure!(
            *entries <= MAX_FILES * MAX_DEPTH,
            "too many skill directory entries"
        );
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("skill path is not UTF-8"))?;
        let relative = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if relative == METADATA_FILE {
            continue;
        }
        validate_path(&relative)?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            fs::read_link(&path).is_err(),
            "skill files cannot contain symbolic links"
        );
        if metadata.is_dir() {
            read_files(&path, &relative, executable_files, files, bytes, entries)?;
            continue;
        }
        ensure!(
            metadata.is_file(),
            "skill contents must be plain files or directories"
        );
        ensure!(
            files.len() < MAX_FILES,
            "a skill must contain at most 64 files"
        );
        let mut content = Vec::new();
        fs::File::open(&path)?
            .take((MAX_BYTES - *bytes + 1) as u64)
            .read_to_end(&mut content)?;
        *bytes += content.len();
        ensure!(
            *bytes <= MAX_BYTES,
            "decoded skill files must total at most 1 MiB"
        );
        let executable = executable_files.contains(&relative);
        files.insert(
            relative,
            SkillFile {
                content_base64: STANDARD.encode(content),
                executable,
            },
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
