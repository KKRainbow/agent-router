use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{
    io::AsyncWriteExt,
    process::{Child, Command},
    sync::Mutex,
};

use crate::executor::TurnCancellation;

pub const LOCAL_MACHINE_ID: &str = "local";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineConfig {
    pub id: String,
    pub kind: MachineKind,
    pub host: Option<String>,
    pub workspace_root: Option<String>,
    pub env: BTreeMap<String, String>,
    pub skill_roots: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MachineKind {
    Local,
    Ssh,
}

#[derive(Debug, Clone)]
pub struct MachineRegistry {
    machines: BTreeMap<String, MachineConfig>,
    materialization_locks: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
}

#[derive(Debug, Clone)]
pub struct MachinePrepareRequest<'a> {
    pub machine_id: &'a str,
    pub session_key: &'a str,
    pub router_workspace: Option<&'a Path>,
    pub executor_cwd: Option<&'a Path>,
    pub command: &'a str,
    pub args: &'a [String],
    pub env: &'a BTreeMap<String, String>,
    pub cancel: Option<&'a TurnCancellation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMachineCommand {
    pub machine_id: String,
    pub workspace: Option<MachineWorkspaceRecord>,
    pub stdio: StdioCommand,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdioCommand {
    pub program: String,
    pub args: Vec<String>,
    pub current_dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub env_remove: Vec<String>,
    pub executor_cwd: String,
    pub strict_json_stdout: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineWorkspaceRecord {
    pub machine_id: String,
    pub cwd: String,
    pub materialization: MachineWorkspaceMaterialization,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MachineWorkspaceMaterialization {
    #[default]
    NotNeeded,
    Materialized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectedSkill {
    pub machine_id: String,
    pub root: String,
    pub relative_path: String,
    pub metadata: Option<Value>,
    pub instructions: Result<String, String>,
}

impl MachineRegistry {
    pub fn new(mut machines: BTreeMap<String, MachineConfig>) -> Self {
        machines
            .entry(LOCAL_MACHINE_ID.to_string())
            .or_insert_with(local_machine_config);
        Self {
            machines,
            materialization_locks: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn local_default() -> Self {
        Self::new(BTreeMap::from([(
            LOCAL_MACHINE_ID.to_string(),
            local_machine_config(),
        )]))
    }

    pub fn get(&self, id: &str) -> Option<&MachineConfig> {
        self.machines.get(id)
    }

    pub fn machines(&self) -> &BTreeMap<String, MachineConfig> {
        &self.machines
    }

    pub async fn prepare_executor_command(
        &self,
        request: MachinePrepareRequest<'_>,
    ) -> anyhow::Result<PreparedMachineCommand> {
        ensure_not_cancelled(request.cancel, "machine prepare cancelled").await?;
        let machine = self
            .get(request.machine_id)
            .ok_or_else(|| anyhow::anyhow!("machine `{}` is not configured", request.machine_id))?;
        match machine.kind {
            MachineKind::Local => self.prepare_local_command(machine, request).await,
            MachineKind::Ssh => self.prepare_ssh_command(machine, request).await,
        }
    }

    pub async fn collect_skills(&self) -> Vec<CollectedSkill> {
        let mut skills = Vec::new();
        for machine in self.machines.values() {
            match machine.kind {
                MachineKind::Local => skills.extend(collect_local_skills(machine)),
                MachineKind::Ssh => skills.extend(collect_ssh_skills(machine).await),
            }
        }
        skills
    }

    async fn prepare_local_command(
        &self,
        machine: &MachineConfig,
        request: MachinePrepareRequest<'_>,
    ) -> anyhow::Result<PreparedMachineCommand> {
        let workspace = local_workspace_path(machine, &request)?;
        let cwd = workspace
            .clone()
            .or_else(|| request.executor_cwd.map(Path::to_path_buf))
            .unwrap_or(std::env::current_dir()?);
        if workspace.is_some() {
            ensure_dir_path_without_symlinks(&cwd)?;
        }

        let materialization = if let (Some(router_workspace), Some(workspace)) =
            (request.router_workspace, workspace.as_ref())
        {
            let lock = self
                .materialization_lock(&machine.id, request.session_key)
                .await;
            let _guard = lock.lock().await;
            ensure_not_cancelled(request.cancel, "machine prepare cancelled").await?;
            let router_workspace = canonicalize_lossy(router_workspace);
            let workspace = canonicalize_lossy(workspace);
            if router_workspace != workspace {
                sync_workspace_contents(&router_workspace, &workspace, request.cancel)?;
                MachineWorkspaceMaterialization::Materialized
            } else {
                MachineWorkspaceMaterialization::NotNeeded
            }
        } else {
            MachineWorkspaceMaterialization::NotNeeded
        };
        ensure_not_cancelled(request.cancel, "machine prepare cancelled").await?;

        let artifact_fingerprint = request
            .router_workspace
            .map(workspace_fingerprint)
            .transpose()?;
        let cwd = canonicalize_lossy(&cwd);
        let workspace_record = workspace.map(|_| MachineWorkspaceRecord {
            machine_id: machine.id.clone(),
            cwd: cwd.display().to_string(),
            materialization,
            artifact_fingerprint,
        });
        Ok(PreparedMachineCommand {
            machine_id: machine.id.clone(),
            workspace: workspace_record,
            stdio: StdioCommand {
                program: request.command.to_string(),
                args: request.args.to_vec(),
                current_dir: Some(cwd.clone()),
                env: layered_env(&machine.env, request.env),
                env_remove: Vec::new(),
                executor_cwd: cwd.display().to_string(),
                strict_json_stdout: false,
            },
        })
    }

    async fn prepare_ssh_command(
        &self,
        machine: &MachineConfig,
        request: MachinePrepareRequest<'_>,
    ) -> anyhow::Result<PreparedMachineCommand> {
        let host = machine
            .host
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("ssh machine `{}` has no host", machine.id))?;
        let root = machine
            .workspace_root
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("ssh machine `{}` has no workspace_root", machine.id))?;
        let remote_cwd = remote_machine_workspace_path(root, &machine.id, request.session_key);
        ensure_ssh_workspace(host, &remote_cwd).await?;
        ensure_not_cancelled(request.cancel, "machine prepare cancelled").await?;

        let artifact_fingerprint = request
            .router_workspace
            .map(workspace_fingerprint)
            .transpose()?;
        let materialization = if let Some(router_workspace) = request.router_workspace {
            let lock = self
                .materialization_lock(&machine.id, request.session_key)
                .await;
            let _guard = lock.lock().await;
            ensure_not_cancelled(request.cancel, "machine prepare cancelled").await?;
            materialize_ssh_workspace(host, router_workspace, &remote_cwd, request.cancel).await?;
            MachineWorkspaceMaterialization::Materialized
        } else {
            MachineWorkspaceMaterialization::NotNeeded
        };
        ensure_not_cancelled(request.cancel, "machine prepare cancelled").await?;

        let remote_env = layered_env(&machine.env, request.env);
        let remote_script =
            build_remote_exec_script(&remote_cwd, &remote_env, request.command, request.args)?;
        Ok(PreparedMachineCommand {
            machine_id: machine.id.clone(),
            workspace: Some(MachineWorkspaceRecord {
                machine_id: machine.id.clone(),
                cwd: remote_cwd.clone(),
                materialization,
                artifact_fingerprint,
            }),
            stdio: ssh_stdio_command(host, remote_script, remote_cwd),
        })
    }

    async fn materialization_lock(&self, machine_id: &str, session_key: &str) -> Arc<Mutex<()>> {
        let key = format!("{machine_id}:{}", session_workspace_dir_name(session_key));
        let mut locks = self.materialization_locks.lock().await;
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

impl StdioCommand {
    pub fn spawn(&self) -> anyhow::Result<Child> {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        if let Some(cwd) = &self.current_dir {
            command.current_dir(cwd);
        }
        for key in &self.env_remove {
            command.env_remove(key);
        }
        for (key, value) in &self.env {
            command.env(key, value);
        }
        command.kill_on_drop(true);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
            .spawn()
            .map_err(|err| anyhow::anyhow!("could not start command `{}`: {err}", self.program))
    }
}

pub fn local_machine_config() -> MachineConfig {
    MachineConfig {
        id: LOCAL_MACHINE_ID.to_string(),
        kind: MachineKind::Local,
        host: None,
        workspace_root: None,
        env: BTreeMap::new(),
        skill_roots: Vec::new(),
    }
}

pub fn session_workspace_dir_name(session_key: &str) -> String {
    stable_path_component(session_key, "session", 48)
}

fn machine_workspace_machine_dir_name(machine_id: &str) -> String {
    stable_path_component(machine_id, "machine", 48)
}

fn stable_path_component(value: &str, fallback: &str, max_prefix_len: usize) -> String {
    let raw_prefix = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let mut prefix = raw_prefix
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if prefix.is_empty() {
        prefix = fallback.to_string();
    }
    if prefix.len() > max_prefix_len {
        prefix.truncate(max_prefix_len);
        prefix = prefix.trim_end_matches('-').to_string();
        if prefix.is_empty() {
            prefix = fallback.to_string();
        }
    }
    let digest = Sha256::digest(value.as_bytes());
    let hash = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}-{hash}")
}

pub(crate) fn build_remote_exec_script(
    cwd: &str,
    env: &BTreeMap<String, String>,
    command: &str,
    args: &[String],
) -> anyhow::Result<String> {
    let mut parts = vec![format!("cd -- {}", shell_quote(cwd))];
    for (key, value) in env {
        ensure_env_key(key)?;
        parts.push(format!("export {key}={}", shell_quote_env_value(value)));
    }
    let mut exec = format!("exec {}", shell_quote(command));
    for arg in args {
        exec.push(' ');
        exec.push_str(&shell_quote(arg));
    }
    parts.push(exec);
    Ok(parts.join(" && "))
}

fn local_workspace_path(
    machine: &MachineConfig,
    request: &MachinePrepareRequest<'_>,
) -> anyhow::Result<Option<PathBuf>> {
    if let Some(root) = &machine.workspace_root {
        if let Some(router_workspace) = request.router_workspace {
            let router_root_workspace =
                PathBuf::from(root).join(session_workspace_dir_name(request.session_key));
            if machine.id == LOCAL_MACHINE_ID
                && paths_refer_to_same_workspace(&router_root_workspace, router_workspace)
            {
                return Ok(Some(router_workspace.to_path_buf()));
            }
        }
        return Ok(Some(machine_workspace_path(
            Path::new(root),
            &machine.id,
            request.session_key,
        )));
    }
    Ok(request.router_workspace.map(Path::to_path_buf))
}

fn machine_workspace_path(root: &Path, machine_id: &str, session_key: &str) -> PathBuf {
    root.join(machine_workspace_machine_dir_name(machine_id))
        .join(session_workspace_dir_name(session_key))
}

fn remote_machine_workspace_path(root: &str, machine_id: &str, session_key: &str) -> String {
    let machine_root = join_remote_path(root, &machine_workspace_machine_dir_name(machine_id));
    join_remote_path(&machine_root, &session_workspace_dir_name(session_key))
}

fn paths_refer_to_same_workspace(left: &Path, right: &Path) -> bool {
    left == right || canonicalize_lossy(left) == canonicalize_lossy(right)
}

fn layered_env(
    machine_env: &BTreeMap<String, String>,
    executor_env: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut env = machine_env.clone();
    env.extend(executor_env.clone());
    env
}

async fn ensure_ssh_workspace(host: &str, remote_cwd: &str) -> anyhow::Result<()> {
    let remote_command = create_remote_workspace_script(remote_cwd)?;
    let status = Command::new("ssh")
        .args(ssh_remote_args(host, &remote_command))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map_err(|err| anyhow::anyhow!("could not create SSH workspace on `{host}`: {err}"))?;
    anyhow::ensure!(
        status.success(),
        "could not create SSH workspace `{remote_cwd}` on `{host}`: ssh exited with {status}"
    );
    Ok(())
}

fn create_remote_workspace_script(remote_cwd: &str) -> anyhow::Result<String> {
    let components = remote_path_components(remote_cwd)?;
    let mut current = String::new();
    let mut commands = Vec::new();
    for component in components {
        current = if current.is_empty() {
            format!("/{component}")
        } else {
            join_remote_path(&current, &component)
        };
        let quoted = shell_quote(&current);
        commands.push(format!(
            "if [ -e {quoted} ] || [ -L {quoted} ]; then [ -d {quoted} ] && [ ! -L {quoted} ] || exit 73; else mkdir -- {quoted}; fi"
        ));
    }
    Ok(commands.join(" && "))
}

async fn materialize_ssh_workspace(
    host: &str,
    router_workspace: &Path,
    remote_cwd: &str,
    cancel: Option<&TurnCancellation>,
) -> anyhow::Result<()> {
    let mut tar = Command::new("tar")
        .args(["cf", "-", "-C"])
        .arg(router_workspace)
        .arg(".")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| {
            anyhow::anyhow!(
                "could not start workspace materialization archive for {}: {err}",
                router_workspace.display()
            )
        })?;
    let mut tar_stdout = tar.stdout.take().ok_or_else(|| {
        anyhow::anyhow!("workspace materialization archive did not expose stdout")
    })?;
    let remote_command = materialize_remote_workspace_script(remote_cwd)?;
    let mut ssh = Command::new("ssh")
        .args(ssh_remote_args(host, &remote_command))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| {
            anyhow::anyhow!("could not start SSH workspace materialization to `{host}`: {err}")
        })?;
    let mut ssh_stdin = ssh
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("SSH materialization command did not expose stdin"))?;
    let copy = if let Some(cancel) = cancel {
        tokio::select! {
            result = tokio::io::copy(&mut tar_stdout, &mut ssh_stdin) => result,
            _ = cancel.cancelled() => {
                let _ = tar.start_kill();
                let _ = ssh.start_kill();
                let _ = tar.wait().await;
                let _ = ssh.wait().await;
                anyhow::bail!("machine prepare cancelled");
            }
        }
    } else {
        tokio::io::copy(&mut tar_stdout, &mut ssh_stdin).await
    };
    let shutdown = if let Some(cancel) = cancel {
        tokio::select! {
            result = ssh_stdin.shutdown() => result,
            _ = cancel.cancelled() => {
                let _ = tar.start_kill();
                let _ = ssh.start_kill();
                let _ = tar.wait().await;
                let _ = ssh.wait().await;
                anyhow::bail!("machine prepare cancelled");
            }
        }
    } else {
        ssh_stdin.shutdown().await
    };
    ensure_not_cancelled(cancel, "machine prepare cancelled").await?;
    let tar_status = tar.wait().await?;
    let ssh_status = ssh.wait().await?;
    copy?;
    shutdown?;
    anyhow::ensure!(
        tar_status.success(),
        "workspace materialization archive failed for {}: {tar_status}",
        router_workspace.display()
    );
    anyhow::ensure!(
        ssh_status.success(),
        "workspace materialization to `{host}:{remote_cwd}` failed: {ssh_status}"
    );
    Ok(())
}

fn materialize_remote_workspace_script(remote_cwd: &str) -> anyhow::Result<String> {
    let ensure_workspace = create_remote_workspace_script(remote_cwd)?;
    let quoted = shell_quote(remote_cwd);
    Ok(format!(
        "{ensure_workspace} && cd -P -- {quoted} && [ \"$(pwd -P)\" = {quoted} ] && find . -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} + && tar xf - -C ."
    ))
}

async fn collect_ssh_skills(machine: &MachineConfig) -> Vec<CollectedSkill> {
    let Some(host) = machine.host.as_deref() else {
        return machine
            .skill_roots
            .iter()
            .map(|root| CollectedSkill {
                machine_id: machine.id.clone(),
                root: root.clone(),
                relative_path: String::new(),
                metadata: None,
                instructions: Err(format!("ssh machine `{}` has no host", machine.id)),
            })
            .collect();
    };
    let mut collected = Vec::new();
    for root in &machine.skill_roots {
        let relative_paths = match list_remote_skill_paths(host, root).await {
            Ok(relative_paths) => relative_paths,
            Err(err) => {
                collected.push(CollectedSkill {
                    machine_id: machine.id.clone(),
                    root: root.clone(),
                    relative_path: String::new(),
                    metadata: None,
                    instructions: Err(err.to_string()),
                });
                continue;
            }
        };
        for relative_path in relative_paths {
            let instructions = read_remote_skill(host, root, &relative_path)
                .await
                .map_err(|err| err.to_string());
            let metadata = instructions
                .as_ref()
                .ok()
                .and_then(|text| parse_skill_metadata(text));
            collected.push(CollectedSkill {
                machine_id: machine.id.clone(),
                root: root.clone(),
                relative_path,
                metadata,
                instructions,
            });
        }
    }
    collected
}

async fn list_remote_skill_paths(host: &str, root: &str) -> anyhow::Result<Vec<String>> {
    let script = format!(
        "root={}; [ -d \"$root\" ] || exit 66; for file in \"$root\"/*/SKILL.md; do [ -f \"$file\" ] || continue; rel=${{file#\"$root\"/}}; printf '%s\\n' \"${{rel%/SKILL.md}}\"; done",
        shell_quote(root)
    );
    let output = Command::new("ssh")
        .args(ssh_remote_args(host, &script))
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|err| anyhow::anyhow!("list remote skill root `{root}` on `{host}`: {err}"))?;
    anyhow::ensure!(
        output.status.success(),
        "list remote skill root `{root}` on `{host}` failed: {}",
        output.status
    );
    let text = String::from_utf8(output.stdout)
        .map_err(|err| anyhow::anyhow!("remote skill listing was not utf-8: {err}"))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.contains('/'))
        .map(ToOwned::to_owned)
        .collect())
}

async fn read_remote_skill(host: &str, root: &str, relative_path: &str) -> anyhow::Result<String> {
    let skill_path = join_remote_path(root, relative_path);
    let skill_file = join_remote_path(&skill_path, "SKILL.md");
    let script = format!("cat -- {}", shell_quote(&skill_file));
    let output = Command::new("ssh")
        .args(ssh_remote_args(host, &script))
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|err| anyhow::anyhow!("read remote skill `{skill_file}` on `{host}`: {err}"))?;
    anyhow::ensure!(
        output.status.success(),
        "read remote skill `{skill_file}` on `{host}` failed: {}",
        output.status
    );
    String::from_utf8(output.stdout)
        .map_err(|err| anyhow::anyhow!("remote skill `{skill_file}` was not utf-8: {err}"))
}

fn sync_workspace_contents(
    source: &Path,
    destination: &Path,
    cancel: Option<&TurnCancellation>,
) -> anyhow::Result<()> {
    ensure_dir_path_without_symlinks(destination)?;
    let mut source_names = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(source)
        .map_err(|err| anyhow::anyhow!("read router workspace {}: {err}", source.display()))?
    {
        let entry = entry?;
        source_names.insert(entry.file_name());
    }
    for entry in std::fs::read_dir(destination)
        .map_err(|err| anyhow::anyhow!("read machine workspace {}: {err}", destination.display()))?
    {
        let entry = entry?;
        if source_names.contains(&entry.file_name()) {
            continue;
        }
        if let Some(cancel) = cancel
            && cancel.is_cancelled_now()
        {
            anyhow::bail!("machine prepare cancelled");
        }
        remove_workspace_entry(&entry.path())?;
    }
    for entry in std::fs::read_dir(source)
        .map_err(|err| anyhow::anyhow!("read router workspace {}: {err}", source.display()))?
    {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = std::fs::symlink_metadata(&source_path)?;
        if metadata.file_type().is_symlink() {
            anyhow::bail!(
                "workspace materialization does not copy symlinks: {}",
                source_path.display()
            );
        }
        if metadata.is_dir() {
            if let Some(cancel) = cancel
                && cancel.is_cancelled_now()
            {
                anyhow::bail!("machine prepare cancelled");
            }
            if let Some(destination_metadata) = symlink_metadata_optional(&destination_path)? {
                let destination_is_plain_dir =
                    destination_metadata.is_dir() && !destination_metadata.file_type().is_symlink();
                if !destination_is_plain_dir {
                    remove_workspace_entry(&destination_path)?;
                }
            }
            sync_workspace_contents(&source_path, &destination_path, cancel)?;
        } else if metadata.is_file() {
            if let Some(cancel) = cancel
                && cancel.is_cancelled_now()
            {
                anyhow::bail!("machine prepare cancelled");
            }
            if let Some(destination_metadata) = symlink_metadata_optional(&destination_path)?
                && (destination_metadata.is_dir() || destination_metadata.file_type().is_symlink())
            {
                remove_workspace_entry(&destination_path)?;
            }
            std::fs::copy(&source_path, &destination_path).map_err(|err| {
                anyhow::anyhow!(
                    "copy workspace file {} to {}: {err}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
        } else {
            anyhow::bail!(
                "workspace materialization cannot copy special file: {}",
                source_path.display()
            );
        }
    }
    Ok(())
}

fn symlink_metadata_optional(path: &Path) -> anyhow::Result<Option<std::fs::Metadata>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(anyhow::anyhow!(
            "stat workspace entry {}: {err}",
            path.display()
        )),
    }
}

fn remove_workspace_entry(path: &Path) -> anyhow::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
            .map_err(|err| anyhow::anyhow!("remove stale workspace dir {}: {err}", path.display()))
    } else {
        std::fs::remove_file(path)
            .map_err(|err| anyhow::anyhow!("remove stale workspace file {}: {err}", path.display()))
    }
}

fn workspace_fingerprint(root: &Path) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    hash_workspace(root, root, &mut hasher)?;
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn hash_workspace(root: &Path, path: &Path, hasher: &mut Sha256) -> anyhow::Result<()> {
    let mut entries = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)?;
        let relative = path.strip_prefix(root).unwrap_or(path.as_path());
        hasher.update(relative.to_string_lossy().as_bytes());
        if metadata.file_type().is_symlink() {
            anyhow::bail!(
                "workspace fingerprint does not follow symlinks: {}",
                path.display()
            );
        }
        if metadata.is_dir() {
            hasher.update(b"/");
            hash_workspace(root, &path, hasher)?;
        } else if metadata.is_file() {
            hasher.update(metadata.len().to_le_bytes());
            let modified = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default();
            hasher.update(modified.to_le_bytes());
        } else {
            anyhow::bail!(
                "workspace fingerprint cannot hash special file: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn collect_local_skills(machine: &MachineConfig) -> Vec<CollectedSkill> {
    machine
        .skill_roots
        .iter()
        .flat_map(|root| match std::fs::read_dir(root) {
            Ok(entries) => entries
                .filter_map(Result::ok)
                .filter(|entry| entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false))
                .filter_map(|entry| collect_local_skill(machine, root, entry.path()))
                .collect::<Vec<_>>(),
            Err(err) => vec![CollectedSkill {
                machine_id: machine.id.clone(),
                root: root.clone(),
                relative_path: String::new(),
                metadata: None,
                instructions: Err(format!("read skill root `{root}`: {err}")),
            }],
        })
        .collect()
}

fn collect_local_skill(
    machine: &MachineConfig,
    root: &str,
    skill_path: PathBuf,
) -> Option<CollectedSkill> {
    let instructions_path = skill_path.join("SKILL.md");
    if !instructions_path.is_file() {
        return None;
    }
    let relative_path = skill_path
        .strip_prefix(root)
        .unwrap_or(skill_path.as_path())
        .to_string_lossy()
        .to_string();
    let instructions = std::fs::read_to_string(&instructions_path)
        .map_err(|err| format!("read {}: {err}", instructions_path.display()));
    let metadata = instructions
        .as_ref()
        .ok()
        .and_then(|text| parse_skill_metadata(text));
    Some(CollectedSkill {
        machine_id: machine.id.clone(),
        root: root.to_string(),
        relative_path,
        metadata,
        instructions,
    })
}

fn parse_skill_metadata(text: &str) -> Option<Value> {
    let text = text.strip_prefix("---")?;
    let (frontmatter, _) = text.split_once("---")?;
    serde_yaml::from_str(frontmatter).ok()
}

fn join_remote_path(root: &str, child: &str) -> String {
    if root == "/" {
        format!("/{child}")
    } else {
        format!("{}/{}", root.trim_end_matches('/'), child)
    }
}

fn remote_path_components(path: &str) -> anyhow::Result<Vec<String>> {
    anyhow::ensure!(
        path.starts_with('/'),
        "remote workspace path must be absolute: {path}"
    );
    let components = path
        .split('/')
        .filter(|component| !component.is_empty())
        .map(|component| {
            anyhow::ensure!(
                component != "." && component != "..",
                "remote workspace path contains invalid component: {path}"
            );
            Ok(component.to_string())
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        !components.is_empty(),
        "remote workspace path must not be root: {path}"
    );
    Ok(components)
}

fn ssh_stdio_command(host: &str, remote_script: String, remote_cwd: String) -> StdioCommand {
    StdioCommand {
        program: "ssh".to_string(),
        args: ssh_remote_args(host, &remote_script),
        current_dir: None,
        env: BTreeMap::new(),
        env_remove: Vec::new(),
        executor_cwd: remote_cwd,
        strict_json_stdout: true,
    }
}

fn ssh_remote_args(host: &str, script: &str) -> Vec<String> {
    vec![
        "-T".to_string(),
        host.to_string(),
        remote_shell_command(script),
    ]
}

fn remote_shell_command(script: &str) -> String {
    format!("sh -lc {}", shell_quote(script))
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

async fn ensure_not_cancelled(
    cancel: Option<&TurnCancellation>,
    message: &'static str,
) -> anyhow::Result<()> {
    if let Some(cancel) = cancel
        && cancel.is_cancelled().await
    {
        anyhow::bail!(message);
    }
    Ok(())
}

fn shell_quote_env_value(value: &str) -> String {
    if !value.contains("$PATH") {
        return shell_quote(value);
    }
    value
        .split("$PATH")
        .enumerate()
        .map(|(index, part)| {
            let mut value = String::new();
            if index > 0 {
                value.push_str("$PATH");
            }
            if !part.is_empty() {
                value.push_str(&shell_quote(part));
            }
            value
        })
        .collect::<Vec<_>>()
        .join("")
}

fn ensure_env_key(key: &str) -> anyhow::Result<()> {
    let mut chars = key.chars();
    let first = chars
        .next()
        .ok_or_else(|| anyhow::anyhow!("environment key must not be empty"))?;
    anyhow::ensure!(
        first == '_' || first.is_ascii_alphabetic(),
        "environment key `{key}` is not a portable shell identifier"
    );
    anyhow::ensure!(
        chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()),
        "environment key `{key}` is not a portable shell identifier"
    );
    Ok(())
}

fn canonicalize_lossy(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
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
                    "machine workspace must not contain parent components: {}",
                    path.display()
                );
            }
            Component::Normal(segment) => {
                current.push(segment);
                match std::fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        anyhow::bail!(
                            "machine workspace component is a symlink: {}",
                            current.display()
                        );
                    }
                    Ok(metadata) if metadata.is_dir() => {}
                    Ok(_) => {
                        anyhow::bail!(
                            "machine workspace component is not a directory: {}",
                            current.display()
                        );
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        if let Err(err) = std::fs::create_dir(&current)
                            && err.kind() != std::io::ErrorKind::AlreadyExists
                        {
                            return Err(anyhow::anyhow!(
                                "create machine workspace directory {}: {}",
                                current.display(),
                                err
                            ));
                        }
                        let metadata = std::fs::symlink_metadata(&current).map_err(|err| {
                            anyhow::anyhow!(
                                "stat machine workspace directory {}: {}",
                                current.display(),
                                err
                            )
                        })?;
                        anyhow::ensure!(
                            metadata.is_dir() && !metadata.file_type().is_symlink(),
                            "machine workspace component is invalid after create: {}",
                            current.display()
                        );
                    }
                    Err(err) => {
                        return Err(anyhow::anyhow!(
                            "stat machine workspace component {}: {}",
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

    fn test_local_machine(
        id: &str,
        workspace_root: Option<&Path>,
        skill_roots: Vec<String>,
    ) -> MachineConfig {
        MachineConfig {
            id: id.to_string(),
            kind: MachineKind::Local,
            host: None,
            workspace_root: workspace_root.map(|root| root.display().to_string()),
            env: BTreeMap::new(),
            skill_roots,
        }
    }

    #[test]
    fn remote_exec_uses_no_tty_ssh_and_executor_visible_cwd() {
        let env = BTreeMap::from([
            ("PATH".to_string(), "/opt/bin:$PATH".to_string()),
            ("TOKEN".to_string(), "a b".to_string()),
        ]);
        let script = build_remote_exec_script(
            "/remote/work/session",
            &env,
            "/bin/codex-acp",
            &["--mode".to_string(), "stdio".to_string()],
        )
        .unwrap();

        assert!(script.contains("cd -- '/remote/work/session'"));
        assert!(script.contains("export PATH='/opt/bin:'$PATH"));
        assert!(script.contains("export TOKEN='a b'"));
        assert!(script.contains("exec '/bin/codex-acp' '--mode' 'stdio'"));

        let stdio = ssh_stdio_command("admin@example", script, "/remote/work/session".to_string());
        assert_eq!(stdio.program, "ssh");
        assert_eq!(stdio.args[0], "-T");
        assert_eq!(stdio.args[1], "admin@example");
        assert_eq!(stdio.args.len(), 3);
        assert!(stdio.args[2].starts_with("sh -lc '"));
        assert!(stdio.args[2].contains("cd --"));
        assert!(!stdio.args.iter().any(|arg| arg == "-t" || arg == "-tt"));
        assert_eq!(stdio.executor_cwd, "/remote/work/session");
        assert!(stdio.strict_json_stdout);
    }

    #[test]
    fn remote_workspace_creation_rejects_symlink_workspace() {
        let script = create_remote_workspace_script("/remote/work/session").unwrap();

        assert!(script.contains("[ ! -L '/remote' ]"));
        assert!(script.contains("[ ! -L '/remote/work' ]"));
        assert!(script.contains("[ ! -L '/remote/work/session' ]"));
        assert!(script.contains("mkdir -- '/remote/work/session'"));
    }

    #[test]
    fn remote_workspace_materialization_binds_to_validated_directory() {
        let script = materialize_remote_workspace_script("/remote/work/session").unwrap();

        assert!(script.contains("[ ! -L '/remote/work/session' ]"));
        assert!(script.contains("cd -P -- '/remote/work/session'"));
        assert!(script.contains("[ \"$(pwd -P)\" = '/remote/work/session' ]"));
        assert!(script.contains("find . -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +"));
        assert!(script.contains("tar xf - -C ."));
        assert!(!script.contains("tar xf - -C '/remote/work/session'"));
    }

    #[tokio::test]
    async fn local_machine_materializes_router_workspace_to_machine_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let router_workspace = tmp.path().join("router");
        let machine_root = tmp.path().join("machine");
        std::fs::create_dir_all(router_workspace.join("slack")).unwrap();
        std::fs::write(router_workspace.join("slack/current-thread.md"), "thread").unwrap();
        let stale_workspace = machine_workspace_path(&machine_root, "local", "session-1");
        std::fs::create_dir_all(stale_workspace.join("slack")).unwrap();
        std::fs::write(stale_workspace.join("slack/old.md"), "old").unwrap();
        let registry = MachineRegistry::new(BTreeMap::from([(
            "local".to_string(),
            MachineConfig {
                env: BTreeMap::from([("A".to_string(), "machine".to_string())]),
                ..test_local_machine("local", Some(&machine_root), Vec::new())
            },
        )]));
        let executor_env = BTreeMap::from([("A".to_string(), "executor".to_string())]);

        let prepared = registry
            .prepare_executor_command(MachinePrepareRequest {
                machine_id: "local",
                session_key: "session-1",
                router_workspace: Some(&router_workspace),
                executor_cwd: None,
                command: "agent",
                args: &[],
                env: &executor_env,
                cancel: None,
            })
            .await
            .unwrap();

        let workspace = prepared.workspace.unwrap();
        assert_eq!(
            workspace.materialization,
            MachineWorkspaceMaterialization::Materialized
        );
        assert!(
            Path::new(&workspace.cwd)
                .join("slack/current-thread.md")
                .is_file()
        );
        assert!(!Path::new(&workspace.cwd).join("slack/old.md").exists());
        assert_eq!(prepared.stdio.env["A"], "executor");
    }

    #[tokio::test]
    async fn default_local_machine_reuses_matching_router_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let session_key = "slack:channel:C1:111.000";
        let router_workspace = workspace_root.join(session_workspace_dir_name(session_key));
        std::fs::create_dir_all(router_workspace.join("executor-output")).unwrap();
        std::fs::write(router_workspace.join("executor-output/result.txt"), "keep").unwrap();
        let registry = MachineRegistry::new(BTreeMap::from([(
            LOCAL_MACHINE_ID.to_string(),
            test_local_machine(LOCAL_MACHINE_ID, Some(&workspace_root), Vec::new()),
        )]));
        let env = BTreeMap::new();
        let args = Vec::new();

        let prepared = registry
            .prepare_executor_command(MachinePrepareRequest {
                machine_id: LOCAL_MACHINE_ID,
                session_key,
                router_workspace: Some(&router_workspace),
                executor_cwd: None,
                command: "agent",
                args: &args,
                env: &env,
                cancel: None,
            })
            .await
            .unwrap();

        let workspace = prepared.workspace.unwrap();
        assert_eq!(
            workspace.materialization,
            MachineWorkspaceMaterialization::NotNeeded
        );
        assert_eq!(
            Path::new(&workspace.cwd),
            router_workspace.canonicalize().unwrap().as_path()
        );
        assert_eq!(
            prepared.stdio.current_dir.as_deref(),
            Some(Path::new(&workspace.cwd))
        );
        assert!(router_workspace.join("executor-output/result.txt").exists());
        assert!(
            !workspace_root
                .join(machine_workspace_machine_dir_name(LOCAL_MACHINE_ID))
                .join(session_workspace_dir_name(session_key))
                .exists()
        );
    }

    #[tokio::test]
    async fn machine_workspace_names_are_stable_per_session_and_machine() {
        let tmp = tempfile::tempdir().unwrap();
        let shared_root = tmp.path().join("machines");
        let registry = MachineRegistry::new(BTreeMap::from([
            (
                "machine-a".to_string(),
                test_local_machine("machine-a", Some(&shared_root), Vec::new()),
            ),
            (
                "machine-b".to_string(),
                test_local_machine("machine-b", Some(&shared_root), Vec::new()),
            ),
        ]));
        let env = BTreeMap::new();
        let args = Vec::new();
        let session_a = "slack:channel:C1:111.000";
        let session_b = "slack:channel:C1:222.000";
        let machine_a_dir = machine_workspace_machine_dir_name("machine-a");
        let machine_b_dir = machine_workspace_machine_dir_name("machine-b");
        let session_a_dir = session_workspace_dir_name(session_a);
        let session_b_dir = session_workspace_dir_name(session_b);

        let first_a = registry
            .prepare_executor_command(MachinePrepareRequest {
                machine_id: "machine-a",
                session_key: session_a,
                router_workspace: None,
                executor_cwd: None,
                command: "agent",
                args: &args,
                env: &env,
                cancel: None,
            })
            .await
            .unwrap()
            .workspace
            .unwrap();
        let second_a = registry
            .prepare_executor_command(MachinePrepareRequest {
                machine_id: "machine-a",
                session_key: session_a,
                router_workspace: None,
                executor_cwd: None,
                command: "agent",
                args: &args,
                env: &env,
                cancel: None,
            })
            .await
            .unwrap()
            .workspace
            .unwrap();
        let other_session_a = registry
            .prepare_executor_command(MachinePrepareRequest {
                machine_id: "machine-a",
                session_key: session_b,
                router_workspace: None,
                executor_cwd: None,
                command: "agent",
                args: &args,
                env: &env,
                cancel: None,
            })
            .await
            .unwrap()
            .workspace
            .unwrap();
        let first_b = registry
            .prepare_executor_command(MachinePrepareRequest {
                machine_id: "machine-b",
                session_key: session_a,
                router_workspace: None,
                executor_cwd: None,
                command: "agent",
                args: &args,
                env: &env,
                cancel: None,
            })
            .await
            .unwrap()
            .workspace
            .unwrap();

        assert_eq!(first_a.cwd, second_a.cwd);
        assert_ne!(first_a.cwd, other_session_a.cwd);
        assert_ne!(first_a.cwd, first_b.cwd);
        assert_eq!(first_a.machine_id, "machine-a");
        assert_eq!(other_session_a.machine_id, "machine-a");
        assert_eq!(first_b.machine_id, "machine-b");
        let shared_root = shared_root.canonicalize().unwrap();
        assert_eq!(
            Path::new(&first_a.cwd).strip_prefix(&shared_root).unwrap(),
            Path::new(&machine_a_dir).join(&session_a_dir).as_path()
        );
        assert_eq!(
            Path::new(&other_session_a.cwd)
                .strip_prefix(&shared_root)
                .unwrap(),
            Path::new(&machine_a_dir).join(&session_b_dir).as_path()
        );
        assert_eq!(
            Path::new(&first_b.cwd).strip_prefix(&shared_root).unwrap(),
            Path::new(&machine_b_dir).join(&session_a_dir).as_path()
        );
    }

    #[test]
    fn remote_machine_workspace_path_is_machine_scoped() {
        let session = "slack:channel:C1:111.000";
        let machine_dir = machine_workspace_machine_dir_name("zbs-dev");
        let session_dir = session_workspace_dir_name(session);

        assert_eq!(
            remote_machine_workspace_path("/remote/workspaces", "zbs-dev", session),
            format!("/remote/workspaces/{machine_dir}/{session_dir}")
        );
    }

    #[tokio::test]
    async fn local_skill_identity_includes_machine_id_and_relative_path() {
        let tmp = tempfile::tempdir().unwrap();
        let skill = tmp.path().join("writer");
        std::fs::create_dir_all(&skill).unwrap();
        std::fs::write(
            skill.join("SKILL.md"),
            "---\nname: writer\ndescription: Writes text\n---\n# Writer\n",
        )
        .unwrap();
        let registry = MachineRegistry::new(BTreeMap::from([(
            "local".to_string(),
            test_local_machine("local", None, vec![tmp.path().display().to_string()]),
        )]));

        let skills = registry.collect_skills().await;

        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].machine_id, "local");
        assert_eq!(skills[0].relative_path, "writer");
        assert_eq!(skills[0].metadata.as_ref().unwrap()["name"], "writer");
        assert!(
            skills[0]
                .instructions
                .as_ref()
                .unwrap()
                .contains("# Writer")
        );
    }

    #[tokio::test]
    async fn skill_identity_disambiguates_same_relative_path_across_machines() {
        let tmp = tempfile::tempdir().unwrap();
        let root_a = tmp.path().join("root-a");
        let root_b = tmp.path().join("root-b");
        for root in [&root_a, &root_b] {
            let skill = root.join("writer");
            std::fs::create_dir_all(&skill).unwrap();
            std::fs::write(
                skill.join("SKILL.md"),
                "---\nname: writer\ndescription: Writes text\n---\n# Writer\n",
            )
            .unwrap();
        }
        let registry = MachineRegistry::new(BTreeMap::from([
            (
                "machine-a".to_string(),
                test_local_machine("machine-a", None, vec![root_a.display().to_string()]),
            ),
            (
                "machine-b".to_string(),
                test_local_machine("machine-b", None, vec![root_b.display().to_string()]),
            ),
        ]));

        let identities = registry
            .collect_skills()
            .await
            .into_iter()
            .map(|skill| (skill.machine_id, skill.relative_path))
            .collect::<Vec<_>>();

        assert_eq!(
            identities,
            [
                ("machine-a".to_string(), "writer".to_string()),
                ("machine-b".to_string(), "writer".to_string()),
            ]
        );
    }
}
