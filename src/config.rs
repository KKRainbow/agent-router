use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::machine::{LOCAL_MACHINE_ID, MachineConfig, MachineKind, local_machine_config};
use crate::{router::OrchestratorMode, session::ApprovalMode};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub router: RouterConfig,
    pub approval: ApprovalConfig,
    pub workspace: WorkspaceConfig,
    pub slack: SlackConfig,
    pub qq: QqConfig,
    pub machines: BTreeMap<String, MachineConfig>,
    pub executors: BTreeMap<String, ExecutorConfig>,
}

#[derive(Debug, Clone)]
pub struct ApprovalConfig {
    pub default_mode: ApprovalMode,
}

#[derive(Debug, Clone)]
pub struct RouterConfig {
    pub default_executor: String,
    pub orchestrator: Option<OrchestratorConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    pub enabled: bool,
    pub mode: OrchestratorMode,
    pub executor: String,
    pub policy_file: PathBuf,
    pub max_policy_bytes: usize,
    pub max_transcript_messages: usize,
    pub decision_timeout_ms: u64,
    pub emit_handoff_notice: bool,
}

#[derive(Debug, Clone)]
pub struct WorkspaceConfig {
    pub root: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct SlackConfig {
    pub enabled: bool,
    pub bot_token: String,
    pub app_token: String,
    pub require_mention: bool,
    pub channel_events: ChannelEventMode,
    pub context_sync: SlackContextSyncConfig,
    pub allowed_channels: BTreeSet<String>,
    pub free_response_channels: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct SlackContextSyncConfig {
    pub enabled: bool,
    pub current_thread: bool,
    pub linked_threads: bool,
    pub files: bool,
    pub linked_thread_depth: usize,
    pub max_file_bytes: usize,
    pub max_files_per_turn: usize,
    pub max_linked_threads_per_turn: usize,
}

#[derive(Debug, Clone)]
pub struct QqConfig {
    pub enabled: bool,
    pub app_id: String,
    pub client_secret: String,
    pub sandbox: bool,
    pub intents: u64,
    pub channel_events: ChannelEventMode,
    pub allowed_users: BTreeSet<String>,
    pub allowed_groups: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelEventMode {
    Off,
    Compact,
    Verbose,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorProtocol {
    Acp,
    AppServer,
    ClaudeStreamJson,
    PiRpc,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub name: String,
    pub protocol: ExecutorProtocol,
    pub machine: String,
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    router: Option<FileRouterConfig>,
    approval: Option<FileApprovalConfig>,
    workspace: Option<FileWorkspaceConfig>,
    machines: Option<BTreeMap<String, FileMachineConfig>>,
    slack: Option<FileSlackConfig>,
    qq: Option<FileQqConfig>,
    executors: Option<BTreeMap<String, FileExecutorConfig>>,
}

#[derive(Debug, Default, Deserialize)]
struct FileApprovalConfig {
    default_mode: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileRouterConfig {
    default_executor: Option<String>,
    orchestrator: Option<FileOrchestratorConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct FileOrchestratorConfig {
    enabled: Option<bool>,
    mode: Option<String>,
    executor: Option<String>,
    policy_file: Option<PathBuf>,
    max_policy_bytes: Option<usize>,
    max_transcript_messages: Option<usize>,
    decision_timeout_ms: Option<u64>,
    emit_handoff_notice: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct FileWorkspaceConfig {
    root: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct FileMachineConfig {
    #[serde(rename = "type")]
    kind: Option<String>,
    host: Option<String>,
    workspace_root: Option<String>,
    env: Option<BTreeMap<String, String>>,
    skill_roots: Option<StringList>,
}

#[derive(Debug, Default, Deserialize)]
struct FileSlackConfig {
    enabled: Option<bool>,
    bot_token: Option<String>,
    app_token: Option<String>,
    require_mention: Option<bool>,
    channel_events: Option<String>,
    context_sync: Option<FileSlackContextSyncConfig>,
    allowed_channels: Option<StringList>,
    free_response_channels: Option<StringList>,
}

#[derive(Debug, Default, Deserialize)]
struct FileSlackContextSyncConfig {
    enabled: Option<bool>,
    current_thread: Option<bool>,
    linked_threads: Option<bool>,
    files: Option<bool>,
    linked_thread_depth: Option<usize>,
    max_file_bytes: Option<usize>,
    max_files_per_turn: Option<usize>,
    max_linked_threads_per_turn: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct FileQqConfig {
    enabled: Option<bool>,
    app_id: Option<String>,
    client_secret: Option<String>,
    sandbox: Option<bool>,
    intents: Option<u64>,
    channel_events: Option<String>,
    allowed_users: Option<StringList>,
    allowed_groups: Option<StringList>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringList {
    String(String),
    List(Vec<String>),
}

impl StringList {
    fn into_set(self) -> BTreeSet<String> {
        match self {
            Self::String(raw) => split_csv(&raw),
            Self::List(items) => items
                .into_iter()
                .flat_map(|item| split_csv(&item))
                .collect::<BTreeSet<_>>(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct FileExecutorConfig {
    protocol: Option<String>,
    machine: Option<String>,
    command: Option<String>,
    args: Option<Vec<String>>,
    cwd: Option<PathBuf>,
    env: Option<BTreeMap<String, String>>,
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let file_cfg = match path {
            Some(path) if path.exists() => {
                let text = std::fs::read_to_string(path)?;
                serde_yaml::from_str::<FileConfig>(&text)?
            }
            _ => FileConfig::default(),
        };
        Self::from_file_config(file_cfg, EnvConfig::from_process())
    }

    fn from_file_config(file_cfg: FileConfig, env_cfg: EnvConfig) -> anyhow::Result<Self> {
        let router_file = file_cfg.router.unwrap_or_default();
        let default_executor = env_cfg
            .default_executor
            .filter(|value| !value.trim().is_empty())
            .or(router_file.default_executor.clone())
            .unwrap_or_else(|| "kimi".to_string());
        let orchestrator = parse_orchestrator_config(router_file.orchestrator)?;

        let approval_file = file_cfg.approval.unwrap_or_default();
        let default_approval_mode = env_cfg
            .approval_default_mode
            .or(approval_file.default_mode)
            .map(|value| parse_approval_mode(&value))
            .transpose()?
            .unwrap_or_default();
        let approval = ApprovalConfig {
            default_mode: default_approval_mode,
        };
        let workspace_file = file_cfg.workspace.unwrap_or_default();
        let workspace = WorkspaceConfig {
            root: workspace_file
                .root
                .filter(|root| !root.as_os_str().is_empty()),
        };
        let machines = parse_machine_configs(file_cfg.machines, workspace.root.as_deref())?;

        let slack_file = file_cfg.slack.unwrap_or_default();
        let slack_context_file = slack_file.context_sync.unwrap_or_default();
        let slack_bot_token = env_cfg
            .slack_bot_token
            .or(slack_file.bot_token)
            .unwrap_or_default();
        let slack_app_token = env_cfg
            .slack_app_token
            .or(slack_file.app_token)
            .unwrap_or_default();
        let slack_enabled = env_cfg
            .slack_enabled
            .or(slack_file.enabled)
            .unwrap_or(!slack_bot_token.is_empty() && !slack_app_token.is_empty());
        let slack = SlackConfig {
            enabled: slack_enabled,
            bot_token: slack_bot_token,
            app_token: slack_app_token,
            require_mention: env_cfg
                .slack_require_mention
                .or(slack_file.require_mention)
                .unwrap_or(true),
            channel_events: env_cfg
                .slack_channel_events
                .or(slack_file.channel_events)
                .map(|value| parse_channel_event_mode("slack.channel_events", &value))
                .transpose()?
                .unwrap_or(ChannelEventMode::Compact),
            context_sync: SlackContextSyncConfig {
                enabled: env_cfg
                    .slack_context_sync_enabled
                    .or(slack_context_file.enabled)
                    .unwrap_or(workspace.root.is_some()),
                current_thread: slack_context_file.current_thread.unwrap_or(true),
                linked_threads: slack_context_file.linked_threads.unwrap_or(true),
                files: slack_context_file.files.unwrap_or(true),
                linked_thread_depth: slack_context_file.linked_thread_depth.unwrap_or(1),
                max_file_bytes: env_cfg
                    .slack_context_sync_max_file_bytes
                    .or(slack_context_file.max_file_bytes)
                    .unwrap_or(10 * 1024 * 1024),
                max_files_per_turn: slack_context_file.max_files_per_turn.unwrap_or(20),
                max_linked_threads_per_turn: slack_context_file
                    .max_linked_threads_per_turn
                    .unwrap_or(10),
            },
            allowed_channels: env_cfg
                .slack_allowed_channels
                .or_else(|| slack_file.allowed_channels.map(StringList::into_set))
                .unwrap_or_default(),
            free_response_channels: env_cfg
                .slack_free_response_channels
                .or_else(|| slack_file.free_response_channels.map(StringList::into_set))
                .unwrap_or_default(),
        };

        let qq_file = file_cfg.qq.unwrap_or_default();
        let qq_app_id = env_cfg.qq_app_id.or(qq_file.app_id).unwrap_or_default();
        let qq_client_secret = env_cfg
            .qq_client_secret
            .or(qq_file.client_secret)
            .unwrap_or_default();
        let qq_enabled = env_cfg
            .qq_enabled
            .or(qq_file.enabled)
            .unwrap_or(!qq_app_id.is_empty() && !qq_client_secret.is_empty());
        let qq = QqConfig {
            enabled: qq_enabled,
            app_id: qq_app_id,
            client_secret: qq_client_secret,
            sandbox: env_cfg.qq_sandbox.or(qq_file.sandbox).unwrap_or(false),
            intents: env_cfg
                .qq_intents
                .or(qq_file.intents)
                .unwrap_or((1_u64 << 25) | (1_u64 << 30)),
            channel_events: env_cfg
                .qq_channel_events
                .or(qq_file.channel_events)
                .map(|value| parse_channel_event_mode("qq.channel_events", &value))
                .transpose()?
                .unwrap_or(ChannelEventMode::Compact),
            allowed_users: env_cfg
                .qq_allowed_users
                .or_else(|| qq_file.allowed_users.map(StringList::into_set))
                .unwrap_or_default(),
            allowed_groups: env_cfg
                .qq_allowed_groups
                .or_else(|| qq_file.allowed_groups.map(StringList::into_set))
                .unwrap_or_default(),
        };

        let mut executors = BTreeMap::new();
        if let Some(raw_executors) = file_cfg.executors {
            for (name, raw) in raw_executors {
                let cfg = parse_executor_config(name, raw)?;
                executors.insert(cfg.name.clone(), cfg);
            }
        }
        if executors.is_empty() {
            executors.insert(
                "kimi".to_string(),
                ExecutorConfig {
                    name: "kimi".to_string(),
                    protocol: ExecutorProtocol::Acp,
                    machine: LOCAL_MACHINE_ID.to_string(),
                    command: "kimi".to_string(),
                    args: vec!["acp".to_string()],
                    cwd: None,
                    env: BTreeMap::new(),
                },
            );
        }

        anyhow::ensure!(
            executors.contains_key(&default_executor),
            "default executor `{default_executor}` is not configured"
        );
        if let Some(orchestrator) = &orchestrator
            && orchestrator.enabled
        {
            anyhow::ensure!(
                executors.contains_key(&orchestrator.executor),
                "router.orchestrator.executor `{}` is not configured",
                orchestrator.executor
            );
            anyhow::ensure!(
                orchestrator.executor != default_executor,
                "router.orchestrator.executor must not be the default executor"
            );
        }
        for cfg in executors.values() {
            anyhow::ensure!(
                machines.contains_key(&cfg.machine),
                "executors.{}.machine `{}` is not configured",
                cfg.name,
                cfg.machine
            );
        }

        Ok(Self {
            router: RouterConfig {
                default_executor,
                orchestrator,
            },
            approval,
            workspace,
            slack,
            qq,
            machines,
            executors,
        })
    }
}

#[derive(Debug, Default)]
struct EnvConfig {
    default_executor: Option<String>,
    approval_default_mode: Option<String>,
    slack_enabled: Option<bool>,
    slack_bot_token: Option<String>,
    slack_app_token: Option<String>,
    slack_require_mention: Option<bool>,
    slack_channel_events: Option<String>,
    slack_context_sync_enabled: Option<bool>,
    slack_context_sync_max_file_bytes: Option<usize>,
    slack_allowed_channels: Option<BTreeSet<String>>,
    slack_free_response_channels: Option<BTreeSet<String>>,
    qq_enabled: Option<bool>,
    qq_app_id: Option<String>,
    qq_client_secret: Option<String>,
    qq_sandbox: Option<bool>,
    qq_intents: Option<u64>,
    qq_channel_events: Option<String>,
    qq_allowed_users: Option<BTreeSet<String>>,
    qq_allowed_groups: Option<BTreeSet<String>>,
}

impl EnvConfig {
    fn from_process() -> Self {
        Self {
            default_executor: nonempty_env("AGENT_ROUTER_DEFAULT_EXECUTOR"),
            approval_default_mode: nonempty_env("AGENT_ROUTER_APPROVAL_DEFAULT_MODE")
                .or_else(|| nonempty_env("AGENT_ROUTER_APPROVAL_MODE")),
            slack_enabled: env_bool("SLACK_ENABLED"),
            slack_bot_token: nonempty_env("SLACK_BOT_TOKEN"),
            slack_app_token: nonempty_env("SLACK_APP_TOKEN"),
            slack_require_mention: env_bool("SLACK_REQUIRE_MENTION"),
            slack_channel_events: nonempty_env("SLACK_CHANNEL_EVENTS"),
            slack_context_sync_enabled: env_bool("SLACK_CONTEXT_SYNC_ENABLED"),
            slack_context_sync_max_file_bytes: env_usize("SLACK_CONTEXT_SYNC_MAX_FILE_BYTES"),
            slack_allowed_channels: env_set("SLACK_ALLOWED_CHANNELS"),
            slack_free_response_channels: env_set("SLACK_FREE_RESPONSE_CHANNELS"),
            qq_enabled: env_bool("QQ_ENABLED").or_else(|| env_bool("QQBOT_ENABLED")),
            qq_app_id: nonempty_env("QQ_APP_ID").or_else(|| nonempty_env("QQBOT_APP_ID")),
            qq_client_secret: nonempty_env("QQ_CLIENT_SECRET")
                .or_else(|| nonempty_env("QQBOT_CLIENT_SECRET")),
            qq_sandbox: env_bool("QQ_SANDBOX").or_else(|| env_bool("QQBOT_SANDBOX")),
            qq_intents: env_u64("QQ_INTENTS").or_else(|| env_u64("QQBOT_INTENTS")),
            qq_channel_events: nonempty_env("QQ_CHANNEL_EVENTS")
                .or_else(|| nonempty_env("QQBOT_CHANNEL_EVENTS")),
            qq_allowed_users: env_set("QQ_ALLOWED_USERS")
                .or_else(|| env_set("QQBOT_ALLOWED_USERS")),
            qq_allowed_groups: env_set("QQ_ALLOWED_GROUPS")
                .or_else(|| env_set("QQBOT_ALLOWED_GROUPS")),
        }
    }
}

pub fn load_dotenv(path: Option<&Path>) {
    if let Some(path) = path {
        let _ = dotenvy::from_path(path);
        return;
    }
    for candidate in dotenv_candidates() {
        if candidate.exists() {
            let _ = dotenvy::from_path(candidate);
            return;
        }
    }
    let _ = dotenvy::dotenv();
}

fn dotenv_candidates() -> Vec<PathBuf> {
    let mut candidates = vec![PathBuf::from(".env"), PathBuf::from("../.env")];
    if let Some(hermes_home) = env::var_os("HERMES_HOME").map(PathBuf::from) {
        candidates.push(hermes_home.join(".env"));
    }
    if let Some(home) = env::var_os("HOME").map(PathBuf::from) {
        candidates.push(home.join(".hermes").join(".env"));
    }
    dedupe_paths(candidates)
}

fn dedupe_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    paths
        .into_iter()
        .filter(|path| seen.insert(path.clone()))
        .collect()
}

pub fn default_config_path() -> Option<PathBuf> {
    [
        PathBuf::from("agent-router.yaml"),
        PathBuf::from("config/agent-router.yaml"),
        PathBuf::from("../config.yaml"),
    ]
    .into_iter()
    .find(|candidate| candidate.exists())
}

fn parse_executor_config(name: String, raw: FileExecutorConfig) -> anyhow::Result<ExecutorConfig> {
    let protocol = match raw.protocol.as_deref().unwrap_or("acp") {
        "acp" => ExecutorProtocol::Acp,
        "app_server" | "codex_app_server" => ExecutorProtocol::AppServer,
        "claude_stream_json" => ExecutorProtocol::ClaudeStreamJson,
        "pi_rpc" => ExecutorProtocol::PiRpc,
        other => anyhow::bail!("executors.{name}.protocol `{other}` is not supported in MVP"),
    };
    let command = raw
        .command
        .filter(|value| !value.trim().is_empty())
        .or_else(|| (protocol == ExecutorProtocol::AppServer).then(|| "codex".to_string()))
        .or_else(|| (protocol == ExecutorProtocol::ClaudeStreamJson).then(|| "claude".to_string()))
        .or_else(|| (protocol == ExecutorProtocol::PiRpc).then(|| "pi".to_string()))
        .ok_or_else(|| anyhow::anyhow!("executors.{name}.command is required"))?;
    let args = match raw.args {
        Some(args) => args,
        None if protocol == ExecutorProtocol::AppServer => vec!["app-server".to_string()],
        None => Vec::new(),
    };
    Ok(ExecutorConfig {
        name,
        protocol,
        machine: raw
            .machine
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| LOCAL_MACHINE_ID.to_string()),
        command,
        args,
        cwd: raw.cwd,
        env: raw.env.unwrap_or_default(),
    })
}

fn parse_orchestrator_config(
    raw: Option<FileOrchestratorConfig>,
) -> anyhow::Result<Option<OrchestratorConfig>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let enabled = raw.enabled.unwrap_or(false);
    let executor = raw.executor.filter(|value| !value.trim().is_empty());
    let policy_file = raw.policy_file.filter(|path| !path.as_os_str().is_empty());
    if !enabled && executor.is_none() && policy_file.is_none() {
        return Ok(None);
    }
    let executor = if enabled {
        executor.ok_or_else(|| anyhow::anyhow!("router.orchestrator.executor is required"))?
    } else {
        executor.unwrap_or_default()
    };
    let policy_file = if enabled {
        policy_file.ok_or_else(|| anyhow::anyhow!("router.orchestrator.policy_file is required"))?
    } else {
        policy_file.unwrap_or_default()
    };
    let mode = raw
        .mode
        .map(|value| parse_orchestrator_mode(&value))
        .transpose()?
        .unwrap_or(OrchestratorMode::Initial);
    Ok(Some(OrchestratorConfig {
        enabled,
        mode,
        executor,
        policy_file,
        max_policy_bytes: raw.max_policy_bytes.unwrap_or(65_536),
        max_transcript_messages: raw.max_transcript_messages.unwrap_or(12),
        decision_timeout_ms: raw.decision_timeout_ms.unwrap_or(15_000),
        emit_handoff_notice: raw.emit_handoff_notice.unwrap_or(false),
    }))
}

fn parse_orchestrator_mode(value: &str) -> anyhow::Result<OrchestratorMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "initial" => Ok(OrchestratorMode::Initial),
        "per_turn" | "per-turn" => Ok(OrchestratorMode::PerTurn),
        other => anyhow::bail!("router.orchestrator.mode `{other}` is not supported"),
    }
}

fn parse_machine_configs(
    raw_machines: Option<BTreeMap<String, FileMachineConfig>>,
    router_workspace_root: Option<&Path>,
) -> anyhow::Result<BTreeMap<String, MachineConfig>> {
    let mut machines = BTreeMap::new();
    if let Some(raw_machines) = raw_machines {
        for (id, raw) in raw_machines {
            let cfg = parse_machine_config(id, raw, router_workspace_root)?;
            machines.insert(cfg.id.clone(), cfg);
        }
    }
    machines
        .entry(LOCAL_MACHINE_ID.to_string())
        .or_insert_with(|| {
            let mut local = local_machine_config();
            local.workspace_root = router_workspace_root.map(|root| root.display().to_string());
            local
        });
    Ok(machines)
}

fn parse_machine_config(
    id: String,
    raw: FileMachineConfig,
    router_workspace_root: Option<&Path>,
) -> anyhow::Result<MachineConfig> {
    let kind = match raw.kind.as_deref().unwrap_or("local") {
        "local" => MachineKind::Local,
        "ssh" => MachineKind::Ssh,
        other => anyhow::bail!("machines.{id}.type `{other}` is not supported"),
    };
    let host = raw.host.filter(|value| !value.trim().is_empty());
    let workspace_root = raw
        .workspace_root
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            (id == LOCAL_MACHINE_ID)
                .then(|| router_workspace_root.map(|root| root.display().to_string()))
                .flatten()
        });
    if kind == MachineKind::Ssh {
        anyhow::ensure!(
            host.is_some(),
            "machines.{id}.host is required for ssh machines"
        );
        anyhow::ensure!(
            workspace_root.is_some(),
            "machines.{id}.workspace_root is required for ssh machines"
        );
    }
    Ok(MachineConfig {
        id,
        kind,
        host,
        workspace_root,
        env: raw.env.unwrap_or_default(),
        skill_roots: raw
            .skill_roots
            .map(StringList::into_set)
            .unwrap_or_default()
            .into_iter()
            .collect(),
    })
}

fn parse_approval_mode(value: &str) -> anyhow::Result<ApprovalMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "normal" => Ok(ApprovalMode::Normal),
        "yolo" => Ok(ApprovalMode::Yolo),
        other => anyhow::bail!("approval.default_mode `{other}` is not supported"),
    }
}

fn parse_channel_event_mode(field: &str, value: &str) -> anyhow::Result<ChannelEventMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "off" | "none" | "false" => Ok(ChannelEventMode::Off),
        "" | "compact" => Ok(ChannelEventMode::Compact),
        "verbose" | "all" | "true" => Ok(ChannelEventMode::Verbose),
        other => anyhow::bail!("{field} `{other}` is not supported"),
    }
}

fn nonempty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

fn env_u64(name: &str) -> Option<u64> {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn env_usize(name: &str) -> Option<usize> {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
}

fn env_set(name: &str) -> Option<BTreeSet<String>> {
    env::var(name).ok().map(|value| split_csv(&value))
}

fn split_csv(raw: &str) -> BTreeSet<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_uses_kimi_acp_executor() {
        let cfg = AppConfig::from_file_config(FileConfig::default(), EnvConfig::default()).unwrap();

        assert_eq!(cfg.router.default_executor, "kimi");
        let kimi = cfg.executors.get("kimi").unwrap();
        assert_eq!(kimi.protocol, ExecutorProtocol::Acp);
        assert_eq!(kimi.machine, LOCAL_MACHINE_ID);
        assert_eq!(kimi.command, "kimi");
        assert_eq!(kimi.args, ["acp"]);
        assert_eq!(cfg.machines[LOCAL_MACHINE_ID].kind, MachineKind::Local);
        assert_eq!(cfg.approval.default_mode, ApprovalMode::Normal);
        assert!(!cfg.slack.enabled);
        assert!(!cfg.qq.enabled);
        assert_eq!(cfg.slack.channel_events, ChannelEventMode::Compact);
        assert!(!cfg.slack.context_sync.enabled);
        assert!(cfg.slack.context_sync.current_thread);
        assert!(cfg.slack.context_sync.linked_threads);
        assert!(cfg.slack.context_sync.files);
        assert_eq!(cfg.slack.context_sync.linked_thread_depth, 1);
        assert_eq!(cfg.slack.context_sync.max_file_bytes, 10 * 1024 * 1024);
        assert_eq!(cfg.slack.context_sync.max_files_per_turn, 20);
        assert_eq!(cfg.slack.context_sync.max_linked_threads_per_turn, 10);
        assert_eq!(cfg.qq.channel_events, ChannelEventMode::Compact);
    }

    #[test]
    fn parses_slack_channel_lists_and_executor_config() {
        let raw = r#"
router:
  default_executor: kimi
slack:
  require_mention: false
  channel_events: verbose
  context_sync:
    enabled: false
    current_thread: true
    linked_threads: false
    files: true
    linked_thread_depth: 0
    max_file_bytes: 4096
    max_files_per_turn: 3
    max_linked_threads_per_turn: 2
  allowed_channels: "C1,C2"
  free_response_channels: ["C3", "C4,C5"]
executors:
  kimi:
    protocol: acp
    command: kimi
    args: ["acp"]
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();

        assert!(!cfg.slack.require_mention);
        assert_eq!(cfg.slack.channel_events, ChannelEventMode::Verbose);
        assert!(!cfg.slack.context_sync.enabled);
        assert!(cfg.slack.context_sync.current_thread);
        assert!(!cfg.slack.context_sync.linked_threads);
        assert!(cfg.slack.context_sync.files);
        assert_eq!(cfg.slack.context_sync.linked_thread_depth, 0);
        assert_eq!(cfg.slack.context_sync.max_file_bytes, 4096);
        assert_eq!(cfg.slack.context_sync.max_files_per_turn, 3);
        assert_eq!(cfg.slack.context_sync.max_linked_threads_per_turn, 2);
        assert_eq!(
            cfg.slack.allowed_channels,
            ["C1".to_string(), "C2".to_string()].into_iter().collect()
        );
        assert_eq!(
            cfg.slack.free_response_channels,
            ["C3", "C4", "C5"].into_iter().map(str::to_string).collect()
        );
    }

    #[test]
    fn parses_local_and_ssh_machines_and_executor_machine_refs() {
        let raw = r#"
router:
  default_executor: local-kimi
workspace:
  root: /router/workspaces
machines:
  zbs-dev:
    type: ssh
    host: admin@172.17.0.2
    workspace_root: /remote/workspaces
    env:
      PATH: /opt/node/bin:$PATH
    skill_roots: ["/home/admin/.codex/skills", "/data/project/ai/skills"]
executors:
  local-kimi:
    protocol: acp
    command: kimi
  remote-codex:
    protocol: acp
    machine: zbs-dev
    command: /opt/node/bin/codex-acp
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();

        assert_eq!(
            cfg.machines[LOCAL_MACHINE_ID].workspace_root.as_deref(),
            Some("/router/workspaces")
        );
        let remote = &cfg.machines["zbs-dev"];
        assert_eq!(remote.kind, MachineKind::Ssh);
        assert_eq!(remote.host.as_deref(), Some("admin@172.17.0.2"));
        assert_eq!(remote.workspace_root.as_deref(), Some("/remote/workspaces"));
        assert_eq!(remote.env["PATH"], "/opt/node/bin:$PATH");
        assert_eq!(
            remote.skill_roots,
            ["/data/project/ai/skills", "/home/admin/.codex/skills"]
        );
        assert_eq!(cfg.executors["local-kimi"].machine, LOCAL_MACHINE_ID);
        assert_eq!(cfg.executors["remote-codex"].machine, "zbs-dev");
    }

    #[test]
    fn parses_orchestrator_config() {
        let raw = r#"
router:
  default_executor: kimi
  orchestrator:
    enabled: true
    mode: per_turn
    executor: route-planner
    policy_file: config/agent-routing.md
    max_policy_bytes: 1234
    max_transcript_messages: 5
    decision_timeout_ms: 2500
    emit_handoff_notice: true
executors:
  kimi:
    protocol: acp
    command: kimi
  route-planner:
    protocol: acp
    command: kimi
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();
        let orchestrator = cfg.router.orchestrator.unwrap();

        assert!(orchestrator.enabled);
        assert_eq!(orchestrator.mode, OrchestratorMode::PerTurn);
        assert_eq!(orchestrator.executor, "route-planner");
        assert_eq!(
            orchestrator.policy_file,
            PathBuf::from("config/agent-routing.md")
        );
        assert_eq!(orchestrator.max_policy_bytes, 1234);
        assert_eq!(orchestrator.max_transcript_messages, 5);
        assert_eq!(orchestrator.decision_timeout_ms, 2500);
        assert!(orchestrator.emit_handoff_notice);
    }

    #[test]
    fn orchestrator_mode_defaults_to_initial() {
        let raw = r#"
router:
  default_executor: kimi
  orchestrator:
    enabled: true
    executor: route-planner
    policy_file: config/agent-routing.md
executors:
  kimi:
    protocol: acp
    command: kimi
  route-planner:
    protocol: acp
    command: kimi
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();
        let orchestrator = cfg.router.orchestrator.unwrap();

        assert_eq!(orchestrator.mode, OrchestratorMode::Initial);
    }

    #[test]
    fn invalid_orchestrator_mode_is_rejected() {
        let raw = r#"
router:
  default_executor: kimi
  orchestrator:
    enabled: true
    mode: always
    executor: route-planner
    policy_file: config/agent-routing.md
executors:
  kimi:
    protocol: acp
    command: kimi
  route-planner:
    protocol: acp
    command: kimi
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let err = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap_err();

        assert!(
            err.to_string()
                .contains("router.orchestrator.mode `always` is not supported")
        );
    }

    #[test]
    fn enabled_orchestrator_requires_configured_non_default_executor() {
        let raw = r#"
router:
  default_executor: kimi
  orchestrator:
    enabled: true
    executor: kimi
    policy_file: config/agent-routing.md
executors:
  kimi:
    protocol: acp
    command: kimi
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let err = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap_err();

        assert!(
            err.to_string()
                .contains("router.orchestrator.executor must not be the default executor")
        );
    }

    #[test]
    fn parses_qq_config() {
        let raw = r#"
qq:
  enabled: true
  app_id: app
  client_secret: secret
  sandbox: true
  channel_events: off
  allowed_users: "u1,u2"
  allowed_groups: ["g1", "g2,g3"]
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();

        assert!(cfg.qq.enabled);
        assert_eq!(cfg.qq.app_id, "app");
        assert_eq!(cfg.qq.client_secret, "secret");
        assert!(cfg.qq.sandbox);
        assert_eq!(cfg.qq.channel_events, ChannelEventMode::Off);
        assert_eq!(cfg.qq.intents, (1_u64 << 25) | (1_u64 << 30));
        assert_eq!(
            cfg.qq.allowed_users,
            ["u1", "u2"].into_iter().map(str::to_string).collect()
        );
        assert_eq!(
            cfg.qq.allowed_groups,
            ["g1", "g2", "g3"].into_iter().map(str::to_string).collect()
        );
    }

    #[test]
    fn parses_approval_default_mode() {
        let raw = r#"
approval:
  default_mode: yolo
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();

        assert_eq!(cfg.approval.default_mode, ApprovalMode::Yolo);
    }

    #[test]
    fn parses_workspace_root() {
        let raw = r#"
workspace:
  root: /tmp/hermes-workspaces
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();

        assert_eq!(
            cfg.workspace.root.as_deref(),
            Some(Path::new("/tmp/hermes-workspaces"))
        );
    }

    #[test]
    fn env_approval_mode_overrides_file_config() {
        let raw = r#"
approval:
  default_mode: normal
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(
            file_cfg,
            EnvConfig {
                approval_default_mode: Some("yolo".to_string()),
                ..EnvConfig::default()
            },
        )
        .unwrap();

        assert_eq!(cfg.approval.default_mode, ApprovalMode::Yolo);
    }

    #[test]
    fn env_qq_credentials_enable_channel() {
        let cfg = AppConfig::from_file_config(
            FileConfig::default(),
            EnvConfig {
                qq_app_id: Some("app".to_string()),
                qq_client_secret: Some("secret".to_string()),
                ..EnvConfig::default()
            },
        )
        .unwrap();

        assert!(cfg.qq.enabled);
        assert_eq!(cfg.qq.app_id, "app");
        assert_eq!(cfg.qq.client_secret, "secret");
    }

    #[test]
    fn partial_channel_credentials_do_not_auto_enable_channels() {
        let cfg = AppConfig::from_file_config(
            FileConfig::default(),
            EnvConfig {
                slack_bot_token: Some("xoxb-token".to_string()),
                qq_app_id: Some("app".to_string()),
                ..EnvConfig::default()
            },
        )
        .unwrap();

        assert!(!cfg.slack.enabled);
        assert!(!cfg.qq.enabled);
    }

    #[test]
    fn parses_codex_app_server_executor_config() {
        let raw = r#"
router:
  default_executor: codex
executors:
  codex:
    protocol: app_server
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();
        let codex = cfg.executors.get("codex").unwrap();

        assert_eq!(cfg.router.default_executor, "codex");
        assert_eq!(codex.protocol, ExecutorProtocol::AppServer);
        assert_eq!(codex.command, "codex");
        assert_eq!(codex.args, ["app-server"]);
    }

    #[test]
    fn parses_claude_stream_json_executor_config() {
        let raw = r#"
router:
  default_executor: claude
executors:
  claude:
    protocol: claude_stream_json
    args: ["--verbose"]
    env:
      CLAUDE_CODE_DIR: /tmp/claude
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();
        let claude = cfg.executors.get("claude").unwrap();

        assert_eq!(cfg.router.default_executor, "claude");
        assert_eq!(claude.protocol, ExecutorProtocol::ClaudeStreamJson);
        assert_eq!(claude.command, "claude");
        assert_eq!(claude.args, ["--verbose"]);
        assert_eq!(
            claude.env.get("CLAUDE_CODE_DIR"),
            Some(&"/tmp/claude".to_string())
        );
    }

    #[test]
    fn parses_pi_rpc_executor_config() {
        let raw = r#"
router:
  default_executor: pi
executors:
  pi:
    protocol: pi_rpc
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();
        let pi = cfg.executors.get("pi").unwrap();

        assert_eq!(cfg.router.default_executor, "pi");
        assert_eq!(pi.protocol, ExecutorProtocol::PiRpc);
        assert_eq!(pi.command, "pi");
        assert!(pi.args.is_empty());
    }

    #[test]
    fn dotenv_candidates_include_hermes_env_locations() {
        let candidates = dedupe_paths(vec![
            PathBuf::from(".env"),
            PathBuf::from("../.env"),
            PathBuf::from("/tmp/hermes/.env"),
            PathBuf::from("/home/test/.hermes/.env"),
            PathBuf::from("/tmp/hermes/.env"),
        ]);

        assert_eq!(
            candidates,
            [
                PathBuf::from(".env"),
                PathBuf::from("../.env"),
                PathBuf::from("/tmp/hermes/.env"),
                PathBuf::from("/home/test/.hermes/.env"),
            ]
        );
    }
}
