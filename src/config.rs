use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::{Path, PathBuf},
};

use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub router: RouterConfig,
    pub slack: SlackConfig,
    pub qq: QqConfig,
    pub executors: BTreeMap<String, ExecutorConfig>,
}

#[derive(Debug, Clone)]
pub struct RouterConfig {
    pub default_executor: String,
}

#[derive(Debug, Clone)]
pub struct SlackConfig {
    pub enabled: bool,
    pub bot_token: String,
    pub app_token: String,
    pub require_mention: bool,
    pub allowed_channels: BTreeSet<String>,
    pub free_response_channels: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct QqConfig {
    pub enabled: bool,
    pub app_id: String,
    pub client_secret: String,
    pub sandbox: bool,
    pub intents: u64,
    pub allowed_users: BTreeSet<String>,
    pub allowed_groups: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorProtocol {
    Acp,
    AppServer,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub name: String,
    pub protocol: ExecutorProtocol,
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    router: Option<FileRouterConfig>,
    slack: Option<FileSlackConfig>,
    qq: Option<FileQqConfig>,
    executors: Option<BTreeMap<String, FileExecutorConfig>>,
}

#[derive(Debug, Default, Deserialize)]
struct FileRouterConfig {
    default_executor: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileSlackConfig {
    enabled: Option<bool>,
    bot_token: Option<String>,
    app_token: Option<String>,
    require_mention: Option<bool>,
    allowed_channels: Option<StringList>,
    free_response_channels: Option<StringList>,
}

#[derive(Debug, Default, Deserialize)]
struct FileQqConfig {
    enabled: Option<bool>,
    app_id: Option<String>,
    client_secret: Option<String>,
    sandbox: Option<bool>,
    intents: Option<u64>,
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
        let default_executor = env_cfg
            .default_executor
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                file_cfg
                    .router
                    .as_ref()
                    .and_then(|router| router.default_executor.clone())
            })
            .unwrap_or_else(|| "kimi".to_string());

        let slack_file = file_cfg.slack.unwrap_or_default();
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
            .unwrap_or(!slack_bot_token.is_empty() || !slack_app_token.is_empty());
        let slack = SlackConfig {
            enabled: slack_enabled,
            bot_token: slack_bot_token,
            app_token: slack_app_token,
            require_mention: env_cfg
                .slack_require_mention
                .or(slack_file.require_mention)
                .unwrap_or(true),
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
            .unwrap_or(!qq_app_id.is_empty() || !qq_client_secret.is_empty());
        let qq = QqConfig {
            enabled: qq_enabled,
            app_id: qq_app_id,
            client_secret: qq_client_secret,
            sandbox: env_cfg.qq_sandbox.or(qq_file.sandbox).unwrap_or(false),
            intents: env_cfg
                .qq_intents
                .or(qq_file.intents)
                .unwrap_or((1_u64 << 25) | (1_u64 << 30)),
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

        Ok(Self {
            router: RouterConfig { default_executor },
            slack,
            qq,
            executors,
        })
    }
}

#[derive(Debug, Default)]
struct EnvConfig {
    default_executor: Option<String>,
    slack_enabled: Option<bool>,
    slack_bot_token: Option<String>,
    slack_app_token: Option<String>,
    slack_require_mention: Option<bool>,
    slack_allowed_channels: Option<BTreeSet<String>>,
    slack_free_response_channels: Option<BTreeSet<String>>,
    qq_enabled: Option<bool>,
    qq_app_id: Option<String>,
    qq_client_secret: Option<String>,
    qq_sandbox: Option<bool>,
    qq_intents: Option<u64>,
    qq_allowed_users: Option<BTreeSet<String>>,
    qq_allowed_groups: Option<BTreeSet<String>>,
}

impl EnvConfig {
    fn from_process() -> Self {
        Self {
            default_executor: nonempty_env("AGENT_ROUTER_DEFAULT_EXECUTOR"),
            slack_enabled: env_bool("SLACK_ENABLED"),
            slack_bot_token: nonempty_env("SLACK_BOT_TOKEN"),
            slack_app_token: nonempty_env("SLACK_APP_TOKEN"),
            slack_require_mention: env_bool("SLACK_REQUIRE_MENTION"),
            slack_allowed_channels: env_set("SLACK_ALLOWED_CHANNELS"),
            slack_free_response_channels: env_set("SLACK_FREE_RESPONSE_CHANNELS"),
            qq_enabled: env_bool("QQ_ENABLED").or_else(|| env_bool("QQBOT_ENABLED")),
            qq_app_id: nonempty_env("QQ_APP_ID").or_else(|| nonempty_env("QQBOT_APP_ID")),
            qq_client_secret: nonempty_env("QQ_CLIENT_SECRET")
                .or_else(|| nonempty_env("QQBOT_CLIENT_SECRET")),
            qq_sandbox: env_bool("QQ_SANDBOX").or_else(|| env_bool("QQBOT_SANDBOX")),
            qq_intents: env_u64("QQ_INTENTS").or_else(|| env_u64("QQBOT_INTENTS")),
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
        other => anyhow::bail!("executors.{name}.protocol `{other}` is not supported in MVP"),
    };
    let command = raw
        .command
        .filter(|value| !value.trim().is_empty())
        .or_else(|| (protocol == ExecutorProtocol::AppServer).then(|| "codex".to_string()))
        .ok_or_else(|| anyhow::anyhow!("executors.{name}.command is required"))?;
    let args = match raw.args {
        Some(args) => args,
        None if protocol == ExecutorProtocol::AppServer => vec!["app-server".to_string()],
        None => Vec::new(),
    };
    Ok(ExecutorConfig {
        name,
        protocol,
        command,
        args,
        cwd: raw.cwd,
        env: raw.env.unwrap_or_default(),
    })
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
        assert_eq!(kimi.command, "kimi");
        assert_eq!(kimi.args, ["acp"]);
        assert!(!cfg.slack.enabled);
        assert!(!cfg.qq.enabled);
    }

    #[test]
    fn parses_slack_channel_lists_and_executor_config() {
        let raw = r#"
router:
  default_executor: kimi
slack:
  require_mention: false
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
    fn parses_qq_config() {
        let raw = r#"
qq:
  enabled: true
  app_id: app
  client_secret: secret
  sandbox: true
  allowed_users: "u1,u2"
  allowed_groups: ["g1", "g2,g3"]
"#;
        let file_cfg = serde_yaml::from_str::<FileConfig>(raw).unwrap();
        let cfg = AppConfig::from_file_config(file_cfg, EnvConfig::default()).unwrap();

        assert!(cfg.qq.enabled);
        assert_eq!(cfg.qq.app_id, "app");
        assert_eq!(cfg.qq.client_secret, "secret");
        assert!(cfg.qq.sandbox);
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
