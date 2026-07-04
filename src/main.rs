use std::{
    collections::BTreeMap, fs::OpenOptions, io::IsTerminal, path::PathBuf, sync::Arc,
    time::Duration,
};

use agent_router::{
    approval::ApprovalBroker,
    channel::{
        qq::QqBotChannel, slack::SlackSocketModeChannel, telegram::TelegramBotChannel,
        tui::TerminalChatChannel, web::WebChannel,
    },
    config::{AppConfig, default_config_path, known_agent_priority, load_dotenv},
    executor::registry::ExecutorRegistry,
    machine::MachineRegistry,
    router::{AgentRouter, RouterService, SessionApprovalPolicy},
    session::store::{ConfiguredExecutor, ProductionSessionStore},
};
use clap::{Parser, Subcommand};
use time::macros::format_description;
use tokio::task::JoinSet;

#[derive(Debug, Parser)]
#[command(version, about = "Channel edge and local chat router for agents")]
struct Cli {
    #[arg(long, env = "AGENT_ROUTER_CONFIG", global = true)]
    config: Option<PathBuf>,

    #[arg(long, env = "AGENT_ROUTER_ENV_FILE", global = true)]
    env_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
enum Command {
    /// Start local terminal chat only.
    Chat,
    /// Start configured external channels.
    Serve,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Chat,
    Serve,
    ChatAndServe,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let explicit_config = cli.config.is_some();
    let config_path = cli.config.or_else(default_config_path);
    let mode = resolve_run_mode(
        cli.command,
        explicit_config,
        std::io::stdin().is_terminal(),
        std::io::stdout().is_terminal(),
    )?;
    let chat_log_path = init_tracing(mode)?;
    load_dotenv(cli.env_file.as_deref());

    if let Some(path) = &config_path {
        tracing::info!(path = %path.display(), "loading config");
    } else {
        tracing::info!("loading config from environment and built-in defaults");
    }
    let config = AppConfig::load(config_path.as_deref())?;

    let (approvals, router) = build_router(&config);
    match mode {
        RunMode::Chat => run_chat_mode(config, router, approvals, chat_log_path).await,
        RunMode::Serve => run_serve_mode(config, router, approvals).await,
        RunMode::ChatAndServe => {
            run_chat_and_serve_mode(config, router, approvals, chat_log_path).await
        }
    }
}

fn resolve_run_mode(
    command: Option<Command>,
    explicit_config: bool,
    stdin_is_tty: bool,
    stdout_is_tty: bool,
) -> anyhow::Result<RunMode> {
    match command {
        Some(Command::Chat) => Ok(RunMode::Chat),
        Some(Command::Serve) => Ok(RunMode::Serve),
        None if explicit_config && stdin_is_tty && stdout_is_tty => Ok(RunMode::ChatAndServe),
        None if stdin_is_tty && stdout_is_tty => Ok(RunMode::Chat),
        None => anyhow::bail!(
            "no run mode selected in a non-interactive context; use `agent-router chat` or `agent-router serve`"
        ),
    }
}

fn init_tracing(mode: RunMode) -> anyhow::Result<Option<PathBuf>> {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "agent_router=info,warn".to_string());
    match mode {
        RunMode::Chat | RunMode::ChatAndServe => {
            let path = chat_log_path();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_timer(tracing_subscriber::fmt::time::LocalTime::new(
                    format_description!(
                        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9][offset_hour]:[offset_minute]"
                    ),
                ))
                .with_writer(move || {
                    file.try_clone()
                        .expect("failed to clone agent-router chat log file")
                })
                .init();
            Ok(Some(path))
        }
        RunMode::Serve => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_timer(tracing_subscriber::fmt::time::LocalTime::new(
                    format_description!(
                        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9][offset_hour]:[offset_minute]"
                    ),
                ))
                .init();
            Ok(None)
        }
    }
}

fn chat_log_path() -> PathBuf {
    if let Some(path) = nonempty_env_path("AGENT_ROUTER_LOG_FILE") {
        return path;
    }
    if let Some(state_home) = nonempty_env_path("XDG_STATE_HOME") {
        return state_home.join("agent-router").join("agent-router.log");
    }
    if cfg!(windows)
        && let Some(local_app_data) = nonempty_env_path("LOCALAPPDATA")
    {
        return local_app_data.join("agent-router").join("agent-router.log");
    }
    if let Some(home) = nonempty_env_path("HOME") {
        return home
            .join(".local")
            .join("state")
            .join("agent-router")
            .join("agent-router.log");
    }
    PathBuf::from("agent-router.log")
}

fn nonempty_env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn build_router(config: &AppConfig) -> (Arc<ApprovalBroker>, Arc<dyn RouterService>) {
    if let Some(root) = &config.workspace.root {
        tracing::info!(workspace_root = %root.display(), "using workspace session persistence");
    } else {
        tracing::info!("workspace.root is not configured; using in-memory session store");
    }
    let session_executors = config
        .executors
        .iter()
        .map(|(name, executor)| {
            (
                name.clone(),
                ConfiguredExecutor::new(executor.protocol.as_str(), executor.machine.clone()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let store = Arc::new(ProductionSessionStore::new(
        config.workspace.root.clone(),
        config.router.default_executor.clone(),
        session_executors,
    ));
    let mut approval_policy = SessionApprovalPolicy::new(
        config.router.default_executor.clone(),
        config.approval.default_mode,
        store.clone(),
    );
    if let Some(orchestrator) = &config.router.orchestrator
        && orchestrator.enabled
    {
        approval_policy =
            approval_policy.with_denied_approval_executor(orchestrator.executor.clone());
    }
    let approvals = Arc::new(ApprovalBroker::with_policy(
        Duration::from_secs(120),
        Arc::new(approval_policy),
    ));
    let executor = Arc::new(ExecutorRegistry::with_machines(
        config.executors.clone(),
        MachineRegistry::new(config.machines.clone()),
        approvals.clone(),
    ));
    let orchestrator =
        config
            .router
            .orchestrator
            .as_ref()
            .map(|cfg| agent_router::router::OrchestratorSettings {
                enabled: cfg.enabled,
                mode: cfg.mode,
                executor: cfg.executor.clone(),
                policy_file: cfg.policy_file.clone(),
                max_policy_bytes: cfg.max_policy_bytes,
                max_transcript_messages: cfg.max_transcript_messages,
                decision_timeout: Duration::from_millis(cfg.decision_timeout_ms),
                emit_handoff_notice: cfg.emit_handoff_notice,
            });
    let router: Arc<dyn RouterService> = Arc::new(
        AgentRouter::with_approval_mode(
            config.router.default_executor.clone(),
            config.approval.default_mode,
            store,
            executor,
            approvals.clone(),
        )
        .with_workspace_root(config.workspace.root.clone())
        .with_owner_user_ids("slack", config.slack.owner_user_ids.clone())
        .with_orchestrator(orchestrator),
    );
    (approvals, router)
}

async fn run_chat_mode(
    config: AppConfig,
    router: Arc<dyn RouterService>,
    approvals: Arc<ApprovalBroker>,
    log_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    tracing::info!("starting terminal chat channel");
    let available_executors = available_executor_names(&config);
    TerminalChatChannel::new(
        config.tui,
        config.router.default_executor,
        available_executors,
        log_path.map(|path| path.display().to_string()),
        approvals,
    )
    .run(router)
    .await
}

async fn run_chat_and_serve_mode(
    config: AppConfig,
    router: Arc<dyn RouterService>,
    approvals: Arc<ApprovalBroker>,
    log_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    let mut channels = spawn_external_channels(config.clone(), router.clone(), approvals.clone())?;
    let mut chat = tokio::spawn(run_chat_mode(config, router, approvals, log_path));

    if channels.is_empty() {
        return chat.await?;
    }

    loop {
        tokio::select! {
            result = &mut chat => {
                abort_and_drain_channels(&mut channels).await;
                return result?;
            }
            result = channels.join_next(), if !channels.is_empty() => {
                if let Some(result) = result {
                    handle_channel_result(result, Some(&mut chat)).await?;
                }
            }
        }
    }
}

async fn run_serve_mode(
    config: AppConfig,
    router: Arc<dyn RouterService>,
    approvals: Arc<ApprovalBroker>,
) -> anyhow::Result<()> {
    let mut channels = spawn_external_channels(config, router, approvals)?;
    anyhow::ensure!(
        !channels.is_empty(),
        "no external channels enabled; configure Slack, Telegram, QQ, or web credentials, or enable a channel, then run `agent-router serve`"
    );

    wait_external_channels(&mut channels).await
}

fn spawn_external_channels(
    config: AppConfig,
    router: Arc<dyn RouterService>,
    approvals: Arc<ApprovalBroker>,
) -> anyhow::Result<JoinSet<(&'static str, anyhow::Result<()>)>> {
    if config.web.enabled {
        config.web.validate_for_serve()?;
    }

    let mut channels: JoinSet<(&'static str, anyhow::Result<()>)> = JoinSet::new();
    if config.telegram.enabled {
        tracing::info!("starting Telegram channel");
        let router = router.clone();
        let approvals = approvals.clone();
        channels.spawn(async move {
            (
                "telegram",
                TelegramBotChannel::new(config.telegram, approvals)
                    .run(router)
                    .await,
            )
        });
    }
    if config.slack.enabled {
        tracing::info!("starting Slack channel");
        let router = router.clone();
        let approvals = approvals.clone();
        channels.spawn(async move {
            (
                "slack",
                SlackSocketModeChannel::new(config.slack, approvals)
                    .run(router)
                    .await,
            )
        });
    }
    if config.qq.enabled {
        tracing::info!("starting QQ channel");
        let router = router.clone();
        let approvals = approvals.clone();
        channels.spawn(async move {
            (
                "qq",
                QqBotChannel::new(config.qq, approvals).run(router).await,
            )
        });
    }
    if config.web.enabled {
        tracing::info!("starting web channel");
        let router = router.clone();
        channels.spawn(async move { ("web", WebChannel::new(config.web).run(router).await) });
    }
    Ok(channels)
}

async fn wait_external_channels(
    channels: &mut JoinSet<(&'static str, anyhow::Result<()>)>,
) -> anyhow::Result<()> {
    while let Some(result) = channels.join_next().await {
        handle_channel_result(result, None).await?;
    }
    Ok(())
}

async fn handle_channel_result(
    result: Result<(&'static str, anyhow::Result<()>), tokio::task::JoinError>,
    chat: Option<&mut tokio::task::JoinHandle<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    match result {
        Ok((channel, Ok(()))) => tracing::info!(channel, "channel task exited"),
        Ok((channel, Err(err))) => {
            tracing::warn!(error = %err, channel, "channel task failed");
            if let Some(chat) = chat {
                chat.abort();
            }
            return Err(err);
        }
        Err(err) => {
            tracing::warn!(error = %err, "channel task join failed");
            if let Some(chat) = chat {
                chat.abort();
            }
            return Err(err.into());
        }
    }
    Ok(())
}

async fn abort_and_drain_channels(channels: &mut JoinSet<(&'static str, anyhow::Result<()>)>) {
    channels.abort_all();
    while channels.join_next().await.is_some() {}
}

fn available_executor_names(config: &AppConfig) -> Vec<String> {
    let mut names = Vec::new();
    for name in known_agent_priority() {
        if config.executors.contains_key(*name) {
            names.push((*name).to_string());
        }
    }
    for name in config.executors.keys() {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn explicit_chat_mode_wins_without_tty() {
        assert_eq!(
            resolve_run_mode(Some(Command::Chat), true, false, false).unwrap(),
            RunMode::Chat
        );
    }

    #[test]
    fn explicit_serve_mode_wins_without_tty() {
        assert_eq!(
            resolve_run_mode(Some(Command::Serve), true, false, false).unwrap(),
            RunMode::Serve
        );
    }

    #[test]
    fn missing_command_without_explicit_config_defaults_to_chat_for_tty() {
        assert_eq!(
            resolve_run_mode(None, false, true, true).unwrap(),
            RunMode::Chat
        );
    }

    #[test]
    fn missing_command_with_explicit_config_defaults_to_chat_and_serve_for_tty() {
        assert_eq!(
            resolve_run_mode(None, true, true, true).unwrap(),
            RunMode::ChatAndServe
        );
    }

    #[test]
    fn missing_command_requires_explicit_mode_without_tty() {
        let err = resolve_run_mode(None, true, false, true).unwrap_err();
        assert!(err.to_string().contains("use `agent-router chat`"));
    }

    #[test]
    fn global_config_arg_is_accepted_after_chat_subcommand() {
        let cli = Cli::try_parse_from(["agent-router", "chat", "--config", "a.yaml"]).unwrap();

        assert_eq!(cli.command, Some(Command::Chat));
        assert_eq!(cli.config, Some(PathBuf::from("a.yaml")));
    }

    #[test]
    fn global_config_arg_is_accepted_after_serve_subcommand() {
        let cli = Cli::try_parse_from(["agent-router", "serve", "--config", "a.yaml"]).unwrap();

        assert_eq!(cli.command, Some(Command::Serve));
        assert_eq!(cli.config, Some(PathBuf::from("a.yaml")));
    }

    #[test]
    fn chat_log_path_prefers_explicit_log_file() {
        let old = std::env::var_os("AGENT_ROUTER_LOG_FILE");
        unsafe {
            std::env::set_var("AGENT_ROUTER_LOG_FILE", "/tmp/agent-router-test.log");
        }

        assert_eq!(
            chat_log_path(),
            Path::new("/tmp/agent-router-test.log").to_path_buf()
        );

        unsafe {
            match old {
                Some(value) => std::env::set_var("AGENT_ROUTER_LOG_FILE", value),
                None => std::env::remove_var("AGENT_ROUTER_LOG_FILE"),
            }
        }
    }
}
