use std::{path::PathBuf, sync::Arc, time::Duration};

use agent_router::{
    approval::ApprovalBroker,
    channel::{qq::QqBotChannel, slack::SlackSocketModeChannel},
    config::{AppConfig, default_config_path, load_dotenv},
    executor::registry::ExecutorRegistry,
    router::{AgentRouter, RouterService, SessionApprovalPolicy},
    session::store::InMemorySessionStore,
};
use clap::Parser;
use tokio::task::JoinSet;

#[derive(Debug, Parser)]
#[command(version, about = "Slack to ACP agent router")]
struct Cli {
    #[arg(long, env = "AGENT_ROUTER_CONFIG")]
    config: Option<PathBuf>,

    #[arg(long, env = "AGENT_ROUTER_ENV_FILE")]
    env_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "agent_router=info,warn".to_string()),
        )
        .init();

    let cli = Cli::parse();
    load_dotenv(cli.env_file.as_deref());

    let config_path = cli.config.or_else(default_config_path);
    if let Some(path) = &config_path {
        tracing::info!(path = %path.display(), "loading config");
    } else {
        tracing::info!("loading config from environment and built-in defaults");
    }
    let config = AppConfig::load(config_path.as_deref())?;

    let store = Arc::new(InMemorySessionStore::default());
    let approvals = Arc::new(ApprovalBroker::with_policy(
        Duration::from_secs(120),
        Arc::new(SessionApprovalPolicy::new(
            config.router.default_executor.clone(),
            config.approval.default_mode,
            store.clone(),
        )),
    ));
    let executor = Arc::new(ExecutorRegistry::new(
        config.executors.clone(),
        approvals.clone(),
    ));
    let router: Arc<dyn RouterService> = Arc::new(AgentRouter::with_approval_mode(
        config.router.default_executor.clone(),
        config.approval.default_mode,
        store,
        executor,
        approvals.clone(),
    ));

    let mut channels: JoinSet<(&'static str, anyhow::Result<()>)> = JoinSet::new();
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
    anyhow::ensure!(
        !channels.is_empty(),
        "no channels enabled; configure Slack or QQ credentials, or set a channel's enabled flag"
    );

    while let Some(result) = channels.join_next().await {
        match result {
            Ok((channel, Ok(()))) => tracing::info!(channel, "channel task exited"),
            Ok((channel, Err(err))) => {
                tracing::warn!(error = %err, channel, "channel task failed");
                return Err(err);
            }
            Err(err) => {
                tracing::warn!(error = %err, "channel task join failed");
                return Err(err.into());
            }
        }
    }
    Ok(())
}
