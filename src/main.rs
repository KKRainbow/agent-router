use std::{path::PathBuf, sync::Arc};

use agent_router::{
    channel::slack::SlackSocketModeChannel,
    config::{AppConfig, default_config_path, load_dotenv},
    executor::acp::AcpExecutorManager,
    router::{AgentRouter, RouterService},
    session::store::InMemorySessionStore,
};
use clap::Parser;

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
    let executor = Arc::new(AcpExecutorManager::new(config.executors.clone()));
    let router: Arc<dyn RouterService> = Arc::new(AgentRouter::new(
        config.router.default_executor.clone(),
        store,
        executor,
    ));

    SlackSocketModeChannel::new(config.slack).run(router).await
}
