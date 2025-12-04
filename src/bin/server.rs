use anyhow::Result;
use clap::Parser;
use std::time::Instant;
use rivetdb::config::AppConfig;
use rivetdb::datafusion::HotDataEngine;
use rivetdb::http::app_server::AppServer;

#[derive(Parser)]
#[command(name = "rivet-server", about = "Rivet HTTP Server")]
struct Cli {
    /// Path to config file
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    tracing::info!("Starting Rivet HTTP Server");

    // Load configuration
    let config = AppConfig::load(&cli.config)?;
    config.validate()?;

    tracing::info!("Configuration '{}' loaded successfully", &cli.config);

    // Initialize engine from config
    let engine = HotDataEngine::from_config(&config)?;

    tracing::info!("Engine initialized");

    // Create router
    let app = AppServer::new(engine);

    // Create server address
    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!("Server started in {}ms", now.elapsed().as_millis());
    tracing::info!("Server listening on {}", addr);

    // Start server
    axum::serve(listener, app.router).await?;

    Ok(())
}
