use anyhow::{Context, Result};
use clap::Parser;
use criptomeria_marketdata::broker::Broker;
use criptomeria_marketdata::config::parse_config;
use criptomeria_marketdata::forward::{build_payload, topic_for};
use criptomeria_marketdata::registry::SharedRegistry;
use criptomeria_marketdata::subscriber::StdoutSubscriber;
use cryptomeria_ingest as ingest;
use futures_util::StreamExt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(
    version,
    about = "Forward LOB/trade market data from cryptomeria-ingest to NNG TCP subscribers"
)]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[arg(long, help = "Do not start the NNG broker; just print a note per item")]
    dry_run: bool,
    #[arg(long, help = "Override the NNG TCP port from config.toml")]
    port: Option<u16>,
    #[arg(
        long,
        help = "Also start the built-in log subscriber that prints all topics to stdout"
    )]
    data_out: bool,
    #[arg(
        long,
        default_value_t = 5,
        help = "Interval in seconds to report subscriber counts per topic (JSON)"
    )]
    show_subscriber_count_secs: u64,
    #[arg(
        long,
        default_value_t = 0,
        help = "Exit automatically after this many seconds (0 = no timeout; for tests/CI)"
    )]
    test_timeout_secs: u64,
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn init_tracing() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() -> Result<()> {
    ingest::init();
    init_tracing();
    let cli = Cli::parse();

    let content = std::fs::read_to_string(&cli.config)
        .with_context(|| format!("failed to read config {}", cli.config))?;
    let mut app = parse_config(&content)?;
    if let Some(port) = cli.port {
        app.nng.port = port;
    }
    let source = app.source.to_data_source()?;

    let registry = SharedRegistry::new();
    let broker = if cli.dry_run {
        tracing::info!("[system]: dry-run: NNG broker not started");
        None
    } else {
        let broker = Broker::bind(app.nng.port, registry.clone())
            .with_context(|| format!("failed to start NNG broker on port {}", app.nng.port))?;
        tracing::info!(
            "[system]: NNG broker listening on tcp://0.0.0.0:{}",
            app.nng.port
        );
        Some(broker)
    };

    let subscriber = if cli.data_out {
        let sub = StdoutSubscriber::connect(app.nng.port, registry.clone())
            .with_context(|| format!("failed to start log subscriber on port {}", app.nng.port))?;
        Some(sub)
    } else {
        None
    };

    if !cli.dry_run && cli.show_subscriber_count_secs > 0 {
        let counts_registry = registry.clone();
        let interval_secs = cli.show_subscriber_count_secs;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                ticker.tick().await;
                let timestamp = now_millis();
                for (topic, count) in counts_registry.snapshot_counts() {
                    let json = serde_json::json!({
                        "topic": topic,
                        "subscribers": count,
                        "timestamp": timestamp,
                    });
                    tracing::info!("[system]: {json}");
                }
            }
        });
    }

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    if cli.test_timeout_secs > 0 {
        let tx = shutdown_tx;
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(cli.test_timeout_secs)).await;
            tracing::info!("[system]: test timeout reached, shutting down");
            let _ = tx.send(());
        });
    }

    let mut stream = ingest::stream(source)
        .await
        .context("failed to create market data stream")?;

    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(item)) => {
                        if let Some(broker) = &broker {
                            let topic = topic_for(&app.source.instrument, &item);
                            match build_payload(&item, &app.source.exchange) {
                                Ok(payload) => {
                                    if let Err(e) = broker.publish(&topic, &payload) {
                                        tracing::error!("[system]: publish failed: {e}");
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("[system]: payload encoding failed: {e}");
                                }
                            }
                        } else {
                            tracing::info!("[system]: dry run: skipped forwarding");
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("[system]: stream error: {e}");
                        break;
                    }
                    None => {
                        tracing::info!("[system]: stream ended");
                        break;
                    }
                }
            }
            _ = &mut shutdown_rx => {
                tracing::info!("[system]: shutdown signal received");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("[system]: Ctrl+C received, shutting down");
                break;
            }
        }
    }

    drop(subscriber);
    drop(broker);
    tracing::info!("[system]: bye");
    Ok(())
}
