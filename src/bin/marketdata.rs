use anyhow::{Context, Result};
use clap::Parser;
use criptomeria_marketdata::broker::Broker;
use criptomeria_marketdata::config::parse_config;
use criptomeria_marketdata::forward::{build_payload, topic_for};
use criptomeria_marketdata::subscriber::StdoutSubscriber;
use cryptomeria_ingest as ingest;
use env_logger::Env;
use futures_util::StreamExt;
use log::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

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
        default_value_t = 0,
        help = "Exit automatically after this many seconds (0 = no timeout; for tests/CI)"
    )]
    test_timeout_secs: u64,
    #[arg(
        long,
        help = "Override WebSocket silence-timeout (seconds) for all exchanges (0 or omitted = use config.toml)"
    )]
    silence_timeout_secs: Option<u64>,
}

fn init_logger() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();
}

/// Drain a single exchange stream, publishing each item under
/// `{type}__{instrument}` to the shared broker. Runs as its own tokio task so
/// several exchanges run in parallel.
async fn run_exchange(
    exchange: String,
    instrument: String,
    source: ingest::DataSourceConfig,
    suffix: Option<String>,
    broker: Option<Arc<Broker>>,
) {
    info!("[{exchange}]: starting stream");

    let mut stream = match ingest::stream(source).await {
        Ok(stream) => stream,
        Err(e) => {
            log::error!("[{exchange}]: failed to create stream: {e}");
            return;
        }
    };

    loop {
        match stream.next().await {
            Some(Ok(item)) => {
                let topic = topic_for(&instrument, &item, suffix.as_deref());
                match build_payload(&item) {
                    Ok(payload) => {
                        if let Some(broker) = &broker {
                            if let Err(e) = broker.publish(&topic, &payload) {
                                log::error!("[{exchange}]: publish failed: {e}");
                            }
                        } else {
                            info!("[{exchange}]: dry-run: skipped forwarding");
                        }
                    }
                    Err(e) => {
                        log::error!("[{exchange}]: payload encoding failed: {e}");
                    }
                }
            }
            Some(Err(e)) => {
                log::error!("[{exchange}]: stream error: {e}");
                break;
            }
            None => {
                info!("[{exchange}]: stream ended");
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cli = Cli::parse();

    let content = std::fs::read_to_string(&cli.config)
        .with_context(|| format!("failed to read config {}", cli.config))?;
    let mut app = parse_config(&content)?;
    if let Some(port) = cli.port {
        app.nng.port = port;
    }
    app.override_silence_timeout_secs(cli.silence_timeout_secs.filter(|&s| s > 0));

    // Build and validate every configured exchange up front so a bad config
    // fails before the broker binds. Each exchange gets its own independent
    // stream, so they run fully in parallel.
    let sources = app
        .validated_sources()
        .with_context(|| "no [source.<exchange>] section found in config")?;

    let broker = if cli.dry_run {
        info!("[system]: dry-run: NNG broker not started");
        None
    } else {
        Some(Arc::new(Broker::bind(app.nng.port).with_context(|| {
            format!("failed to start NNG broker on port {}", app.nng.port)
        })?))
    };

    let subscriber =
        if cli.data_out {
            Some(StdoutSubscriber::connect(app.nng.port).with_context(|| {
                format!("failed to start log subscriber on port {}", app.nng.port)
            })?)
        } else {
            None
        };

    // Shared shutdown trigger: the test-timeout watcher and the Ctrl+C watcher
    // both notify `shutdown`. The main loop drains exchange tasks until a
    // shutdown signal arrives or every source has ended on its own.
    let shutdown = Arc::new(tokio::sync::Notify::new());

    if cli.test_timeout_secs > 0 {
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(cli.test_timeout_secs)).await;
            info!("[system]: test timeout reached, shutting down");
            shutdown.notify_one();
        });
    }

    let ctrl_c_watcher = tokio::spawn({
        let shutdown = Arc::clone(&shutdown);
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("[system]: Ctrl+C received, shutting down");
            shutdown.notify_one();
        }
    });

    let mut set: JoinSet<()> = JoinSet::new();
    for (exchange, instrument, source, suffix) in sources {
        let broker = broker.clone();
        set.spawn(run_exchange(exchange, instrument, source, suffix, broker));
    }

    // Run each exchange as an independent task: a single source ending or
    // erroring does not stop the others. The application only exits on Ctrl+C,
    // the test timeout, or once every source has ended.
    let mut exited = false;
    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                info!("[system]: shutdown signal received");
                break;
            }
            res = set.join_next() => {
                match res {
                    Some(Ok(())) => {
                        info!("[system]: an exchange source ended");
                    }
                    Some(Err(ref e)) if e.is_panic() => {
                        log::error!("[system]: a source task panicked: {e}");
                    }
                    Some(Err(e)) => {
                        info!("[system]: a source task aborted: {e}");
                    }
                    None => {}
                }
                if set.is_empty() {
                    exited = true;
                    break;
                }
            }
        }
    }

    // Abort any tasks still running (e.g. on signal) so none outlive shutdown.
    set.abort_all();
    while set.join_next().await.is_some() {}

    // The Ctrl+C watcher is no longer needed once we are shutting down.
    ctrl_c_watcher.abort();

    drop(subscriber);
    drop(broker);
    if exited {
        info!("[system]: all exchange sources ended, bye");
    } else {
        info!("[system]: bye");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_silence_timeout_secs_flag() {
        let cli = Cli::try_parse_from(["marketdata", "--silence-timeout-secs", "20"]).unwrap();
        assert_eq!(cli.silence_timeout_secs, Some(20));
    }

    #[test]
    fn silence_timeout_secs_defaults_to_none() {
        let cli = Cli::try_parse_from(["marketdata"]).unwrap();
        assert_eq!(cli.silence_timeout_secs, None);
    }
}
