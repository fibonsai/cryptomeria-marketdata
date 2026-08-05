# criptomeria-marketdata

A Rust application that subscribes to limit order book (LOB) and trade
market data from multiple crypto exchanges (OKX, Kraken, Bitstamp) via
[`cryptomeria-ingest`](https://github.com/fibonsai/cryptomeria-ingest)
and forwards the normalized events to a NATS broker.

## Features

- Multi-exchange support through `cryptomeria-ingest` (OKX, Kraken, Bitstamp)
- Normalized LOB snapshots/updates and trade executions
- Configurable depth, level filtering, and resilience (exponential backoff + jitter)
- NATS publishing with per-kind subjects
- Dry-run mode for local testing
- Graceful shutdown on Ctrl+C

## Quick Start

```bash
# Build
cargo build --release

# Run (requires a running NATS server at nats://localhost:4222)
./target/release/marketdata --config config.toml

# Dry-run (no NATS connection, prints JSON to stdout)
./target/release/marketdata --dry-run --config config.toml
```

## Configuration

Edit `config.toml` (see the example file):

```toml
[source]
exchange = "okx"           # okx | kraken | bitstamp
region = "global"          # global | europe
instrument = "BTC-USDT"    # exchange-native symbol
data_kind = "both"         # lob | trade | both | lob|trade
max_level = 10             # optional: limit order book depth per side
snapshot_depth = 400       # depth for Bitstamp REST snapshot

[nats]
url = "nats://localhost:4222"
subject_lob = "marketdata.lob"
subject_trade = "marketdata.trade"
```

See the [cryptomeria-ingest documentation](https://github.com/fibonsai/cryptomeria-ingest)
for details on `max_level_pct`, resilience settings, and exchange-specific
instrument formats.

## Output Subjects

By default:
- LOB snapshots/updates → `marketdata.lob`
- Trade executions → `marketdata.trade`

Each message is a JSON-encoded `MarketDataItem` (see `cryptomeria-ingest` types).

## Architecture

This app is a thin wrapper around `cryptomeria-ingest`. The ingestion
library handles WebSocket connection management, snapshot-first
synchronization, automatic reconnection, and data normalization. This
binary loads the config, creates the stream, and forwards each item to
NATS.

See `adr/ADR-1.md` for the architectural decision record.

## License

Apache-2.0