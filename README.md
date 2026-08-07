# criptomeria-marketdata

A Rust application that subscribes to limit order book (LOB) and trade
market data from multiple crypto exchanges (OKX, Kraken, Bitstamp) via
[`cryptomeria-ingest`](https://github.com/fibonsai/cryptomeria-ingest)
and forwards the normalized events to subscribers over a TCP socket using
[NNG](https://nng.nanomsg.org/) pub/sub.

## Features

- Multi-exchange support through `cryptomeria-ingest` (OKX, Kraken, Bitstamp)
- Normalized LOB snapshots/updates and trade executions
- NNG PUB/SUB broker on `tcp://0.0.0.0:14242` with native topic filtering
- Dynamic per-item topics named `{type}__{instrument}` (e.g. `lob__btcusd`,
  `trade__btcusd`)
- Payload JSON augmented with the `exchange` field when missing
- Built-in log subscriber (gated by `--data-out`) that connects locally
  and logs every topic to stdout with the `[type-exchange]` prefix tag
- Periodic per-topic subscriber counts emitted as JSON, every
  `--show-subscriber-count-secs` seconds (default `5`)
- Async logging via `tracing` (no `println!`)
- Configurable depth, level filtering, and resilience (exponential backoff + jitter)
- Dry-run mode for local testing
- Graceful shutdown on Ctrl+C, with `--test-timeout-secs` for CI

## Quick Start

```bash
# Build
cargo build --release

# Run the NNG broker + connect to the exchange WS stream
./target/release/marketdata --config config.toml

# Run with the built-in log subscriber (prints every topic to stdout)
./target/release/marketdata --data-out --config config.toml

# CI / verification: auto-exit after 10 seconds
./target/release/marketdata --data-out --test-timeout-secs 10 --config config.toml

# Dry-run (no NNG broker, just note per item)
./target/release/marketdata --dry-run --config config.toml
```

Subscribers (e.g. `nngcat`, or any NNG SUB client) can connect to
`tcp://127.0.0.1:14242` and subscribe to topics like `lob__btcusd` or the
empty prefix to receive every topic.

## CLI

| Flag | Description |
|------|-------------|
| `-c, --config <path>` | Path to TOML config (default `config.toml`) |
| `--dry-run` | Do not start the NNG broker; log one line per item |
| `--port <port>` | Override the NNG TCP port from `config.toml` |
| `--data-out` | Also start the built-in log subscriber that prints every topic to stdout |
| `--show-subscriber-count-secs <secs>` | Interval in seconds for per-topic subscriber-count JSON lines (default `5`) |
| `--test-timeout-secs <secs>` | Exit automatically after this many seconds (`0` = no timeout; intended for tests/CI) |

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

[source.resilience]
initial_backoff_ms = 1000
max_backoff_ms = 60000
backoff_multiplier = 2.0
jitter_ms = 1000
heartbeat_interval_secs = 0
max_attempts = 0

[nng]
port = 14242
```

See the [cryptomeria-ingest documentation](https://github.com/fibonsai/cryptomeria-ingest)
for details on `max_level_pct`, resilience settings, and exchange-specific
instrument formats.

## Wire Format

Each message sent on the broker is `topic\0payload` — the topic stays the
prefix that NNG's native SUB topic filter matches against, while the
subscriber splits the frame back into `(topic, payload)`.

- `topic` = `{lob|trade}__{normalized_instrument}`
  (e.g. `lob__btcusdt`, `trade__ethusdt`)
- `payload` = JSON of the `MarketDataItem` from `cryptomeria-ingest`,
  augmented with `"exchange":"<exchange>"` if missing

Example payload:

```json
{"Lob":{"asks":[{"price":64738.0,"size":0.55}], "bids":[...], "ts":1786036204909},"exchange":"okx"}
```

## Logging

All log lines include a prefix tag identifying the source:

- `[lob-okx]:`, `[trade-okx]:`, ... — a received data item
- `[stdout_subscriber]:` — the built-in log subscriber lifecycle
- `[system]:` — broker, count reporter, timeouts and shutdown

Example per-topic count output:

```
[system]: {"subscribers":1,"timestamp":1786036263543,"topic":"lob__btcusdt"}
[system]: {"subscribers":1,"timestamp":1786036263543,"topic":"trade__btcusdt"}
```

## Architecture

This app is a thin wrapper around `cryptomeria-ingest`. The ingestion
library handles WebSocket connection management, snapshot-first
synchronization, automatic reconnection, and data normalization. This
binary loads the config, creates the stream, publishes to the NNG
broker, and (optionally) runs the built-in log subscriber and the
subscriber-count reporter.

Module layout (domain-first):

- `src/config.rs` — TOML parsing, `AppConfig`/`SourceConfig`/`NngConfig`
- `src/forward.rs` — pure helpers: topic construction, JSON payload
  building (with `exchange` augmentation), frame splitting, log prefix
- `src/registry.rs` — in-process `SubscriberRegistry` for per-topic
  subscriber counts (NNG PUB/SUB does not expose subscription state to
  the publisher)
- `src/broker.rs` — NNG `Pub0` socket + dedicated sender thread
- `src/subscriber.rs` — built-in NNG `Sub0` log subscriber
- `src/bin/marketdata.rs` — CLI, orchestration, reporting task, shutdown

See the architecture decision records:

- `docs/adr/Core Architecture/ADR-001-...-using-cryptomeria-ingest.md`
- `docs/adr/Core Architecture/ADR-002-...-replace-nats-with-nng-tcp-subscriber.md`

## License

Apache-2.0
