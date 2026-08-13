# criptomeria-marketdata

A Rust application that subscribes to limit order book (LOB) and trade
market data from multiple crypto exchanges (OKX, Kraken, Bitstamp, Bitvavo) via
[`cryptomeria-ingest`](https://github.com/fibonsai/cryptomeria-ingest) 
and forwards the normalized events to subscribers over a TCP socket using
[NNG](https://nng.nanomsg.org/) pub/sub.

## Features

- Multi-exchange support through `cryptomeria-ingest` (OKX, Kraken, Bitstamp, Bitvavo)
- Normalized LOB snapshots/updates and trade executions
- NNG PUB/SUB broker on `tcp://0.0.0.0:14242` with native topic filtering
- Dynamic per-item topics named `{type}__{instrument}` (e.g. `lob__btcusd`,
  `trade__btcusd`), with optional `suffix_topic` override per instrument
- Payload JSON preserved exactly as received from `cryptomeria-ingest`
- Built-in log subscriber (gated by `--data-out`) that connects locally
  and logs every topic to stdout as structured JSON:
  `{"topic":"...","payload":{...}}`
- Async logging via `env_logger` behind the `log` facade (no `println!`); controlled by `RUST_LOG`
- Configurable depth, level filtering, resilience (exponential backoff + jitter), and instrument fallback;
  supports multiple instruments per exchange under `[source.<exchange>.instrument.<alias>]`
- Dry-run mode for local testing
- Graceful shutdown on Ctrl+C, with `--test-timeout-secs` for CI

## Quick Start

```bash
# Build
cargo build --release

# Use config example and adjust
cp config.toml.example config.toml

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
| `--test-timeout-secs <secs>` | Exit automatically after this many seconds (`0` = no timeout; intended for tests/CI) |

## Configuration

> [!TIP]
> Copy config.toml.example to create your config.toml file.

Edit `config.toml` (see the example file):

```toml
[source.okx]
# The section name "okx" is the exchange id; there is no `exchange` field.
region = "global"          # global | europe
data_kind = "both"         # lob | trade | both | lob|trade

# Instruments are configured under [source.<exchange>.instrument.<alias>].
# The <alias> key is used to look up the matching fallback mapping under
# [source.<exchange>.fallback.<alias>]. An empty-string alias ("") selects
# the exchange-only fallback rule.
[source.okx.instrument.btcusd]
instrument = "BTC-USDT"    # exchange-native symbol
suffix_topic = "okx_btcusd"  # optional: verbatim topic suffix (no normalization)
max_level = 10             # optional: limit order book depth per side
max_level_pct = 0.0        # optional: max % from best price (conflicts with max_level)
# checksum_log = true      # optional: warn on Kraken checksum mismatch (default false)
# crossguard_log = true    # optional: warn on Kraken crossing-guard rejection (default false)

[source.okx.resilience]
initial_backoff_ms = 1000
max_backoff_ms = 60000
backoff_multiplier = 2.0
jitter_ms = 1000
heartbeat_interval_secs = 0
max_attempts = 0

# Optional: instrument symbol fallback. See cryptomeria-ingest README.
# [source.okx.fallback.btcusd]
# base_mappings = ["BTC", "XBT"]
# quote_mappings = ["USDT", "USD"]
# separator_mappings = ["-", "/"]
# case_fallback = "upper"

[nng]
port = 14242
```

Multiple exchanges run in parallel. Add more `[source.<exchange>]` sections and
each is consumed by its own independent background task, all publishing to the
shared NNG broker. **Topics are `{type}__{instrument}` only — the exchange is not
part of the topic** — so use distinct instruments per exchange to avoid
collisions.

When the default normalized instrument is not sufficient (e.g. two exchanges
share the same instrument symbol), set `suffix_topic` to a verbatim string that
becomes the topic segment: `{type}__{suffix_topic}`.

```toml
[source.okx]
region = "global"
data_kind = "trade"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"

[source.kraken]
region = "global"
data_kind = "trade"

[source.kraken.instrument.btcusd]
instrument = "XBT/USD"

[nng]
port = 14242
```

See the [cryptomeria-ingest documentation](https://github.com/fibonsai/cryptomeria-ingest)
for details on `max_level_pct`, resilience settings, exchange-specific instrument
formats, and instrument validation/fallback via `alias` and `fallback`.

## Wire Format

Each message sent on the broker is `topic\0payload` — the topic stays the
prefix that NNG's native SUB topic filter matches against, while the
subscriber splits the frame back into `(topic, payload)`.

- `topic` = `{lob|trade}__{normalized_instrument}`
  (e.g. `lob__btcusdt`, `trade__ethusdt`)
- `payload` = JSON of the `MarketDataItem` from `cryptomeria-ingest`,
  preserved byte-for-byte (no fields added)

Example payload:

```json
{"trade":{"exchange":"okx","price":100.0,"size":1.0,"side":"buy","ts":1786036204909}}
```

## Logging

All log lines include a prefix tag identifying the source:

- `[okx]:`, `[kraken]:`, ... — per-exchange lifecycle: stream start, errors,
  publish failures, and (in `--dry-run`) per-item skipped forwarding
- `[stdout_subscriber]:` — the built-in log subscriber lifecycle
- `[system]:` — broker, timeouts and shutdown

## Architecture

This app is a thin wrapper around `cryptomeria-ingest`. The ingestion
library handles WebSocket connection management, snapshot-first
synchronization, automatic reconnection, and data normalization. This
binary loads the config, spawns one independent background task per
configured exchange (all publishing to the shared NNG broker), and
(optionally) runs the built-in log subscriber.

Module layout (domain-first):

- `src/config.rs` — TOML parsing, `AppConfig`/`SourceConfig`/`NngConfig`,
  multi-source collection (`exchange_sources`, `validated_sources`)
- `src/forward.rs` — pure helpers: topic construction, JSON payload
  building, frame splitting
- `src/broker.rs` — NNG `Pub0` socket + dedicated sender thread, shared via
  `Arc<Broker>` across exchange tasks
- `src/subscriber.rs` — built-in NNG `Sub0` log subscriber
- `src/bin/marketdata.rs` — CLI, orchestration of parallel exchange tasks, shutdown

See the architecture decision records:

- `docs/adr/Core Architecture/ADR-001-...-using-cryptomeria-ingest.md`
- `docs/adr/Core Architecture/ADR-002-...-replace-nats-with-nng-tcp-subscriber.md`
- `docs/adr/Core Architecture/ADR-003-...-remove-subscriber-registry.md`
- `docs/adr/Core Architecture/ADR-004-...-pass-exchange-to-subscriber-from-config.md`
- `docs/adr/Core Architecture/ADR-005-...-remove-exchange-param-from-log-subscriber.md`
- `docs/adr/Core Architecture/ADR-006-...-restructure-config-schema-to-use-exchange-id-as-subkey.md`
- `docs/adr/Core Architecture/ADR-007-...-multi-exchange-parallel-ingestion.md`
- `docs/adr/Core Architecture/ADR-008-...-add-suffix-topic-config-field.md`
- `docs/adr/Core Architecture/ADR-009-...-migrate-logging-from-tracing-to-rasant.md`
- `docs/adr/Core Architecture/ADR-010-...-migrate-logging-from-rasant-to-log-envlogger.md`
- `docs/adr/Integration/ADR-011-...-bitvavo-credential-resolution.md`
- `docs/adr/Core Architecture/ADR-012-...-env-file-loading.md`
- `docs/adr/Core Architecture/ADR-013-...-expose-crossguard-log-config.md`
- `docs/adr/Core Architecture/ADR-014-...-support-multi-instruments-per-exchange.md`

## License

Apache-2.0
