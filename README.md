# cryptomeria-marketdata

A Rust application that subscribes to limit order book (LOB) and trade
market data from multiple crypto exchanges (OKX, Kraken, Bitstamp, Bitvavo) via
[`cryptomeria-ingest`](https://github.com/fibonsai/cryptomeria-ingest) and
forwards the normalized events to subscribers over a TCP socket using
[NNG](https://nng.nanomsg.org/) pub/sub.

## Table of Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [CLI](#cli)
- [Data Kinds](#data-kinds)
- [Configuration](#configuration)
  - [.env File & Secrets](#env-file--secrets)
  - [Multi-Exchange & Multi-Instrument](#multi-exchange--multi-instrument)
- [Topics & Wire Format](#topics--wire-format)
  - [Consuming the Stream](#consuming-the-stream)
- [Logging](#logging)
- [Architecture](#architecture)
- [Development](#development)
- [License](#license)

## Quick Start

```bash
# Build
cargo build --release

# Copy the example config and adjust to your needs
cp config.toml.example config.toml

# Run the NNG broker + connect to the exchange WebSocket streams
./target/release/marketdata --config config.toml

# Run with the built-in log subscriber (prints every topic to stdout)
./target/release/marketdata --data-out --config config.toml

# CI / verification: auto-exit after 10 seconds
./target/release/marketdata --data-out --test-timeout-secs 10 --config config.toml

# Dry-run (no NNG broker; log one note per item)
./target/release/marketdata --dry-run --config config.toml
```

Subscribers (e.g. `nngcat`, or any NNG SUB client) can connect to
`tcp://127.0.0.1:14242` and subscribe to topics like `lob__btcusdt` or an
empty prefix to receive every topic. See [Consuming the Stream](#consuming-the-stream).

## Features

- Multi-exchange support through `cryptomeria-ingest` (OKX, Kraken, Bitstamp, Bitvavo)
- Normalized LOB snapshots/updates and trade executions via the unified `MarketDataItem` enum
- Snapshot-first stream pattern (the first LOB item on each stream is a full book snapshot)
- NNG PUB/SUB broker on `tcp://0.0.0.0:14242` with native topic filtering
- Dynamic per-item topics named `{type}__{instrument}` (e.g. `lob__btcusd`,
  `trade__btcusd`), with optional `suffix_topic` override per instrument
- Payload JSON preserved byte-for-byte (no fields added)
- Built-in log subscriber (gated by `--data-out`) that connects locally
  and logs every topic to stdout as structured JSON:
  `{"topic":"...","payload":{...}}`
- Async logging via `env_logger` behind the `log` facade (no `println!`); controlled by `RUST_LOG`
- Configurable depth, level filtering, resilience (exponential backoff + jitter),
  instrument fallback, and delta-buffering warm-up; supports multiple instruments
  per exchange under `[source.<exchange>.instrument.<alias>]`
- Automatic reconnection with exponential backoff + jitter (via `cryptomeria-ingest`)
- Graceful shutdown on Ctrl+C, with `--test-timeout-secs` for CI
- No task leaks: per-exchange tasks are aborted and drained from the `JoinSet` on shutdown
- Dry-run mode for local testing
- Best-effort `.env` file loading at startup

## CLI

| Flag | Description |
|------|-------------|
| `-c, --config <path>` | Path to TOML config (default `config.toml`) |
| `--dry-run` | Do not start the NNG broker; log one line per item |
| `--port <port>` | Override the NNG TCP port from `config.toml` |
| `--data-out` | Also start the built-in log subscriber that prints every topic to stdout |
| `--test-timeout-secs <secs>` | Exit automatically after this many seconds (`0` = no timeout; for tests/CI) |
| `--silence-timeout-secs <secs>` | Override the WebSocket silence timeout (seconds) for all exchanges (`0` or omitted = use config) |

## Data Kinds

The `data_kind` field controls which data types each exchange subscribes to:

| Value | Description |
|-------|-------------|
| `lob` | Limit order book snapshots and updates only |
| `trade` | Trade executions only |
| `both` | Both LOB and trades |
| `lob\|trade` | Equivalent to `both` |

## Configuration

> [!TIP]
> Copy `config.toml.example` to create your `config.toml` file.

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
suffix_topic = "okx_btcusdt"  # optional: verbatim topic suffix (no normalization)
max_level = 10             # optional: limit order book depth per side
max_level_pct = 0.0        # optional: max % from best price (conflicts with max_level)
# checksum_log = true      # optional: warn on Kraken checksum mismatch (default false)
# crossguard_log = true    # optional: warn on Kraken crossing-guard rejection (default false)
# snapshot_delay = 6       # optional: diff-order-book deltas to buffer before REST snapshot (default 6; 0 = disabled)

[source.okx.resilience]
initial_backoff_ms = 1000
max_backoff_ms = 60000
backoff_multiplier = 2.0
jitter_ms = 1000
heartbeat_interval_secs = 0
max_attempts = 0
# silence_timeout_secs = 30  # optional: auto-reconnect after N seconds of silence

# Optional: instrument symbol fallback. See cryptomeria-ingest README.
# [source.okx.fallback.btcusd]
# base_mappings = ["BTC", "XBT"]
# quote_mappings = ["USDT", "USD"]
# separator_mappings = ["-", "/"]
# case_fallback = "upper"

[nng]
port = 14242
```

### `.env` File & Secrets

The app automatically loads a `.env` file from the current working directory at
startup (best-effort — a missing file is silently ignored). Place secrets such as
`BITVAVO_API_KEY` and `BITVAVO_API_SECRET` in `.env` (which is git-ignored) instead
of `config.toml`.

Precedence: explicit shell env vars > `.env` file > `config.toml` credential fields.

```bash
# .env (do NOT commit — add to .gitignore)
BITVAVO_API_KEY=your-bitvavo-api-key
BITVAVO_API_SECRET=your-bitvavo-api-secret
```

### Multi-Exchange & Multi-Instrument

Multiple exchanges run in parallel. Add more `[source.<exchange>]` sections and
each is consumed by its own independent background task, all publishing to the
shared NNG broker. **Topics are `{type}__{instrument}` only — the exchange is not
part of the topic** — so use distinct instruments per exchange to avoid collisions.

When the default normalized instrument is not sufficient (e.g. two exchanges
share the same instrument symbol), set `suffix_topic` to a verbatim string that
becomes the topic segment: `{type}__{suffix_topic}`.

```toml
[source.okx]
region = "global"
data_kind = "trade"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"
# Use "okx_btcusd" so the topic is "trade__okx_btcusd"
suffix_topic = "okx_btcusd"

[source.kraken]
region = "global"
data_kind = "trade"

[source.kraken.instrument.btcusd]
instrument = "XBT/USD"
# Use "kraken_btcusd" to disambiguate from OKX
suffix_topic = "kraken_btcusd"

[nng]
port = 14242
```

A single exchange can also subscribe to multiple instruments:

```toml
[source.okx]
region = "global"
data_kind = "both"

[source.okx.instrument.btcusd]
instrument = "BTC-USDT"
suffix_topic = "btcusd"
max_level = 10

[source.okx.instrument.ethusd]
instrument = "ETH-USDT"
suffix_topic = "ethusd"
max_level = 10

[source.okx.resilience]
initial_backoff_ms = 1000
max_backoff_ms = 60000
backoff_multiplier = 2.0
jitter_ms = 1000
max_attempts = 0

[nng]
port = 14242
```

See the [cryptomeria-ingest documentation](https://github.com/fibonsai/cryptomeria-ingest)
for details on `max_level_pct`, resilience settings, exchange-specific instrument
formats, and instrument validation/fallback via `alias` and `fallback`.

## Topics & Wire Format

Each message sent on the broker is `topic\0payload` — the topic stays the
prefix that NNG's native SUB topic filter matches against, while the
subscriber splits the frame back into `(topic, payload)`.

- `topic` = `{lob|trade}__{normalized_instrument}` (e.g. `lob__btcusdt`,
  `trade__ethusdt`). When `suffix_topic` is set, the instrument segment is
  used verbatim instead of normalized.
- `payload` = JSON of the `MarketDataItem` from `cryptomeria-ingest`,
  preserved byte-for-byte (no fields added)

Example **trade** payload:

```json
{"trade":{"exchange":"okx","price":100.0,"size":1.0,"side":"buy","ts":1786036204909}}
```

Example **LOB** payload (snapshot):

```json
{"lob":{"exchange":"kraken","bids":[["100.0",1.0],["99.5",2.0]],"asks":[["100.5",1.5]],"ts":1786036204909}}
```

### Consuming the Stream

Any NNG SUB client can connect to `tcp://127.0.0.1:14242` and subscribe to
topics by prefix. The message body is the framed `topic\0payload` bytes.

**Using `nngcat`:**

```bash
# Subscribe to all topics (empty prefix)
nngcat -l tcp://127.0.0.1:14242 -s ""

# Subscribe to LOB updates for BTC/USDT only
nngcat -l tcp://127.0.0.1:14242 -s "lob__btcusdt"
```

**Using a Rust NNG SUB client:**

```rust
use nng::{Protocol, Socket};
use nng::options::protocol::pubsub::Subscribe;

let sub = Socket::new(Protocol::Sub0).unwrap();
sub.set_opt::<Subscribe>(b"trade__btcusdt".to_vec()).unwrap(); // subscribe to trade BTC/USDT
// Or subscribe to everything:
// sub.set_opt::<Subscribe>(Vec::<u8>::new()).unwrap();
sub.dial("tcp://127.0.0.1:14242").unwrap();

loop {
    let msg = sub.recv().unwrap();
    // msg.as_slice() = b"trade__btcusdt\0{...}"
    // Split on the null byte to recover (topic, payload)
}
```

## Logging

All log lines carry a prefix tag identifying the source:

| Prefix | Source |
|--------|--------|
| `[okx]:`, `[kraken]:`, etc. | Per-exchange lifecycle: stream start, errors, publish failures, and (in `--dry-run`) per-item skip notes |
| `[broker]:` | NNG broker lifecycle: subscriber connect/disconnect events |
| `[stdout_subscriber]:` | Built-in log subscriber lifecycle and receive errors |
| `[system]:` | Broker overflow warnings, shutdown signals, and system lifecycle |

Control log verbosity with `RUST_LOG` (default: `info`):

```bash
RUST_LOG=debug ./target/release/marketdata --data-out --config config.toml
```

## Architecture

This app is a thin wrapper around `cryptomeria-ingest`. The ingestion library
handles WebSocket connection management, snapshot-first synchronization, automatic
reconnection, and data normalization. This binary loads the config, spawns one
independent background task per configured exchange instrument (all publishing to
the shared NNG broker), and (optionally) runs the built-in log subscriber.

```
┌─────────────────────────────────────────────────────────┐
│                      marketdata (CLI)                     │
│  ┌──────────┐   ┌────────┐   ┌──────────────┐          │
│  │  config  │──▶│ broker │──▶│  NNG PUB/SUB  │── tcp://0.0.0.0:14242
│  │  parse   │   │ (shared) │   └──────────────┘          │
│  └──────────┘   └────┬─────┘                            │
│         │             │                                   │
│         │   ┌─────────┴──────────┐                        │
│         │   │  ┌────────────┐   │   ┌───────────────┐   │
│         └──▶│  │  exchange A │   │  │  ┌───────────┐ │   │
│         │   │  │  instrument 1││   │  │  StdoutSub │ │   │
│         │   │  │  instrument 2││   │  │  (optional)│ │   │
│         │   │  │  ...          ││   │  └───────────┘ │   │
│         │   │  └─────────────┘│   └───────────────────┘   │
│          (each exchange +        │                         │
│           instrument = 1 task)   │                         │
│                              JoinSet                     │
└─────────────────────────────────────────────────────────┘
```

Module layout (domain-first):

- `src/config.rs` — TOML parsing, `AppConfig`/`SourceConfig`/`InstrumentConfig`/`NngConfig`,
  multi-source collection (`exchange_sources`, `validated_sources`), credential resolution,
  data-kind parsing
- `src/forward.rs` — pure helpers: topic construction (`topic_for`), payload building
  (`build_payload`), frame splitting (`frame_message`/`split_frame`), log entry building (`build_log_entry`)
- `src/broker.rs` — NNG `Pub0` socket + dedicated sender thread, shared via `Arc<Broker>`
  across exchange tasks; `publish` is non-blocking with overflow dropping
- `src/subscriber.rs` — built-in NNG `Sub0` log subscriber (logs structured JSON to stdout)
- `src/env.rs` — best-effort `.env` file loading at startup
- `src/bin/marketdata.rs` — CLI entry point; spawns one parallel per-exchange/instrument task
  (`JoinSet`) sharing the broker; handles graceful shutdown (Ctrl+C, test timeout, or all
  sources ended)

See the architecture decision records:

**Core Architecture:**
- `docs/adr/Core Architecture/ADR-001` — Use cryptomeria-ingest as library and forward to NNG
- `docs/adr/Core Architecture/ADR-002` — Replace NATS with NNG + TCP subscriber protocol
- `docs/adr/Core Architecture/ADR-003` — Remove in-process subscriber registry and count reporting
- `docs/adr/Core Architecture/ADR-004` — Pass exchange to StdoutSubscriber from config
- `docs/adr/Core Architecture/ADR-005` — Remove exchange parameter from log subscriber
- `docs/adr/Core Architecture/ADR-006` — Restructure config schema to use exchange id as subkey
- `docs/adr/Core Architecture/ADR-007` — Run multiple exchange sources in parallel
- `docs/adr/Core Architecture/ADR-008` — Add suffix_topic config field to override topic instrument segment
- `docs/adr/Core Architecture/ADR-009` — Migrate logging from tracing to rasant
- `docs/adr/Core Architecture/ADR-010` — Migrate logging from rasant to log + env_logger
- `docs/adr/Core Architecture/ADR-014` — Support multi-instruments per exchange

**Integration:**
- `docs/adr/Integration/ADR-011` — Bitvavo WebSocket credential resolution (config or env vars)
- `docs/adr/Integration/ADR-012` — Load environment variables from a `.env` file at startup
- `docs/adr/Integration/ADR-013` — Expose `crossguard_log` config for Kraken crossing-guard warnings
- `docs/adr/Integration/ADR-015` — Expose `snapshot_delay` config for Bitstamp delta-buffering warm-up

## Development

```bash
make help        # List all available targets
make build       # Debug build (cargo build)
make build-release    # Release build (cargo build --release)
make test        # Run all tests (cargo test)
make test-integration   # Run integration tests
make lint        # Run Clippy linter (treated as error in CI)
make fmt         # Format code with rustfmt
make coverage    # Run tests with coverage (XML + HTML reports)
make audit       # Run cargo-audit (fails on vulnerabilities)
make clean       # Remove build artifacts
```

See [`AGENTS.md`](AGENTS.md) for the full development workflow and conventions.

## License

Apache-2.0
