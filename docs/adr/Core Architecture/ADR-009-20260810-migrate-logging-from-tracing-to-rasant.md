# ADR-009: Migrate logging from tracing to rasant; emit structured JSON from --data-out

- **Category**: Core Architecture
- **Status**: Accepted
- **Created**: 2026-08-10 00:00
- **Deciders**: tuxmonteiro
- **Relates**: Issue #27 — `--data-out` stdout divergence from system logs.

## Context

Running the app with `--data-out`, the stdout output appears very different from
the regular system logs. Investigation found that no `println!`/`eprintln!` calls
exist anywhere in the codebase — all logging already uses `tracing`.

The root cause: `StdoutSubscriber::log_message` (`src/subscriber.rs`) logs the
**raw NNG wire frame** (`topic\0payload`) verbatim, leaking the topic prefix and
null separator into stdout. The existing `split_frame` helper in `src/forward.rs`
is not used.

The issue requests migrating the logging backend from `tracing` to `rasant`
(v1.1.0, MIT, minimal deps — depends only on `ntime`), and fixing `log_message`
to emit a structured JSON schema.

## Options Considered

### tracing's global subscriber model

`tracing` uses a global subscriber initialized once in `main()` via
`tracing_subscriber::fmt().with_env_filter(...).init()`. Every `tracing::info!` /
`tracing::warn!` / `tracing::error!` call implicitly references this global
subscriber — no logger handle is passed around.

- **Pros**: Zero-boilerplate logging calls scattered throughout the codebase;
  no API changes needed when adding new log sites.
- **Cons**: Global mutable state in tests; hidden coupling; the `--data-out`
  subscriber reuses `tracing::info!`, making it impossible to distinguish
  system log lines from data log lines on stdout.

### rasant with per-thread Logger clones (chosen)

`rasant` has no global logger. Every `rasant::info!(log, ...)` macro call
requires an explicit `&Logger` reference. Loggers are cheap to `Clone` and are
`Send`, so the rasant idiom is to clone a root logger into each thread/task.

- **Pros**: Explicit, testable; no hidden global state. Cloning is cheap
  (sinks are shared via `Arc<Mutex<…>>`). Each component owns a clone, enabling
  fine-grained control (e.g. the subscriber thread could add extra sinks).
- **Cons**: All call sites must thread a `Logger` handle — a larger diff.
  `log_message` must `clone()` per call since `rasant::info!` requires
  `&mut self`.

## Decision

Adopt **rasant with per-thread Logger clones**.

- Replace `tracing = "0.1"` and `tracing-subscriber` with `rasant = "1"` in
  `Cargo.toml`.
- `init_tracing()` becomes `init_rasant()`, returning an owned `rasant::Logger`
  configured with a stdout sink at the level given by `RUST_LOG` (default
  `Info`). The `Level::try_from(&str)` impl parses the env var.
- `Broker::bind(port, log)` and `StdoutSubscriber::connect(port, log)` now accept
  a `Logger` parameter, stored as a field. Each component clamps for spawned
  threads.
- `run_exchange` gains a `mut log: Logger` parameter; each spawned task receives
  a `log.clone()`.
- All 18 `tracing::` call sites across `src/bin/marketdata.rs`, `src/broker.rs`,
  and `src/subscriber.rs` are replaced with `rasant::info!` / `rasant::warn!` /
  `rasant::error!` equivalents. Since rasant macros take a single `$msg:expr`
  (not a `format!`-style string), inline format args like
  `tracing::info!("[{exchange}]: starting stream")` become
  `rasant::info!(log, &format!("[{exchange}]: starting stream"))`.

### --data-out JSON schema change

`log_message` is rewritten to call the new pure helper `build_log_entry` in
`src/forward.rs`, which:

1. Calls `split_frame(framed)` to extract `(topic, payload_bytes)`.
2. Parses `payload_bytes` as `serde_json::Value`.
3. Returns `serde_json::to_string(&json!({"topic": topic, "payload": payload_value}))`.

The `--data-out` stdout now emits compact JSON:
```json
{"topic":"lob__btcusdt","payload":{"exchange":"okx","ts":123}}
```
instead of the raw wire frame `lob__btcusdt\0{"exchange":"okx","ts":123}`.

## Consequences

**Positive:**
- System logs and `--data-out` data logs are clearly separated: system logs go
  through rasant's formatted output; data logs go through a structured JSON
  line per message.
- No global mutable state; each component owns a logger clone.
- `build_log_entry` is a pure, unit-tested function in `forward.rs`.

**Negative:**
- Verbose call-site changes (every `tracing::` → `rasant::` with logger arg).
- Inline format args require explicit `format!()` wrapping since rasant macros
  don't support Rust's `format!`-style string interpolation.
- `log_message` clones the logger per call (acceptable — cloning is cheap).
- `build_log_entry` returns an error when the payload is not valid JSON; such
  messages are logged at `Warning` level instead of being silently printed.
