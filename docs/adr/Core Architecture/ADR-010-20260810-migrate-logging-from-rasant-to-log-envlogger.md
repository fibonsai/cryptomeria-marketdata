# ADR-010: Migrate logging from rasant to log + env_logger

**Category:** Core Architecture
**Status:** Accepted
**Implemented:** (PR link to be added)
**Created:** 2026-08-10 00:00

## Context

The application previously used `rasant` as its logging backend. `rasant::Logger` is a per-thread logger that is **not `Send`** — it uses a `Mutex` internally whose guard cannot cross thread boundaries. This forced every `rasant::info!`/`rasant::warn!` call to receive an explicit `&mut Logger` parameter, cluttering call sites and making it impossible to use `rasant::Logger` inside `tokio::task::spawn` futures that require `Send`. `cryptomeria-ingest` itself also depended on rasant, so `ingest::stream()` produced a `!Send` future, breaking `JoinSet::spawn`.

## Options Considered

### Option A: Keep rasant, work around !Send limitations

Wrap `ingest::stream()` in `tokio::task::block_in_place` to avoid the `!Send` future crossing thread boundaries. Keep `rasant::Logger` and pass it explicitly to every call site.

- **Pros:** Minimal change to existing logging code.
- **Cons:** Perpetuates a non-standard, `!Send` logger; every new async spawn site must remember the `block_in_place` workaround; rasant is unmaintained.

### Option B: Migrate to the `log` facade + `env_logger` backend (chosen)

Replace `rasant` with the standard `log` crate facade and `env_logger` as the backend.

- **Pros:**
  - `env_logger` is `Send`-compatible by design, eliminating the `!Send` constraint.
  - Standard Rust ecosystem; `RUST_LOG` controls filtering.
  - No need to thread `Logger` instances through every function.
  - `JoinSet::spawn` works directly — `block_in_place` no longer needed once `cryptomeria-ingest` also removes rasant.
- **Cons:**
  - Loss of rasant's custom `black_hole`/`memory` sinks in tests; tests assert on `build_log_entry` output directly instead.
  - Log format changes from rasant's custom format to env_logger's default.

## Decision

Adopt **Option B**: migrate from `rasant` to `log` + `env_logger`.

- Remove `rasant` from `Cargo.toml`; add `log = "0.4"` and `env_logger = "0.11"`.
- Remove all `rasant::Logger` parameters and `.clone()` calls from `Broker`, `StdoutSubscriber`, and `run_exchange`.
- Replace all `rasant::info!`/`rasant::error!`/`rasant::warn!` macro calls with `log::info!`/`log::error!`/`log::warn!` (or imported `info!` etc.).
- Initialize `env_logger` in `main()` with `RUST_LOG` support and a default filter of `info`.
- Update tests to assert on structured output (`build_log_entry`) directly rather than rasant memory sinks.

## Consequences

- **Positive:** Eliminates `!Send` friction; standardizes on the `log` facade; no more explicit `Logger` threading; enables straightforward use of `JoinSet::spawn` once `cryptomeria-ingest` also drops rasant.
- **Negative:** Log output format changes; rasant's memory sink is no longer available in tests (tests were restructured to assert on `build_log_entry` output instead).
