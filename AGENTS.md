# Criptomeria-Marketdata Agent Instructions

## Project Overview
Rust application that connects to crypto exchange WebSocket streams via
`cryptomeria-ingest` and forwards normalized LOB/trade data to a NATS
broker.

## Essential Commands

### Build & Test
- `make build` - Debug build
- `make build-release` - Release build
- `make test` - Run all tests
- `make test-integration` - Run integration tests only (not yet implemented)
- `make lint` - Run Clippy linter
- `make fmt` - Format code with rustfmt
- `make install` - Install release binary
- `make clean` - Remove build artifacts
- `make coverage-install` - Install cargo-tarpaulin
- `make coverage` - Run tests with coverage (XML + HTML reports)
- `make coverage-report` - Serve HTML coverage report locally
- `make audit` - Run cargo-audit (fails on vulnerabilities)
- `cargo run --bin marketdata -- --help` - Show CLI help
- `cargo run --bin marketdata -- --dry-run` - Test without NATS

### Testing Details
- Unit tests: Located alongside source in `src/config.rs` and `src/forward.rs`
- Integration tests: Not yet implemented (would require NATS + exchange WS)

**ALWAYS load `rust-tdd` skill before create or update tests.**

## Project Structure
- `src/lib.rs` - Library exports (`config`, `forward`)
- `src/config.rs` - Application configuration parsing/validation
- `src/forward.rs` - NATS subject resolution, encoding, publisher
- `src/bin/marketdata.rs` - CLI entry point
- `config.toml` - Default configuration
- `docs/ADR-*.md` - Architecture decision record


## Key Implementation Details
- Uses `cryptomeria-ingest` as a library dependency (path: `../cryptomeria-ingest`)
- Normalizes data into `MarketDataItem` enum (Lob or Trade variants)
- Implements snapshot-first stream pattern (first LobItem is full snapshot)
- Automatic reconnection with exponential backoff + jitter (via `cryptomeria-ingest`)
- NATS publishing with `async-nats` (Tokio-compatible)
- No task leaks: background tasks abort when stream is dropped
- Pure functions for parsing/subscription building (testable without I/O)

## Development Guidelines
- Follow Rust idioms and Rustfmt conventions
- Clippy warnings treated as errors in CI
- Documentation comments encouraged for public APIs
- Add new config fields: Extend `SourceConfig`/`NatsConfig` in `src/config.rs`
- Add new data handling: Extend `forward.rs`
- Configuration includes resilience settings, snapshot depth, level filtering

## Configuration
- See `src/config.rs` for `AppConfig`, `SourceConfig`, `NatsConfig`
- Supported exchanges: "okx", "kraken", "bitstamp"
- Data kinds: "lob", "trade", "both", "lob|trade"
- Resilience settings: initial_backoff_ms, max_backoff_ms, backoff_multiplier, jitter_ms, heartbeat_interval_secs, max_attempts

## Adding Tests
- Unit tests live in `#[cfg(test)] mod tests` blocks in `config.rs` and `forward.rs`
- Follow AAA pattern (Arrange/Act/Assert)
- Name tests to describe behavior, not implementation
- For new pure functions: write failing test first (RED), minimal code (GREEN), refactor (REFACTOR)
