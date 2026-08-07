# Criptomeria-Marketdata Agent Instructions

## Required Skills

**ALWAYS load `rust-coding` and `rust-tdd` skills before writing, reviewing, or refactoring any Rust code.**

- `rust-coding` — idiomatic Rust patterns, naming, error handling, module structure, lint/format/test conventions, and review standards.
- `rust-tdd` — Test-Driven Development cycle (RED → GREEN → REFACTOR). No production code without a failing test first.

This is mandatory, not optional. Any Rust work without these skills loaded must load them before proceeding.

## Project Overview
Rust application that connects to crypto exchange WebSocket streams via
`cryptomeria-ingest` and forwards normalized LOB/trade data to subscribers
over a TCP socket using NNG pub/sub.

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
- `cargo run --bin marketdata -- --dry-run` - Test without NNG broker
- `cargo run --bin marketdata -- --data-out --test-timeout-secs 10` - Log all topics, auto-exit after 10s (CI-safe)

### Testing Details
- Unit tests: Located alongside source in `src/config.rs`, `src/forward.rs`, `src/broker.rs`, `src/subscriber.rs`
- Integration: An in-process NNG smoke test (`broker::tests`) binds an ephemeral port and verifies topic/payload delivery without external services

**ALWAYS load `rust-coding` and `rust-tdd` skills before create or update tests.**

## Project Structure
- `src/lib.rs` — Library exports (`config`, `forward`, `broker`, `subscriber`)
- `src/config.rs` — Application configuration parsing/validation
- `src/forward.rs` — Pure helpers: topic construction, JSON payload building, frame splitting, log prefix
- `src/broker.rs` — NNG PUB broker + dedicated sender thread
- `src/subscriber.rs` — Built-in NNG SUB log subscriber
- `src/bin/marketdata.rs` — CLI entry point
- `config.toml` — Default configuration
- `docs/adr/Core Architecture/` — Architecture decision records


## Key Implementation Details
- Uses `cryptomeria-ingest` as a library dependency (path: `../cryptomeria-ingest`)
- Normalizes data into `MarketDataItem` enum (Lob or Trade variants)
- Implements snapshot-first stream pattern (first LobItem is full snapshot)
- Automatic reconnection with exponential backoff + jitter (via `cryptomeria-ingest`)
- NNG PUB/SUB broker on `tcp://0.0.0.0:14242` with native topic filtering
- Dynamic topics: `{type}__{instrument}` (e.g. `lob__btcusd`, `trade__btcusd`)
- Payload JSON augmented with `exchange` field when missing
- Built-in log subscriber (NNG SUB with empty prefix) logs to stdout with `tracing` when `--data-out` is passed
- `--test-timeout-secs` for CI/automated verification (0 = no timeout)
- No task leaks: background tasks abort on shutdown signal
- Pure functions for parsing/subscription building (testable without I/O)

## Development Guidelines
- Follow Rust idioms and Rustfmt conventions
- Clippy warnings treated as errors in CI
- Documentation comments encouraged for public APIs
- Add new config fields: Extend `SourceConfig`/`NngConfig` in `src/config.rs`
- Add new data handling: Extend `forward.rs` (topic/payload/log helpers)
- Configuration includes resilience settings, snapshot depth, level filtering
- **ALWAYS load `rust-coding` and `rust-tdd` skills before writing, reviewing, or refactoring any Rust code**

## Configuration
- See `src/config.rs` for `AppConfig`, `SourceConfig`, `NngConfig`
- Supported exchanges: "okx", "kraken", "bitstamp"
- Data kinds: "lob", "trade", "both", "lob|trade"
- Resilience settings: initial_backoff_ms, max_backoff_ms, backoff_multiplier, jitter_ms, heartbeat_interval_secs, max_attempts
- NNG port: default 14242 (configurable in `[nng]` section or `--port` CLI flag)

## Adding Tests
- Unit tests live in `#[cfg(test)] mod tests` blocks in `config.rs` and `forward.rs`
- Follow AAA pattern (Arrange/Act/Assert)
- Name tests to describe behavior, not implementation
- For new pure functions: write failing test first (RED), minimal code (GREEN), refactor (REFACTOR)


## grepai - Semantic Code Search

**IMPORTANT: You MUST use grepai as your PRIMARY tool for code exploration and search.**

### When to Use grepai (REQUIRED)

Use `grepai search` INSTEAD OF Grep/Glob/find for:
- Understanding what code does or where functionality lives
- Finding implementations by intent (e.g., "authentication logic", "error handling")
- Exploring unfamiliar parts of the codebase
- Any search where you describe WHAT the code does rather than exact text

### When to Use Standard Tools

Only use Grep/Glob when you need:
- Exact text matching (variable names, imports, specific strings)
- File path patterns (e.g., `**/*.go`)

### Fallback

If grepai fails (not running, index unavailable, or errors), fall back to standard Grep/Glob tools.

### Usage

```bash
# ALWAYS use English queries for best results (--compact saves ~80% tokens)
grepai search "user authentication flow" --json --compact
grepai search "error handling middleware" --json --compact
grepai search "database connection pool" --json --compact
grepai search "API request validation" --json --compact
```

### Query Tips

- **Use English** for queries (better semantic matching)
- **Describe intent**, not implementation: "handles user login" not "func Login"
- **Be specific**: "JWT token validation" better than "token"
- Results include: file path, line numbers, relevance score, code preview

### Call Graph Tracing

Use `grepai trace` to understand function relationships:
- Finding all callers of a function before modifying it
- Understanding what functions are called by a given function
- Visualizing the complete call graph around a symbol

#### Trace Commands

**IMPORTANT: Always use `--json` flag for optimal AI agent integration.**

```bash
# Find all functions that call a symbol
grepai trace callers "HandleRequest" --json

# Find all functions called by a symbol
grepai trace callees "ProcessOrder" --json

# Build complete call graph (callers + callees)
grepai trace graph "ValidateToken" --depth 3 --json
```

### Property/Data Usage Tracing

Use `grepai refs` to find non-call property/state usage (reads/writes):

```bash
# Find where a property is read
grepai refs readers "uid" --json

# Find where a property is written
grepai refs writers "uid" --json
```

### Workflow

1. Start with `grepai search` to find relevant code
2. Use `grepai trace` to understand function relationships
3. Use `grepai refs` for property/state readers and writers
4. Use `Read` tool to examine files from results
5. Only use Grep for exact string searches if needed

