# ADR-1: Use cryptomeria-ingest as library and forward to NATS

## Status
Accepted

## Date
2026-08-05

## Context
We need a Rust application that:
1. Connects to multiple crypto exchange WebSocket feeds (OKX, Kraken, Bitstamp)
2. Normalizes LOB snapshots/updates and trade executions into a common format
3. Forwards these events to a NATS broker for downstream consumers

The `cryptomeria-ingest` crate already provides:
- Exchange WebSocket clients with reconnection/backoff
- Snapshot-first normalization (first LOB item = full book)
- Unified `MarketDataItem` enum (Lob / Trade)
- Configurable depth, filtering, and resilience

The decision is how to integrate: vendor the code, fork it, or depend on it as a
library.

## Options Considered

### 1. Copy/vendoring the ingest code into this repo
**Pros:**
- Zero external dependency risk
- Full control over modifications

**Cons:**
- Duplicates logic; future fixes in upstream must be manually synced
- Increases maintenance surface
- Violates DRY

### 2. Fork `cryptomeria-ingest` and add NATS publishing there
**Pros:**
- Single binary
- Could expose NATS as a feature flag

**Cons:**
- Ties the ingestion library to a specific transport (NATS)
- Makes the library less reusable for other sinks (Kafka, TimescaleDB, etc.)
- Couples library release cycle to application features

### 3. Depend on `cryptomeria-ingest` as a library (path dependency) and forward in a thin app
**Pros:**
- Clear separation: ingestion library is transport-agnostic
- App stays thin and focused on config/NATS glue
- Both can evolve independently; library consumers unaffected
- Enables future apps (Kafka forwarder, DB writer) to reuse the library

**Cons:**
- Requires managing two Cargo workspaces (or separate repos)
- Slightly more complex deployment (two artifacts)

## Decision
Use **Option 3**: `criptomeria-marketdata` depends on `cryptomeria-ingest` via
a local path (`../cryptomeria-ingest`). The application crate contains only
configuration parsing, NATS subject resolution/encoding, and the CLI binary.

The NATS client (`async-nats`) is a dependency of the application, not the
library. The library remains a pure ingestion component.

## Consequences

### Positive
- Single source of truth for exchange normalization logic
- Other teams can depend on `cryptomeria-ingest` without pulling in NATS
- Easy to add new sinks (Kafka, gRPC, etc.) as separate thin apps
- Library can be published to crates.io independently

### Negative
- Two Cargo projects to build/test
- Must ensure library and app stay compatible (path dep avoids version drift)
- Deploy pipeline must produce both artifacts or build the app with the lib

## Notes
- The async-nats version was updated to 0.50 (current) from the planned 0.20
  because 0.20's `tokio-compat` feature no longer exists; 0.30+ uses Tokio natively.
- The library's `ResilienceConfig` handles reconnection/backoff; NATS client
  handles its own reconnection via `async-nats` internals.
- This ADR is stored in the `docs/` folder and should be uploaded to the GitHub
  Wiki when the repository is created.

## References
- `cryptomeria-ingest` crate: https://github.com/fibonsai/cryptomeria-ingest
- `async-nats` crate: https://crates.io/crates/async-nats
