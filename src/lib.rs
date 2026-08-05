//! criptomeria-marketdata — LOB/trade ingestion forwarded to NATS.
//!
//! This library exposes the configuration model and the NATS forwarding
//! helpers used by the `marketdata` binary. It depends on the
//! `cryptomeria-ingest` crate for the exchange WebSocket streams.

pub mod config;
pub mod forward;
