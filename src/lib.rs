//! criptomeria-marketdata — LOB/trade ingestion forwarded to NNG subscribers.
//!
//! This library exposes the configuration model, NNG broker/subscriber helpers
//! and the pure forwarding functions used by the `marketdata` binary. It
//! depends on the `cryptomeria-ingest` crate for the exchange WebSocket streams.

pub mod broker;
pub mod config;
pub mod forward;
pub mod registry;
pub mod subscriber;
