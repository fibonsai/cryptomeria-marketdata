# ADR-004: Pass exchange to StdoutSubscriber from config instead of parsing from payload

## Category
Core Architecture

## Status
Accepted

## Created
2026-08-07 14:30

## Context

The built-in `StdoutSubscriber` logs all NNG topics to stdout with a prefix like `[lob-okx]` or `[trade-bitstamp]`. Previously, it extracted the exchange name from each JSON payload on every message using `extract_exchange()`, which required deserializing the JSON payload in the hot receive loop.

This approach had two problems:
1. **Performance**: JSON parsing on every message in the hot path adds unnecessary CPU overhead
2. **Coupling**: The subscriber was coupled to the payload structure, making it brittle to changes

The exchange name is already known at subscriber creation time from `app.source.exchange` in the configuration.

## Options Considered

### Option 1: Keep per-message extraction (status quo)
- **Pros**: No code changes required
- **Cons**: Ongoing performance cost, tight coupling to payload structure

### Option 2: Encode exchange in topic name (e.g., `lob__okx__btcusdt`)
- **Pros**: Exchange available in topic without parsing payload
- **Cons**: Breaking change to wire format; requires updating all publishers, subscribers, topic construction, and frame splitting logic

### Option 3: Pass exchange from config at subscriber creation (chosen)
- **Pros**: Zero wire format changes; zero per-message parsing; minimal code changes; exchange known at config load time
- **Cons**: Subscriber must be created with correct exchange (already guaranteed by single-source config)

## Decision

Pass the exchange name from `app.source.exchange` directly to `StdoutSubscriber::connect(port, exchange)` at creation time in `marketdata.rs`. Store it in the subscriber struct and thread it to the receive loop. Remove `extract_exchange()` from `forward.rs` entirely.

## Consequences

### Positive
- Eliminates JSON deserialization in the hot receive path
- Removes coupling between subscriber and payload structure
- Simpler code: `extract_exchange()` and its tests removed
- No breaking changes to wire protocol or external consumers

### Negative
- `StdoutSubscriber` now requires exchange at creation (but this is always available from config)
- Slightly larger struct (one `String` field)
EOF