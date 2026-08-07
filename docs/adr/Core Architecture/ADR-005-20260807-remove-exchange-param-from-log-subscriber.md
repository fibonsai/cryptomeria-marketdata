# ADR-005: Remove exchange parameter from log subscriber

## Category
Core Architecture

## Status
Accepted

## Created
2026-08-07 15:40

## Context

The built-in `StdoutSubscriber` (log subscriber) previously required an `exchange` parameter passed to its `connect()` method to build log prefixes like `[lob-okx]`. However, the payload JSON already contains the exchange field (e.g., `LobItem.exchange`, `TradeItem.exchange`). The topic (data type + instrument) is also redundant since it's in the payload.

This created unnecessary configuration coupling and redundant logging - the subscriber was configured with exchange info that was already available in the message payload.

## Options Considered

### Option 1: Keep exchange parameter and prefix logging
- Pro: Maintains existing log format
- Con: Redundant configuration, exchange must be passed from config, prefix duplicates payload data

### Option 2: Remove exchange parameter, log raw payload JSON only
- Pro: Self-contained subscriber, no redundant config, all metadata in payload, simpler code
- Con: Log format changes (no prefix), may need adjustment for log parsing tools

### Option 3: Parse exchange from payload but keep prefix format
- Pro: Keeps familiar log format
- Con: Adds JSON parsing overhead per message, still couples subscriber to exchange config

## Decision

**Option 2**: Remove the `exchange` parameter from `StdoutSubscriber::connect()` and log the raw payload JSON directly without any prefix. The payload already contains all necessary metadata (data type, instrument, exchange).

## Consequences

### Positive
- Subscriber is self-contained - no external configuration needed
- Eliminates redundant exchange parameter propagation through the call chain
- Simpler code - no frame splitting, topic parsing, or prefix building
- All metadata available in structured JSON payload for downstream processing
- Reduced coupling between config and subscriber

### Negative
- Log output format changes - consumers expecting `[lob-okx]: {...}` format will see just `{...}`
- Log parsing tools may need updates
- Slightly larger log lines (full JSON vs. prefix + payload)
