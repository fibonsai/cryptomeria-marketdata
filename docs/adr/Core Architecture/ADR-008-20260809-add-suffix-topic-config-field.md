# ADR-008: Add suffix_topic config field to override topic instrument segment

- **Category**: Core Architecture
- **Status**: Accepted
- **Created**: 2026-08-09 00:00
- **Deciders**: tuxmonteiro
- **Relates**: ADR-007's parallel per-exchange ingestion where topics omit the
  exchange name.

## Context

ADR-007 runs multiple exchanges in parallel, each publishing to a shared NNG
broker under topics of the form `{type}__{normalized_instrument}` (e.g.
`lob__btcusdt`). Because the exchange name is intentionally excluded from the
topic, two exchanges subscribed to the same instrument (e.g. BTC-USDT on both
OKX and Kraken) would share a single topic. Publishers would interleave payloads
and the last writer wins on `publish` — subscribers cannot distinguish which
exchange a given LOB update or trade came from.

Operators need a way to disambiguate topics on a per-exchange basis without
changing the wire format for existing single-exchange deployments or embedding
the exchange name directly in the topic scheme.

## Options Considered

### Option 1: Embed exchange name in the default topic

Change `topic_for` to always produce `{type}__{exchange}_{instrument}` (e.g.
`lob__okx_btcusdt`).

- **Pros**: No config change; always disambiguated.
- **Cons**: Breaks the existing `{type}__{instrument}` wire format that
  subscribers may already be filtering on. Affects all deployments, even single-
  exchange ones that don't need disambiguation. A breaking change.

### Option 2: Use the `instrument` field as the topic segment verbatim

Drop `normalize_instrument` and use the raw instrument symbol in topics.

- **Pros**: Operator controls the full segment value.
- **Cons**: Instrument symbols are exchange-native (e.g. `BTC-USDT`, `XBT/USD`)
  and contain hyphens, slashes, and mixed case — not valid for clean topic
  names without normalization. Doesn't solve the parallel collision problem.

### Option 3: Add an optional `suffix_topic` config field (chosen)

Add an optional `suffix_topic: Option<String>` to `SourceConfig`. When `Some`,
`topic_for` uses the suffix **verbatim** as the topic segment, producing
`{type}__{suffix}` (e.g. `lob__okx_btcusdt`). When `None` (the default),
`topic_for` normalizes the instrument as before, preserving backward
compatibility.

- **Pros**: Opt-in, zero breaking change for existing configs. Operators get
  full control over the topic segment when disambiguation is needed. Does not
  touch `DataSourceConfig` or `cryptomeria-ingest` (stays marketdata-only).
- **Cons**: Requires operators to set `suffix_topic` on each exchange if they
  want disambiguation — it is not automatic. Two deployments with the same
  instrument but no suffix still collide (operator responsibility, as
  documented in ADR-007).

## Decision

Adopt **Option 3**.

- Add `#[serde(default)] pub suffix_topic: Option<String>` to `SourceConfig`
  in `src/config.rs`. The `#[serde(default)]` attribute ensures existing
  configs that omit the field continue to parse (defaulting to `None`).
- `suffix_topic` is NOT forwarded to `cryptomeria-ingest`'s `DataSourceConfig`;
  it is purely a marketdata-layer concern, threaded from `SourceConfig` through
  `validated_sources()` into `run_exchange`.
- `validated_sources()` returns a new `ValidatedSource` type alias
  (`(String, String, DataSourceConfig, Option<String>)`) so the 4-tuple is
  readable and avoids Clippy `type_complexity` warnings.
- `topic_for` in `src/forward.rs` is extended to accept `suffix: Option<&str>`.
  When `Some(s)`, the suffix is used verbatim (no normalization). When `None`,
  the existing normalization behavior is preserved exactly.
- The suffix is used verbatim (not normalized) because operators may want to
  preserve case, separators, or custom prefixes that normalization would strip.

## Consequences

**Positive:**
- Zero breaking change for single-exchange or non-colliding multi-exchange
  deployments — `suffix_topic` defaults to `None` and `topic_for` behaves
  exactly as before.
- Full operator control over the topic segment when disambiguation is needed.
- Clean separation: `suffix_topic` stays in the marketdata layer;
  `cryptomeria-ingest` is unaware of it.

**Negative:**
- Operators must manually configure `suffix_topic` on each exchange to avoid
  collisions — there is no automatic per-exchange prefix. This is an accepted
  trade-off to preserve the existing wire format and avoid breaking existing
  subscribers.
- The type alias `ValidatedSource` adds a small abstraction for the
  `validated_sources()` return type, but improves readability of the 4-tuple.
