# ADR-014: Support multi-instruments per exchange via `[source.<exchange>.instrument.<alias>]` config sections

- **Category**: Core Architecture
- **Status**: Proposed
- **Implemented**: (pending implementation)
- **Created**: 2026-08-13 12:00
- **Replaces**: (none — extends the schema introduced in ADR-006 and ADR-008)

## Context

The current config schema places instrument-level fields (`instrument`, `alias`,
`suffix_topic`, `max_level`, `max_level_pct`) directly under
`[source.<exchange>]`. Because `SourceConfig` is a single struct per exchange,
an operator can only subscribe to **one instrument per exchange** — running two
instruments on the same exchange requires duplicating the exchange section with
different credentials, fallback mappings, and resilience settings, or spawning
two process instances.

The NNG topic schema already uses `{type}__{instrument}` (the exchange is not
part of the topic, per ADR-008), so operators must use `suffix_topic` to
disambiguate when the same instrument appears on multiple exchanges. Supporting
multiple instruments per exchange in the config is the natural next step: each
instrument gets its own symbol, topic suffix, and depth limits, while sharing
the exchange-level settings (region, data kind, resilience, credentials,
checksum/crossguard log gating).

## Options Considered

### Option A: Keep flat fields, add a top-level list

Add an `instruments` array at the `[nng]` or application level that references
exchange sections by id.

- **Pro**: Minimal change to `SourceConfig`.
- **Con**: Splits exchange-level and instrument-level config across distant
  sections, making the TOML hard to read and error-prone (dangling references,
  mismatched fallback keys).

### Option B: Nested `instrument` sub-section keyed by alias (chosen)

Move `instrument`, `suffix_topic`, `max_level`, `max_level_pct` into a new
`[source.<exchange>.instrument.<alias>]` sub-section. The `<alias>` key doubles
as the `DataSourceConfig.alias` value and as the lookup key into the sibling
`[source.<exchange>.fallback.<alias>]` section.

```toml
[source.kraken]
region = "global"
data_kind = "both"

[source.kraken.instrument.btcusd]
instrument = "btcusd"
suffix_topic = "btcusd"
max_level = 3
max_level_pct = 0.0

[source.kraken.fallback.btcusd]
base_mappings = ["BTC", "XBT"]
quote_mappings = ["USDC", "USDT", "USD"]
separator_mappings = ["-", "/", ""]
case_fallback = "upper"
```

- **Pro**: Groups all per-instrument settings together; the alias key naturally
  binds an instrument to its fallback mapping; supports any number of
  instruments per exchange; exchange-level settings (credentials, resilience,
  log gating) are shared automatically.
- **Con**: Breaking change — existing `config.toml` files with `instrument` at
  the `[source.<exchange>]` level will need migration.

### Option C: Duplicate `[source.<exchange>]` sections with subkeys

Allow `[source.kraken.btcusd]`, `[source.kraken.ethusd]`, etc.

- **Pro**: No new nesting level.
- **Con**: Each duplicated section would need its own `region`,
  `data_kind`, `resilience`, credentials — massive duplication, confusing
  precedence rules, and TOML key collisions (`source.kraken` alone vs
  `source.kraken.btcusd`).

## Decision

**Option B** is chosen. The `SourceConfig` struct gains an
`instruments: HashMap<String, InstrumentConfig>` field keyed by alias. The new
`InstrumentConfig` struct holds `instrument`, `suffix_topic`, `max_level`, and
`max_level_pct`. The `alias` field is removed from `SourceConfig` — it is now
implicit in the HashMap key, which also serves as the fallback lookup key.

`validated_sources()` flattens over all (exchange, instrument) pairs, producing
one `ValidatedSource` per instrument. The `ValidatedSource` tuple type and the
`run_exchange` consumer in `src/bin/marketdata.rs` are unchanged — each
instrument simply gets its own spawned task, exactly as different exchanges do
today.

## Consequences

### Positive
- Operators can subscribe to multiple instruments on a single exchange without
  duplicating credentials, resilience, or fallback config.
- Each instrument's `suffix_topic` can override the NNG topic segment, enabling
  clean cross-exchange disambiguation (e.g., `okx__btcusd` vs `kraken__btcusd`).
- The alias-to-fallback binding is enforced by shared key naming, eliminating
  the possibility of an instrument referencing a fallback that doesn't exist.

### Negative
- **Breaking config change**: existing `config.toml` files with instrument
  fields at `[source.<exchange>]` will silently ignore those fields (serde
  `#[serde(default)]` makes new/missing fields optional). A future enhancement
  could add `#[serde(deny_unknown_fields)]` on `SourceConfig` to surface a hard
  error, but that is out of scope for this ADR.
- Existing unit tests and integration test TOML constants need migration.

## References

- ADR-006: Restructure config schema to use exchange-id as subkey
- ADR-008: Add suffix_topic config field
- ADR-013: Expose crossguard_log config
- [cryptomeria-ingest `DataSourceConfig` and `ExchangeFallbackMapping` docs](src/config.rs)
