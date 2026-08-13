# ADR-015: Expose `snapshot_delay` config field for Bitstamp delta-buffering warm-up

- **Category**: Integration
- **Status**: Accepted
- **Created**: 2026-08-13 13:00
- **Deciders**: tuxmonteiro
- **Relates**: [ADR-026](https://github.com/fibonsai/cryptomeria-ingest/blob/main/docs/adr/Integration/ADR-026-20260813-bitstamp-delta-buffering-ccxt-pro.md)
  in `cryptomeria-ingest` (Bitstamp delta-buffering with snapshot merge, CCXT Pro pattern)

## Context

`cryptomeria-ingest` added a `snapshot_delay: usize` field to its `DataSourceConfig`
(per ADR-026). This field controls how many diff-order-book deltas to buffer before
fetching a REST snapshot — mirroring CCXT Pro's `delta_cache_limit`. The default
is `6`; setting it to `0` disables delta buffering (fetch the snapshot immediately).

The `criptomeria-marketdata` application's `SourceConfig::to_data_source()` in
`src/config.rs` constructs a `DataSourceConfig` struct literal. Because the new
field was added to the ingest library after the marketdata code was written, the
literal was missing `snapshot_delay`, causing a hard compile error:

```
error[E0063]: missing field `snapshot_delay` in initializer of `DataSourceConfig`
```

Operators have no way to tune the delta-buffering warm-up period without this
config field being wired through.

## Options Considered

### Option 1: Hard-code `snapshot_delay` to the ingest default in `to_data_source()`

Set `snapshot_delay: 6` directly in the `DataSourceConfig` literal, without adding
a config field.

- **Pros**: Simple; the build compiles; uses the ingest library's default behavior.
- **Cons**: Operators cannot tune the warm-up period — a critical knob for
  exchanges (e.g. Bitstamp) where delta-buffering latency matters. Inconsistent
  with how every other `DataSourceConfig` field is exposed (`max_level`,
  `max_level_pct`, `checksum_log`, `crossguard_log`, `resilience`, etc.).

### Option 2: Expose `snapshot_delay` as an instrument-level config field (chosen)

Add a `snapshot_delay: usize` field to `InstrumentConfig` with
`#[serde(default = "default_snapshot_delay")]` (default `6`), and forward it in
`to_data_source()`. Replace the derived `Default` with a manual `Default` impl
that sets `snapshot_delay` to `default_snapshot_delay()`, keeping the serde
default and the Rust `Default` trait consistent.

- **Pros**: Operators can tune per-instrument (e.g. `snapshot_delay = 0` to
  disable buffering, `snapshot_delay = 10` for more buffer). Consistent with how
  `max_level` and `max_level_pct` are already per-instrument fields. Zero
  breaking change — the field defaults to `6`, matching the ingest library, so
  existing configs that omit it behave exactly as before. The `default_snapshot_delay()`
  helper is defined locally in `config.rs` rather than imported from the ingest
  library, avoiding a cross-crate module path dependency in the serde attribute.
- **Cons**: Adds one more config knob; the manual `Default` impl for
  `InstrumentConfig` is slightly more verbose than `#[derive(Default)]`, but
  ensures the serde default and Rust `Default` agree (both yield `6`).

### Option 3: Expose `snapshot_delay` as an exchange-level config field

Add the field to `SourceConfig` instead of `InstrumentConfig`, sharing the value
across all instruments under an exchange (like `checksum_log` and
`crossguard_log`).

- **Pros**: Fewer config fields when all instruments on an exchange share the same
  warm-up preference.
- **Cons**: `snapshot_delay` is fundamentally a per-stream (per-instrument)
  concern — different instruments may have different volatility and require
  different buffering. Placing it on `SourceConfig` forces a single value across
  all instruments, which is more restrictive than the ingest library's per-stream
  model. The field is already on `DataSourceConfig` (per-stream), so per-instrument
  placement at the marketdata layer is the natural fit.

## Decision

Adopt **Option 2**.

- Add `pub fn default_snapshot_delay() -> usize { 6 }` to `src/config.rs`.
- Add `#[serde(default = "default_snapshot_delay")] pub snapshot_delay: usize`
  to `InstrumentConfig`, with a doc comment explaining the CCXT Pro pattern and
  the `0` = disabled semantics.
- Replace `#[derive(..., Default, ...)]` on `InstrumentConfig` with a manual
  `Default` impl that sets `snapshot_delay: default_snapshot_delay()`.
- Forward `snapshot_delay: instrument_cfg.snapshot_delay` in
  `SourceConfig::to_data_source()`.
- Add unit tests mirroring the existing `max_level`/`max_level_pct` test suite.

## Consequences

**Positive:**
- Operators can tune the delta-buffering warm-up period per instrument.
- Zero breaking change — the field defaults to `6`, matching the ingest library.
- Fixes the `E0063` compile error caused by the missing field.
- Consistent with the pattern used for other ingest-config fields (`max_level`,
  `max_level_pct` on `InstrumentConfig`; `checksum_log`, `crossguard_log` on
  `SourceConfig`).

**Negative:**
- Adds a third `usize`/`Option<usize>` config field to `InstrumentConfig` (after
  `max_level` and `max_level_pct`), slightly increasing the struct's surface area.
  Mitigated by the doc comment and the ADR reference.
- The manual `Default` impl for `InstrumentConfig` is more verbose than the
  derive, but this is necessary to keep the serde default and Rust `Default`
  consistent (both `6`).
