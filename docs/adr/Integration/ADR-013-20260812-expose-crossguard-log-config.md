# ADR-013: Expose `crossguard_log` config field for Kraken crossing-guard warnings

- **Category**: Integration
- **Status**: Accepted
- **Created**: 2026-08-12 00:00
- **Deciders**: tuxmonteiro
- **Relates**: ADR-001 (using cryptomeria-ingest), ADR-021 in cryptomeria-ingest
  (gate checksum mismatch logging), [ADR-022 in
  cryptomeria-ingest](https://github.com/fibonsai/cryptomeria-ingest/blob/main/docs/adr/Operations/ADR-022-20260812-gate-crossing-guard-logging-prevent-log-spoofing.md)
  (gate crossing-guard rejection logging)

## Context

On the Kraken WebSocket v2 `book` channel, `cryptomeria-ingest`'s `OrderBook`
enforces a crossing guard: any bid whose price would rise above the best ask
(`ask ≤ best bid`), or any ask whose price would fall below the best bid (`bid ≥
best ask`), is **rejected and dropped** from the in-memory book. Before
cryptomeria-ingest's ADR-022, each rejection emitted an unconditional `warn!`
with an exchange-controlled price interpolated into the log line, creating a
log-spoofing/flooding vector identical to the one addressed for CRC32 checksum
mismatches in cryptomeria-ingest's ADR-021.

The `crossguard_log` parameter was added to `DataSourceConfig` in
`cryptomeria-ingest` to gate that `warn!`: the rejection is logged only when
`crossguard_log == true` **or** the runtime log level is `DEBUG`. The guard
always drops the crossed level regardless of this setting.

The `criptomeria-marketdata` application's `SourceConfig` already exposes the
sibling field `checksum_log` (forwarded to `DataSourceConfig`), but it did **not**
expose `crossguard_log`. Because `DataSourceConfig` requires the field, the
`to_data_source()` method in `src/config.rs` must set it; operators have no way
to opt into crossing-guard diagnostic warnings without this config field.

## Options Considered

### Option 1: Hard-code `crossguard_log` to `false` in `to_data_source()`

- **Pros**: Simple; the build compiles; crossing-guard rejections are silent at
  default log level (safe from spoofing).
- **Cons**: Operators cannot opt into crossing-guard `warn!` diagnostics without
  running at `DEBUG` log level. Inconsistent with the `checksum_log` pattern,
  which is already exposed as a config field for exactly the same class of
  problem.

### Option 2: Expose `crossguard_log` as a config field (chosen)

Add a `crossguard_log: bool` field to `SourceConfig` with `#[serde(default)]`
(defaulting to `false`), and forward it in `to_data_source()`, exactly mirroring
the existing `checksum_log` field.

- **Pros**: Consistent with the `checksum_log` pattern; operators can opt into
  crossing-guard warnings independently of log level. Zero breaking change (field
  defaults to `false`, existing configs omit it). The `warn!`-gating decision
  itself is delegated to `cryptomeria-ingest` (ADR-022); this repo only wires the
  config flag.
- **Cons**: Another config knob to document; operators must understand that
  `crossguard_log` only affects the diagnostic `warn!`, not the always-on
  drop/reject behavior. This is mitigated by the doc comment on the field and
  the reference to the upstream ADR-022.

## Decision

Adopt **Option 2**.

- Add `#[serde(default)] pub crossguard_log: bool` to `SourceConfig` in
  `src/config.rs`, placed immediately after `checksum_log`. The doc comment
  mirrors `checksum_log`'s style, explaining that the field gates only the
  diagnostic `warn!` (the guard always drops crossed levels) and links to
  cryptomeria-ingest's ADR-022.
- Forward `crossguard_log: self.crossguard_log` in `SourceConfig::to_data_source()`,
  placed immediately after `checksum_log`.
- Add unit tests mirroring the `checksum_log` test suite:
  - `crossguard_log_defaults_to_false_when_omitted`
  - `parses_crossguard_log_when_present`
  - `to_data_source_forwards_crossguard_log`
  - `to_data_source_crossguard_log_defaults_to_false_when_omitted`

## Consequences

**Positive:**
- Operators can opt into Kraken crossing-guard rejection warnings via
  `crossguard_log = true` in `config.toml`, consistent with the existing
  `checksum_log` opt-in.
- Zero breaking change — the field defaults to `false` via `#[serde(default)]`,
  and existing configs that omit it behave exactly as before.
- The `warn!`-gating decision remains centralized in `cryptomeria-ingest` (ADR-022);
  this repo only wires the configuration surface, mirroring `checksum_log`.

**Negative:**
- Another optional config field adds to the surface area; mitigated by the
  doc comment and the ADR reference.
- Like `checksum_log`, tests for `crossguard_log` only assert config parsing and
  forwarding (the actual crossing-guard drop behavior is tested in
  `cryptomeria-ingest`).
