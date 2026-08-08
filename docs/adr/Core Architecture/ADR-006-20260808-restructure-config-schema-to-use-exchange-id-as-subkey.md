# ADR-006: Restructure config schema to use exchange id as subkey

- **Category**: Core Architecture
- **Status**: Proposed
- **Created**: 2026-08-08 07:30
- **Deciders**: tuxmonteiro

## Context

The application config (`config.toml`) used a flat `[source]` table with an
explicit `exchange = "okx"` field. Fallback mappings were nested as
`[source.fallback.<exchange>.<alias>]`, and resilience settings lived at
`[source.resilience]`. This flat structure makes it awkward to extend toward
multi-exchange configs and duplicates the exchange identity as both a key
(`source.<exchange>`) and a value (`exchange` field).

## Options Considered

### Option 1: Keep `[source]` flat, add multi-source support later

Keep the existing schema and address multi-exchange needs in a future ADR.
This avoids any migration cost but defers the structural debt.

### Option 2: Nest source settings under `[source.<exchange>]` (chosen)

Move all per-exchange settings under `[source.<exchange>]` subsections. The
exchange name becomes purely a section key; the `exchange` field is removed
from `SourceConfig`. Fallback mappings simplify to
`[source.<exchange>.fallback.<alias>]` and resilience to
`[source.<exchange>.resilience]`.

### Option 3: Top-level per-exchange tables

Use `[okx]`, `[kraken]`, etc. at the top level of the TOML file instead of
under a `[source]` umbrella. This is the most direct but loses the
namespacing that `[source.*]` provides for future source types (e.g. REST
fallback).

## Decision

Adopt **Option 2**. The `[source.<exchange>]` structure:

- Makes the exchange identity a key, not a repeated value.
- Simplifies `fallback` to a single alias → mapping level within each
  exchange (the exchange is already implied by the section path).
- Provides a natural extension path to multiple exchanges if needed.
- Keeps all source-related config under the `[source]` umbrella.

The application still enforces exactly one exchange at runtime via
`AppConfig::exchange_source()`, which errors on zero or multiple
`[source.*]` sections.

## Consequences

**Positive:**
- Cleaner TOML hierarchy; no redundant `exchange` field.
- Simpler `SourceConfig.fallback` type
  (`HashMap<String, ExchangeFallbackMapping>` instead of two levels).
- Foundation for future multi-exchange expansion.

**Negative:**
- Breaking change: existing `config.toml` files must migrate from
  `[source]` to `[source.<exchange>]`.
- `SourceConfig::to_data_source()` now requires the exchange name to be
  passed as a parameter (previously read from `self.exchange`).
