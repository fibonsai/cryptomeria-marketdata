# ADR-011: Resolve Bitvavo WebSocket credentials via config or environment variables

- **Category**: Integration
- **Status**: Accepted
- **Created**: 2026-08-11 00:00
- **Deciders**: tuxmonteiro
- **Relates**: ADR-001 (using cryptomeria-ingest), ADR-007 (multi-exchange parallel ingestion)

## Context

Bitvavo's Market Data Pro WebSocket feed requires HMAC-SHA256 authentication
with an API key and secret before any market data channels can be subscribed.
Unlike OKX, Kraken, and Bitstamp (which offer public LOB/trade streams),
Bitvavo mandates credentials on every connection.

The `cryptomeria-ingest` library (v0.0.14) already supports Bitvavo and exposes
two optional fields on its `DataSourceConfig`: `api_key: Option<String>` and
`api_secret: Option<String>`. The library's `validate()` enforces that both are
present (non-empty) when `exchange == "bitvavo"`, returning
`ConfigError::MissingCredentials` otherwise. The credentials are forwarded to
`BitvavoAdapter`, which sends an `authenticate` message as the first outbound
frame.

The `criptomeria-marketdata` application needs to surface these credentials in its
own `SourceConfig` (TOML) so operators can configure them per-exchange under
`[source.bitvavo]`. Additionally, for CI/CD and containerized deployments,
operators should be able to provide credentials via environment variables
(`BITVAVO_API_KEY`, `BITVAVO_API_SECRET`) rather than embedding secrets in
config files.

## Options Considered

### Option 1: Credentials in config.toml only

Add `api_key` and `api_secret` as required fields in `[source.bitvavo]`.

- **Pros**: Simple; all config in one place.
- **Cons**: Secrets must be stored in config files (or templated at runtime),
  which conflicts with 12-factor app practices and CI/CD secret injection.

### Option 2: Credentials via environment variables only

Require `BITVAVO_API_KEY` and `BITVAVO_API_SECRET` env vars; omit fields from
config.toml entirely.

- **Pros**: Follows 12-factor principles; secrets managed externally.
- **Cons**: No way to override or document credentials in config.toml;
  operators cannot mix static config with env-driven secrets for different
  exchanges in the same file.

### Option 3: Config with env-var fallback (chosen)

Add optional `api_key` and `api_secret` fields to `SourceConfig`. When a field is
absent (or `None`), fall back to the corresponding environment variable. Config
takes precedence over env.

- **Pros**: Flexible — supports both config.toml and env var workflows. Config
  values take precedence (explicit > implicit), which is the least-surprising
  behavior. Env vars are Bitvavo-specific (only consulted for `exchange ==
  "bitvavo"`), so other exchanges are unaffected.
- **Cons**: Two sources of truth for credentials; if both are set, the env var is
  silently shadowed by the config value. Requires documentation to avoid
  confusion.

## Decision

Adopt **Option 3**.

- Add `#[serde(default)] pub api_key: Option<String>` and
  `#[serde(default)] pub api_secret: Option<String>` to `SourceConfig` in
  `src/config.rs`. The `#[serde(default)]` attribute ensures existing configs for
  OKX/Kraken/Bitstamp (which omit these fields) continue to parse without error.
- Add `SourceConfig::resolve_credentials(&self, exchange: &str) ->
  (Option<String>, Option<String>)` which:
  1. Returns the config value when `Some` (precedence).
  2. For `exchange == "bitvavo"`, falls back to `BITVAVO_API_KEY` /
     `BITVAVO_API_SECRET` env vars (filtered to exclude empty strings).
  3. For non-Bitvavo exchanges, returns `None` (credentials are not consulted).
- `to_data_source()` calls `resolve_credentials()` and writes the resolved values
  into `DataSourceConfig.api_key` / `api_secret` before `validate()`.
- Env var names are Bitvavo-specific (`BITVAVO_API_KEY`, `BITVAVO_API_SECRET`),
  matching the exchange's branding and avoiding ambiguity with other exchanges.

## Consequences

**Positive:**
- Supports both config.toml and env-var credential workflows, suitable for
  local development, CI/CD, and containerized deployments.
- Zero breaking change for existing non-Bitvavo configs (fields are optional with
  `#[serde(default)]`).
- Config precedence is explicit and predictable (documented).
- Non-Bitvavo exchanges are completely unaffected by the env-var lookup.

**Negative:**
- Two credential sources can lead to confusion if both are set simultaneously.
  This is mitigated by config taking precedence and clear documentation in the
  ADR and `config.toml`.
- Environment variable reads are a side effect in `resolve_credentials`,
  requiring serialised test isolation (a `Mutex` guard in test code).
- Env vars are Bitvavo-specific; if future exchanges require credentials, a
  similar pattern would need to be extended.
