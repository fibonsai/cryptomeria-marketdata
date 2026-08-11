# ADR-012: Load environment variables from a `.env` file at startup

- **Category**: Integration
- **Status**: Accepted
- **Created**: 2026-08-11 12:00
- **Deciders**: tuxmonteiro
- **Relates**: ADR-001 (using cryptomeria-ingest), ADR-011 (Bitvavo credential resolution)

## Context

The `criptomeria-marketdata` application reads credentials and configuration from
`config.toml` and, for exchanges that require authentication (e.g. Bitvavo), from
environment variables such as `BITVAVO_API_KEY` and `BITVAVO_API_SECRET` (see
ADR-011).

In containerized and CI/CD deployments, operators must set these environment
variables explicitly via shell exports or secret-injection mechanisms. There is
no way to provide them from a local, git-ignored file.

This creates friction for:
- Local development (developers must remember to `export` variables or use `direnv`).
- CI pipelines that want a simple `.env`-style secret file mounted from a secret store.
- Container deployments that prefer a `.env` file over explicit `--env-file` flags.

## Options Considered

### Option 1: dotenvy crate (chosen)

Add the `dotenvy` crate (a maintained fork of `dotenv`) and call
`dotenvy::dotenv()` at the top of `main()`, before CLI parsing and config
deserialization.

- **Pros**: Minimal dependency; loads `.env` into the process environment so all
  existing `std::env::var` calls (including credential resolution in
  `SourceConfig::resolve_credentials`) automatically pick up the values.
  Best-effort by default — a missing file is silently ignored, which is the
  desired behavior for CI/container environments where env vars are set
  directly.
- **Cons**: Loads all variables into `std::env` globally, with no scoping or
  filtering. The `dotenv` format is simple (KEY=VALUE) and does not support
  nested config structures.

### Option 2: config-rs crate

Replace TOML parsing with `config-rs`, which natively supports `.env`,
`config.toml`, environment variables, and CLI args with a layered precedence
model.

- **Pros**: Unified config layer; built-in env var support; type-safe merging.
- **Cons**: Significant refactor of the existing `parse_config` / `AppConfig` /
  `SourceConfig` / `NngConfig` deserialization pipeline. The current `toml`
  crate dependency would be replaced, and the `#[serde(default)]` pattern used
  throughout would need to be re-evaluated. High churn for a feature that is
  primarily about loading a flat `.env` file.
- **Rejected**: Over-engineered for the current need; introduces a large
  refactor risk with no immediate benefit beyond `.env` loading.

### Option 3: Manual `.env` parsing

Write a small parser that reads a `KEY=VALUE` file and calls
`std::env::set_var` for each line.

- **Pros**: Zero additional dependencies.
- **Cons**: Reinvents a well-tested wheel; must handle comments, quoting,
  whitespace trimming, encoding, and error cases. Maintenance burden.
- **Rejected**: `dotenvy` is a tiny, battle-tested crate; rolling our own
  invites bugs in a security-sensitive area (credential handling).

## Decision

Adopt **Option 1**: use the `dotenvy` crate.

- Add `dotenvy = "0.15"` to `[dependencies]` in `Cargo.toml`.
- Create a new `src/env.rs` module with:
  - `load_env()` — calls `dotenvy::from_path(".env")`; logs at `info!` on
    success, `debug!` on missing file (not an error), `debug!` on other load
    failures. Never panics.
  - `load_env_from<P: AsRef<Path>>(path: P)` — same logic for a given path;
    enables unit testing with temporary directories.
- Export the module from `src/lib.rs` (`pub mod env;`).
- Call `criptomeria_marketdata::env::load_env()` in `main()` immediately after
  `init_logger()` and before `Cli::parse()` / config file reading, so that env
  vars are available to all downstream code.
- Add `.env` to `.gitignore` (currently only `.env.local` is covered).
- Add unit tests: a present `.env` file populates env vars; a missing file does
  not panic.
- Precedence: `dotenvy` only sets variables that are **not already present** in
  the environment, so explicit shell exports take precedence over `.env`.
  For credential resolution (ADR-011), config.toml values take precedence over
  env vars, so the full precedence chain is: explicit env var > `.env` file >
  config.toml credential field.

## Consequences

**Positive:**
- Developers and operators can place secrets in a local `.env` file that is
  git-ignored and loaded automatically at startup.
- No breaking change: `.env` is optional; absence is silently ignored.
- Zero refactor risk: the existing TOML-based config parsing is untouched.
- Minimal dependency footprint (one small, audited crate).
- Existing credential resolution (`resolve_credentials`) and CLI override
  patterns continue to work unchanged.

**Negative:**
- Global env var pollution: `dotenvy` sets vars for the entire process. This
  is acceptable for a CLI tool but would not be suitable for a library.
- The `.env` format is flat (`KEY=VALUE`) with no support for nested
  structures or typed values; complex config must still go in `config.toml`.
- Precedence subtlety: env vars set by the shell take precedence over `.env`,
  and config.toml takes precedence over env vars. This may surprise operators
  who set both. Documented in comments and this ADR.
