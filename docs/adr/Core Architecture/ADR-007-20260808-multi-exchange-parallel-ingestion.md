# ADR-007: Run multiple exchange sources in parallel

- **Category**: Core Architecture
- **Status**: Proposed
- **Created**: 2026-08-08 08:07
- **Deciders**: tuxmonteiro
- **Supplants**: ADR-006's "exactly one exchange at runtime" guard
  (`AppConfig::exchange_source`).

## Context

The configuration schema now stores sources as a `HashMap<String, SourceConfig>`
keyed by exchange id (ADR-006), but the runtime still enforces exactly one
exchange via `AppConfig::exchange_source()`, which errors on zero or multiple
`[source.*]` sections. Operators want to consume several exchanges at once
(e.g. OKX and Kraken for the same instrument) and forward the normalized
LOB/trade stream to a single NNG PUB broker.

`cryptomeria_ingest::stream(DataSourceConfig)` is stateless: each call validates
its own config, opens its own WebSocket, and reconnects internally with
exponential backoff + jitter. Publishing is already non-blocking
(`Broker::publish` does a `try_send` on a bounded `SyncSender` drained by a
dedicated sender thread), so a single `Broker` can fan out to many streams.

Key constraints from the task:
- Each exchange must run in its own background task (independent reconnect).
- Topics must stay `{type}__{instrument}` (e.g. `lob__btcusdt`); the exchange
  must **not** be embedded in the topic, to preserve the existing wire format
  for subscribers.

## Options Considered

### Option 1: Keep single-exchange, reject multi-source configs

Preserve `exchange_source()` and error on multiple `[source.*]` sections.

- **Pros**: No concurrency changes; simplest.
- **Cons**: Does not satisfy the requirement. A second exchange requires a
  second process/broker, defeating the purpose.

### Option 2: Run exchanges sequentially

Iterate sources one at a time within a single stream loop.

- **Pros**: Trivial to implement; single task.
- **Cons**: Exchanges are NOT parallel, which is the explicit requirement.

### Option 3: Parallel per-exchange tasks sharing one `Arc<Broker>` (chosen)

Spawn one `tokio::task` per exchange on a `JoinSet`, all sharing a single
`Arc<Broker>`. The application exits only on Ctrl+C, the `--test-timeout-secs`
timer, or once **every** exchange source has ended. A single source ending or
failing does not stop the others (independent resilience).

- **Pros**: True parallelism; independent per-exchange reconnection; one
  broker; no task leaks on shutdown (remaining tasks are aborted and drained).
- **Cons**: Topic collisions if two exchanges subscribe to the same
  instrument+kind (operator responsibility). `Broker` must become `Sync` to be
  shareable as `Arc<Broker>`; this is achieved by guarding the join handle in a
  `Mutex` (publish stays lock-free).

### Option 3b: Parallel tasks, abort-all on the first source ending

Same as Option 3 but the app exits the moment *any* task completes
(fail-fast, mirroring the old single-exchange "stream ended → app exits").

- **Pros**: Simpler main loop; matches the old exit-on-stream-end behavior.
- **Cons**: One flaky or misconfigured exchange kills all healthy streams,
  undermining the multi-exchange resilience the feature is meant to provide.
  Rejected in favor of Option 3.

## Decision

Adopt **Option 3**.

- `AppConfig::exchange_source()` is replaced by
  `exchange_sources() -> Result<Vec<(&String, &SourceConfig)>, ConfigError>`,
  returning every configured exchange sorted by id, erroring only on zero
  sources. A derived helper `validated_sources()` builds and locally validates
  one `DataSourceConfig` per exchange up front (fail fast before binding the
  broker).
- `Broker` is made `Sync` by wrapping its `JoinHandle` in a `Mutex`; `publish`
  remains lock-free. The broker is shared as `Arc<Broker>` across tasks.
- `topic_for` is unchanged: topics remain `{type}__{instrument}`; the exchange
  is intentionally absent (documented in `forward.rs`).
- The main `select!` exits on Ctrl+C, the test-timeout oneshot, or when the
  `JoinSet` is empty after a task completes; on exit it `abort_all`s and drains
  the set so no task outlives shutdown.

## Consequences

**Positive:**
- Multiple exchanges consumed concurrently through one NNG broker.
- A single broken/ending exchange does not take down the others.
- No task leaks: all per-exchange tasks are aborted and joined before the
  broker/subscriber are dropped.
- Existing single-exchange configs keep working unchanged.

**Negative:**
- Operators must choose distinct instruments per exchange; otherwise topics
  collide and payloads are interleaved (last writer wins on `publish`). This is
  an accepted trade-off for keeping the topic wire format stable.
- `Broker` now contains a `Mutex` (used only at close time, never on `publish`),
  a minor structural change to a previously binary-only component.
- With the independent-exit model, a permanently-broken exchange will leave its
  task ended while healthy exchanges keep serving; operators rely on logs to
  notice the failed source.
