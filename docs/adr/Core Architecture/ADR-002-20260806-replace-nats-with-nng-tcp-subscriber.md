# ADR-2: Replace NATS with NNG + TCP subscriber protocol

## Status
Accepted

## Date
2026-08-06

## Context
ADR-1 chose NATS (`async-nats`) as the forwarder from `criptomeria-marketdata` to
downstream subscribers. Operational requirements now demand:

1. A subscriber-side TCP protocol on port `14242` so any external client can
   receive the stream without depending on NATS infrastructure.
2. An *inner* log subscriber that connects to the same port, subscribes to all
   topics (current and future) and logs received messages to stdout using a
   proper async logger (not `println!`). The inner subscriber must only load
   when `--data-out` is passed.
3. Dynamic per-item topics named `{type}__{instrument}` (e.g. `lob__btcusd`,
   `trade__btcusd`), with the publisher augmented with the `exchange` field if
   absent.
4. Periodic per-topic subscriber counts in JSON
   `{"topic":"...","subscribers":N,"timestamp":...}` at
   `--show-subscriber-count-secs` intervals (default 5s).
5. All logs must carry a prefix tag identifying the source
   (`[lob-okx]`, `[trade-bitstamp]`, `[stdout_subscriber]`, `[system]`, …).

A `--test-timeout-secs` flag must be added so the long-running service exits
automatically in CI / automated verification contexts (otherwise `cargo run`
hangs forever).

## Options Considered

### 1. Keep NATS + bolt on a TCP subscriber
**Pros:** Minimal change; existing tests stay valid.

**Cons:** NATS subjects are static; the requirement of *dynamic* topics
`type__instrument` (and "current and future" topic semantics) does not map
naturally to NATS subjects. The subscriber-count-per-topic requirement is
awkward to satisfy on top of NATS without a second channel. The whole
`async-nats` dependency becomes an indirection for what is, at heart, a
one-way topic stream.

### 2. Use ZeroMQ (`zmq` crate) with XPUB/XSUB
**Pros:** XPUB lets the publisher observe subscription events, giving accurate
per-topic subscriber counts for free.

**Cons:** Rust `zmq` crate (0.10) is synchronous/blocking — every recv/send
must run on dedicated threads and be bridged to Tokio via channels.
Build of the system libzmq on the target machine requires either a system
`libzmq-dev` package (not installed) or compiling libzmq from source
(`zeromq-src`), which is heavier than the rest of the dependency tree and
fragile in offline/sandboxed environments.

### 3. Pure Tokio TCP server (no messaging library)
**Pros:** Zero native deps; full control over framing and per-connection
topic tracking; counts are exact.

**Cons:** Re-implements broker semantics (broadcast, slow-subscriber
buffering, reconnect) that a mature messaging library gives for free. Issue
explicitly calls for "zeroMQ" (now NNG) — a generic TCP server does not
satisfy the spirit of the requirement.

### 4. Use NNG (`nng` crate) with a custom subscriber-tracking registry (chosen)
**Pros:**
- Vendored build via the `cmake` crate + gcc produces a self-contained
  binary with no system libnng/libzmq dependency.
- NNG has *native* pub/sub topic filtering (`Subscribe`/`Unsubscribe`
  options) — Sub sockets match message prefixes against subscribed topics.
- AIO-based async I/O (callback-based) integrates cleanly with the
  existing Tokio main loop via a dedicated send/receive thread bridge.
- Per-topic subscriber counts are tracked in a shared in-process
  `SubscriberRegistry` populated by subscribers that opt into the
  protocol. Known topics are recorded by the broker on each publish.

**Cons:**
- NNG PUB/SUB does not expose subscription state to the publisher, so
  subscriber counts must be maintained by the application rather than read
  from the socket — counts therefore reflect subscribers that use the
  in-protocol registry, not external raw NNG SUB clients.
- Vendored build of libnng via `cmake` adds ~30s to the initial
  compilation.

## Decision
Use **Option 4**. Replace `async-nats` with the `nng` crate (default
`build-nng` feature → vendored libnng built via `cmake`):

- The broker (`src/broker.rs`) binds an NNG `Pub0` socket on
  `tcp://0.0.0.0:14242`. Wire frames are `topic\0payload` so the topic
  remains the body prefix that NNG's native SUB topic filter matches
  against, while the subscriber can split the frame back into
  `(topic, payload)`.
- Publishing is queued to a dedicated sender thread so the async caller
  never blocks on a slow subscriber; a bounded channel with `try_send`
  drops messages and warns on overflow instead of blocking the Tokio
  executor.
- The inner log subscriber (`src/subscriber.rs`) dials `127.0.0.1:14242`,
  calls `Subscribe(Vec::new())` to subscribe to all topics, and registers
  itself in a shared `SubscriberRegistry` (`src/registry.rs`) so the
  service can report per-topic counts.
- Subscriber-count reporting runs as a Tokio task with
  `tokio::time::interval`, snapshots the registry, and logs one JSON line
  per known topic.
- Logging uses `tracing` + `tracing-subscriber`; each log line embeds the
  required prefix tag (`[lob-okx]`, `[stdout_subscriber]`, `[system]`, …).
- `--test-timeout-secs` (default `0` = no timeout) spawns a Tokio task
  that sleeps and signals shutdown via a `tokio::sync::oneshot`. Without
  this, the binary never exits under `cargo run`.

## Consequences

### Positive
- Zero external broker dependency — the binary is self-contained and uses
  only well-known async primitives (`tracing`, `nng`, `tokio`).
- Native pub/sub topic filtering keeps wire format compact and the
  subscriber loop trivial.
- Dynamic `type__instrument` topic naming, exchange augmentation, prefix
  tags and JSON per-topic counts all map cleanly onto the registry +
  frame-splitter design.
- `--test-timeout-secs` makes CI / verification deterministic.
- Domain-first module split (`config`, `forward`, `registry`, `broker`,
  `subscriber`) keeps each concern small and independently testable.

### Negative
- Subscriber counts reflect participants registered in the in-process
  `SubscriberRegistry`. External raw NNG `Sub` clients that do not opt
  into the registry will not be counted.
- Vendored libnng via `cmake` adds a one-time build cost.
- The blocking `nng` socket is wrapped in dedicated OS threads (one for
  the broker send loop, one for the inner subscriber recv loop); this is
  the documented pattern for the synchronous `nng` crate and integrates
  cleanly with Tokio via channels and `Arc<AtomicBool>` shutdown flags.

## Notes
- The `zmq` crate's blocking model and build requirements motivated the
  switch to NNG; the rationale and the build verification are recorded
  in the worktree history (commit message + PR description).
- Tests cover pure helpers (config parsing, topic construction, frame
  splitting, exchange extraction) and an end-to-end NNG smoke test that
  binds a broker on an ephemeral port, dials a `Sub` socket and asserts
  the received `(topic, payload)` matches what was published.
- AGENTS.md mandates loading the `rust-coding` and `rust-tdd` skills
  before any Rust work; tests were written and refined using that cycle.

## References
- NNG crate: https://crates.io/crates/nng
- NNG pub/sub docs: https://nanomsg.github.io/nng/man/v1.2.2/nng_pub.7.html
- Issue: "Replace NATS with zeroMQ and implement TCP subscriber protocol"
  (this repo, GitHub issue #1)
