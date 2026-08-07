# ADR-003: Remove in-process subscriber registry and count reporting

- **Category**: Core Architecture
- **Status**: Accepted
- **Created**: 2026-08-07 09:20

## Context

ADR-002 introduced an in-process `SubscriberRegistry` (`src/registry.rs`) to
satisfy a requirement for periodic per-topic subscriber counts, reported as
JSON lines by the `marketdata` binary (`--show-subscriber-count-secs`,
default 5).

NNG PUB/SUB does not expose subscription state to the publisher. To report any
counts at all, the service had to track subscribers itself: the built-in log
subscriber registered on connect and unregistered on shutdown, and the broker
recorded every topic it published so it could enumerate known topics.

This design only ever counted the service's *own* in-process subscriber. Any
external client — an `nngcat`, a Rust/Go/Python NNG SUB client — dialing the
TCP socket directly never touched the registry, so the reported counts were
always wrong or misleading for the audience that mattered. The requirement was
effectively unusable in the presence of the very external subscribers the
protocol exists for, and it added a `Mutex`-guarded registry plus a background
reporting task to the forwarding hot path.

## Options Considered

### 1. Keep the registry as-is
**Pros:** No work; existing in-process subscriber still counted.
**Cons:** Counts are wrong by construction for external clients; a misleading
feature is worse than none.

### 2. Count subscribers by inspecting NNG internals
**Pros:** Would reflect real subscribers.
**Cons:** NNG PUB/SUB does not expose subscription state to the publisher; not
supported by the `nng` crate API. Not viable.

### 3. Remove the registry and count reporting (chosen)
**Pros:** Deletes a misleading feature and the shared mutable state on the hot
path; NNG handles topic filtering and delivery natively with no visibility
needed; smaller, simpler binary.
**Cons:** No subscriber visibility at all — operators cannot observe connected
subscribers from the service.

## Decision

Remove the entire register subsystem and the subscriber-count reporter.

- Delete `src/registry.rs` (`Subscriber`, `SubscriberRegistry`,
  `SharedRegistry`).
- Remove the registry field and plumbing from `Broker` and
  `StdoutSubscriber`.
- Remove the `--show-subscriber-count-secs` flag and the periodic JSON
  reporting task from `src/bin/marketdata.rs`.
- Keep the NNG broker and the built-in log subscriber; NNG continues to
  filter and deliver topics natively.

## Consequences

**Positive**
- Removes a feature that never reported accurate counts for external
  subscribers.
- Removes a `Mutex`-guarded registry and a background reporting task from the
  forwarding path.
- Fewer modules (`registry.rs` gone), fewer CLI flags, simpler shutdown.
- NNG's native pub/sub delivery is the single source of truth.

**Negative**
- No per-topic subscriber visibility from the service itself.
- If accurate subscriber metrics are ever required later, they must come from
  the transport (e.g. an NNG-backed metrics exporter or a proxy) rather than
  an in-process registry.

## Supersedes

Partially supersedes ADR-002, specifically option "4. Use NNG (`nng` crate)
with a custom subscriber-tracking registry" and the accompanying registry
decision. The rest of ADR-002 (NNG TCP pub/sub protocol on port 14242, frame
format, dedicated sender thread) remains in force.