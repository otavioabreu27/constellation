# ADR 0004: Dispatch Ordering and Demand Semantics

## Status

Accepted

## Context

The core supports demand, broadcast, and partition dispatch strategies. Their
fairness, ordering, and buffering guarantees must be explicit because events
may be sent to independent runtime processes.

## Decision

`demand_strategy` assigns each event to one active subscription using
round-robin. The rotation is persisted in stage state, so fairness applies
across commands and not only within one event batch. Subscriptions without
demand are skipped.

`broadcast_strategy` uses a strict barrier. It emits an event only when every active
subscription has demand, and each emitted event consumes one demand unit from
each subscription. Cancelled subscriptions are not part of the barrier.

`partition_strategy` routes an event using its partition key. Multiple subscriptions may
own the same partition and are selected round-robin. An event without an owner
that has demand remains buffered, while events from other partitions may
continue. This avoids head-of-line blocking between partitions.

All built-in strategies preserve FIFO order for each subscription. No global
processing order is promised across different subscriptions because runtime
consumers may execute concurrently. Buffered events retain their relative
order. Custom selectors may intentionally skip an earlier event, so they own
the ordering policy while the library still enforces target and demand safety.

Cancelled subscriptions are removed from the active dispatch set. Their IDs
are retained separately so cancellation remains idempotent and later commands
can distinguish cancelled IDs from unknown IDs.

## Consequences

- Demand fairness is deterministic across repeated `Push` commands.
- A slow broadcast subscriber applies backpressure to all broadcast delivery.
- Cancelling a broadcast subscriber can immediately unblock buffered events.
- Partition streams preserve per-subscription order but not global order.
- Cancelled ID tombstones consume a small amount of memory for the stage's
  lifetime in exchange for idempotent lifecycle semantics.
