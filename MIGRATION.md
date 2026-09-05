# Migration guide

## 0.2.0 (unreleased)

The planned 0.1.1 changes were not published and are included in 0.2.0.

### Worker pool values

Function calls remain on `constellation/worker_pool`. Import constructors from
`constellation/worker_pool/types`; this includes events, errors and Snapshot.
Type aliases such as `worker_pool.Event` and `worker_pool.PoolError` remain valid.

```gleam
import constellation/worker_pool
import constellation/worker_pool/types as pool_types

let assert Ok(pool_types.Snapshot(..)) = worker_pool.snapshot(pool)

```

### Telemetry and supervision

Low-level Stage reporters now execute asynchronously, just like pool reporters.
Do not depend on a callback running before an API call returns or completing
before stop returns. Callback panics drop that telemetry event without disabling
future delivery. A notifier process killed externally stops its owner; an
application supervisor can then restart the unit. Mailboxes remain unbounded.

`worker_pool.supervised` returns a restart-stable handle and applies the
configured shutdown timeout. In-flight calls are not replayed. Calls during
restart may be unavailable. To remove a permanent child, use your supervisor's
child lifecycle rather than treating `pool.stop` as removal from the tree.

### Source protocol

`StaleGrant` is a rejected delivery, not an ACK. It never runs user code.
Reservation progress and Stage admission now commit together. Applications
retain ownership of durable recovery after stale or failed supply.

## 0.1.0

This is Constellation's first public release, so there is no earlier public API to migrate from.

Before `1.0.0`, breaking changes follow semantic versioning for `0.x` releases and are documented in [CHANGELOG.md](CHANGELOG.md).
