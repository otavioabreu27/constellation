# Changelog

All notable changes to Constellation are documented here.

## 0.2.0 - Unreleased

The previously prepared, unpublished 0.1.1 changes are included here. The
constructor namespace change requires a minor-version bump for this 0.x API.

### Fixed

- Revoked/stale source grants cannot deliver events. Failed Stage admission does
  not consume a reservation or acknowledge its offset.
- Supervised pool handles survive restart. Worker completion/exit messages use
  an incarnation-local mailbox, not the restart-stable public name.
- Supervisor shutdown timeout follows pool configuration.
- Low-level Stage telemetry cannot block protocol processing. Reporter callback
  panics drop only that event; unexpected notifier death stops the owner.

### Changed

- Pool responsibilities split into façade, canonical values, configuration,
  mailbox coordinator, lifecycle, effect routing, worker and supply protocol.
- Event/error/snapshot constructors moved to `constellation/worker_pool/types`.
  Functions and façade type aliases remain; update pattern-match imports.
- Source callbacks remain fail-fast; telemetry is asynchronous and best-effort.

### Added

- Bounded buffering for the public `worker_pool` API through
  `with_buffer_capacity`.
- Linked worker-pool startup through `start_link` and
  `start_with_source_link`.
- Validated OTP child specifications through `supervised`.

## 0.1.0 - 2026-09-04

### Added

- Pure functional core for subscriptions, demand, buffering, lifecycle, and dispatch.
- Demand, broadcast, partition, and safe custom dispatch strategies.
- Generic runtime and OTP Stage adapter with typed transport errors and telemetry.
- Stateful OTP consumer with automatic lifecycle monitoring.
- Resilient worker pools with bounded prefetch, automatic demand renewal, worker replacement, and graceful draining.
- Demand-driven asynchronous sources with partial, idempotent grants and explicit availability control.
- Mist dashboard, HTTP integration, and sequential-versus-OTP benchmark examples.

### Scope

- Processing is at-most-once when a worker fails after receiving a batch.
- Persistence, retries, backoff, leases, durable acknowledgements, dead-letter queues, and distributed coordination remain application concerns.
