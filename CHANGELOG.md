# Changelog

All notable changes to Constellation are documented here.

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
