# Architecture

The pure Stage core, dispatcher strategies and runtime-independent routing
remain unchanged. The refactor targets the OTP pool shell and callback boundary.

## Responsibilities

- `worker_pool`: public configuration builders and capability façade.
- `worker_pool/types`: canonical events, errors, identities and snapshots.
- `worker_pool/internal/config`: validation before process creation.
- `worker_pool/internal/model`: implementation-only configuration and state.
- `worker_pool/internal/server`: mailbox dispatch and command coordination.
- `worker_pool/internal/lifecycle`: startup, replacement and draining.
- `worker_pool/internal/worker`: one worker actor and registry operations.
- `worker_pool/internal/dispatch`: delivery of core effects and source notices.
- `worker_pool/internal/protocol`: pure, atomic source/Stage supply transition.
- `runtime/otp/notifier`: callback isolation shared by pools and low-level Stage.

Internal modules do not import the public façade. Canonical boundary values
are defined once; the façade exposes type aliases. No class hierarchy, generic
plugin framework or persistence policy was introduced.

## Invariants

Only Accepted supply can produce Stage delivery effects. Duplicate/StaleGrant
return no effects. A failed Stage admission returns neither updated reservation
state nor acknowledgement: callers cannot accidentally commit half a transition.

A supervised handle uses one stable name allocated with its child specification.
Each pool incarnation also creates a private mailbox for worker completion and
exit messages. A late completion from an old worker cannot renew replacement
capacity. Source capabilities remain tied to their originating incarnation.

Telemetry callbacks run asynchronously. Callback panics are isolated per event;
source callback failures stop the pool because dropping protocol notifications
would strand capacity. Unexpected notifier death stops its owner so the
application's supervisor can recover rather than silently lose instrumentation.

## Scope and limitations

Pool state and delivered work remain ephemeral and at-most-once. Durable ACK,
lease fencing, retries and idempotency remain Quasar/Store concerns. Process
monitors are observed between worker batches; a handler may finish effects
after its owner dies. A timeout is not proof that an operation was rejected.

Reporter mailboxes are unbounded; reporters must remain inexpensive. Shutdown
does not wait for telemetry callbacks. Graceful shutdown drains accepted work;
supervisor termination is not a promise to finish arbitrary long-running work.

## Validation

Regression tests cover revoked delivery before/after availability resumes,
atomic rollback on failed admission, original-handle reuse after actual restart,
late old-worker completion, configured supervisor timeout, blocked and panicking
reporters, and externally killed notifiers. Existing demand, FIFO, overflow,
worker replacement, source failure and shutdown tests remain in the suite.

See [migration](../MIGRATION.md) for the 0.2 constructor namespace change.
