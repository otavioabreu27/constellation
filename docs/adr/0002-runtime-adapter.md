# ADR 0002: Runtime Coordinator and OTP Adapter

## Status

Accepted

## Context

The first execution target is `gleam_otp`, but participant routing and core
effect resolution are not inherently OTP concerns. Keeping those rules inside
an actor would require each future runtime adapter to implement them again.

## Decision

The runtime is split into two layers:

- `stage/runtime` is a pure coordinator parameterized by the participant handle
  type. It owns participant and subscription registries, applies core commands,
  and resolves core effects into validated `Outbound` values.
- `stage/runtime/otp` owns the actor, typed subjects, synchronous calls, and the
  execution of `Outbound` values using `process.send`.

The generic runtime returns outbound actions as data instead of accepting
side-effecting functions. This keeps registry transitions atomic and allows
them to be tested without BEAM processes.

## Consequences

- Core protocol semantics remain independent of every runtime.
- Registry and routing semantics can be reused by future adapters.
- OTP integration tests focus only on actor transport and message delivery.
- Runtime adapters have a small imperative surface where outbound actions are
  executed.
- The OTP adapter monitors each registered participant process once, regardless
  of how many subscriptions it owns. A `DOWN` message is translated into the
  pure runtime's `ParticipantDown` command.
- Monitors are removed when explicit cancellation removes a participant's final
  subscription. Participant failure therefore cannot leave active subscriptions
  or stale monitor registrations behind.
