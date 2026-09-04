# ADR 0005: Modular Boundaries and Public Extension Points

## Status

Accepted

## Context

The initial implementation preserved the functional-core/imperative-shell
boundary, but core transitions, dispatch algorithms, runtime routing, and OTP
effects each accumulated in large modules with several reasons to change.
The package is still pre-1.0, so its public contracts can be corrected before
compatibility guarantees are established.

## Decision

- Public modules are small façades and stable extension points.
- Cross-module implementation declarations are annotated with `@internal`.
- Core transitions are separated into model, subscription, dispatch, and
  lifecycle modules.
- Built-in dispatch algorithms live in separate modules. `Strategy` is an
  opaque pure policy, and `custom_strategy` permits extension without changing
  the library.
- Generic runtime routing is separated into a registry and effect router.
- The OTP shell separates client calls, actor protocol/state, server handling,
  outbound execution, participant monitoring, lifecycle, and telemetry.
- OTP calls represent runtime, timeout, and unavailable-process failures as
  `CallError` values rather than crashing callers.
- High-level consumer calls likewise represent Stage and consumer transport
  failures as `ConsumerError` values.
- Telemetry is structured and accepts a custom reporter. Standard logging is
  one reporter implementation, not a server dependency.
- Stage shutdown cancels active subscriptions before actor termination.
- Consumption reporting is observability only and is not represented as a
  protocol acknowledgement.
- Call timeout and producer-side buffer capacity are explicit configuration.
  Capacity is evaluated after immediate dispatch, so demanded events do not
  consume buffer allowance.

## Consequences

- No library source module needs to own every implementation for a layer.
- Dispatch policies and telemetry reporters are open for extension through
  pure functions.
- Internal modules can evolve without expanding the supported public surface.
- Applications must handle OTP transport failures explicitly.
- The breaking API changes require a pre-1.0 package version.
- Buffer capacity and protocol-level delivery acknowledgements remain separate
  future features rather than being implied by telemetry.
