# Constellation - Project Guidelines

> A Gleam-native, typed implementation of demand-driven event processing inspired by Elixir's GenStage.

## 1. Project Goal

The goal of this project is to build a **Gleam-native stage abstraction for the BEAM** with:

- producers
- consumers
- producer-consumers
- subscriptions
- explicit demand
- backpressure
- buffering
- dispatch strategies
- lifecycle handling
- typed event pipelines

The first implementation target will be the BEAM using `gleam_otp`, but the **core model must not depend on OTP**.

The architecture must allow the runtime adapter to be replaced later without rewriting the protocol semantics.

---

## 2. Core Architectural Principle

The project is split into two conceptual layers:

```text
┌─────────────────────────────────────┐
│           constellation_core          │
│                                     │
│  Pure protocol and state machine    │
│                                     │
│  - stage state                      │
│  - subscriptions                    │
│  - demand                           │
│  - buffering                        │
│  - dispatching                      │
│  - lifecycle transitions            │
│  - commands                         │
│  - effects                          │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│         constellation_runtime         │
│                                     │
│  Runtime adapter                    │
│                                     │
│  Initial implementation: gleam_otp  │
│                                     │
│  - actors                           │
│  - subjects                         │
│  - monitors                         │
│  - supervision                      │
│  - timers                           │
└──────────────────┬──────────────────┘
                   │
                   ▼
                  BEAM
```

The core must remain a deterministic state machine.

Conceptually:

```text
State + Command
      │
      ▼
   transition
      │
      ├── New State
      └── Effects
```

Example:

```gleam
pub fn update(
  state: StageState(event),
  command: Command(event),
) -> #(StageState(event), List(Effect(event)))
```

The core **must not know** about:

- `process.Subject`
- `actor.Actor`
- process identifiers
- monitors
- supervisors
- OTP-specific messages
- Erlang FFI

Those belong to the runtime layer.

---

## 3. Design Philosophy

### 3.1 Gleam-native, not a literal Elixir port

The project should preserve the **semantics** of GenStage, not necessarily its API.

Do not mechanically translate:

```elixir
handle_demand/2
handle_events/3
```

into Gleam.

Prefer APIs that make illegal states harder or impossible to represent.

Example goal:

```text
Producer(Order)
    │
    ▼
Consumer(Order)
```

should be valid, while:

```text
Producer(Order)
    │
    ▼
Consumer(User)
```

should fail at compile time whenever possible.

---

### 3.2 Functional core, imperative shell

Protocol decisions should be pure.

Runtime interaction should be isolated.

Good:

```text
Command
  ↓
pure transition
  ↓
Effects
  ↓
OTP adapter executes effects
```

Avoid:

```text
core function
  ↓
directly sends process messages
  ↓
mutates runtime state implicitly
```

---

### 3.3 Explicit effects

The core does not perform side effects.

Instead, it returns descriptions of what the runtime should do.

Example:

```gleam
pub type Effect(event) {
  SendEvents(
    subscription: SubscriptionId,
    events: List(event),
  )

  RequestDemand(
    subscription: SubscriptionId,
    amount: Int,
  )

  MonitorParticipant(ParticipantId)

  StopMonitoringParticipant(ParticipantId)

  NotifyCancelled(SubscriptionId)
}
```

The runtime interprets these effects.

---

## 4. Conceptual Model

## 4.1 Stage

A stage participates in a demand-driven event pipeline.

A stage may have one or both capabilities:

```text
Produces events
Consumes events
```

This gives us:

```text
Producer
Consumer
ProducerConsumer
```

Do not assume that these must be three entirely separate implementations.

Prefer shared protocol machinery with capability-specific behavior.

---

## 4.2 Subscription

A subscription represents a logical relationship between an upstream producer and a downstream consumer.

Conceptually:

```gleam
pub type Subscription {
  Subscription(
    id: SubscriptionId,
    participant: ParticipantId,
    demand: Int,
    status: SubscriptionStatus,
  )
}
```

Possible status:

```gleam
pub type SubscriptionStatus {
  Active
  Cancelled
}
```

Important invariant:

```text
demand >= 0
```

Always.

---

## 4.3 Demand

Demand represents downstream capacity.

Events flow downstream:

```text
Producer  ───── events ─────>  Consumer
```

Demand flows upstream:

```text
Producer  <──── demand ─────  Consumer
```

The producer must never emit more events to a subscription than that subscription has requested.

This is the central invariant of the project.

---

## 4.4 Buffer

A producer may receive or generate events while there is insufficient demand.

Those events may need to be buffered.

Conceptually:

```text
incoming events
      │
      ▼
   producer
      │
      ├── available demand → dispatch
      │
      └── excess           → buffer
```

Buffer policy must remain explicit.

Initial implementation:

```text
FIFO
```

Future policies may include:

```text
bounded buffer
drop oldest
drop newest
error on overflow
custom strategy
```

Do not implement these in the first milestone.

---

## 4.5 Dispatcher

The dispatcher decides how available events are distributed among subscriptions.

Dispatcher must be modeled as a strategy, not as an independent runtime process unless a future requirement proves otherwise.

Initial strategy:

```text
DemandDispatcher
```

Future strategies:

```text
BroadcastDispatcher
PartitionDispatcher
```

---

## 5. Protocol Commands

The exact API may evolve, but the core should understand concepts similar to:

```gleam
pub type Command(event) {
  Subscribe(
    participant: ParticipantId,
  )

  Ask(
    subscription: SubscriptionId,
    amount: Int,
  )

  Push(
    events: List(event),
  )

  Cancel(
    subscription: SubscriptionId,
  )

  ParticipantDown(
    participant: ParticipantId,
  )
}
```

Names are provisional.

Do not commit to public API naming before the core semantics are tested.

---

## 6. Core Invariants

These invariants are more important than API design.

### Demand

1. Demand must never become negative.
2. A stage must never emit more events than requested.
3. Demand belongs to a specific subscription.
4. Demand from one subscriber must not leak into another subscription.
5. Zero demand means zero emitted events.

### Events

6. Buffered events must not disappear.
7. FIFO ordering must be preserved when the dispatcher promises ordering.
8. Cancelled subscriptions must never receive new events.
9. Dead participants must not continue receiving events.
10. Events must only be emitted through valid active subscriptions.

### Lifecycle

11. Cancelling a subscription is idempotent where possible.
12. Participant failure must invalidate associated subscriptions.
13. Outstanding demand from a dead subscriber must be discarded.

### State machine

14. Every command must produce a deterministic state transition.
15. The same `State + Command` must always produce the same `State + Effects`.
16. Core tests must not require BEAM processes.

---

## 7. Initial Repository Structure

Recommended initial structure:

```text
src/
├── constellation.gleam
│
├── constellation/
│   ├── core.gleam
│   ├── command.gleam
│   ├── effect.gleam
│   ├── stage.gleam
│   ├── subscription.gleam
│   ├── demand.gleam
│   ├── buffer.gleam
│   ├── dispatcher.gleam
│   └── runtime/
│       └── otp.gleam
│
test/
├── core_test.gleam
├── demand_test.gleam
├── subscription_test.gleam
├── buffer_test.gleam
├── dispatcher_test.gleam
└── runtime/
    └── otp_test.gleam
```

Do not split modules prematurely.

Start smaller if the abstractions are not yet clear.

A valid first structure could simply be:

```text
src/
├── constellation.gleam
├── constellation/core.gleam
└── constellation/runtime/otp.gleam
```

Refactor only when concepts become stable.

---

## 8. Milestones

# M0 — Pure demand protocol

Goal:

```text
1 producer
    │
    ▼
1 consumer
```

Implement conceptually:

- subscription creation
- demand tracking
- event emission
- excess event buffering

No OTP yet.

Example scenario:

```text
Given:
  demand = 5
  buffer = []

When:
  Push([1,2,3,4,5,6,7])

Then:
  Emit [1,2,3,4,5]
  Buffer [6,7]
  demand = 0
```

Deliverable:

```text
pure state machine + tests
```

---

# M1 — Subscription lifecycle

Add:

- cancellation
- invalid subscription handling
- participant-down semantic event
- outstanding demand cleanup

Still no OTP requirement.

---

# M2 — Multiple consumers

Support:

```text
             ┌── Consumer A
Producer ────┼── Consumer B
             └── Consumer C
```

Each subscription owns independent demand.

Implement the first dispatcher:

```text
DemandDispatcher
```

Questions to explicitly answer:

- Which subscriber receives the next event?
- Is dispatch fair?
- Is ordering guaranteed?
- What happens when one subscriber has no demand?
- How does demand influence dispatch order?

Document decisions in ADRs.

---

# M3 — OTP runtime adapter

Only after M0–M2 semantics are stable.

Add:

```text
gleam_otp.Actor
```

Responsibilities:

```text
runtime message
      ↓
convert to Command
      ↓
core.update
      ↓
New State + Effects
      ↓
execute Effects
```

Example runtime loop:

```text
Actor mailbox
     │
     ▼
RuntimeMessage
     │
     ▼
Command
     │
     ▼
core.update
     │
     ├── state
     └── effects
              │
              ▼
      process.send / monitor / ...
```

---

# M4 — Runtime lifecycle

Integrate:

- process monitoring
- `DOWN` handling
- actor termination
- cancellation propagation

Verify real process failure behavior.

---

# M5 — ProducerConsumer

Support:

```text
Producer
    │
    ▼
ProducerConsumer
    │
    ▼
Consumer
```

Demand flows:

```text
Consumer
    │
    │ demand
    ▼
ProducerConsumer
    │
    │ upstream demand
    ▼
Producer
```

Events flow in the opposite direction.

Do not implement automatic `map/filter` abstractions yet.

---

# M6 — Additional dispatchers

Add one at a time.

### BroadcastDispatcher

```text
          ┌── A
Producer ─┼── B
          └── C
```

Define exactly how demand is calculated.

### PartitionDispatcher

```text
event
  │
partition(key)
  │
  ├── worker 0
  ├── worker 1
  └── worker 2
```

Ordering guarantees must be documented.

---

# M7 — Public API stabilization

Only after the protocol works.

Design ergonomic public APIs for:

```text
producer
consumer
producer-consumer
subscribe
cancel
```

Potential direction:

```gleam
constellation.producer(...)
constellation.consumer(...)
constellation.subscribe(...)
```

But API naming is not part of the early milestones.

---

# M8 — Flow-like abstractions

Out of scope until GenStage semantics are stable.

Possible future package:

```text
gleam_flow
    │
    ▼
constellation
```

Possible operations:

```text
map
filter
flat_map
reduce
partition
window
batch
```

Do not build this inside the initial project.

---

## 9. Testing Strategy

Use three levels of tests.

### Level 1 — Pure protocol tests

Most important suite.

No actors.

No processes.

No timers.

No concurrency.

Example:

```text
State
+
Command
↓
expected State
+
expected Effects
```

---

### Level 2 — Property / invariant tests

Test invariants across many transitions.

Examples:

```text
demand is never negative
```

```text
emitted_count <= requested_count
```

```text
cancelled subscriptions receive nothing
```

```text
all non-emitted events remain buffered
```

If suitable tooling is available, property-based testing should be considered.

---

### Level 3 — OTP integration tests

Only runtime semantics:

```text
actor receives message
actor sends effect
actor monitors peer
actor sees peer death
actor terminates correctly
```

Do not retest core business rules through actors when a pure test already covers them.

---

## 10. First End-to-End Demo

The first runtime demo should be intentionally boring:

```text
Infinite Counter Producer
          │
          ▼
     Slow Consumer
```

Producer can theoretically create events instantly.

Consumer intentionally sleeps or processes slowly.

Expected behavior:

```text
consumer asks 5
producer emits 5

consumer processes...

consumer asks 5
producer emits 5

consumer processes...
```

Run for at least several minutes.

Observe:

```text
mailbox does not grow indefinitely
buffer remains bounded by scenario
producer never exceeds demand
```

This proves the fundamental value of the library.

---

## 11. Non-Goals for v0.1

Do NOT implement yet:

- Flow
- Broadway
- distributed stages
- cross-node subscriptions
- persistence
- durable queues
- Kafka integration
- HTTP
- WebSockets
- telemetry framework
- custom schedulers
- hot code upgrade support
- automatic retries
- dynamic supervision API
- every GenStage callback
- exact Elixir API compatibility
- direct FFI to Elixir GenStage

The project should first prove its core protocol.

---

## 12. OTP Adapter Rules

When OTP integration begins:

### Allowed

```text
gleam_otp.Actor
gleam_erlang.process
subjects
selectors
monitors
links
supervision
timers
```

### Avoid

Directly embedding OTP types in core domain structures.

Bad:

```gleam
type Subscription {
  Subscription(
    consumer: process.Subject(Message),
  )
}
```

inside the core.

Prefer:

```gleam
type ParticipantId {
  ParticipantId(String)
}
```

and let the runtime own:

```text
ParticipantId -> Subject
```

mapping.

---

## 13. Runtime Registry

The runtime may need to maintain associations such as:

```text
ParticipantId
    ↓
process.Subject(...)
```

This is runtime state.

It should not leak into protocol state.

Conceptually:

```text
Core State
    +
Runtime References
```

are different concerns.

Keep them separate.

---

## 14. Error Model

Prefer typed errors.

Possible early shape:

```gleam
pub type StageError {
  UnknownSubscription(SubscriptionId)
  InvalidDemand(Int)
  SubscriptionCancelled(SubscriptionId)
  ParticipantUnavailable(ParticipantId)
}
```

Do not use crashes for protocol validation errors.

Crashes should remain available for genuinely exceptional runtime failure, consistent with OTP philosophy.

---

## 15. Documentation Expectations

Every public concept should explain:

1. What problem it solves.
2. Which invariant it protects.
3. Whether it belongs to core or runtime.
4. Whether ordering is guaranteed.
5. Whether it can buffer.
6. What happens when a participant fails.

Important architecture decisions should be written as ADRs.

Recommended:

```text
docs/
└── adr/
    ├── 0001-functional-core.md
    ├── 0002-runtime-adapter.md
    ├── 0003-demand-semantics.md
    └── 0004-dispatch-ordering.md
```

---

## 16. Suggested ADR #0001

```markdown
# ADR 0001 — Functional Core and Runtime Adapter

## Status

Accepted

## Context

The project will initially execute stages using gleam_otp actors.
However, protocol semantics should not depend on the lifecycle or API
of a specific runtime abstraction.

## Decision

All demand, subscription, buffering and dispatch semantics will live in
a pure functional core.

Runtime implementations translate runtime messages into core commands
and interpret returned effects.

## Consequences

Positive:

- deterministic testing
- low coupling to gleam_otp
- easier runtime replacement
- easier protocol reasoning
- fewer concurrency bugs in domain logic

Negative:

- explicit effect model
- runtime/core translation layer
- some duplicated identifiers/reference mappings
```

---

## 17. Git / Contribution Strategy

Keep commits small and semantic.

Examples:

```text
feat(core): model subscriptions
feat(core): add demand transition
test(core): prevent negative demand
feat(dispatcher): add demand dispatcher
feat(runtime): start producer actor
```

Avoid commits like:

```text
stuff
fix
changes
wip
```

During exploration, WIP commits are fine locally, but squash them before a meaningful PR if appropriate.

---

## 18. Versioning Strategy

Do not promise API stability before the architecture settles.

Suggested:

```text
0.1.x — protocol exploration
0.2.x — OTP runtime stabilization
0.3.x — producer-consumer
0.4.x — additional dispatchers
```

Reach `1.0.0` only when:

- public API is deliberate
- lifecycle semantics are documented
- dispatch guarantees are documented
- compatibility policy exists
- major invariants have strong test coverage

---

## 19. Naming

Do not over-optimize naming on day one.

The selected package name is:

```text
constellation
```

The name was available on Hex when selected. Verify availability again before
the first release.

The architecture matters more than the package name in the first milestone.

---

## 20. First Development Session

The first coding session should only attempt this:

### Step 1

Create project.

```bash
gleam new constellation
cd constellation
```

### Step 2

Create minimal domain types:

```text
SubscriptionId
ParticipantId
Subscription
StageState
Command
Effect
```

### Step 3

Implement:

```text
Subscribe
```

purely.

### Step 4

Implement:

```text
Ask(subscription, amount)
```

and enforce:

```text
amount > 0
demand never negative
```

### Step 5

Implement:

```text
Push(events)
```

with:

```text
available demand → SendEvents effect
excess events    → buffer
```

### Step 6

Write tests before adding OTP.

End the first session when this passes:

```text
Given demand 3
When 5 events arrive
Then 3 are emitted and 2 are buffered
```

Do not integrate `gleam_otp` yet.

---

## 21. Definition of Success for the First Prototype

The first prototype is successful when the following can be represented and tested without runtime dependencies:

```text
Consumer subscribes
        ↓
Consumer asks for 3
        ↓
Producer receives 5 events
        ↓
3 events are emitted
        ↓
2 events are buffered
        ↓
Consumer asks for 2
        ↓
buffer is drained
        ↓
buffer becomes empty
```

The output of every step must be visible as:

```text
new state
+
effects
```

If this works cleanly, the protocol foundation is ready for OTP integration.

---

## 22. Guiding Question

Whenever a design decision is unclear, ask:

> Is this a GenStage semantic rule, or is this merely how `gleam_otp`
> happens to execute it today?

If it is a semantic rule:

```text
core
```

If it is an execution detail:

```text
runtime adapter
```

This boundary should guide the entire project.
