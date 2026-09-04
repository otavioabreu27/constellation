import constellation/core
import constellation/domains/command
import constellation/domains/dispatcher
import constellation/domains/effect
import constellation/domains/stage_error.{type StageError}
import constellation/runtime/effect_router
import constellation/runtime/registry
import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/list
import gleam/option.{type Option, None}

/// Messages delivered by a runtime adapter to a subscribed participant.
pub type ParticipantMessage(event) {
  Events(subscription_id: SubscriptionId, events: List(event))
  Cancelled(subscription_id: SubscriptionId)
}

/// A validated delivery that a concrete runtime adapter must execute.
pub type Outbound(event, participant) {
  Deliver(to: participant, message: ParticipantMessage(event))
}

/// Errors produced by protocol validation or participant routing.
pub type RuntimeError {
  Protocol(StageError)
  ParticipantAlreadyRegistered(ParticipantId)
  MissingSubscriptionRoute(SubscriptionId)
  MissingParticipantRoute(ParticipantId)
  InvalidConsumptionReport(amount: Int)
}

/// Runtime-independent state that joins the pure core to participant handles.
pub opaque type Runtime(event, participant) {
  Runtime(
    core: core.StageState(event),
    registry: registry.Registry(participant),
  )
}

/// Creates runtime state using demand-based round-robin dispatching.
pub fn new() -> Runtime(event, participant) {
  new_with_strategy(dispatcher.demand_strategy())
}

/// Creates runtime state with the supplied pure dispatch strategy.
pub fn new_with_strategy(
  strategy: dispatcher.Strategy(event),
) -> Runtime(event, participant) {
  new_configured(strategy, None)
}

@internal
pub fn new_configured(
  strategy: dispatcher.Strategy(event),
  buffer_capacity: Option(Int),
) -> Runtime(event, participant) {
  Runtime(
    core: core.new_configured(strategy, buffer_capacity),
    registry: registry.new(),
  )
}

/// Registers a participant handle and creates its core subscription atomically.
pub fn subscribe(
  runtime: Runtime(event, participant),
  id: SubscriptionId,
  participant_id: ParticipantId,
  partition: Int,
  participant: participant,
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  let Runtime(core: core_state, registry: current_registry) = runtime
  case registry.register(current_registry, id, participant_id, participant) {
    Error(error) -> Error(registry_error(error))
    Ok(updated_registry) ->
      case
        core.update(
          core_state,
          command.Subscribe(
            id: id,
            participant_id: participant_id,
            partition: partition,
          ),
        )
      {
        Error(error) -> Error(Protocol(error))
        Ok(#(updated_core, effects)) ->
          resolve_effects(
            Runtime(core: updated_core, registry: updated_registry),
            effects,
          )
      }
  }
}

/// Adds demand and resolves core effects into participant-specific outbound data.
pub fn ask(
  runtime: Runtime(event, participant),
  subscription_id: SubscriptionId,
  amount: Int,
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  apply_command(
    runtime,
    command.Ask(subscription_id: subscription_id, amount: amount),
  )
}

/// Pushes events and resolves core effects without performing side effects.
pub fn push(
  runtime: Runtime(event, participant),
  events: List(event),
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  apply_command(runtime, command.Push(events: events))
}

/// Cancels a subscription and resolves its notification before removing its route.
pub fn cancel(
  runtime: Runtime(event, participant),
  subscription_id: SubscriptionId,
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  apply_command(runtime, command.Cancel(subscription_id: subscription_id))
}

/// Invalidates every subscription owned by a participant that went down.
pub fn participant_down(
  runtime: Runtime(event, participant),
  participant_id: ParticipantId,
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  apply_command(
    runtime,
    command.ParticipantDown(participant_id: participant_id),
  )
}

/// Cancels every active subscription and resolves their notifications.
pub fn shutdown(
  runtime: Runtime(event, participant),
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  apply_command(runtime, command.Shutdown)
}

/// Returns whether a participant still owns at least one subscription.
pub fn has_participant(
  runtime: Runtime(event, participant),
  participant_id: ParticipantId,
) -> Bool {
  let Runtime(registry: current_registry, ..) = runtime
  registry.has_participant(current_registry, participant_id)
}

/// Returns whether a subscription still has an active participant route.
pub fn has_subscription(
  runtime: Runtime(event, participant),
  subscription_id: SubscriptionId,
) -> Bool {
  let Runtime(registry: current_registry, ..) = runtime
  registry.has_subscription(current_registry, subscription_id)
}

fn apply_command(
  runtime: Runtime(event, participant),
  command: command.Command(event),
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  let Runtime(core: core_state, registry: current_registry) = runtime
  case core.update(core_state, command) {
    Error(error) -> Error(Protocol(error))
    Ok(#(updated_core, effects)) ->
      resolve_effects(
        Runtime(core: updated_core, registry: current_registry),
        effects,
      )
  }
}

fn resolve_effects(
  runtime: Runtime(event, participant),
  effects: List(effect.Effect(event)),
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  let Runtime(core: core_state, registry: current_registry) = runtime
  case effect_router.resolve(current_registry, effects) {
    Error(error) -> Error(registry_error(error))
    Ok(#(updated_registry, routed)) ->
      Ok(#(
        Runtime(core: core_state, registry: updated_registry),
        routed_effects_to_outbound(routed),
      ))
  }
}

fn routed_effects_to_outbound(
  routed: List(effect_router.RoutedEffect(event, participant)),
) -> List(Outbound(event, participant)) {
  list.map(routed, fn(routed_effect) {
    case routed_effect {
      effect_router.EventsRouted(
        to: participant,
        subscription_id: id,
        events: events,
      ) -> Deliver(to: participant, message: Events(id, events))
      effect_router.CancellationRouted(to: participant, subscription_id: id) ->
        Deliver(to: participant, message: Cancelled(id))
    }
  })
}

fn registry_error(error: registry.RegistryError) -> RuntimeError {
  case error {
    registry.ParticipantAlreadyRegistered(id) ->
      ParticipantAlreadyRegistered(id)
    registry.MissingSubscriptionRoute(id) -> MissingSubscriptionRoute(id)
    registry.MissingParticipantRoute(id) -> MissingParticipantRoute(id)
  }
}
