import gleam/dict
import gleam/list
import stage/core
import stage/domains/command
import stage/domains/dispatcher
import stage/domains/effect
import stage/domains/stage_error.{type StageError}
import stage/value_objects/participant_id.{type ParticipantId}
import stage/value_objects/subscription_id.{type SubscriptionId}

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
}

/// Runtime-independent state that joins the pure core to participant handles.
pub opaque type Runtime(event, participant) {
  Runtime(
    core: core.StageState(event),
    participants: dict.Dict(ParticipantId, participant),
    subscriptions: dict.Dict(SubscriptionId, ParticipantId),
  )
}

/// Creates runtime state using demand-based round-robin dispatching.
pub fn new() -> Runtime(event, participant) {
  new_with_strategy(dispatcher.Demand)
}

/// Creates runtime state with the supplied pure dispatch strategy.
pub fn new_with_strategy(
  strategy: dispatcher.Strategy(event),
) -> Runtime(event, participant) {
  Runtime(
    core: core.new_with_strategy(strategy),
    participants: dict.new(),
    subscriptions: dict.new(),
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
  let Runtime(participants: participants, ..) = runtime
  case dict.get(participants, participant_id) {
    Error(_) ->
      apply_subscribe(runtime, id, participant_id, partition, participant)
    Ok(registered) ->
      case registered == participant {
        True ->
          apply_subscribe(runtime, id, participant_id, partition, participant)
        False -> Error(ParticipantAlreadyRegistered(participant_id))
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

/// Returns whether a participant still owns at least one subscription.
pub fn has_participant(
  runtime: Runtime(event, participant),
  participant_id: ParticipantId,
) -> Bool {
  let Runtime(participants: participants, ..) = runtime
  dict.has_key(participants, participant_id)
}

fn apply_subscribe(
  runtime: Runtime(event, participant),
  id: SubscriptionId,
  participant_id: ParticipantId,
  partition: Int,
  participant: participant,
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  let Runtime(
    core: core_state,
    participants: participants,
    subscriptions: subscriptions,
  ) = runtime
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
        Runtime(
          core: updated_core,
          participants: dict.insert(participants, participant_id, participant),
          subscriptions: dict.insert(subscriptions, id, participant_id),
        ),
        effects,
      )
  }
}

fn apply_command(
  runtime: Runtime(event, participant),
  command: command.Command(event),
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  let Runtime(
    core: core_state,
    participants: participants,
    subscriptions: subscriptions,
  ) = runtime
  case core.update(core_state, command) {
    Error(error) -> Error(Protocol(error))
    Ok(#(updated_core, effects)) ->
      resolve_effects(
        Runtime(
          core: updated_core,
          participants: participants,
          subscriptions: subscriptions,
        ),
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
  resolve_effects_loop(runtime, effects, [])
}

fn resolve_effects_loop(
  runtime: Runtime(event, participant),
  effects: List(effect.Effect(event)),
  outbound: List(Outbound(event, participant)),
) -> Result(
  #(Runtime(event, participant), List(Outbound(event, participant))),
  RuntimeError,
) {
  case effects {
    [] -> Ok(#(runtime, list.reverse(outbound)))
    [first, ..rest] ->
      case resolve_effect(runtime, first) {
        Error(error) -> Error(error)
        Ok(#(updated, delivery)) ->
          resolve_effects_loop(updated, rest, [delivery, ..outbound])
      }
  }
}

fn resolve_effect(
  runtime: Runtime(event, participant),
  effect: effect.Effect(event),
) -> Result(
  #(Runtime(event, participant), Outbound(event, participant)),
  RuntimeError,
) {
  case effect {
    effect.SendEvents(subscription_id: id, events: events) ->
      case participant_for(runtime, id) {
        Error(error) -> Error(error)
        Ok(participant) ->
          Ok(#(runtime, Deliver(to: participant, message: Events(id, events))))
      }
    effect.NotifyCancelled(subscription_id: id) ->
      case participant_for(runtime, id) {
        Error(error) -> Error(error)
        Ok(participant) ->
          Ok(#(
            remove_subscription(runtime, id),
            Deliver(to: participant, message: Cancelled(id)),
          ))
      }
  }
}

fn participant_for(
  runtime: Runtime(event, participant),
  subscription_id: SubscriptionId,
) -> Result(participant, RuntimeError) {
  let Runtime(participants: participants, subscriptions: subscriptions, ..) =
    runtime
  case dict.get(subscriptions, subscription_id) {
    Error(_) -> Error(MissingSubscriptionRoute(subscription_id))
    Ok(participant_id) ->
      case dict.get(participants, participant_id) {
        Error(_) -> Error(MissingParticipantRoute(participant_id))
        Ok(participant) -> Ok(participant)
      }
  }
}

fn remove_subscription(
  runtime: Runtime(event, participant),
  id: SubscriptionId,
) -> Runtime(event, participant) {
  let Runtime(
    core: core_state,
    participants: participants,
    subscriptions: subscriptions,
  ) = runtime
  case dict.get(subscriptions, id) {
    Error(_) -> runtime
    Ok(participant_id) ->
      remove_registered_subscription(
        core_state,
        participants,
        dict.delete(subscriptions, id),
        participant_id,
      )
  }
}

fn remove_registered_subscription(
  core_state: core.StageState(event),
  participants: dict.Dict(ParticipantId, participant),
  subscriptions: dict.Dict(SubscriptionId, ParticipantId),
  participant_id: ParticipantId,
) -> Runtime(event, participant) {
  let participants = case
    participant_is_registered(subscriptions, participant_id)
  {
    True -> participants
    False -> dict.delete(participants, participant_id)
  }
  Runtime(
    core: core_state,
    participants: participants,
    subscriptions: subscriptions,
  )
}

fn participant_is_registered(
  subscriptions: dict.Dict(SubscriptionId, ParticipantId),
  participant_id: ParticipantId,
) -> Bool {
  list.any(dict.to_list(subscriptions), fn(pair) {
    let #(_, registered) = pair
    registered == participant_id
  })
}
