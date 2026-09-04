import constellation/core/dispatch
import constellation/core/lifecycle
import constellation/core/model
import constellation/core/subscription as subscription_transitions
import constellation/domains/command.{
  type Command, Ask, Cancel, ParticipantDown, Push, Shutdown, Subscribe,
}
import constellation/domains/dispatcher
import constellation/domains/effect.{type Effect}
import constellation/domains/stage_error.{type StageError}
import constellation/domains/subscription
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/option.{type Option, None}

/// Protocol state owned by a stage.
///
/// Its representation is hidden so subscriptions, demand, dispatch order, and
/// buffered events can only change through validated commands.
pub opaque type StageState(event) {
  StageState(model.State(event))
}

/// Creates an empty stage using demand-based round-robin dispatching.
pub fn new() -> StageState(event) {
  new_with_strategy(dispatcher.demand_strategy())
}

/// Creates an empty stage with the supplied dispatch strategy.
pub fn new_with_strategy(
  strategy: dispatcher.Strategy(event),
) -> StageState(event) {
  new_configured(strategy, None)
}

@internal
pub fn new_configured(
  strategy: dispatcher.Strategy(event),
  buffer_capacity: Option(Int),
) -> StageState(event) {
  StageState(model.new(strategy, buffer_capacity))
}

/// Returns buffered events in FIFO order for inspection and testing.
pub fn buffered_events(state: StageState(event)) -> List(event) {
  let StageState(state) = state
  model.buffered_events(state)
}

/// Looks up an active subscription and distinguishes cancelled IDs from unknown IDs.
pub fn subscription(
  state: StageState(event),
  id: SubscriptionId,
) -> Result(subscription.Subscription, StageError) {
  let StageState(state) = state
  model.subscription(state, id)
}

/// Reports whether an ID belongs to a subscription cancelled in this stage.
pub fn is_cancelled(state: StageState(event), id: SubscriptionId) -> Bool {
  let StageState(state) = state
  model.is_cancelled(state, id)
}

/// Applies one protocol command and returns the new state plus runtime effects.
pub fn update(
  state: StageState(event),
  command: Command(event),
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let StageState(state) = state
  let transition = case command {
    Subscribe(id: id, participant_id: participant, partition: partition) ->
      subscription_transitions.subscribe(state, id, participant, partition)
    Ask(subscription_id: id, amount: amount) ->
      subscription_transitions.ask(state, id, amount)
    Push(events: events) -> dispatch.push(state, events)
    Cancel(subscription_id: id) -> lifecycle.cancel(state, id)
    ParticipantDown(participant_id: participant_id) ->
      lifecycle.participant_down(state, participant_id)
    Shutdown -> Ok(lifecycle.shutdown(state))
  }
  wrap_transition(transition)
}

fn wrap_transition(
  transition: Result(#(model.State(event), List(Effect(event))), StageError),
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  case transition {
    Error(error) -> Error(error)
    Ok(#(state, effects)) -> Ok(#(StageState(state), effects))
  }
}
