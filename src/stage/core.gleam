import gleam/dict
import gleam/list
import gleam/set.{type Set}
import stage/domains/buffer.{type Buffer}
import stage/domains/command.{
  type Command, Ask, Cancel, ParticipantDown, Push, Subscribe,
}
import stage/domains/dispatcher
import stage/domains/effect.{type Effect, NotifyCancelled, SendEvents}
import stage/domains/stage_error.{
  type StageError, DuplicateSubscription, InvalidDemand, SubscriptionCancelled,
  UnknownSubscription,
}
import stage/domains/subscription
import stage/value_objects/participant_id.{type ParticipantId}
import stage/value_objects/subscription_id.{type SubscriptionId}

/// Protocol state owned by a stage.
///
/// Its representation is hidden so subscriptions, demand, dispatch order, and
/// buffered events can only change through validated commands.
pub opaque type StageState(event) {
  StageState(
    subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
    cancelled: Set(SubscriptionId),
    dispatch_order: List(SubscriptionId),
    buffer: Buffer(event),
    strategy: dispatcher.Strategy(event),
  )
}

/// Creates an empty stage using demand-based round-robin dispatching.
pub fn new() -> StageState(event) {
  new_with_strategy(dispatcher.Demand)
}

/// Creates an empty stage with the supplied dispatch strategy.
pub fn new_with_strategy(
  strategy: dispatcher.Strategy(event),
) -> StageState(event) {
  StageState(
    subscriptions: dict.new(),
    cancelled: set.new(),
    dispatch_order: [],
    buffer: buffer.new(),
    strategy: strategy,
  )
}

/// Returns buffered events in FIFO order for inspection and testing.
pub fn buffered_events(state: StageState(event)) -> List(event) {
  let StageState(buffer: events, ..) = state
  buffer.to_list(events)
}

/// Looks up an active subscription and distinguishes cancelled IDs from unknown IDs.
pub fn subscription(
  state: StageState(event),
  id: SubscriptionId,
) -> Result(subscription.Subscription, StageError) {
  let StageState(subscriptions: subscriptions, cancelled: cancelled, ..) = state
  case dict.get(subscriptions, id) {
    Ok(value) -> Ok(value)
    Error(_) ->
      case set.contains(cancelled, id) {
        True -> Error(SubscriptionCancelled(id))
        False -> Error(UnknownSubscription(id))
      }
  }
}

/// Reports whether an ID belongs to a subscription cancelled in this stage.
pub fn is_cancelled(state: StageState(event), id: SubscriptionId) -> Bool {
  let StageState(cancelled: cancelled, ..) = state
  set.contains(cancelled, id)
}

/// Applies one protocol command and returns the new state plus runtime effects.
pub fn update(
  state: StageState(event),
  command: Command(event),
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  case command {
    Subscribe(id: id, participant_id: participant, partition: partition) ->
      subscribe(state, id, participant, partition)
    Ask(subscription_id: id, amount: amount) -> ask(state, id, amount)
    Push(events: events) -> Ok(dispatch_buffer(push_events(state, events)))
    Cancel(subscription_id: id) -> cancel(state, id)
    ParticipantDown(participant_id: participant_id) ->
      participant_down(state, participant_id)
  }
}

fn subscribe(
  state: StageState(event),
  id: SubscriptionId,
  participant: ParticipantId,
  partition: Int,
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let StageState(
    subscriptions: subscriptions,
    cancelled: cancelled,
    dispatch_order: dispatch_order,
    buffer: events,
    strategy: strategy,
  ) = state
  case dict.has_key(subscriptions, id) || set.contains(cancelled, id) {
    True -> Error(DuplicateSubscription(id))
    False ->
      Ok(
        #(
          StageState(
            subscriptions: dict.insert(
              subscriptions,
              id,
              subscription.new_with_partition(id, participant, partition),
            ),
            cancelled: cancelled,
            dispatch_order: list.append(dispatch_order, [id]),
            buffer: events,
            strategy: strategy,
          ),
          [],
        ),
      )
  }
}

fn ask(
  state: StageState(event),
  id: SubscriptionId,
  amount: Int,
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let StageState(
    subscriptions: subscriptions,
    cancelled: cancelled,
    dispatch_order: dispatch_order,
    buffer: events,
    strategy: strategy,
  ) = state
  case dict.get(subscriptions, id) {
    Error(_) ->
      case set.contains(cancelled, id) {
        True -> Error(SubscriptionCancelled(id))
        False -> Error(UnknownSubscription(id))
      }
    Ok(value) ->
      case subscription.add_demand(value, amount) {
        Error(subscription.InvalidDemand(invalid)) ->
          Error(InvalidDemand(invalid))
        Error(subscription.InsufficientDemand) -> Error(InvalidDemand(amount))
        Ok(updated) ->
          Ok(
            dispatch_buffer(StageState(
              subscriptions: dict.insert(subscriptions, id, updated),
              cancelled: cancelled,
              dispatch_order: dispatch_order,
              buffer: events,
              strategy: strategy,
            )),
          )
      }
  }
}

fn push_events(
  state: StageState(event),
  events: List(event),
) -> StageState(event) {
  let StageState(
    subscriptions: subscriptions,
    cancelled: cancelled,
    dispatch_order: dispatch_order,
    buffer: current,
    strategy: strategy,
  ) = state
  StageState(
    subscriptions: subscriptions,
    cancelled: cancelled,
    dispatch_order: dispatch_order,
    buffer: buffer.push(current, events),
    strategy: strategy,
  )
}

fn dispatch_buffer(
  state: StageState(event),
) -> #(StageState(event), List(Effect(event))) {
  let StageState(
    subscriptions: subscriptions,
    cancelled: cancelled,
    dispatch_order: dispatch_order,
    buffer: events,
    strategy: strategy,
  ) = state
  let targets = targets_in_order(dispatch_order, subscriptions)
  case buffer.is_empty(events) || !dispatcher.has_capacity(strategy, targets) {
    True -> #(state, [])
    False ->
      perform_dispatch(subscriptions, cancelled, events, strategy, targets)
  }
}

fn perform_dispatch(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  cancelled: Set(SubscriptionId),
  events: Buffer(event),
  strategy: dispatcher.Strategy(event),
  targets: List(dispatcher.Target),
) -> #(StageState(event), List(Effect(event))) {
  let result = dispatcher.dispatch(strategy, targets, buffer.to_list(events))
  #(
    StageState(
      subscriptions: apply_demands(subscriptions, result.targets),
      cancelled: cancelled,
      dispatch_order: list.map(result.targets, dispatcher.subscription_id),
      buffer: buffer.from_list(result.remaining),
      strategy: strategy,
    ),
    effects_for(result.deliveries),
  )
}

fn targets_in_order(
  order: List(SubscriptionId),
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
) -> List(dispatcher.Target) {
  case order {
    [] -> []
    [id, ..rest] ->
      case dict.get(subscriptions, id) {
        Error(_) -> targets_in_order(rest, subscriptions)
        Ok(value) -> target_and_rest(id, value, rest, subscriptions)
      }
  }
}

fn target_and_rest(
  id: SubscriptionId,
  value: subscription.Subscription,
  rest: List(SubscriptionId),
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
) -> List(dispatcher.Target) {
  let target = dispatcher.target(id, subscription.partition(value))
  let assert Ok(target) =
    dispatcher.with_demand(target, subscription.demand(value))
  [target, ..targets_in_order(rest, subscriptions)]
}

fn apply_demands(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  targets: List(dispatcher.Target),
) -> dict.Dict(SubscriptionId, subscription.Subscription) {
  list.fold(targets, subscriptions, fn(acc, target) {
    let id = dispatcher.subscription_id(target)
    case dict.get(acc, id) {
      Error(_) -> acc
      Ok(value) -> apply_target_demand(acc, id, value, target)
    }
  })
}

fn apply_target_demand(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  id: SubscriptionId,
  value: subscription.Subscription,
  target: dispatcher.Target,
) -> dict.Dict(SubscriptionId, subscription.Subscription) {
  let consumed = subscription.demand(value) - dispatcher.demand(target)
  case consumed {
    0 -> subscriptions
    _ -> consume_target_demand(subscriptions, id, value, consumed)
  }
}

fn consume_target_demand(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  id: SubscriptionId,
  value: subscription.Subscription,
  consumed: Int,
) -> dict.Dict(SubscriptionId, subscription.Subscription) {
  let assert Ok(updated) = subscription.consume_demand(value, consumed)
  dict.insert(subscriptions, id, updated)
}

fn effects_for(
  deliveries: List(dispatcher.Delivery(event)),
) -> List(Effect(event)) {
  case deliveries {
    [] -> []
    [dispatcher.Delivery(subscription_id: id, events: events), ..rest] ->
      case events {
        [] -> effects_for(rest)
        _ -> [
          SendEvents(subscription_id: id, events: events),
          ..effects_for(rest)
        ]
      }
  }
}

fn cancel(
  state: StageState(event),
  id: SubscriptionId,
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let StageState(subscriptions: subscriptions, cancelled: cancelled, ..) = state
  case dict.get(subscriptions, id) {
    Ok(_) -> cancel_active(state, id)
    Error(_) ->
      case set.contains(cancelled, id) {
        True -> Ok(#(state, []))
        False -> Error(UnknownSubscription(id))
      }
  }
}

fn cancel_active(
  state: StageState(event),
  id: SubscriptionId,
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let updated = remove_subscriptions(state, [id])
  let #(dispatched, effects) = dispatch_buffer(updated)
  Ok(#(dispatched, [NotifyCancelled(subscription_id: id), ..effects]))
}

fn participant_down(
  state: StageState(event),
  participant_id: ParticipantId,
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let StageState(subscriptions: subscriptions, dispatch_order: order, ..) =
    state
  let ids =
    list.filter(order, fn(id) {
      case dict.get(subscriptions, id) {
        Error(_) -> False
        Ok(value) -> subscription.participant_id(value) == participant_id
      }
    })
  let updated = remove_subscriptions(state, ids)
  let #(dispatched, effects) = dispatch_buffer(updated)
  let notifications =
    list.map(ids, fn(id) { NotifyCancelled(subscription_id: id) })
  Ok(#(dispatched, list.append(notifications, effects)))
}

fn remove_subscriptions(
  state: StageState(event),
  ids: List(SubscriptionId),
) -> StageState(event) {
  let StageState(
    subscriptions: subscriptions,
    cancelled: cancelled,
    dispatch_order: dispatch_order,
    buffer: events,
    strategy: strategy,
  ) = state
  StageState(
    subscriptions: list.fold(ids, subscriptions, dict.delete),
    cancelled: list.fold(ids, cancelled, set.insert),
    dispatch_order: list.filter(dispatch_order, fn(id) {
      !list.contains(ids, id)
    }),
    buffer: events,
    strategy: strategy,
  )
}
