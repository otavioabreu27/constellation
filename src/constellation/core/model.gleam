import constellation/domains/buffer.{type Buffer}
import constellation/domains/dispatcher
import constellation/domains/stage_error.{
  type StageError, SubscriptionCancelled, UnknownSubscription,
}
import constellation/domains/subscription
import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/dict
import gleam/list
import gleam/option.{type Option}
import gleam/set.{type Set}

@internal
pub opaque type State(event) {
  State(
    subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
    cancelled: Set(SubscriptionId),
    dispatch_order: List(SubscriptionId),
    buffer: Buffer(event),
    strategy: dispatcher.Strategy(event),
    buffer_capacity: Option(Int),
  )
}

@internal
pub fn new(
  strategy: dispatcher.Strategy(event),
  buffer_capacity: Option(Int),
) -> State(event) {
  State(
    subscriptions: dict.new(),
    cancelled: set.new(),
    dispatch_order: [],
    buffer: buffer.new(),
    strategy: strategy,
    buffer_capacity: buffer_capacity,
  )
}

@internal
pub fn buffered_events(state: State(event)) -> List(event) {
  buffer.to_list(state.buffer)
}

@internal
pub fn subscription(
  state: State(event),
  id: SubscriptionId,
) -> Result(subscription.Subscription, StageError) {
  case dict.get(state.subscriptions, id) {
    Ok(value) -> Ok(value)
    Error(_) ->
      case set.contains(state.cancelled, id) {
        True -> Error(SubscriptionCancelled(id))
        False -> Error(UnknownSubscription(id))
      }
  }
}

@internal
pub fn is_cancelled(state: State(event), id: SubscriptionId) -> Bool {
  set.contains(state.cancelled, id)
}

@internal
pub fn has_subscription_id(state: State(event), id: SubscriptionId) -> Bool {
  dict.has_key(state.subscriptions, id) || set.contains(state.cancelled, id)
}

@internal
pub fn add_subscription(
  state: State(event),
  id: SubscriptionId,
  participant: ParticipantId,
  partition: Int,
) -> State(event) {
  State(
    ..state,
    subscriptions: dict.insert(
      state.subscriptions,
      id,
      subscription.new_with_partition(id, participant, partition),
    ),
    dispatch_order: list.append(state.dispatch_order, [id]),
  )
}

@internal
pub fn put_subscription(
  state: State(event),
  id: SubscriptionId,
  value: subscription.Subscription,
) -> State(event) {
  State(..state, subscriptions: dict.insert(state.subscriptions, id, value))
}

@internal
pub fn push_events(state: State(event), events: List(event)) -> State(event) {
  State(..state, buffer: buffer.push(state.buffer, events))
}

@internal
pub fn buffer_size(state: State(event)) -> Int {
  buffer.size(state.buffer)
}

@internal
pub fn available_demand(state: State(event)) -> Int {
  state.subscriptions
  |> dict.values
  |> list.fold(0, fn(total, value) { total + subscription.demand(value) })
}

@internal
pub fn buffer_capacity(state: State(event)) -> Option(Int) {
  state.buffer_capacity
}

@internal
pub fn dispatch_data(
  state: State(event),
) -> #(
  dict.Dict(SubscriptionId, subscription.Subscription),
  List(SubscriptionId),
  Buffer(event),
  dispatcher.Strategy(event),
) {
  #(state.subscriptions, state.dispatch_order, state.buffer, state.strategy)
}

@internal
pub fn complete_dispatch(
  state: State(event),
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  dispatch_order: List(SubscriptionId),
  events: Buffer(event),
) -> State(event) {
  State(
    ..state,
    subscriptions: subscriptions,
    dispatch_order: dispatch_order,
    buffer: events,
  )
}

@internal
pub fn subscriptions_for_participant(
  state: State(event),
  participant_id: ParticipantId,
) -> List(SubscriptionId) {
  list.filter(state.dispatch_order, fn(id) {
    case dict.get(state.subscriptions, id) {
      Error(_) -> False
      Ok(value) -> subscription.participant_id(value) == participant_id
    }
  })
}

@internal
pub fn subscription_ids(state: State(event)) -> List(SubscriptionId) {
  state.dispatch_order
}

@internal
pub fn remove_subscriptions(
  state: State(event),
  ids: List(SubscriptionId),
) -> State(event) {
  State(
    ..state,
    subscriptions: list.fold(ids, state.subscriptions, dict.delete),
    cancelled: list.fold(ids, state.cancelled, set.insert),
    dispatch_order: list.filter(state.dispatch_order, fn(id) {
      !list.contains(ids, id)
    }),
  )
}
