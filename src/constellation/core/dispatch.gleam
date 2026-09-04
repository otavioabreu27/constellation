import constellation/core/model
import constellation/domains/buffer.{type Buffer}
import constellation/domains/dispatcher
import constellation/domains/effect.{type Effect, SendEvents}
import constellation/domains/stage_error.{
  type StageError, BufferCapacityExceeded,
}
import constellation/domains/subscription
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/dict
import gleam/list
import gleam/option.{None, Some}

@internal
pub fn push(
  state: model.State(event),
  events: List(event),
) -> Result(#(model.State(event), List(Effect(event))), StageError) {
  let result = dispatch_buffer(model.push_events(state, events))
  let #(updated, _) = result
  let buffered = model.buffer_size(updated)
  case model.buffer_capacity(state) {
    Some(capacity) if buffered > capacity ->
      Error(BufferCapacityExceeded(capacity, buffered))
    Some(_) | None -> Ok(result)
  }
}

@internal
pub fn dispatch_buffer(
  state: model.State(event),
) -> #(model.State(event), List(Effect(event))) {
  let #(subscriptions, dispatch_order, events, strategy) =
    model.dispatch_data(state)
  let targets = targets_in_order(dispatch_order, subscriptions)
  case buffer.is_empty(events) || !dispatcher.has_capacity(strategy, targets) {
    True -> #(state, [])
    False -> perform_dispatch(state, subscriptions, events, strategy, targets)
  }
}

fn perform_dispatch(
  state: model.State(event),
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  events: Buffer(event),
  strategy: dispatcher.Strategy(event),
  targets: List(dispatcher.Target),
) -> #(model.State(event), List(Effect(event))) {
  let result = dispatcher.dispatch(strategy, targets, buffer.to_list(events))
  #(
    model.complete_dispatch(
      state,
      apply_demands(subscriptions, result.targets),
      list.map(result.targets, dispatcher.subscription_id),
      buffer.from_list(result.remaining),
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
  case consumed > 0 {
    False -> subscriptions
    True -> consume_target_demand(subscriptions, id, value, consumed)
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
