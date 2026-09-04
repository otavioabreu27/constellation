import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/list

@internal
pub type Target {
  Target(subscription_id: SubscriptionId, demand: Int, partition: Int)
}

@internal
pub type Delivery(event) {
  Delivery(subscription_id: SubscriptionId, events: List(event))
}

@internal
pub type Dispatch(event) {
  Dispatch(
    targets: List(Target),
    deliveries: List(Delivery(event)),
    remaining: List(event),
  )
}

@internal
pub fn target(subscription_id: SubscriptionId, partition: Int) -> Target {
  Target(subscription_id: subscription_id, demand: 0, partition: partition)
}

@internal
pub fn subscription_id(target: Target) -> SubscriptionId {
  target.subscription_id
}

@internal
pub fn demand(target: Target) -> Int {
  target.demand
}

@internal
pub fn partition(target: Target) -> Int {
  target.partition
}

@internal
pub fn with_demand(target: Target, demand: Int) -> Target {
  Target(..target, demand: demand)
}

@internal
pub fn decrease_demand(target: Target) -> Target {
  decrease_by(target, 1)
}

@internal
pub fn decrease_by(target: Target, amount: Int) -> Target {
  Target(..target, demand: target.demand - amount)
}

/// Groups events by target without changing their final FIFO order.
@internal
pub fn add_delivery(
  deliveries: List(Delivery(event)),
  id: SubscriptionId,
  event: event,
) -> List(Delivery(event)) {
  case deliveries {
    [] -> [Delivery(subscription_id: id, events: [event])]
    [Delivery(subscription_id: delivery_id, events: events), ..rest] ->
      case delivery_id == id {
        True -> [
          Delivery(subscription_id: delivery_id, events: [event, ..events]),
          ..rest
        ]
        False -> [
          Delivery(subscription_id: delivery_id, events: events),
          ..add_delivery(rest, id, event)
        ]
      }
  }
}

/// Restores FIFO order after events were prepended during accumulation.
@internal
pub fn normalize_deliveries(
  deliveries: List(Delivery(event)),
) -> List(Delivery(event)) {
  list.map(deliveries, fn(delivery) {
    Delivery(..delivery, events: list.reverse(delivery.events))
  })
}
