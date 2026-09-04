import gleam/int
import gleam/list
import stage/value_objects/subscription_id.{type SubscriptionId}

/// A subscriber and the capacity currently available to it.
pub opaque type Target {
  Target(subscription_id: SubscriptionId, demand: Int, partition: Int)
}

pub type TargetError {
  InvalidDemand(Int)
}

/// Selects how events are assigned to subscribers.
pub type Strategy(event) {
  Demand
  Broadcast
  Partition(key: fn(event) -> Int)
}

/// Events assigned to one subscriber by a dispatch operation.
pub type Delivery(event) {
  Delivery(subscription_id: SubscriptionId, events: List(event))
}

/// The result of dispatching, including updated demand and undelivered events.
pub type DispatchResult(event) {
  DispatchResult(
    targets: List(Target),
    deliveries: List(Delivery(event)),
    remaining: List(event),
  )
}

/// Dispatches events with the selected strategy without performing side effects.
///
/// Demand is consumed in the returned targets. Events in `remaining` were not
/// delivered and must be retained by the core buffer.
/// Demand and partition strategies preserve FIFO per subscription. They do not
/// promise a global processing order across different subscribers.
pub fn dispatch(
  strategy: Strategy(event),
  targets: List(Target),
  events: List(event),
) -> DispatchResult(event) {
  case strategy {
    Demand -> demand_dispatch(targets, events)
    Broadcast -> broadcast_dispatch(targets, events)
    Partition(key) -> partition_dispatch(targets, events, key)
  }
}

/// Reports whether the strategy can currently deliver at least one event.
pub fn has_capacity(strategy: Strategy(event), targets: List(Target)) -> Bool {
  case strategy {
    Broadcast ->
      case targets {
        [] -> False
        _ -> list.all(targets, fn(target) { target.demand > 0 })
      }
    Demand | Partition(_) -> list.any(targets, fn(target) { target.demand > 0 })
  }
}

/// Creates a dispatch target with no available demand.
pub fn target(subscription_id: SubscriptionId, partition: Int) -> Target {
  Target(subscription_id: subscription_id, demand: 0, partition: partition)
}

/// Returns the subscription represented by a target.
pub fn subscription_id(target: Target) -> SubscriptionId {
  target.subscription_id
}

/// Returns the remaining capacity represented by a target.
pub fn demand(target: Target) -> Int {
  target.demand
}

/// Returns the partition assigned to a target.
pub fn partition(target: Target) -> Int {
  target.partition
}

/// Updates a target's demand, rejecting negative capacity.
pub fn with_demand(value: Target, demand: Int) -> Result(Target, TargetError) {
  case demand < 0 {
    True -> Error(InvalidDemand(demand))
    False -> set_target_demand(value, demand)
  }
}

// Rebuilds a target after validating its new demand.
fn set_target_demand(
  value: Target,
  demand: Int,
) -> Result(Target, TargetError) {
  let Target(subscription_id: id, partition: partition, ..) = value
  Ok(Target(subscription_id: id, demand: demand, partition: partition))
}

// Assigns events one at a time and rotates targets for fair distribution.
fn demand_dispatch(
  targets: List(Target),
  events: List(event),
) -> DispatchResult(event) {
  demand_loop(targets, events, [], [])
}

// Continues demand dispatch while retaining events without available demand.
fn demand_loop(
  targets: List(Target),
  events: List(event),
  deliveries: List(Delivery(event)),
  remaining: List(event),
) -> DispatchResult(event) {
  case events {
    [] ->
      DispatchResult(
        targets: targets,
        deliveries: normalize_deliveries(deliveries),
        remaining: list.reverse(remaining),
      )
    [event, ..rest] ->
      case select_target(targets, []) {
        Error(_) ->
          DispatchResult(
            targets: targets,
            deliveries: normalize_deliveries(deliveries),
            remaining: list.append(list.reverse(remaining), [event, ..rest]),
          )
        Ok(#(selected, reordered)) ->
          demand_one_event(
            rest,
            deliveries,
            remaining,
            selected,
            reordered,
            event,
          )
      }
  }
}

// Applies one demand delivery and continues with the next event.
fn demand_one_event(
  rest: List(event),
  deliveries: List(Delivery(event)),
  remaining: List(event),
  selected: Target,
  reordered: List(Target),
  event: event,
) -> DispatchResult(event) {
  let updated = decrease_demand(selected)
  let updated_targets = list.append(reordered, [updated])
  let next_deliveries =
    add_delivery(deliveries, selected.subscription_id, event)
  demand_loop(updated_targets, rest, next_deliveries, remaining)
}

// Finds the next target with capacity and moves it to the rotation's end.
fn select_target(
  targets: List(Target),
  before: List(Target),
) -> Result(#(Target, List(Target)), Nil) {
  case targets {
    [] -> Error(Nil)
    [first, ..rest] ->
      case first.demand > 0 {
        True -> Ok(#(first, list.append(rest, list.reverse(before))))
        False -> select_target(rest, [first, ..before])
      }
  }
}

// Consumes one unit of capacity from a target.
fn decrease_demand(value: Target) -> Target {
  Target(
    subscription_id: value.subscription_id,
    demand: value.demand - 1,
    partition: value.partition,
  )
}

// Groups events by target without changing their FIFO order.
fn add_delivery(
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

// Restores FIFO order after events were prepended during accumulation.
fn normalize_deliveries(
  deliveries: List(Delivery(event)),
) -> List(Delivery(event)) {
  list.map(deliveries, fn(delivery) {
    let Delivery(subscription_id: id, events: events) = delivery
    Delivery(subscription_id: id, events: list.reverse(events))
  })
}

// Sends the same prefix to every target using their shared minimum capacity.
fn broadcast_dispatch(
  targets: List(Target),
  events: List(event),
) -> DispatchResult(event) {
  let count = broadcast_count(targets, list.length(events))
  let delivered = list.take(events, count)
  let remaining = list.drop(events, count)
  let updated_targets =
    list.map(targets, fn(value) { decrease_by(value, count) })
  let deliveries =
    list.map(targets, fn(value) {
      Delivery(subscription_id: value.subscription_id, events: delivered)
    })
  DispatchResult(
    targets: updated_targets,
    deliveries: deliveries,
    remaining: remaining,
  )
}

// Calculates how many events all broadcast targets can accept together.
fn broadcast_count(targets: List(Target), event_count: Int) -> Int {
  case targets {
    [] -> 0
    [first, ..rest] ->
      list.fold(rest, int.min(first.demand, event_count), fn(acc, value) {
        int.min(acc, value.demand)
      })
  }
}

// Consumes a shared broadcast batch from one target.
fn decrease_by(value: Target, amount: Int) -> Target {
  Target(
    subscription_id: value.subscription_id,
    demand: value.demand - amount,
    partition: value.partition,
  )
}

// Routes each event to the target assigned to its partition key.
fn partition_dispatch(
  targets: List(Target),
  events: List(event),
  key: fn(event) -> Int,
) -> DispatchResult(event) {
  partition_loop(targets, events, key, [], [])
}

// Processes partitioned events while retaining unroutable events in FIFO order.
fn partition_loop(
  targets: List(Target),
  events: List(event),
  key: fn(event) -> Int,
  deliveries: List(Delivery(event)),
  remaining: List(event),
) -> DispatchResult(event) {
  case events {
    [] ->
      DispatchResult(
        targets: targets,
        deliveries: normalize_deliveries(deliveries),
        remaining: list.reverse(remaining),
      )
    [event, ..rest] ->
      case take_partition_target(targets, key(event), []) {
        Error(_) ->
          partition_loop(targets, rest, key, deliveries, [event, ..remaining])
        Ok(#(selected, reordered)) ->
          partition_one_event(
            rest,
            key,
            deliveries,
            remaining,
            selected,
            reordered,
            event,
          )
      }
  }
}

// Applies one partition delivery and continues with the next event.
fn partition_one_event(
  rest: List(event),
  key: fn(event) -> Int,
  deliveries: List(Delivery(event)),
  remaining: List(event),
  selected: Target,
  reordered: List(Target),
  event: event,
) -> DispatchResult(event) {
  let updated = decrease_demand(selected)
  let next_deliveries =
    add_delivery(deliveries, selected.subscription_id, event)
  partition_loop(
    list.append(reordered, [updated]),
    rest,
    key,
    next_deliveries,
    remaining,
  )
}

// Finds a target with the requested partition and at least one demand unit.
fn take_partition_target(
  targets: List(Target),
  partition: Int,
  before: List(Target),
) -> Result(#(Target, List(Target)), Nil) {
  case targets {
    [] -> Error(Nil)
    [first, ..rest] ->
      case first.partition == partition && first.demand > 0 {
        True -> Ok(#(first, list.append(rest, list.reverse(before))))
        False -> take_partition_target(rest, partition, [first, ..before])
      }
  }
}
