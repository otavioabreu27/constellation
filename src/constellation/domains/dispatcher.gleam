//// Pure, demand-aware event dispatch strategies.

import constellation/domains/dispatcher/broadcast
import constellation/domains/dispatcher/common
import constellation/domains/dispatcher/custom
import constellation/domains/dispatcher/demand as demand_dispatcher
import constellation/domains/dispatcher/partition as partition_dispatcher
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/list
import gleam/option.{type Option}

/// A subscriber and the capacity currently available to it.
pub opaque type Target {
  Target(inner: common.Target)
}

/// Errors returned while constructing dispatch targets.
pub type TargetError {
  InvalidDemand(Int)
}

/// A pure policy for assigning events to subscribers.
pub opaque type Strategy(event) {
  Strategy(
    run: fn(List(common.Target), List(event)) -> common.Dispatch(event),
    capacity: fn(List(common.Target)) -> Bool,
  )
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
  let Strategy(run: run, ..) = strategy
  let targets = list.map(targets, fn(target) { target.inner })
  let result = run(targets, events)

  DispatchResult(
    targets: list.map(result.targets, Target),
    deliveries: list.map(result.deliveries, fn(delivery) {
      Delivery(
        subscription_id: delivery.subscription_id,
        events: delivery.events,
      )
    }),
    remaining: result.remaining,
  )
}

/// Reports whether the strategy can currently deliver at least one event.
pub fn has_capacity(strategy: Strategy(event), targets: List(Target)) -> Bool {
  let Strategy(capacity: capacity, ..) = strategy
  capacity(list.map(targets, fn(target) { target.inner }))
}

/// Creates a demand-based round-robin strategy.
pub fn demand_strategy() -> Strategy(event) {
  Strategy(run: demand_dispatcher.dispatch, capacity: any_target_has_demand)
}

/// Creates a strict broadcast strategy.
pub fn broadcast_strategy() -> Strategy(event) {
  Strategy(run: broadcast.dispatch, capacity: fn(targets) {
    case targets {
      [] -> False
      _ -> list.all(targets, fn(target) { common.demand(target) > 0 })
    }
  })
}

/// Creates a partition strategy using an event key function.
pub fn partition_strategy(key: fn(event) -> Int) -> Strategy(event) {
  Strategy(
    run: fn(targets, events) {
      partition_dispatcher.dispatch(targets, events, key)
    },
    capacity: any_target_has_demand,
  )
}

/// Creates a single-target strategy from a user-defined pure selector.
///
/// Returning an unknown target, a target without demand, or `None` keeps the
/// event buffered. The library retains ownership of demand and delivery state.
pub fn custom_strategy(
  select: fn(List(Target), event) -> Option(SubscriptionId),
) -> Strategy(event) {
  Strategy(
    run: fn(targets, events) {
      custom.dispatch(targets, events, fn(current, event) {
        select(list.map(current, Target), event)
      })
    },
    capacity: any_target_has_demand,
  )
}

/// Creates a dispatch target with no available demand.
pub fn target(subscription_id: SubscriptionId, partition: Int) -> Target {
  Target(common.target(subscription_id, partition))
}

/// Returns the subscription represented by a target.
pub fn subscription_id(target: Target) -> SubscriptionId {
  common.subscription_id(target.inner)
}

/// Returns the remaining capacity represented by a target.
pub fn demand(target: Target) -> Int {
  common.demand(target.inner)
}

/// Returns the partition assigned to a target.
pub fn partition(target: Target) -> Int {
  common.partition(target.inner)
}

/// Updates a target's demand, rejecting negative capacity.
pub fn with_demand(value: Target, demand: Int) -> Result(Target, TargetError) {
  case demand < 0 {
    True -> Error(InvalidDemand(demand))
    False -> Ok(Target(common.with_demand(value.inner, demand)))
  }
}

fn any_target_has_demand(targets: List(common.Target)) -> Bool {
  list.any(targets, fn(target) { common.demand(target) > 0 })
}
