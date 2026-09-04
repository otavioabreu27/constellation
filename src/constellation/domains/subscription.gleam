//// Subscription identity, partition, and demand values.

import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}

/// An active downstream subscription and its remaining demand.
pub opaque type Subscription {
  Subscription(
    id: SubscriptionId,
    participant_id: ParticipantId,
    demand: Int,
    partition: Int,
  )
}

/// Creates an active subscription with zero demand in the default partition.
pub fn new(id: SubscriptionId, participant: ParticipantId) -> Subscription {
  new_with_partition(id, participant, 0)
}

/// Creates an active subscription with zero demand in a dispatcher partition.
pub fn new_with_partition(
  id: SubscriptionId,
  participant: ParticipantId,
  partition: Int,
) -> Subscription {
  Subscription(
    id: id,
    participant_id: participant,
    demand: 0,
    partition: partition,
  )
}

/// Returns this subscription's stable identity.
pub fn id(subscription: Subscription) -> SubscriptionId {
  let Subscription(id: value, ..) = subscription
  value
}

/// Returns the participant that owns this subscription.
pub fn participant_id(subscription: Subscription) -> ParticipantId {
  let Subscription(participant_id: value, ..) = subscription
  value
}

/// Returns the subscription's remaining event capacity.
pub fn demand(subscription: Subscription) -> Int {
  let Subscription(demand: value, ..) = subscription
  value
}

/// Returns the dispatcher partition assigned to this subscription.
pub fn partition(subscription: Subscription) -> Int {
  let Subscription(partition: value, ..) = subscription
  value
}

/// Errors returned while changing subscription demand.
pub type SubscriptionError {
  InvalidDemand(Int)
  InsufficientDemand
}

/// Adds positive downstream capacity without reactivating cancelled subscriptions.
pub fn add_demand(
  subscription: Subscription,
  amount: Int,
) -> Result(Subscription, SubscriptionError) {
  case amount <= 0 {
    True -> Error(InvalidDemand(amount))
    False -> set_demand(subscription, demand(subscription) + amount)
  }
}

/// Consumes positive capacity while preventing demand from becoming negative.
pub fn consume_demand(
  subscription: Subscription,
  amount: Int,
) -> Result(Subscription, SubscriptionError) {
  case amount <= 0 {
    True -> Error(InvalidDemand(amount))
    False ->
      case amount > demand(subscription) {
        True -> Error(InsufficientDemand)
        False -> set_demand(subscription, demand(subscription) - amount)
      }
  }
}

fn set_demand(
  subscription: Subscription,
  value: Int,
) -> Result(Subscription, SubscriptionError) {
  let Subscription(
    id: id,
    participant_id: participant_id,
    demand: _,
    partition: partition,
  ) = subscription
  Ok(Subscription(
    id: id,
    participant_id: participant_id,
    demand: value,
    partition: partition,
  ))
}
