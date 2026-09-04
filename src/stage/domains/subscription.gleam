import stage/value_objects/participant_id.{type ParticipantId}
import stage/value_objects/subscription_id.{type SubscriptionId}

pub type SubscriptionStatus {
  Active
  Cancelled
}

pub opaque type Subscription {
  Subscription(
    id: SubscriptionId,
    participant_id: ParticipantId,
    demand: Int,
    status: SubscriptionStatus,
  )
}

pub fn new(id: SubscriptionId, participant: ParticipantId) -> Subscription {
  Subscription(id: id, participant_id: participant, demand: 0, status: Active)
}

pub fn id(subscription: Subscription) -> SubscriptionId {
  let Subscription(id: value, ..) = subscription
  value
}

pub fn participant_id(subscription: Subscription) -> ParticipantId {
  let Subscription(participant_id: value, ..) = subscription
  value
}

pub fn demand(subscription: Subscription) -> Int {
  let Subscription(demand: value, ..) = subscription
  value
}

pub fn status(subscription: Subscription) -> SubscriptionStatus {
  let Subscription(status: value, ..) = subscription
  value
}

pub type SubscriptionError {
  InvalidDemand(Int)
  SubscriptionCancelled
  InsufficientDemand
}

pub fn add_demand(
  subscription: Subscription,
  amount: Int,
) -> Result(Subscription, SubscriptionError) {
  case amount <= 0 {
    True -> Error(InvalidDemand(amount))
    False ->
      case status(subscription) {
        Cancelled -> Error(SubscriptionCancelled)
        Active -> set_demand(subscription, demand(subscription) + amount)
      }
  }
}

pub fn consume_demand(
  subscription: Subscription,
  amount: Int,
) -> Result(Subscription, SubscriptionError) {
  case amount <= 0 {
    True -> Error(InvalidDemand(amount))
    False ->
      case status(subscription) {
        Cancelled -> Error(SubscriptionCancelled)
        Active ->
          case amount > demand(subscription) {
            True -> Error(InsufficientDemand)
            False -> set_demand(subscription, demand(subscription) - amount)
          }
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
    status: status,
  ) = subscription
  Ok(Subscription(
    id: id,
    participant_id: participant_id,
    demand: value,
    status: status,
  ))
}
