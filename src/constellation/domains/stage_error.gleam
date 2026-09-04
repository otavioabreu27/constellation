import constellation/value_objects/subscription_id.{type SubscriptionId}

pub type StageError {
  InvalidDemand(amount: Int)
  UnknownSubscription(subscription_id: SubscriptionId)
  DuplicateSubscription(subscription_id: SubscriptionId)
  SubscriptionCancelled(subscription_id: SubscriptionId)
  BufferCapacityExceeded(capacity: Int, attempted: Int)
}
