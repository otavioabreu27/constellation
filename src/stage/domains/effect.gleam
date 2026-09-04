import stage/value_objects/subscription_id.{type SubscriptionId}

pub type Effect(event) {
  SendEvents(subscription_id: SubscriptionId, events: List(event))
}
