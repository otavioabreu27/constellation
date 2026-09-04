import constellation/value_objects/subscription_id.{type SubscriptionId}

pub type Effect(event) {
  SendEvents(subscription_id: SubscriptionId, events: List(event))
  NotifyCancelled(subscription_id: SubscriptionId)
}
