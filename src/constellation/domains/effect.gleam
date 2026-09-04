//// Side effects described by the pure Stage protocol.

import constellation/value_objects/subscription_id.{type SubscriptionId}

/// An outbound action for a runtime shell to interpret.
pub type Effect(event) {
  SendEvents(subscription_id: SubscriptionId, events: List(event))
  NotifyCancelled(subscription_id: SubscriptionId)
}
