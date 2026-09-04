//// Commands accepted by the pure Stage protocol.

import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}

/// A validated request to mutate Stage protocol state.
pub type Command(event) {
  Subscribe(id: SubscriptionId, participant_id: ParticipantId, partition: Int)
  Ask(subscription_id: SubscriptionId, amount: Int)
  Push(events: List(event))
  Cancel(subscription_id: SubscriptionId)
  ParticipantDown(participant_id: ParticipantId)
  Shutdown
}
