import stage/value_objects/participant_id.{type ParticipantId}
import stage/value_objects/subscription_id.{type SubscriptionId}

pub type Command(event) {
  Subscribe(id: SubscriptionId, participant_id: ParticipantId)
  Ask(subscription_id: SubscriptionId, amount: Int)
  Push(events: List(event))
  Cancel(subscription_id: SubscriptionId)
  ParticipantDown(participant_id: ParticipantId)
}
