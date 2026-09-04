import stage/domains/subscription
import stage/value_objects/participant_id
import stage/value_objects/subscription_id

pub fn new_preserves_subscription_id_test() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  let participant = participant_id.new("consumer-1")
  let value = subscription.new(id, participant)

  assert subscription.id(value) == id
}

pub fn new_preserves_participant_id_test() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  let participant = participant_id.new("consumer-1")
  let value = subscription.new(id, participant)

  assert subscription.participant_id(value) == participant
}

pub fn new_starts_with_zero_demand_test() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  let value = subscription.new(id, participant_id.new("consumer-1"))

  assert subscription.demand(value) == 0
}

pub fn new_starts_active_test() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  let value = subscription.new(id, participant_id.new("consumer-1"))

  assert subscription.status(value) == subscription.Active
}

pub fn empty_subscription_id_is_rejected_test() {
  assert subscription_id.new("") == Error(subscription_id.Empty)
}
