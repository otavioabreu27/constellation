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

pub fn new_uses_default_partition_test() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  let value = subscription.new(id, participant_id.new("consumer-1"))

  assert subscription.partition(value) == 0
}

pub fn empty_subscription_id_is_rejected_test() {
  assert subscription_id.new("") == Error(subscription_id.Empty)
}

pub fn demand_is_accumulated_and_consumed_test() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  let value = subscription.new(id, participant_id.new("consumer-1"))
  let assert Ok(value) = subscription.add_demand(value, 5)
  let assert Ok(value) = subscription.consume_demand(value, 2)

  assert subscription.demand(value) == 3
}

pub fn demand_cannot_become_negative_test() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  let value = subscription.new(id, participant_id.new("consumer-1"))
  let assert Ok(value) = subscription.add_demand(value, 1)

  assert subscription.consume_demand(value, 2)
    == Error(subscription.InsufficientDemand)
}

pub fn non_positive_demand_is_rejected_test() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  let value = subscription.new(id, participant_id.new("consumer-1"))

  assert subscription.demand(value) == 0
  assert subscription.add_demand(value, 0)
    == Error(subscription.InvalidDemand(0))
}
