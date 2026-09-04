import constellation/core
import constellation/domains/command.{Ask, Subscribe}
import constellation/domains/stage_error
import constellation/domains/subscription
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id

fn subscription_id() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  id
}

fn participant_id() {
  participant_id.new("consumer-1")
}

pub fn subscribe_creates_active_subscription_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, effects)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )

  assert effects == []
  let assert Ok(value) = core.subscription(state, id)
  assert subscription.demand(value) == 0
}

pub fn ask_rejects_non_positive_amount_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )

  assert core.update(state, Ask(subscription_id: id, amount: 0))
    == Error(stage_error.InvalidDemand(0))
}

pub fn ask_rejects_negative_amount_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )

  assert core.update(state, Ask(subscription_id: id, amount: -1))
    == Error(stage_error.InvalidDemand(-1))
}

pub fn ask_rejects_unknown_subscription_test() {
  let id = subscription_id()

  assert core.update(core.new(), Ask(subscription_id: id, amount: 1))
    == Error(stage_error.UnknownSubscription(id))
}
