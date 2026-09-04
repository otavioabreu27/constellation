import gleam/dict
import stage/core
import stage/domains/command.{Ask, Push, Subscribe}
import stage/domains/effect.{SendEvents}
import stage/domains/stage_error
import stage/domains/subscription
import stage/value_objects/participant_id
import stage/value_objects/subscription_id

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
    core.update(state, Subscribe(id: id, participant_id: participant_id()))

  assert effects == []
  let assert Ok(value) = dict.get(state.subscriptions, id)
  assert subscription.demand(value) == 0
}

pub fn ask_rejects_non_positive_amount_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(state, Subscribe(id: id, participant_id: participant_id()))

  assert core.update(state, Ask(subscription_id: id, amount: 0))
    == Error(stage_error.InvalidDemand(0))
}

pub fn push_emits_only_requested_events_and_buffers_excess_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(state, Subscribe(id: id, participant_id: participant_id()))
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: id, amount: 3))
  let assert Ok(#(state, effects)) =
    core.update(state, Push(events: [1, 2, 3, 4, 5]))

  assert effects == [SendEvents(subscription_id: id, events: [1, 2, 3])]
  assert state.buffer == [4, 5]
}

pub fn ask_drains_buffer_in_fifo_order_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(state, Subscribe(id: id, participant_id: participant_id()))
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: id, amount: 3))
  let assert Ok(#(state, _)) = core.update(state, Push(events: [1, 2, 3, 4, 5]))
  let assert Ok(#(state, effects)) =
    core.update(state, Ask(subscription_id: id, amount: 2))

  assert effects == [SendEvents(subscription_id: id, events: [4, 5])]
  assert state.buffer == []
}
