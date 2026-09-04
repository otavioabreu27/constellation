import stage/domains/stage_error
import stage/runtime
import stage/value_objects/participant_id
import stage/value_objects/subscription_id

fn id(value: String) {
  let assert Ok(id) = subscription_id.new(value)
  id
}

pub fn pure_runtime_resolves_deliveries_to_participant_handles_test() {
  let subscription_id = id("subscription-1")
  let participant_id = participant_id.new("consumer-1")
  let state = runtime.new()
  let assert Ok(#(state, [])) =
    runtime.subscribe(state, subscription_id, participant_id, 0, "subject-1")
  let assert Ok(#(state, [])) = runtime.ask(state, subscription_id, 2)
  let assert Ok(#(_, outbound)) = runtime.push(state, [1, 2, 3])

  assert outbound
    == [
      runtime.Deliver(
        to: "subject-1",
        message: runtime.Events(subscription_id: subscription_id, events: [1, 2]),
      ),
    ]
}

pub fn pure_runtime_wraps_protocol_errors_test() {
  let subscription_id = id("subscription-1")

  assert runtime.ask(runtime.new(), subscription_id, 1)
    == Error(runtime.Protocol(stage_error.UnknownSubscription(subscription_id)))
}

pub fn pure_runtime_rejects_replacing_participant_handle_test() {
  let participant_id = participant_id.new("consumer-1")
  let state = runtime.new()
  let assert Ok(#(state, [])) =
    runtime.subscribe(
      state,
      id("subscription-1"),
      participant_id,
      0,
      "subject-1",
    )

  assert runtime.subscribe(
      state,
      id("subscription-2"),
      participant_id,
      0,
      "subject-2",
    )
    == Error(runtime.ParticipantAlreadyRegistered(participant_id))
}

pub fn pure_runtime_keeps_shared_participant_until_last_subscription_cancels_test() {
  let participant_id = participant_id.new("consumer-1")
  let first_id = id("subscription-1")
  let second_id = id("subscription-2")
  let state = runtime.new()
  let assert Ok(#(state, [])) =
    runtime.subscribe(state, first_id, participant_id, 0, "subject-1")
  let assert Ok(#(state, [])) =
    runtime.subscribe(state, second_id, participant_id, 0, "subject-1")
  let assert Ok(#(state, outbound)) = runtime.cancel(state, first_id)
  assert outbound
    == [
      runtime.Deliver(
        to: "subject-1",
        message: runtime.Cancelled(subscription_id: first_id),
      ),
    ]

  let assert Ok(#(state, [])) = runtime.ask(state, second_id, 1)
  let assert Ok(#(_, outbound)) = runtime.push(state, [42])
  assert outbound
    == [
      runtime.Deliver(
        to: "subject-1",
        message: runtime.Events(subscription_id: second_id, events: [42]),
      ),
    ]
}
