import constellation/domains/stage_error
import constellation/runtime
import constellation/runtime/otp
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import gleam/erlang/process

fn subscription_id() {
  let assert Ok(id) = subscription_id.new("subscription-1")
  id
}

pub fn actor_delivers_only_requested_events_and_drains_buffer_test() {
  let id = subscription_id()
  let participant = participant_id.new("consumer-1")
  let recipient = process.new_subject()
  let assert Ok(started) = otp.start()
  let stage = started.data

  assert otp.subscribe(stage, id, participant, 0, recipient) == Ok(Nil)
  assert otp.ask(stage, id, 3) == Ok(Nil)
  assert otp.push(stage, [1, 2, 3, 4, 5]) == Ok(Nil)
  assert process.receive(recipient, within: 1000)
    == Ok(runtime.Events(subscription_id: id, events: [1, 2, 3]))

  assert otp.ask(stage, id, 2) == Ok(Nil)
  assert process.receive(recipient, within: 1000)
    == Ok(runtime.Events(subscription_id: id, events: [4, 5]))

  assert otp.stop(stage) == Ok(Nil)
}

pub fn consumer_can_report_processed_events_test() {
  let id = subscription_id()
  let recipient = process.new_subject()
  let assert Ok(started) = otp.start()
  let stage = started.data

  assert otp.subscribe(
      stage,
      id,
      participant_id.new("consumer-1"),
      0,
      recipient,
    )
    == Ok(Nil)
  assert otp.ask(stage, id, 1) == Ok(Nil)
  assert otp.push(stage, [1]) == Ok(Nil)
  assert process.receive(recipient, within: 1000)
    == Ok(runtime.Events(subscription_id: id, events: [1]))

  otp.report_consumed(stage, id, 1)
  assert otp.ask(stage, id, 1) == Ok(Nil)
  assert otp.stop(stage) == Ok(Nil)
}

pub fn consumption_reports_are_validated_test() {
  let id = subscription_id()
  let recipient = process.new_subject()
  let assert Ok(started) = otp.start()

  assert otp.subscribe(
      started.data,
      id,
      participant_id.new("consumer-1"),
      0,
      recipient,
    )
    == Ok(Nil)
  otp.report_consumed(started.data, id, 0)
  let assert Ok(unknown) = subscription_id.new("unknown")
  otp.report_consumed(started.data, unknown, 1)
  assert otp.push(started.data, [1]) == Ok(Nil)

  assert otp.stop(started.data) == Ok(Nil)
}

pub fn actor_returns_core_validation_errors_test() {
  let id = subscription_id()
  let participant = participant_id.new("consumer-1")
  let recipient = process.new_subject()
  let assert Ok(started) = otp.start()
  let stage = started.data

  assert otp.subscribe(stage, id, participant, 0, recipient) == Ok(Nil)
  assert otp.ask(stage, id, 0)
    == Error(otp.Runtime(runtime.Protocol(stage_error.InvalidDemand(0))))

  assert otp.stop(stage) == Ok(Nil)
}

pub fn actor_routes_round_robin_deliveries_to_each_participant_test() {
  let assert Ok(first_id) = subscription_id.new("subscription-1")
  let assert Ok(second_id) = subscription_id.new("subscription-2")
  let first_recipient = process.new_subject()
  let second_recipient = process.new_subject()
  let assert Ok(started) = otp.start()
  let stage = started.data

  assert otp.subscribe(
      stage,
      first_id,
      participant_id.new("consumer-1"),
      0,
      first_recipient,
    )
    == Ok(Nil)
  assert otp.subscribe(
      stage,
      second_id,
      participant_id.new("consumer-2"),
      0,
      second_recipient,
    )
    == Ok(Nil)
  assert otp.ask(stage, first_id, 2) == Ok(Nil)
  assert otp.ask(stage, second_id, 2) == Ok(Nil)
  assert otp.push(stage, [1, 2, 3, 4]) == Ok(Nil)

  assert process.receive(first_recipient, within: 1000)
    == Ok(runtime.Events(subscription_id: first_id, events: [1, 3]))
  assert process.receive(second_recipient, within: 1000)
    == Ok(runtime.Events(subscription_id: second_id, events: [2, 4]))

  assert otp.stop(stage) == Ok(Nil)
}

pub fn actor_rejects_replacing_participant_subject_test() {
  let assert Ok(first_id) = subscription_id.new("subscription-1")
  let assert Ok(second_id) = subscription_id.new("subscription-2")
  let participant = participant_id.new("consumer-1")
  let first_recipient = process.new_subject()
  let second_recipient = process.new_subject()
  let assert Ok(started) = otp.start()
  let stage = started.data

  assert otp.subscribe(stage, first_id, participant, 0, first_recipient)
    == Ok(Nil)
  assert otp.subscribe(stage, second_id, participant, 0, second_recipient)
    == Error(otp.Runtime(runtime.ParticipantAlreadyRegistered(participant)))

  assert otp.stop(stage) == Ok(Nil)
}

pub fn calls_to_stopped_stage_return_transport_error_test() {
  let assert Ok(started) = otp.start()
  assert otp.stop(started.data) == Ok(Nil)

  let assert Error(otp.StageUnavailable(_)) = otp.push(started.data, [1])
}

pub fn configured_buffer_capacity_rejects_excess_events_test() {
  let id = subscription_id()
  let recipient = process.new_subject()
  let assert Ok(config) = otp.with_buffer_capacity(otp.config(), 2)
  let assert Ok(started) = otp.start_with_config(config)

  assert otp.push(started.data, [1, 2]) == Ok(Nil)
  assert otp.push(started.data, [3])
    == Error(
      otp.Runtime(runtime.Protocol(stage_error.BufferCapacityExceeded(2, 3))),
    )
  assert otp.subscribe(
      started.data,
      id,
      participant_id.new("consumer-1"),
      0,
      recipient,
    )
    == Ok(Nil)
  assert otp.ask(started.data, id, 2) == Ok(Nil)
  assert process.receive(recipient, within: 1000)
    == Ok(runtime.Events(subscription_id: id, events: [1, 2]))
  assert otp.stop(started.data) == Ok(Nil)
}

pub fn buffer_capacity_allows_events_dispatched_immediately_test() {
  let id = subscription_id()
  let recipient = process.new_subject()
  let assert Ok(config) = otp.with_buffer_capacity(otp.config(), 2)
  let assert Ok(started) = otp.start_with_config(config)

  assert otp.subscribe(
      started.data,
      id,
      participant_id.new("consumer-1"),
      0,
      recipient,
    )
    == Ok(Nil)
  assert otp.ask(started.data, id, 3) == Ok(Nil)
  assert otp.push(started.data, [1, 2, 3]) == Ok(Nil)
  assert process.receive(recipient, within: 1000)
    == Ok(runtime.Events(subscription_id: id, events: [1, 2, 3]))
  assert otp.stop(started.data) == Ok(Nil)
}
