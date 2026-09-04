import gleam/erlang/process
import gleam/otp/actor
import stage/domains/stage_error
import stage/runtime
import stage/runtime/otp
import stage/value_objects/participant_id
import stage/value_objects/subscription_id

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

  otp.stop(stage)
}

pub fn consumer_can_acknowledge_processed_events_test() {
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

  otp.consumed(stage, id, 1)
  assert otp.ask(stage, id, 1) == Ok(Nil)
  otp.stop(stage)
}

pub fn participant_death_automatically_invalidates_subscriptions_test() {
  let assert Ok(first_id) = subscription_id.new("subscription-1")
  let assert Ok(second_id) = subscription_id.new("subscription-2")
  let participant = participant_id.new("consumer-1")
  let assert Ok(consumer) =
    actor.new(Nil)
    |> actor.on_message(fn(state, _message) { actor.continue(state) })
    |> actor.start
  let assert Ok(started) = otp.start()
  let stage = started.data

  assert otp.subscribe(stage, first_id, participant, 0, consumer.data)
    == Ok(Nil)
  assert otp.subscribe(stage, second_id, participant, 0, consumer.data)
    == Ok(Nil)
  process.unlink(consumer.pid)
  process.kill(consumer.pid)
  process.sleep(50)

  assert otp.ask(stage, first_id, 1)
    == Error(runtime.Protocol(stage_error.SubscriptionCancelled(first_id)))
  assert otp.ask(stage, second_id, 1)
    == Error(runtime.Protocol(stage_error.SubscriptionCancelled(second_id)))

  otp.stop(stage)
}

pub fn actor_delivers_cancellation_notification_test() {
  let id = subscription_id()
  let participant = participant_id.new("consumer-1")
  let recipient = process.new_subject()
  let assert Ok(started) = otp.start()
  let stage = started.data

  assert otp.subscribe(stage, id, participant, 0, recipient) == Ok(Nil)
  assert otp.cancel(stage, id) == Ok(Nil)
  assert process.receive(recipient, within: 1000)
    == Ok(runtime.Cancelled(subscription_id: id))

  otp.stop(stage)
}

pub fn actor_returns_core_validation_errors_test() {
  let id = subscription_id()
  let participant = participant_id.new("consumer-1")
  let recipient = process.new_subject()
  let assert Ok(started) = otp.start()
  let stage = started.data

  assert otp.subscribe(stage, id, participant, 0, recipient) == Ok(Nil)
  assert otp.ask(stage, id, 0)
    == Error(runtime.Protocol(stage_error.InvalidDemand(0)))

  otp.stop(stage)
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

  otp.stop(stage)
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
    == Error(runtime.ParticipantAlreadyRegistered(participant))

  otp.stop(stage)
}
