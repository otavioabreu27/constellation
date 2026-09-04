import constellation/domains/stage_error
import constellation/runtime
import constellation/runtime/otp
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import gleam/erlang/process
import gleam/otp/actor

fn subscription_id() {
  let assert Ok(id) = subscription_id.new("subscription-1")
  id
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
    == Error(
      otp.Runtime(runtime.Protocol(stage_error.SubscriptionCancelled(first_id))),
    )
  assert otp.ask(stage, second_id, 1)
    == Error(
      otp.Runtime(
        runtime.Protocol(stage_error.SubscriptionCancelled(second_id)),
      ),
    )

  assert otp.stop(stage) == Ok(Nil)
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

  assert otp.stop(stage) == Ok(Nil)
}
