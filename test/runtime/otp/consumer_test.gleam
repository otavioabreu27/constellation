import constellation/domains/stage_error
import constellation/runtime
import constellation/runtime/otp
import constellation/runtime/otp/consumer
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import gleam/erlang/process
import gleam/list

fn subscription_id() {
  let assert Ok(id) = subscription_id.new("consumer-subscription")
  id
}

pub fn consumer_processes_demanded_events_and_exposes_state_test() {
  let id = subscription_id()
  let assert Ok(started_stage) = otp.start()
  let assert Ok(started_consumer) =
    consumer.start(
      started_stage.data,
      id,
      participant_id.new("consumer"),
      [],
      list.append,
    )

  assert otp.push(started_stage.data, [1, 2, 3]) == Ok(Nil)
  assert consumer.state(started_consumer.data) == Ok([])
  assert consumer.ask(started_consumer.data, 2) == Ok(Nil)
  assert consumer.state(started_consumer.data) == Ok([1, 2])

  assert consumer.stop(started_consumer.data) == Ok(Nil)
  assert otp.ask(started_stage.data, id, 1)
    == Error(
      otp.Runtime(runtime.Protocol(stage_error.SubscriptionCancelled(id))),
    )
  assert otp.stop(started_stage.data) == Ok(Nil)
}

pub fn consumer_returns_demand_validation_errors_without_stopping_test() {
  let id = subscription_id()
  let assert Ok(started_stage) = otp.start()
  let assert Ok(started_consumer) =
    consumer.start(
      started_stage.data,
      id,
      participant_id.new("consumer"),
      0,
      fn(total, events) { total + list.length(events) },
    )

  assert consumer.ask(started_consumer.data, 0)
    == Error(
      consumer.Stage(
        otp.Runtime(runtime.Protocol(stage_error.InvalidDemand(0))),
      ),
    )
  assert consumer.ask(started_consumer.data, 1) == Ok(Nil)
  assert consumer.state(started_consumer.data) == Ok(0)

  assert consumer.stop(started_consumer.data) == Ok(Nil)
  assert otp.stop(started_stage.data) == Ok(Nil)
}

pub fn stopping_stage_terminates_subscribed_consumer_test() {
  let id = subscription_id()
  let assert Ok(started_stage) = otp.start()
  let assert Ok(started_consumer) =
    consumer.start(
      started_stage.data,
      id,
      participant_id.new("consumer"),
      [],
      list.append,
    )
  let monitor = process.monitor(started_consumer.pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })

  assert otp.stop(started_stage.data) == Ok(Nil)

  let assert Ok(process.ProcessDown(pid: pid, ..)) =
    process.selector_receive(selector, 1000)
  assert pid == started_consumer.pid
  let assert Error(consumer.ConsumerUnavailable(_)) =
    consumer.state(started_consumer.data)
}
