import constellation/runtime/otp
import constellation/runtime/otp/telemetry
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import gleam/erlang/process

pub fn custom_reporter_receives_structured_stage_events_test() {
  let reports = process.new_subject()
  let assert Ok(started) =
    otp.config()
    |> otp.with_reporter(fn(event) { process.send(reports, event) })
    |> otp.start_with_config

  assert otp.push(started.data, [1]) == Ok(Nil)

  let assert Ok(telemetry.Event(operation: "push", phase: "received", ..)) =
    process.receive(reports, within: 1000)
  let assert Ok(telemetry.Event(operation: "push", phase: "dispatched", ..)) =
    process.receive(reports, within: 1000)

  assert otp.stop(started.data) == Ok(Nil)
}

pub fn event_formatter_preserves_structured_fields_test() {
  let event =
    telemetry.Event(
      trace_id: 7,
      stage_pid: "<0.42.0>",
      operation: "deliver",
      phase: "mailbox_enqueued",
      detail: "events=3",
    )

  assert telemetry.format(event)
    == "constellation trace_id=7 stage_pid=<0.42.0> operation=deliver phase=mailbox_enqueued events=3"
}

pub fn invalid_consumption_report_emits_rejected_telemetry_test() {
  let reports = process.new_subject()
  let recipient = process.new_subject()
  let assert Ok(subscription) = subscription_id.new("consumer")
  let assert Ok(started) =
    otp.config()
    |> otp.with_reporter(fn(event) { process.send(reports, event) })
    |> otp.start_with_config
  assert otp.subscribe(
      started.data,
      subscription,
      participant_id.new("consumer"),
      0,
      recipient,
    )
    == Ok(Nil)
  let assert Ok(_) = process.receive(reports, within: 1000)
  let assert Ok(_) = process.receive(reports, within: 1000)

  otp.report_consumed(started.data, subscription, 0)

  let assert Ok(telemetry.Event(operation: "consume", phase: "rejected", ..)) =
    process.receive(reports, within: 1000)
  assert otp.stop(started.data) == Ok(Nil)
}
