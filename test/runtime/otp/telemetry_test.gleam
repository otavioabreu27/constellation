import constellation/runtime/otp
import constellation/runtime/otp/telemetry
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import gleam/erlang/process
import gleam/string

pub fn killed_stage_reporter_stops_owner_test() {
  let reports = process.new_subject()
  let assert Ok(started) =
    otp.config()
    |> otp.with_reporter(fn(_) { process.send(reports, process.self()) })
    |> otp.start_with_config
  process.unlink(started.pid)
  assert otp.push(started.data, [1]) == Ok(Nil)
  let assert Ok(pid) = process.receive(reports, within: 1000)
  let monitor = process.monitor(started.pid)
  process.kill(pid)
  let assert Ok(_) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(1000)
  let assert Error(otp.StageUnavailable(_)) = otp.push(started.data, [2])
}

pub fn blocked_reporter_does_not_block_stage_commands_test() {
  let entered = process.new_subject()
  let assert Ok(config) =
    otp.config()
    |> otp.with_reporter(fn(event) {
      case event.operation, event.phase {
        "push", "received" -> {
          let gate = process.new_subject()
          process.send(entered, #(gate, event.stage_pid))
          let assert Ok(Nil) = process.receive(gate, within: 2000)
          Nil
        }
        _, _ -> Nil
      }
    })
    |> otp.with_call_timeout(100)
  let assert Ok(started) = otp.start_with_config(config)
  assert otp.push(started.data, [1]) == Ok(Nil)
  let assert Ok(#(gate, stage_pid)) = process.receive(entered, within: 1000)
  assert stage_pid == string.inspect(started.pid)
  assert otp.stop(started.data) == Ok(Nil)
  process.send(gate, Nil)
}

pub fn reporter_panic_does_not_stop_stage_or_later_reports_test() {
  let reports = process.new_subject()
  let assert Ok(started) =
    otp.config()
    |> otp.with_reporter(fn(event) {
      case event.trace_id {
        1 -> panic as "telemetry only"
        _ -> process.send(reports, event)
      }
    })
    |> otp.start_with_config
  assert otp.push(started.data, [1]) == Ok(Nil)
  assert otp.push(started.data, [2]) == Ok(Nil)
  let assert Ok(telemetry.Event(
    trace_id: 2,
    operation: "push",
    phase: "received",
    ..,
  )) = process.receive(reports, within: 1000)
  assert otp.stop(started.data) == Ok(Nil)
}

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
