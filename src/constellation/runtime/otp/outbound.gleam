import constellation/runtime
import constellation/runtime/otp/telemetry
import constellation/runtime/otp/trace
import constellation/value_objects/subscription_id
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option}

@internal
pub fn execute(
  outbound: List(
    runtime.Outbound(event, Subject(runtime.ParticipantMessage(event))),
  ),
  reporter: Option(fn(telemetry.Event) -> Nil),
  trace_id: Int,
) -> Nil {
  case outbound {
    [] -> Nil
    [runtime.Deliver(to: recipient, message: message), ..rest] -> {
      process.send(recipient, message)
      case message {
        runtime.Events(subscription, events) ->
          trace.log(
            reporter,
            trace_id,
            "deliver",
            "mailbox_enqueued",
            "consumer_pid="
              <> trace.subject_owner(recipient)
              <> " subscription="
              <> subscription_id.to_string(subscription)
              <> " events="
              <> int.to_string(list.length(events)),
          )
        runtime.Cancelled(subscription) ->
          trace.log(
            reporter,
            trace_id,
            "cancel",
            "mailbox_enqueued",
            "consumer_pid="
              <> trace.subject_owner(recipient)
              <> " subscription="
              <> subscription_id.to_string(subscription),
          )
      }
      execute(rest, reporter, trace_id)
    }
  }
}

@internal
pub fn event_count(
  outbound: List(
    runtime.Outbound(event, Subject(runtime.ParticipantMessage(event))),
  ),
) -> Int {
  list.fold(outbound, 0, fn(total, item) {
    case item {
      runtime.Deliver(message: runtime.Events(events: events, ..), ..) ->
        total + list.length(events)
      runtime.Deliver(message: runtime.Cancelled(..), ..) -> total
    }
  })
}
