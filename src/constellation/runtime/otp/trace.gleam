import constellation/runtime/otp/notifier.{type Notifier}
import constellation/runtime/otp/telemetry
import gleam/erlang/process.{type Subject}
import gleam/option.{type Option, None, Some}
import gleam/string

@internal
pub fn subject_owner(subject: Subject(message)) -> String {
  case process.subject_owner(subject) {
    Ok(pid) -> string.inspect(pid)
    Error(_) -> "unknown"
  }
}

@internal
pub fn log(
  reporter: Option(Notifier(telemetry.Event)),
  trace_id: Int,
  operation: String,
  phase: String,
  detail: String,
) -> Nil {
  case reporter {
    None -> Nil
    Some(report) ->
      notifier.send(
        report,
        telemetry.Event(
          trace_id: trace_id,
          stage_pid: string.inspect(process.self()),
          operation: operation,
          phase: phase,
          detail: detail,
        ),
      )
  }
}
