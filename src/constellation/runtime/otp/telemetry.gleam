import gleam/int
import logging

/// A structured observation emitted by an OTP stage.
pub type Event {
  Event(
    trace_id: Int,
    stage_pid: String,
    operation: String,
    phase: String,
    detail: String,
  )
}

/// Writes a structured event through the standard Erlang logger.
pub fn log(event: Event) -> Nil {
  logging.log(logging.Info, format(event))
}

/// Formats an event using the default key-value representation.
pub fn format(event: Event) -> String {
  let suffix = case event.detail {
    "" -> ""
    detail -> " " <> detail
  }
  "constellation trace_id="
  <> int.to_string(event.trace_id)
  <> " stage_pid="
  <> event.stage_pid
  <> " operation="
  <> event.operation
  <> " phase="
  <> event.phase
  <> suffix
}
