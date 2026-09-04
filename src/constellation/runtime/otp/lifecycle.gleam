import constellation/runtime
import constellation/runtime/otp/model
import constellation/runtime/otp/outbound
import constellation/runtime/otp/participant_monitors
import constellation/runtime/otp/trace
import constellation/value_objects/participant_id
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/string

@internal
pub fn participant_down(
  state: model.State(event),
  down: process.Down,
) -> actor.Next(model.State(event), model.Message(event)) {
  case down {
    process.PortDown(..) -> actor.continue(state)
    process.ProcessDown(monitor, pid, reason) ->
      case participant_monitors.take_down(state.monitors, monitor) {
        Error(_) -> actor.continue(state)
        Ok(#(monitors, participant)) -> {
          log(
            state,
            "participant_down",
            "received",
            "participant="
              <> participant_id.to_string(participant)
              <> " consumer_pid="
              <> string.inspect(pid)
              <> " reason="
              <> string.inspect(reason),
          )
          let current = model.State(..state, monitors: monitors)
          case runtime.participant_down(state.runtime, participant) {
            Error(error) -> {
              log(
                current,
                "participant_down",
                "rejected",
                "error=" <> string.inspect(error),
              )
              actor.continue(increment_trace(current))
            }
            Ok(#(updated, deliveries)) -> {
              log(
                current,
                "participant_down",
                "cancelled",
                "subscriptions=" <> int.to_string(list.length(deliveries)),
              )
              outbound.execute(
                deliveries,
                current.reporter,
                current.next_trace_id,
              )
              actor.continue(
                model.State(
                  ..current,
                  runtime: updated,
                  next_trace_id: current.next_trace_id + 1,
                ),
              )
            }
          }
        }
      }
  }
}

fn increment_trace(state: model.State(event)) -> model.State(event) {
  model.State(..state, next_trace_id: state.next_trace_id + 1)
}

fn log(
  state: model.State(event),
  operation: String,
  phase: String,
  detail: String,
) -> Nil {
  trace.log(state.reporter, state.next_trace_id, operation, phase, detail)
}
