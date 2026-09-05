//// Asynchronous notifications with explicit callback failure policy.
//// Telemetry is best-effort; source protocol callbacks are fail-fast.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/otp/actor

pub type FailurePolicy {
  Isolate
  Propagate
}

type Message(message) {
  Notify(message)
  Stop
  OwnerDown(process.Down)
}

pub opaque type Notifier(message) {
  Notifier(subject: Subject(Message(message)), pid: Pid)
}

pub fn pid(notifier: Notifier(message)) -> Pid {
  notifier.pid
}

pub fn start(
  callback: fn(message) -> Nil,
  policy: FailurePolicy,
) -> Result(Notifier(message), actor.StartError) {
  let owner = process.self()
  let builder =
    actor.new_with_initialiser(1000, fn(subject) {
      let monitor = process.monitor(owner)
      Ok(
        actor.initialised(Nil)
        |> actor.selecting(
          process.new_selector()
          |> process.select(subject)
          |> process.select_specific_monitor(monitor, OwnerDown),
        )
        |> actor.returning(subject),
      )
    })
    |> actor.on_message(fn(state, message) {
      case message {
        Notify(value) -> {
          case policy {
            Isolate -> run_safely(fn() { callback(value) })
            Propagate -> callback(value)
          }
          actor.continue(state)
        }
        Stop | OwnerDown(_) -> actor.stop()
      }
    })
  case actor.start(builder) {
    Error(error) -> Error(error)
    Ok(started) -> {
      process.unlink(started.pid)
      process.monitor(started.pid)
      Ok(Notifier(started.data, started.pid))
    }
  }
}

pub fn send(notifier: Notifier(message), message: message) -> Nil {
  process.send(notifier.subject, Notify(message))
}

pub fn stop(notifier: Notifier(message)) -> Nil {
  process.send(notifier.subject, Stop)
}

@external(erlang, "constellation_notifier_ffi", "run_safely")
fn run_safely(callback: fn() -> Nil) -> Nil
