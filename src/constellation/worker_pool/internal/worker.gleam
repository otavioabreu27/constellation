//// One worker incarnation and its registry operations. No pool protocol.

import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/erlang/process.{type Monitor, type Pid, type Subject}
import gleam/list
import gleam/otp/actor

pub type Message(event) {
  Work(List(event))
  StopWorker
  OwnerDown(process.Down)
}

pub type Worker(id, event) {
  Worker(
    id: id,
    slot: Int,
    subscription_id: SubscriptionId,
    participant_id: participant_id.ParticipantId,
    subject: Subject(Message(event)),
    pid: Pid,
    monitor: Monitor,
  )
}

/// The callback owns execution and completion reporting. The worker owns only
/// mailbox processing and lifetime; owner termination is observed between batches.
pub fn start(
  owner: Pid,
  timeout: Int,
  initialise: fn() -> state,
  handle: fn(state, List(event)) -> state,
  exited: fn() -> Nil,
) -> actor.StartResult(Subject(Message(event))) {
  let builder =
    actor.new_with_initialiser(timeout, fn(subject) {
      let monitor = process.monitor(owner)
      Ok(
        actor.initialised(initialise())
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
        Work(events) -> actor.continue(handle(state, events))
        StopWorker -> {
          exited()
          actor.stop()
        }
        OwnerDown(_) -> actor.stop()
      }
    })
  case actor.start(builder) {
    Ok(started) -> {
      process.unlink(started.pid)
      Ok(started)
    }
    Error(error) -> Error(error)
  }
}

pub fn find(workers: List(Worker(id, event)), id: id) {
  list.find(workers, fn(worker) { worker.id == id })
}

pub fn by_subscription(workers: List(Worker(id, event)), id: SubscriptionId) {
  list.find(workers, fn(worker) { worker.subscription_id == id })
}

pub fn take_id(workers: List(Worker(id, event)), id: id) {
  take(workers, fn(worker) { worker.id == id }, [])
}

pub fn take_pid(workers: List(Worker(id, event)), pid: Pid) {
  take(workers, fn(worker) { worker.pid == pid }, [])
}

fn take(
  workers: List(Worker(id, event)),
  matches: fn(Worker(id, event)) -> Bool,
  before: List(Worker(id, event)),
) {
  case workers {
    [] -> Error(Nil)
    [worker, ..rest] ->
      case matches(worker) {
        True -> Ok(#(worker, list.append(list.reverse(before), rest)))
        False -> take(rest, matches, [worker, ..before])
      }
  }
}
