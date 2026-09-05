//// Internal pool configuration and lifecycle state.

import constellation/core
import constellation/runtime/otp/notifier.{type Notifier}
import constellation/source
import constellation/source/core as source_core
import constellation/worker_pool/internal/worker
import constellation/worker_pool/types.{
  type Event, type PoolError, type Snapshot, type WorkerId,
}
import gleam/erlang/process.{type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/option.{type Option}

/// Worker pool configuration.
pub type Config(event, worker_state) {
  Config(
    size: Int,
    prefetch: Int,
    timeout: Int,
    buffer_capacity: Option(Int),
    initial_state: fn(Int) -> worker_state,
    handle_batch: fn(worker_state, List(event)) -> worker_state,
    reporter: fn(Event) -> Nil,
  )
}

pub type Pool(event, state) {
  Pool(subject: Subject(Message(event, state)), timeout: Int)
}

pub type Worker(event) =
  worker.Worker(WorkerId, event)

pub type State(event, worker_state) {
  State(
    pool_subject: Subject(Message(event, worker_state)),
    source_id: Reference,
    stage: core.StageState(event),
    workers: List(Worker(event)),
    next_worker_id: Int,
    config: Config(event, worker_state),
    reporter: Notifier(Event),
    source_state: Option(source_core.State),
    source_notifier: Option(Notifier(source.Event)),
    stopping: Bool,
    shutdown_started: Bool,
    stop_waiters: List(Subject(Result(Nil, PoolError))),
  )
}

@internal
pub type Message(event, worker_state) {
  Push(List(event), Subject(Result(Nil, PoolError)))
  SnapshotRequest(Subject(Result(Snapshot, PoolError)))
  Stop(Subject(Result(Nil, PoolError)))
  WorkerCompleted(WorkerId, Int)
  WorkerExited(WorkerId)
  SourceRequest(source.Request(event))
  WorkerDown(process.Down)
}
