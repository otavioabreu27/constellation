//// Canonical public values for worker pools.

import constellation/domains/stage_error.{type StageError}
import gleam/otp/actor

/// Stable identity assigned to one worker incarnation.
pub opaque type WorkerId {
  WorkerId(Int)
}

/// Lifecycle and processing events emitted asynchronously by a pool.
pub type Event {
  WorkerStarted(id: WorkerId, slot: Int)
  WorkerStopped(id: WorkerId, slot: Int)
  WorkerReplaced(previous: WorkerId, replacement: WorkerId, slot: Int)
  BatchStarted(id: WorkerId, event_count: Int)
  BatchCompleted(id: WorkerId, event_count: Int)
  PoolStopping
  PoolStopped
}

/// Invalid worker pool settings.
pub type ConfigError {
  InvalidSize(Int)
  InvalidPrefetch(Int)
  InvalidTimeout(Int)
  InvalidBufferCapacity(Int)
}

/// Errors that can prevent a pool from starting.
pub type StartError {
  InvalidConfig(ConfigError)
  ActorStart(actor.StartError)
}

/// Runtime errors returned by pool operations.
pub type PoolError {
  PoolUnavailable
  PoolTimeout
  AlreadyStopping
  SourceManaged
  StageProtocol(StageError)
}

/// Current observable pool state.
pub type Snapshot {
  Snapshot(workers: List(WorkerId), buffered_events: Int, stopping: Bool)
}

@internal
pub fn new_worker_id(value: Int) -> WorkerId {
  WorkerId(value)
}

pub fn worker_id_to_int(id: WorkerId) -> Int {
  let WorkerId(value) = id
  value
}
