//// Resilient OTP worker pools backed by Constellation demand.
//// Workers renew demand only after the handler returns. Delivered batches are
//// at-most-once: restart never replays them. Constructors live in worker_pool/types.

import constellation/runtime/otp/client
import constellation/source.{type Source}
import constellation/worker_pool/internal/config
import constellation/worker_pool/internal/model
import constellation/worker_pool/internal/server
import constellation/worker_pool/types
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision.{type ChildSpecification}
import gleam/result
import gleam/string

pub type WorkerId =
  types.WorkerId

pub type Event =
  types.Event

pub type ConfigError =
  types.ConfigError

pub type StartError =
  types.StartError

pub type PoolError =
  types.PoolError

pub type Snapshot =
  types.Snapshot

pub opaque type Config(event, state) {
  Config(model.Config(event, state))
}

pub opaque type Pool(event, state) {
  Pool(model.Pool(event, state))
}

/// Builds a batch-processing worker pool configuration.
pub fn new(
  size size: Int,
  prefetch prefetch: Int,
  initial_state initial_state: fn(Int) -> state,
  handle_batch handle_batch: fn(state, List(event)) -> state,
) -> Config(event, state) {
  Config(
    model.Config(
      size:,
      prefetch:,
      timeout: 5000,
      buffer_capacity: None,
      initial_state:,
      handle_batch:,
      reporter: fn(_) { Nil },
    ),
  )
}

/// Processes each event in a delivered batch, in order.
pub fn each(
  size size: Int,
  prefetch prefetch: Int,
  initial_state initial_state: fn(Int) -> state,
  handle_event handle_event: fn(state, event) -> state,
) -> Config(event, state) {
  new(size:, prefetch:, initial_state:, handle_batch: fn(state, events) {
    list.fold(events, state, handle_event)
  })
}

/// Sets synchronous call and supervisor shutdown timeouts.
pub fn with_timeout(
  config: Config(event, state),
  milliseconds: Int,
) -> Config(event, state) {
  let Config(inner) = config
  Config(model.Config(..inner, timeout: milliseconds))
}

/// Bounds undelivered events, excluding work already delivered to workers.
pub fn with_buffer_capacity(
  config: Config(event, state),
  capacity: Int,
) -> Config(event, state) {
  let Config(inner) = config
  Config(model.Config(..inner, buffer_capacity: Some(capacity)))
}

/// Asynchronous best-effort telemetry; a callback panic drops only that event.
/// Keep callbacks inexpensive: the notifier mailbox is not bounded.
pub fn with_reporter(
  config: Config(event, state),
  reporter: fn(Event) -> Nil,
) -> Config(event, state) {
  let Config(inner) = config
  Config(model.Config(..inner, reporter:))
}

/// Starts an unlinked push-driven pool.
pub fn start(
  config: Config(event, state),
) -> Result(Pool(event, state), StartError) {
  start_mode(config, None, False, process.new_name("constellation-pool"))
  |> result.map(fn(started) { Pool(started.data.0) })
}

/// Starts a pool linked to its calling owner.
pub fn start_link(
  config: Config(event, state),
) -> Result(Pool(event, state), StartError) {
  start_mode(config, None, True, process.new_name("constellation-pool"))
  |> result.map(fn(started) { Pool(started.data.0) })
}

/// Returns a child specification whose handle resolves the restarted pool.
/// In-flight work is not replayed; calls during restart may be unavailable.
pub fn supervised(
  config: Config(event, state),
) -> Result(ChildSpecification(Pool(event, state)), ConfigError) {
  let Config(inner) = config
  use _ <- result.try(config.validate(inner))
  let name = process.new_name("constellation-pool")
  Ok(
    supervision.worker(fn() {
      case start_mode(config, None, True, name) {
        Ok(started) -> Ok(actor.Started(started.pid, Pool(started.data.0)))
        Error(types.ActorStart(error)) -> Error(error)
        Error(types.InvalidConfig(error)) ->
          Error(actor.InitFailed(string.inspect(error)))
      }
    })
    |> supervision.timeout(inner.timeout),
  )
}

/// Source callbacks are asynchronous but fail-fast: a callback crash stops
/// the pool rather than silently losing capacity notifications.
pub fn start_with_source(
  config: Config(event, state),
  notify_source: fn(source.Event) -> Nil,
) -> Result(#(Pool(event, state), Source(event)), StartError) {
  start_mode(
    config,
    Some(notify_source),
    False,
    process.new_name("constellation-pool"),
  )
  |> result.map(source_started)
}

/// Source-backed pool linked to an owner that coordinates the source lifetime.
pub fn start_with_source_link(
  config: Config(event, state),
  notify_source: fn(source.Event) -> Nil,
) -> Result(#(Pool(event, state), Source(event)), StartError) {
  start_mode(
    config,
    Some(notify_source),
    True,
    process.new_name("constellation-pool"),
  )
  |> result.map(source_started)
}

fn source_started(
  started: actor.Started(#(model.Pool(event, state), Option(Source(event)))),
) {
  let assert Some(source) = started.data.1
  #(Pool(started.data.0), source)
}

fn start_mode(config: Config(event, state), source, linked, name) {
  let Config(inner) = config
  server.start(inner, source, linked, name)
}

/// Pushes one atomic batch. A timeout does not imply the push was rejected.
pub fn push(
  pool: Pool(event, state),
  events: List(event),
) -> Result(Nil, PoolError) {
  call_pool(pool, model.Push(events, _))
}

pub fn snapshot(pool: Pool(event, state)) -> Result(Snapshot, PoolError) {
  call_pool(pool, model.SnapshotRequest)
}

/// Drains accepted work. Timeout does not cancel a running handler.
pub fn stop(pool: Pool(event, state)) -> Result(Nil, PoolError) {
  call_pool(pool, model.Stop)
}

pub fn worker_id_to_int(id: WorkerId) -> Int {
  types.worker_id_to_int(id)
}

fn call_pool(pool: Pool(event, state), make_message) {
  let Pool(inner) = pool
  case client.call(inner.subject, inner.timeout, make_message) {
    Ok(result) -> result
    Error(client.Timeout) -> Error(types.PoolTimeout)
    Error(client.StageUnavailable(_)) -> Error(types.PoolUnavailable)
  }
}
