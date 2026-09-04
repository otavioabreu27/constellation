//// Resilient OTP worker pools backed by Constellation demand.
////
//// The pool owns the Stage protocol and worker lifecycle. Workers request a
//// bounded prefetch window and renew demand only after their handler returns.

import constellation/core
import constellation/domains/command
import constellation/domains/effect
import constellation/domains/stage_error.{type StageError}
import constellation/runtime/otp/client
import constellation/source.{type Source}
import constellation/source/core as source_core
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/erlang/process.{type Monitor, type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string

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

/// Worker pool configuration.
pub opaque type Config(event, worker_state) {
  Config(
    size: Int,
    prefetch: Int,
    timeout: Int,
    initial_state: fn(Int) -> worker_state,
    handle_batch: fn(worker_state, List(event)) -> worker_state,
    reporter: fn(Event) -> Nil,
  )
}

/// A running worker pool capability.
pub opaque type Pool(event, worker_state) {
  Pool(subject: Subject(Message(event, worker_state)), timeout: Int)
}

type WorkerMessage(event) {
  Work(List(event))
  StopWorker
}

type NotifierMessage(message) {
  Notify(message)
  StopNotifier
}

type Worker(event) {
  Worker(
    id: WorkerId,
    slot: Int,
    subscription_id: SubscriptionId,
    participant_id: participant_id.ParticipantId,
    subject: Subject(WorkerMessage(event)),
    pid: Pid,
    monitor: Monitor,
  )
}

type State(event, worker_state) {
  State(
    pool_subject: Subject(Message(event, worker_state)),
    stage: core.StageState(event),
    workers: List(Worker(event)),
    next_worker_id: Int,
    config: Config(event, worker_state),
    reporter: Subject(NotifierMessage(Event)),
    source_state: Option(source_core.State),
    source_notifier: Option(Subject(NotifierMessage(source.Event))),
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

/// Builds a batch-processing worker pool configuration.
pub fn new(
  size size: Int,
  prefetch prefetch: Int,
  initial_state initial_state: fn(Int) -> worker_state,
  handle_batch handle_batch: fn(worker_state, List(event)) -> worker_state,
) -> Config(event, worker_state) {
  Config(
    size:,
    prefetch:,
    timeout: 5000,
    initial_state:,
    handle_batch:,
    reporter: fn(_) { Nil },
  )
}

/// Builds a configuration whose handler processes one event at a time.
pub fn each(
  size size: Int,
  prefetch prefetch: Int,
  initial_state initial_state: fn(Int) -> worker_state,
  handle_event handle_event: fn(worker_state, event) -> worker_state,
) -> Config(event, worker_state) {
  new(size:, prefetch:, initial_state:, handle_batch: fn(state, events) {
    list.fold(events, state, handle_event)
  })
}

/// Sets the timeout used by synchronous pool and source calls.
pub fn with_timeout(
  config: Config(event, worker_state),
  milliseconds: Int,
) -> Config(event, worker_state) {
  Config(..config, timeout: milliseconds)
}

/// Installs a non-blocking lifecycle and telemetry reporter.
pub fn with_reporter(
  config: Config(event, worker_state),
  reporter: fn(Event) -> Nil,
) -> Config(event, worker_state) {
  Config(..config, reporter:)
}

/// Starts a push-driven worker pool.
pub fn start(
  config: Config(event, worker_state),
) -> Result(Pool(event, worker_state), StartError) {
  start_mode(config, None)
  |> result.map(fn(pair) { pair.0 })
}

/// Starts a pool connected to an asynchronous demand source.
///
/// Source callbacks run in a dedicated notifier process and never block the
/// pool's Stage process.
pub fn start_with_source(
  config: Config(event, worker_state),
  notify_source: fn(source.Event) -> Nil,
) -> Result(#(Pool(event, worker_state), Source(event)), StartError) {
  start_mode(config, Some(notify_source))
  |> result.map(fn(pair) {
    let assert Some(source) = pair.1
    #(pair.0, source)
  })
}

/// Pushes events into a push-driven pool.
pub fn push(
  pool: Pool(event, worker_state),
  events: List(event),
) -> Result(Nil, PoolError) {
  call_pool(pool, Push(events, _))
}

/// Returns worker identities and current buffered event count.
pub fn snapshot(
  pool: Pool(event, worker_state),
) -> Result(Snapshot, PoolError) {
  call_pool(pool, SnapshotRequest)
}

/// Gracefully stops workers after already delivered batches finish.
pub fn stop(pool: Pool(event, worker_state)) -> Result(Nil, PoolError) {
  call_pool(pool, Stop)
}

/// Returns the integer representation of a worker identity.
pub fn worker_id_to_int(id: WorkerId) -> Int {
  let WorkerId(value) = id
  value
}

fn call_pool(
  pool: Pool(event, worker_state),
  make_message: fn(Subject(Result(value, PoolError))) ->
    Message(event, worker_state),
) -> Result(value, PoolError) {
  case client.call(pool.subject, pool.timeout, make_message) {
    Ok(result) -> result
    Error(client.Timeout) -> Error(PoolTimeout)
    Error(client.StageUnavailable(_)) -> Error(PoolUnavailable)
  }
}

fn start_mode(
  config: Config(event, worker_state),
  notify_source: Option(fn(source.Event) -> Nil),
) -> Result(#(Pool(event, worker_state), Option(Source(event))), StartError) {
  use _ <- result.try(validate(config))
  let builder =
    actor.new_with_initialiser(config.timeout, fn(pool_subject) {
      initialise(pool_subject, notify_source, config)
    })
    |> actor.on_message(handle_message)
  case actor.start(builder) {
    Error(error) -> Error(ActorStart(error))
    Ok(started) -> {
      process.unlink(started.pid)
      let source =
        started.data.1
        |> option.map(fn(subject) { source.new_source(subject, config.timeout) })
      Ok(#(Pool(started.data.0, config.timeout), source))
    }
  }
}

fn validate(config: Config(event, state)) -> Result(Nil, StartError) {
  case config.size <= 0, config.prefetch <= 0, config.timeout <= 0 {
    True, _, _ -> Error(InvalidConfig(InvalidSize(config.size)))
    _, True, _ -> Error(InvalidConfig(InvalidPrefetch(config.prefetch)))
    _, _, True -> Error(InvalidConfig(InvalidTimeout(config.timeout)))
    False, False, False -> Ok(Nil)
  }
}

fn initialise(
  pool_subject: Subject(Message(event, worker_state)),
  source_callback: Option(fn(source.Event) -> Nil),
  config: Config(event, worker_state),
) -> Result(
  actor.Initialised(
    State(event, worker_state),
    Message(event, worker_state),
    #(
      Subject(Message(event, worker_state)),
      Option(Subject(source.Request(event))),
    ),
  ),
  String,
) {
  use reporter <- result.try(start_notifier(config.reporter))
  use source_notifier <- result.try(case source_callback {
    Some(callback) -> start_notifier(callback) |> result.map(Some)
    None -> Ok(None)
  })
  let source_subject =
    source_callback
    |> option.map(fn(_) { process.new_subject() })
  let state =
    State(
      pool_subject:,
      stage: core.new(),
      workers: [],
      next_worker_id: 1,
      config:,
      reporter:,
      source_state: source_callback |> option.map(fn(_) { source_core.new() }),
      source_notifier:,
      stopping: False,
      shutdown_started: False,
      stop_waiters: [],
    )
  use state <- result.try(start_slots(state, pool_subject, 0, config.size))
  let state = reconcile_source(state)
  let selector =
    process.new_selector()
    |> process.select(pool_subject)
    |> add_source_selector(source_subject)
    |> process.select_monitors(WorkerDown)
  Ok(
    actor.initialised(state)
    |> actor.selecting(selector)
    |> actor.returning(#(pool_subject, source_subject)),
  )
}

fn start_notifier(
  callback: fn(message) -> Nil,
) -> Result(Subject(NotifierMessage(message)), String) {
  case
    actor.start(
      actor.new(Nil)
      |> actor.on_message(fn(state, message) {
        case message {
          Notify(message) -> {
            callback(message)
            actor.continue(state)
          }
          StopNotifier -> actor.stop()
        }
      }),
    )
  {
    Error(error) -> Error(string.inspect(error))
    Ok(started) -> {
      process.unlink(started.pid)
      Ok(started.data)
    }
  }
}

fn add_source_selector(
  selector: process.Selector(Message(event, state)),
  source_subject: Option(Subject(source.Request(event))),
) -> process.Selector(Message(event, state)) {
  case source_subject {
    None -> selector
    Some(subject) -> process.select_map(selector, subject, SourceRequest)
  }
}

fn start_slots(
  state: State(event, worker_state),
  pool_subject: Subject(Message(event, worker_state)),
  slot: Int,
  count: Int,
) -> Result(State(event, worker_state), String) {
  case slot == count {
    True -> Ok(state)
    False -> {
      use state <- result.try(start_worker(state, pool_subject, slot, None))
      start_slots(state, pool_subject, slot + 1, count)
    }
  }
}

fn start_worker(
  state: State(event, worker_state),
  pool_subject: Subject(Message(event, worker_state)),
  slot: Int,
  replacing: Option(WorkerId),
) -> Result(State(event, worker_state), String) {
  let id = WorkerId(state.next_worker_id)
  let worker_state = state.config.initial_state(slot)
  let reporter = state.reporter
  let handler = state.config.handle_batch
  let worker_builder =
    actor.new(worker_state)
    |> actor.on_message(fn(worker_state, message) {
      case message {
        Work(events) -> {
          process.send(reporter, Notify(BatchStarted(id, list.length(events))))
          let worker_state = handler(worker_state, events)
          process.send(
            reporter,
            Notify(BatchCompleted(id, list.length(events))),
          )
          process.send(pool_subject, WorkerCompleted(id, list.length(events)))
          actor.continue(worker_state)
        }
        StopWorker -> {
          process.send(pool_subject, WorkerExited(id))
          actor.stop()
        }
      }
    })
  use started <- result.try(
    actor.start(worker_builder)
    |> result.map_error(string.inspect),
  )
  process.unlink(started.pid)
  let monitor = process.monitor(started.pid)
  let id_string = int.to_string(state.next_worker_id)
  let assert Ok(subscription) = subscription_id.new("pool-worker-" <> id_string)
  let participant = participant_id.new("pool-worker-" <> id_string)
  let worker =
    Worker(
      id:,
      slot:,
      subscription_id: subscription,
      participant_id: participant,
      subject: started.data,
      pid: started.pid,
      monitor:,
    )
  let state =
    State(
      ..state,
      workers: list.append(state.workers, [worker]),
      next_worker_id: state.next_worker_id + 1,
    )
  let assert Ok(state) =
    apply_stage(state, command.Subscribe(subscription, participant, 0))
  let assert Ok(state) =
    apply_stage(state, command.Ask(subscription, state.config.prefetch))
  process.send(state.reporter, Notify(WorkerStarted(id, slot)))
  case replacing {
    Some(previous) ->
      process.send(state.reporter, Notify(WorkerReplaced(previous, id, slot)))
    None -> Nil
  }
  Ok(state)
}

fn handle_message(
  state: State(event, worker_state),
  message: Message(event, worker_state),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case message {
    Push(events, reply) -> handle_push(state, events, reply)
    SnapshotRequest(reply) -> {
      process.send(
        reply,
        Ok(Snapshot(
          workers: list.map(state.workers, fn(worker) { worker.id }),
          buffered_events: core.buffer_size(state.stage),
          stopping: state.stopping,
        )),
      )
      actor.continue(state)
    }
    Stop(reply) -> handle_stop(state, reply)
    WorkerCompleted(id, count) -> handle_completed(state, id, count)
    WorkerExited(id) -> handle_worker_exit(state, id)
    WorkerDown(down) -> handle_worker_down(state, down)
    SourceRequest(request) -> handle_source_request(state, request)
  }
}

fn handle_push(
  state: State(event, worker_state),
  events: List(event),
  reply: Subject(Result(Nil, PoolError)),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case state.stopping, state.source_state {
    True, _ -> reply_and_continue(state, reply, Error(AlreadyStopping))
    False, Some(_) -> reply_and_continue(state, reply, Error(SourceManaged))
    False, None ->
      case apply_stage(state, command.Push(events)) {
        Error(error) ->
          reply_and_continue(state, reply, Error(StageProtocol(error)))
        Ok(state) -> reply_and_continue(state, reply, Ok(Nil))
      }
  }
}

fn handle_completed(
  state: State(event, worker_state),
  id: WorkerId,
  count: Int,
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case state.shutdown_started, find_worker(state.workers, id) {
    True, _ | _, Error(_) -> actor.continue(state)
    False, Ok(worker) -> {
      let result =
        apply_stage(state, command.Ask(worker.subscription_id, count))
      let assert Ok(state) = result
      case state.stopping && core.buffer_size(state.stage) == 0 {
        True -> begin_shutdown(state)
        False -> actor.continue(reconcile_source(state))
      }
    }
  }
}

fn handle_source_request(
  state: State(event, worker_state),
  request: source.Request(event),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case request, state.source_state, state.stopping {
    source.Supply(_, _, _, reply), _, True ->
      reply_and_continue(state, reply, Error(source.SourceUnavailable))
    source.Supply(_, _, _, reply), None, _ ->
      reply_and_continue(state, reply, Error(source.SourceUnavailable))
    source.Supply(grant, offset, events, reply), Some(source_state), False -> {
      let supplied =
        source_core.supply(
          source_state,
          source.grant_id(grant),
          offset,
          list.length(events),
        )
      case supplied {
        Error(error) ->
          reply_and_continue(state, reply, Error(source.SupplyProtocol(error)))
        Ok(#(source_state, source.Duplicate as result)) -> {
          process.send(reply, Ok(result))
          actor.continue(State(..state, source_state: Some(source_state)))
        }
        Ok(#(source_state, result)) -> {
          let state = State(..state, source_state: Some(source_state))
          case apply_stage(state, command.Push(events)) {
            Error(_) ->
              reply_and_continue(state, reply, Error(source.SourceUnavailable))
            Ok(state) -> {
              process.send(reply, Ok(result))
              actor.continue(reconcile_source(state))
            }
          }
        }
      }
    }
    source.SetAvailable(available), Some(source_state), False -> {
      let capacity = source_capacity(state.stage)
      let #(source_state, actions) = case available {
        True -> source_core.available(source_state, capacity)
        False -> source_core.unavailable(source_state)
      }
      let state = State(..state, source_state: Some(source_state))
      notify_source_actions(state, actions)
      actor.continue(state)
    }
    source.SetAvailable(_), _, _ -> actor.continue(state)
  }
}

fn handle_stop(
  state: State(event, worker_state),
  reply: Subject(Result(Nil, PoolError)),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case state.stopping {
    True -> reply_and_continue(state, reply, Error(AlreadyStopping))
    False -> {
      process.send(state.reporter, Notify(PoolStopping))
      let state =
        shutdown_source(
          State(..state, stopping: True, stop_waiters: [
            reply,
            ..state.stop_waiters
          ]),
        )
      case core.buffer_size(state.stage) == 0 {
        True -> begin_shutdown(state)
        False -> actor.continue(state)
      }
    }
  }
}

fn handle_worker_exit(
  state: State(event, worker_state),
  id: WorkerId,
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case take_worker_by_id(state.workers, id, []) {
    Error(_) -> actor.continue(state)
    Ok(#(worker, workers)) -> {
      process.demonitor_process(worker.monitor)
      process.send(
        state.reporter,
        Notify(WorkerStopped(worker.id, worker.slot)),
      )
      finish_if_stopped(State(..state, workers:))
    }
  }
}

fn handle_worker_down(
  state: State(event, worker_state),
  down: process.Down,
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case down {
    process.PortDown(..) -> actor.continue(state)
    process.ProcessDown(pid: pid, ..) ->
      case take_worker_by_pid(state.workers, pid, []) {
        Error(_) -> actor.continue(state)
        Ok(#(worker, workers)) -> {
          process.send(
            state.reporter,
            Notify(WorkerStopped(worker.id, worker.slot)),
          )
          let state = State(..state, workers:)
          case state.shutdown_started {
            True -> finish_if_stopped(state)
            False -> {
              let assert Ok(state) =
                apply_stage(
                  state,
                  command.ParticipantDown(worker.participant_id),
                )
              case
                start_worker(
                  state,
                  state.pool_subject,
                  worker.slot,
                  Some(worker.id),
                )
              {
                Ok(state) ->
                  case state.stopping && core.buffer_size(state.stage) == 0 {
                    True -> begin_shutdown(state)
                    False -> actor.continue(reconcile_source(state))
                  }
                Error(reason) -> actor.stop_abnormal(reason)
              }
            }
          }
        }
      }
  }
}

fn begin_shutdown(
  state: State(event, worker_state),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  let assert Ok(state) = apply_stage(state, command.Shutdown)
  actor.continue(State(..state, shutdown_started: True))
}

fn apply_stage(
  state: State(event, worker_state),
  command: command.Command(event),
) -> Result(State(event, worker_state), StageError) {
  use updated <- result.try(core.update(state.stage, command))
  let #(stage, effects) = updated
  let state = State(..state, stage:)
  run_effects(state, effects)
  Ok(state)
}

fn run_effects(
  state: State(event, worker_state),
  effects: List(effect.Effect(event)),
) {
  list.each(effects, fn(stage_effect) {
    case stage_effect {
      effect.SendEvents(subscription_id, events) ->
        case find_worker_by_subscription(state.workers, subscription_id) {
          Ok(worker) -> process.send(worker.subject, Work(events))
          Error(_) -> Nil
        }
      effect.NotifyCancelled(subscription_id) ->
        case find_worker_by_subscription(state.workers, subscription_id) {
          Ok(worker) -> process.send(worker.subject, StopWorker)
          Error(_) -> Nil
        }
    }
  })
}

fn reconcile_source(
  state: State(event, worker_state),
) -> State(event, worker_state) {
  case state.source_state {
    None -> state
    Some(source_state) -> {
      let #(source_state, actions) =
        source_core.reconcile(source_state, source_capacity(state.stage))
      let state = State(..state, source_state: Some(source_state))
      notify_source_actions(state, actions)
      state
    }
  }
}

fn source_capacity(stage: core.StageState(event)) -> Int {
  int.max(0, core.available_demand(stage) - core.buffer_size(stage))
}

fn shutdown_source(
  state: State(event, worker_state),
) -> State(event, worker_state) {
  case state.source_state {
    None -> state
    Some(source_state) -> {
      let #(source_state, actions) = source_core.shutdown(source_state)
      let state = State(..state, source_state: Some(source_state))
      notify_source_actions(state, actions)
      state
    }
  }
}

fn notify_source_actions(
  state: State(event, worker_state),
  actions: List(source_core.Action),
) -> Nil {
  case state.source_notifier {
    None -> Nil
    Some(notifier) ->
      list.each(actions, fn(action) {
        let event = case action {
          source_core.GrantCapacity(id, amount) ->
            source.DemandGranted(source.new_grant(id, amount))
          source_core.RevokeGrant(id, amount) ->
            source.GrantRevoked(source.new_grant(id, amount))
          source_core.StopSource -> source.SourceStopped
        }
        process.send(notifier, Notify(event))
      })
  }
}

fn finish_if_stopped(
  state: State(event, worker_state),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case state.stopping && list.is_empty(state.workers) {
    False -> actor.continue(state)
    True -> {
      process.send(state.reporter, Notify(PoolStopped))
      process.send(state.reporter, StopNotifier)
      case state.source_notifier {
        Some(notifier) -> process.send(notifier, StopNotifier)
        None -> Nil
      }
      list.each(state.stop_waiters, fn(reply) { process.send(reply, Ok(Nil)) })
      actor.stop()
    }
  }
}

fn reply_and_continue(
  state: State(event, worker_state),
  reply: Subject(reply),
  value: reply,
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  process.send(reply, value)
  actor.continue(state)
}

fn find_worker(
  workers: List(Worker(event)),
  id: WorkerId,
) -> Result(Worker(event), Nil) {
  list.find(workers, fn(worker) { worker.id == id })
}

fn find_worker_by_subscription(
  workers: List(Worker(event)),
  id: SubscriptionId,
) -> Result(Worker(event), Nil) {
  list.find(workers, fn(worker) { worker.subscription_id == id })
}

fn take_worker_by_id(
  workers: List(Worker(event)),
  id: WorkerId,
  before: List(Worker(event)),
) -> Result(#(Worker(event), List(Worker(event))), Nil) {
  case workers {
    [] -> Error(Nil)
    [worker, ..rest] ->
      case worker.id == id {
        True -> Ok(#(worker, list.append(list.reverse(before), rest)))
        False -> take_worker_by_id(rest, id, [worker, ..before])
      }
  }
}

fn take_worker_by_pid(
  workers: List(Worker(event)),
  pid: Pid,
  before: List(Worker(event)),
) -> Result(#(Worker(event), List(Worker(event))), Nil) {
  case workers {
    [] -> Error(Nil)
    [worker, ..rest] ->
      case worker.pid == pid {
        True -> Ok(#(worker, list.append(list.reverse(before), rest)))
        False -> take_worker_by_pid(rest, pid, [worker, ..before])
      }
  }
}
