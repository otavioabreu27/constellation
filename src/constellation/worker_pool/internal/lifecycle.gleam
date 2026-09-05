//// Pool process startup, worker replacement and graceful shutdown.

import constellation/core
import constellation/domains/command
import constellation/domains/dispatcher
import constellation/runtime/otp/notifier
import constellation/source.{type Source}
import constellation/source/core as source_core
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import constellation/worker_pool/internal/config
import constellation/worker_pool/internal/dispatch
import constellation/worker_pool/internal/model.{
  type Config, type Message, type Pool, type State, Pool, SourceRequest, State,
  WorkerCompleted, WorkerDown, WorkerExited,
}
import constellation/worker_pool/internal/worker
import constellation/worker_pool/types.{
  type PoolError, type StartError, type WorkerId, ActorStart, AlreadyStopping,
  BatchCompleted, BatchStarted, InvalidConfig, PoolStopped, PoolStopping,
  WorkerReplaced, WorkerStarted, WorkerStopped,
}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/erlang/reference.{type Reference}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string

pub fn start(
  config: Config(event, worker_state),
  notify_source: Option(fn(source.Event) -> Nil),
  linked: Bool,
  name: process.Name(Message(event, worker_state)),
  on_message: fn(State(event, worker_state), Message(event, worker_state)) ->
    actor.Next(State(event, worker_state), Message(event, worker_state)),
) -> Result(
  actor.Started(#(Pool(event, worker_state), Option(Source(event)))),
  StartError,
) {
  use _ <- result.try(
    config.validate(config) |> result.map_error(InvalidConfig),
  )
  let source_id = reference.new()
  let builder =
    actor.new_with_initialiser(config.timeout, fn(pool_subject) {
      initialise(pool_subject, source_id, notify_source, config)
    })
    |> actor.named(name)
    |> actor.on_message(on_message)
  case actor.start(builder) {
    Error(error) -> Error(ActorStart(error))
    Ok(started) -> {
      case linked {
        True -> Nil
        False -> process.unlink(started.pid)
      }
      let source =
        started.data.1
        |> option.map(fn(subject) {
          source.new_source(source_id, subject, config.timeout)
        })
      Ok(
        actor.Started(pid: started.pid, data: #(
          Pool(started.data.0, config.timeout),
          source,
        )),
      )
    }
  }
}

fn initialise(
  public_subject: Subject(Message(event, worker_state)),
  source_id: Reference,
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
  let pool_subject = process.new_subject()
  use reporter <- result.try(
    notifier.start(config.reporter, notifier.Isolate)
    |> result.map_error(string.inspect),
  )
  use source_notifier <- result.try(case source_callback {
    Some(callback) ->
      notifier.start(callback, notifier.Propagate)
      |> result.map_error(string.inspect)
      |> result.map(Some)
    None -> Ok(None)
  })
  let source_subject =
    source_callback
    |> option.map(fn(_) { process.new_subject() })
  let state =
    State(
      pool_subject:,
      source_id:,
      stage: core.new_configured(
        dispatcher.demand_strategy(),
        config.buffer_capacity,
      ),
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
  let state = dispatch.reconcile_source(state)
  let selector =
    process.new_selector()
    |> process.select(pool_subject)
    |> process.select(public_subject)
    |> add_source_selector(source_subject)
    |> process.select_monitors(WorkerDown)
  Ok(
    actor.initialised(state)
    |> actor.selecting(selector)
    |> actor.returning(#(public_subject, source_subject)),
  )
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
  let id = types.new_worker_id(state.next_worker_id)
  let reporter = state.reporter
  let handler = state.config.handle_batch
  let initial_state = state.config.initial_state
  use started <- result.try(
    worker.start(
      process.self(),
      state.config.timeout,
      fn() { initial_state(slot) },
      fn(worker_state, events) {
        notifier.send(reporter, BatchStarted(id, list.length(events)))
        let worker_state = handler(worker_state, events)
        notifier.send(reporter, BatchCompleted(id, list.length(events)))
        process.send(pool_subject, WorkerCompleted(id, list.length(events)))
        worker_state
      },
      fn() { process.send(pool_subject, WorkerExited(id)) },
    )
    |> result.map_error(string.inspect),
  )
  let monitor = process.monitor(started.pid)
  let id_string = int.to_string(state.next_worker_id)
  let assert Ok(subscription) = subscription_id.new("pool-worker-" <> id_string)
  let participant = participant_id.new("pool-worker-" <> id_string)
  let worker =
    worker.Worker(
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
    dispatch.apply_stage(state, command.Subscribe(subscription, participant, 0))
  let assert Ok(state) =
    dispatch.apply_stage(
      state,
      command.Ask(subscription, state.config.prefetch),
    )
  notifier.send(state.reporter, WorkerStarted(id, slot))
  case replacing {
    Some(previous) ->
      notifier.send(state.reporter, WorkerReplaced(previous, id, slot))
    None -> Nil
  }
  Ok(state)
}

pub fn handle_stop(
  state: State(event, worker_state),
  reply: Subject(Result(Nil, PoolError)),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case state.stopping {
    True -> {
      process.send(reply, Error(AlreadyStopping))
      actor.continue(state)
    }
    False -> {
      notifier.send(state.reporter, PoolStopping)
      let state =
        dispatch.shutdown_source(
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

pub fn handle_worker_exit(
  state: State(event, worker_state),
  id: WorkerId,
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case worker.take_id(state.workers, id) {
    Error(_) -> actor.continue(state)
    Ok(#(worker, workers)) -> {
      process.demonitor_process(worker.monitor)
      notifier.send(state.reporter, WorkerStopped(worker.id, worker.slot))
      finish_if_stopped(State(..state, workers:))
    }
  }
}

pub fn handle_worker_down(
  state: State(event, worker_state),
  down: process.Down,
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case down {
    process.PortDown(..) -> actor.continue(state)
    process.ProcessDown(pid: pid, ..) ->
      case is_source_notifier(state, pid), notifier.pid(state.reporter) == pid {
        True, _ -> actor.stop_abnormal("asynchronous source callback stopped")
        _, True -> actor.stop_abnormal("telemetry notifier stopped")
        False, False ->
          case worker.take_pid(state.workers, pid) {
            Error(_) -> actor.continue(state)
            Ok(#(worker, workers)) -> {
              notifier.send(
                state.reporter,
                WorkerStopped(worker.id, worker.slot),
              )
              let state = State(..state, workers:)
              case state.shutdown_started {
                True -> finish_if_stopped(state)
                False -> {
                  let assert Ok(state) =
                    dispatch.apply_stage(
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
                      case
                        state.stopping && core.buffer_size(state.stage) == 0
                      {
                        True -> begin_shutdown(state)
                        False ->
                          actor.continue(dispatch.reconcile_source(state))
                      }
                    Error(reason) -> actor.stop_abnormal(reason)
                  }
                }
              }
            }
          }
      }
  }
}

fn is_source_notifier(state: State(event, worker_state), pid: Pid) -> Bool {
  case state.source_notifier {
    Some(notifier) -> notifier.pid(notifier) == pid
    None -> False
  }
}

pub fn begin_shutdown(
  state: State(event, worker_state),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  let assert Ok(state) = dispatch.apply_stage(state, command.Shutdown)
  actor.continue(State(..state, shutdown_started: True))
}

fn finish_if_stopped(
  state: State(event, worker_state),
) -> actor.Next(State(event, worker_state), Message(event, worker_state)) {
  case state.stopping && list.is_empty(state.workers) {
    False -> actor.continue(state)
    True -> {
      notifier.send(state.reporter, PoolStopped)
      notifier.stop(state.reporter)
      case state.source_notifier {
        Some(notifier) -> notifier.stop(notifier)
        None -> Nil
      }
      list.each(state.stop_waiters, fn(reply) { process.send(reply, Ok(Nil)) })
      actor.stop()
    }
  }
}
