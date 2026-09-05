//// Pool mailbox coordinator. Lifecycle and protocol transitions are delegated.

import constellation/core
import constellation/domains/command
import constellation/source
import constellation/source/core as source_core
import constellation/worker_pool/internal/dispatch
import constellation/worker_pool/internal/lifecycle
import constellation/worker_pool/internal/model.{
  type Config, type Message, type State, Push, SnapshotRequest, SourceRequest,
  State, Stop, WorkerCompleted, WorkerDown, WorkerExited,
}
import constellation/worker_pool/internal/protocol
import constellation/worker_pool/internal/worker
import constellation/worker_pool/types.{
  type PoolError, type WorkerId, AlreadyStopping, Snapshot, SourceManaged,
  StageProtocol,
}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor

pub fn start(config: Config(event, state), source, linked, name) {
  lifecycle.start(config, source, linked, name, handle_message)
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
    Stop(reply) -> lifecycle.handle_stop(state, reply)
    WorkerCompleted(id, count) -> handle_completed(state, id, count)
    WorkerExited(id) -> lifecycle.handle_worker_exit(state, id)
    WorkerDown(down) -> lifecycle.handle_worker_down(state, down)
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
      case dispatch.apply_stage(state, command.Push(events)) {
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
  case state.shutdown_started, worker.find(state.workers, id) {
    True, _ | _, Error(_) -> actor.continue(state)
    False, Ok(worker) -> {
      let result =
        dispatch.apply_stage(state, command.Ask(worker.subscription_id, count))
      let assert Ok(state) = result
      case state.stopping && core.buffer_size(state.stage) == 0 {
        True -> lifecycle.begin_shutdown(state)
        False -> actor.continue(dispatch.reconcile_source(state))
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
      case protocol.supply(state.stage, source_state, grant, offset, events) {
        Error(error) -> reply_and_continue(state, reply, Error(error))
        Ok(#(stage, source_state, effects, result)) -> {
          let state = State(..state, stage:, source_state: Some(source_state))
          dispatch.run_effects(state, effects)
          process.send(reply, Ok(result))
          actor.continue(dispatch.reconcile_source(state))
        }
      }
    }
    source.SetAvailable(available), Some(source_state), False -> {
      let capacity = dispatch.source_capacity(state.stage)
      let #(source_state, actions) = case available {
        True -> source_core.available(source_state, capacity)
        False -> source_core.unavailable(source_state)
      }
      let state = State(..state, source_state: Some(source_state))
      dispatch.notify_source_actions(state, actions)
      actor.continue(state)
    }
    source.SetAvailable(_), _, _ -> actor.continue(state)
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
