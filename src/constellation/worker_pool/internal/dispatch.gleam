//// Routes core effects and source capacity notifications to owned processes.

import constellation/core
import constellation/domains/command
import constellation/domains/effect
import constellation/domains/stage_error.{type StageError}
import constellation/runtime/otp/notifier
import constellation/source
import constellation/source/core as source_core
import constellation/worker_pool/internal/model.{type State, State}
import constellation/worker_pool/internal/worker
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result

pub fn apply_stage(
  state: State(event, worker_state),
  command: command.Command(event),
) -> Result(State(event, worker_state), StageError) {
  use updated <- result.try(core.update(state.stage, command))
  let #(stage, effects) = updated
  let state = State(..state, stage:)
  run_effects(state, effects)
  Ok(state)
}

pub fn run_effects(
  state: State(event, worker_state),
  effects: List(effect.Effect(event)),
) {
  list.each(effects, fn(stage_effect) {
    case stage_effect {
      effect.SendEvents(subscription_id, events) ->
        case worker.by_subscription(state.workers, subscription_id) {
          Ok(worker) -> process.send(worker.subject, worker.Work(events))
          Error(_) -> Nil
        }
      effect.NotifyCancelled(subscription_id) ->
        case worker.by_subscription(state.workers, subscription_id) {
          Ok(worker) -> process.send(worker.subject, worker.StopWorker)
          Error(_) -> Nil
        }
    }
  })
}

pub fn reconcile_source(
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

pub fn source_capacity(stage: core.StageState(event)) -> Int {
  int.max(0, core.available_demand(stage) - core.buffer_size(stage))
}

pub fn shutdown_source(
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

pub fn notify_source_actions(
  state: State(event, worker_state),
  actions: List(source_core.Action),
) -> Nil {
  case state.source_notifier {
    None -> Nil
    Some(notifier) ->
      list.each(actions, fn(action) {
        let event = case action {
          source_core.GrantCapacity(id, amount) ->
            source.DemandGranted(source.new_grant(state.source_id, id, amount))
          source_core.RevokeGrant(id, amount) ->
            source.GrantRevoked(source.new_grant(state.source_id, id, amount))
          source_core.StopSource -> source.SourceStopped
        }
        notifier.send(notifier, event)
      })
  }
}
