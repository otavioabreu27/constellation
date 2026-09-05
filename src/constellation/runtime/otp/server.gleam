import constellation/runtime
import constellation/runtime/otp/lifecycle
import constellation/runtime/otp/model
import constellation/runtime/otp/notifier
import constellation/runtime/otp/outbound
import constellation/runtime/otp/participant_monitors
import constellation/runtime/otp/trace
import constellation/value_objects/subscription_id
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string

@internal
pub fn start(
  config: model.Config(event),
  wrap: fn(Subject(model.Message(event))) -> handle,
) -> actor.StartResult(handle) {
  actor.new_with_initialiser(model.default_start_timeout, fn(subject) {
    use reporter <- result.try(case model.reporter(config) {
      None -> Ok(None)
      Some(callback) ->
        notifier.start(callback, notifier.Isolate)
        |> result.map(Some)
        |> result.map_error(string.inspect)
    })
    let selector =
      process.new_selector()
      |> process.select(subject)
      |> process.select_monitors(model.ParticipantWentDown)
    actor.initialised(model.State(
      runtime: runtime.new_configured(
        model.strategy(config),
        model.buffer_capacity(config),
      ),
      monitors: participant_monitors.new(),
      reporter:,
      next_trace_id: 1,
    ))
    |> actor.selecting(selector)
    |> actor.returning(wrap(subject))
    |> Ok
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_message(
  state: model.State(event),
  message: model.Message(event),
) -> actor.Next(model.State(event), model.Message(event)) {
  case message {
    model.Subscribe(id, participant_id, partition, recipient, reply) -> {
      log(
        state,
        "subscribe",
        "received",
        "source_pid="
          <> trace.subject_owner(reply)
          <> " consumer_pid="
          <> trace.subject_owner(recipient)
          <> " subscription="
          <> subscription_id.to_string(id),
      )
      let result =
        runtime.subscribe(
          state.runtime,
          id,
          participant_id,
          partition,
          recipient,
        )
      let monitored_state = case result {
        Ok(_) ->
          model.State(
            ..state,
            monitors: participant_monitors.ensure(
              state.monitors,
              participant_id,
              recipient,
            ),
          )
        Error(_) -> state
      }
      continue_with_result(monitored_state, result, reply, "subscribe")
    }
    model.Ask(subscription, amount, reply) -> {
      log(
        state,
        "ask",
        "received",
        "source_pid="
          <> trace.subject_owner(reply)
          <> " subscription="
          <> subscription_id.to_string(subscription)
          <> " amount="
          <> int.to_string(amount),
      )
      continue_with_result(
        state,
        runtime.ask(state.runtime, subscription, amount),
        reply,
        "ask",
      )
    }
    model.Push(events, reply) -> {
      log(
        state,
        "push",
        "received",
        "source_pid="
          <> trace.subject_owner(reply)
          <> " events="
          <> int.to_string(list.length(events)),
      )
      continue_with_result(
        state,
        runtime.push(state.runtime, events),
        reply,
        "push",
      )
    }
    model.Cancel(subscription, reply) -> {
      log(
        state,
        "cancel",
        "received",
        "source_pid="
          <> trace.subject_owner(reply)
          <> " subscription="
          <> subscription_id.to_string(subscription),
      )
      continue_with_result(
        state,
        runtime.cancel(state.runtime, subscription),
        reply,
        "cancel",
      )
    }
    model.ConsumptionReported(subscription, amount, consumer_pid) -> {
      case amount > 0 && runtime.has_subscription(state.runtime, subscription) {
        False -> {
          let error = case amount > 0 {
            False -> runtime.InvalidConsumptionReport(amount)
            True -> runtime.MissingSubscriptionRoute(subscription)
          }
          log(state, "consume", "rejected", "error=" <> string.inspect(error))
          actor.continue(increment_trace(state))
        }
        True -> {
          log(
            state,
            "consume",
            "reported",
            "consumer_pid="
              <> consumer_pid
              <> " subscription="
              <> subscription_id.to_string(subscription)
              <> " events="
              <> int.to_string(amount),
          )
          actor.continue(increment_trace(state))
        }
      }
    }
    model.ParticipantWentDown(down) -> {
      case down, state.reporter {
        process.ProcessDown(pid: pid, ..), Some(reporter) ->
          case notifier.pid(reporter) == pid {
            True -> actor.stop_abnormal("telemetry notifier stopped")
            False -> lifecycle.participant_down(state, down)
          }
        _, _ -> lifecycle.participant_down(state, down)
      }
    }
    model.Stop(reply) -> {
      log(state, "stop", "received", "")
      case runtime.shutdown(state.runtime) {
        Error(error) -> {
          log(state, "stop", "rejected", "error=" <> string.inspect(error))
          process.send(reply, Error(error))
          actor.continue(increment_trace(state))
        }
        Ok(#(_, deliveries)) -> {
          log(
            state,
            "stop",
            "cancelled",
            "subscriptions=" <> int.to_string(list.length(deliveries)),
          )
          outbound.execute(deliveries, state.reporter, state.next_trace_id)
          case state.reporter {
            Some(reporter) -> notifier.stop(reporter)
            None -> Nil
          }
          process.send(reply, Ok(Nil))
          actor.stop()
        }
      }
    }
  }
}

fn continue_with_result(
  current: model.State(event),
  result: Result(
    #(
      runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
      List(runtime.Outbound(event, Subject(runtime.ParticipantMessage(event)))),
    ),
    runtime.RuntimeError,
  ),
  reply: Subject(Result(Nil, runtime.RuntimeError)),
  operation: String,
) -> actor.Next(model.State(event), model.Message(event)) {
  case result {
    Error(error) -> {
      log(current, operation, "rejected", "error=" <> string.inspect(error))
      process.send(reply, Error(error))
      actor.continue(increment_trace(current))
    }
    Ok(#(updated, deliveries)) -> {
      log(
        current,
        operation,
        "dispatched",
        "batches="
          <> int.to_string(list.length(deliveries))
          <> " events="
          <> int.to_string(outbound.event_count(deliveries)),
      )
      outbound.execute(deliveries, current.reporter, current.next_trace_id)
      process.send(reply, Ok(Nil))
      actor.continue(
        model.State(
          ..current,
          runtime: updated,
          monitors: participant_monitors.remove_unused(
            current.monitors,
            updated,
          ),
          next_trace_id: current.next_trace_id + 1,
        ),
      )
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
