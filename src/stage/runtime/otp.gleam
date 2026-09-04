import gleam/erlang/process.{type Monitor, type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/string
import logging
import stage/domains/dispatcher
import stage/runtime
import stage/value_objects/participant_id.{type ParticipantId}
import stage/value_objects/subscription_id.{type SubscriptionId}

const call_timeout = 5000

/// A running OTP stage. Its actor subject is intentionally hidden.
pub opaque type Stage(event) {
  Stage(Subject(Message(event)))
}

/// Configuration for an OTP stage.
pub opaque type Config(event) {
  Config(strategy: dispatcher.Strategy(event), logging_enabled: Bool)
}

type Message(event) {
  Subscribe(
    id: SubscriptionId,
    participant_id: ParticipantId,
    partition: Int,
    recipient: Subject(runtime.ParticipantMessage(event)),
    reply: Subject(Result(Nil, runtime.RuntimeError)),
  )
  Ask(
    subscription_id: SubscriptionId,
    amount: Int,
    reply: Subject(Result(Nil, runtime.RuntimeError)),
  )
  Push(events: List(event), reply: Subject(Result(Nil, runtime.RuntimeError)))
  Cancel(
    subscription_id: SubscriptionId,
    reply: Subject(Result(Nil, runtime.RuntimeError)),
  )
  Consumed(subscription_id: SubscriptionId, amount: Int, reply: Subject(Nil))
  ParticipantWentDown(process.Down)
  Stop(reply: Subject(Nil))
}

type ParticipantMonitor {
  ParticipantMonitor(participant_id: ParticipantId, pid: Pid, monitor: Monitor)
}

type State(event) {
  State(
    runtime: runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
    monitors: List(ParticipantMonitor),
    logging_enabled: Bool,
    next_trace_id: Int,
  )
}

/// Returns the default demand-dispatch stage configuration.
pub fn config() -> Config(event) {
  Config(strategy: dispatcher.Demand, logging_enabled: False)
}

/// Replaces the dispatch strategy in a stage configuration.
pub fn with_strategy(
  config: Config(event),
  strategy: dispatcher.Strategy(event),
) -> Config(event) {
  Config(..config, strategy: strategy)
}

/// Enables lifecycle logging through the standard Erlang logger.
pub fn with_logging(config: Config(event)) -> Config(event) {
  Config(..config, logging_enabled: True)
}

/// Starts a stage using demand-based round-robin dispatching.
pub fn start() -> actor.StartResult(Stage(event)) {
  start_with_config(config())
}

/// Starts a stage actor with the supplied pure dispatch strategy.
pub fn start_with_strategy(
  strategy: dispatcher.Strategy(event),
) -> actor.StartResult(Stage(event)) {
  start_with_config(config() |> with_strategy(strategy))
}

/// Starts a stage actor with explicit configuration.
pub fn start_with_config(
  config: Config(event),
) -> actor.StartResult(Stage(event)) {
  actor.new_with_initialiser(call_timeout, fn(subject) {
    let selector =
      process.new_selector()
      |> process.select(subject)
      |> process.select_monitors(ParticipantWentDown)
    actor.initialised(State(
      runtime: runtime.new_with_strategy(config.strategy),
      monitors: [],
      logging_enabled: config.logging_enabled,
      next_trace_id: 1,
    ))
    |> actor.selecting(selector)
    |> actor.returning(Stage(subject))
    |> Ok
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

/// Registers a participant subject and creates its core subscription.
pub fn subscribe(
  stage: Stage(event),
  id: SubscriptionId,
  participant_id: ParticipantId,
  partition: Int,
  recipient: Subject(runtime.ParticipantMessage(event)),
) -> Result(Nil, runtime.RuntimeError) {
  let Stage(subject) = stage
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    Subscribe(id, participant_id, partition, recipient, reply)
  })
}

/// Adds downstream demand and synchronously returns validation errors.
pub fn ask(
  stage: Stage(event),
  subscription_id: SubscriptionId,
  amount: Int,
) -> Result(Nil, runtime.RuntimeError) {
  let Stage(subject) = stage
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    Ask(subscription_id, amount, reply)
  })
}

/// Pushes events and executes any resulting outbound deliveries.
pub fn push(
  stage: Stage(event),
  events: List(event),
) -> Result(Nil, runtime.RuntimeError) {
  let Stage(subject) = stage
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    Push(events, reply)
  })
}

/// Cancels a subscription and delivers its cancellation notification.
pub fn cancel(
  stage: Stage(event),
  subscription_id: SubscriptionId,
) -> Result(Nil, runtime.RuntimeError) {
  let Stage(subject) = stage
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    Cancel(subscription_id, reply)
  })
}

/// Records that a consumer finished processing a previously received batch.
///
/// Delivery to a mailbox is not proof of consumption, so consumers call this
/// after their own message handler has completed its work.
pub fn consumed(
  stage: Stage(event),
  subscription_id: SubscriptionId,
  amount: Int,
) -> Nil {
  let Stage(subject) = stage
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    Consumed(subscription_id, amount, reply)
  })
}

/// Stops the stage actor after all earlier messages have been handled.
pub fn stop(stage: Stage(event)) -> Nil {
  let Stage(subject) = stage
  actor.call(subject, waiting: call_timeout, sending: Stop)
}

fn handle_message(
  state: State(event),
  message: Message(event),
) -> actor.Next(State(event), Message(event)) {
  case message {
    Subscribe(id, participant_id, partition, recipient, reply) -> {
      trace(
        state,
        "subscribe",
        "received",
        "source_pid="
          <> subject_owner(reply)
          <> " consumer_pid="
          <> subject_owner(recipient)
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
        Ok(_) -> ensure_monitor(state, participant_id, recipient)
        Error(_) -> state
      }
      continue_with_result(monitored_state, result, reply, "subscribe")
    }
    Ask(subscription, amount, reply) -> {
      trace(
        state,
        "ask",
        "received",
        "source_pid="
          <> subject_owner(reply)
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
    Push(events, reply) -> {
      trace(
        state,
        "push",
        "received",
        "source_pid="
          <> subject_owner(reply)
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
    Cancel(subscription, reply) -> {
      trace(
        state,
        "cancel",
        "received",
        "source_pid="
          <> subject_owner(reply)
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
    Consumed(subscription, amount, reply) -> {
      trace(
        state,
        "consume",
        "consumed",
        "consumer_pid="
          <> subject_owner(reply)
          <> " subscription="
          <> subscription_id.to_string(subscription)
          <> " events="
          <> int.to_string(amount),
      )
      process.send(reply, Nil)
      actor.continue(increment_trace(state))
    }
    ParticipantWentDown(down) -> handle_participant_down(state, down)
    Stop(reply) -> {
      trace(state, "stop", "received", "")
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

fn continue_with_result(
  current: State(event),
  result: Result(
    #(
      runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
      List(runtime.Outbound(event, Subject(runtime.ParticipantMessage(event)))),
    ),
    runtime.RuntimeError,
  ),
  reply: Subject(Result(Nil, runtime.RuntimeError)),
  operation: String,
) -> actor.Next(State(event), Message(event)) {
  case result {
    Error(error) -> {
      trace(current, operation, "rejected", "error=" <> string.inspect(error))
      process.send(reply, Error(error))
      actor.continue(increment_trace(current))
    }
    Ok(#(updated, outbound)) -> {
      trace(
        current,
        operation,
        "dispatched",
        "batches="
          <> int.to_string(list.length(outbound))
          <> " events="
          <> int.to_string(outbound_event_count(outbound)),
      )
      execute_outbound(outbound, current)
      process.send(reply, Ok(Nil))
      actor.continue(
        State(
          ..current,
          runtime: updated,
          monitors: remove_unused_monitors(current.monitors, updated),
          next_trace_id: current.next_trace_id + 1,
        ),
      )
    }
  }
}

fn execute_outbound(
  outbound: List(
    runtime.Outbound(event, Subject(runtime.ParticipantMessage(event))),
  ),
  state: State(event),
) -> Nil {
  case outbound {
    [] -> Nil
    [runtime.Deliver(to: recipient, message: message), ..rest] -> {
      process.send(recipient, message)
      case message {
        runtime.Events(subscription, events) ->
          trace(
            state,
            "deliver",
            "mailbox_enqueued",
            "consumer_pid="
              <> subject_owner(recipient)
              <> " subscription="
              <> subscription_id.to_string(subscription)
              <> " events="
              <> int.to_string(list.length(events)),
          )
        runtime.Cancelled(subscription) ->
          trace(
            state,
            "cancel",
            "mailbox_enqueued",
            "consumer_pid="
              <> subject_owner(recipient)
              <> " subscription="
              <> subscription_id.to_string(subscription),
          )
      }
      execute_outbound(rest, state)
    }
  }
}

fn ensure_monitor(
  state: State(event),
  participant_id: ParticipantId,
  recipient: Subject(runtime.ParticipantMessage(event)),
) -> State(event) {
  case
    list.any(state.monitors, fn(registered) {
      registered.participant_id == participant_id
    })
  {
    True -> state
    False ->
      case process.subject_owner(recipient) {
        Error(_) -> state
        Ok(pid) ->
          State(..state, monitors: [
            ParticipantMonitor(participant_id, pid, process.monitor(pid)),
            ..state.monitors
          ])
      }
  }
}

fn remove_unused_monitors(
  monitors: List(ParticipantMonitor),
  updated: runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
) -> List(ParticipantMonitor) {
  case monitors {
    [] -> []
    [registered, ..rest] ->
      case runtime.has_participant(updated, registered.participant_id) {
        True -> [registered, ..remove_unused_monitors(rest, updated)]
        False -> {
          process.demonitor_process(registered.monitor)
          remove_unused_monitors(rest, updated)
        }
      }
  }
}

fn handle_participant_down(
  state: State(event),
  down: process.Down,
) -> actor.Next(State(event), Message(event)) {
  case down {
    process.PortDown(..) -> actor.continue(state)
    process.ProcessDown(monitor, pid, reason) ->
      case
        list.find(state.monitors, fn(registered) {
          registered.monitor == monitor
        })
      {
        Error(_) -> actor.continue(state)
        Ok(registered) -> {
          trace(
            state,
            "participant_down",
            "received",
            "participant="
              <> participant_id.to_string(registered.participant_id)
              <> " consumer_pid="
              <> string.inspect(pid)
              <> " reason="
              <> string.inspect(reason),
          )
          let current =
            State(
              ..state,
              monitors: list.filter(state.monitors, fn(candidate) {
                candidate.monitor != monitor
              }),
            )
          case
            runtime.participant_down(state.runtime, registered.participant_id)
          {
            Error(error) -> {
              trace(
                current,
                "participant_down",
                "rejected",
                "error=" <> string.inspect(error),
              )
              actor.continue(increment_trace(current))
            }
            Ok(#(updated, outbound)) -> {
              trace(
                current,
                "participant_down",
                "cancelled",
                "subscriptions=" <> int.to_string(list.length(outbound)),
              )
              execute_outbound(outbound, current)
              actor.continue(
                State(
                  ..current,
                  runtime: updated,
                  next_trace_id: current.next_trace_id + 1,
                ),
              )
            }
          }
        }
      }
  }
}

fn outbound_event_count(
  outbound: List(
    runtime.Outbound(event, Subject(runtime.ParticipantMessage(event))),
  ),
) -> Int {
  list.fold(outbound, 0, fn(total, item) {
    case item {
      runtime.Deliver(message: runtime.Events(events: events, ..), ..) ->
        total + list.length(events)
      runtime.Deliver(message: runtime.Cancelled(..), ..) -> total
    }
  })
}

fn increment_trace(state: State(event)) -> State(event) {
  State(..state, next_trace_id: state.next_trace_id + 1)
}

fn subject_owner(subject: Subject(message)) -> String {
  case process.subject_owner(subject) {
    Ok(pid) -> string.inspect(pid)
    Error(_) -> "unknown"
  }
}

fn trace(
  state: State(event),
  operation: String,
  phase: String,
  detail: String,
) -> Nil {
  case state.logging_enabled {
    False -> Nil
    True -> {
      let suffix = case detail {
        "" -> ""
        value -> " " <> value
      }
      logging.log(
        logging.Info,
        "gleam_stage trace_id="
          <> int.to_string(state.next_trace_id)
          <> " stage_pid="
          <> string.inspect(process.self())
          <> " operation="
          <> operation
          <> " phase="
          <> phase
          <> suffix,
      )
    }
  }
}
