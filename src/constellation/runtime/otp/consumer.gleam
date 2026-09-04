//// Stateful OTP consumers with automatic subscription lifecycle handling.

import constellation/runtime
import constellation/runtime/otp
import constellation/runtime/otp/client
import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/erlang/process.{type ExitReason, type Subject}
import gleam/list
import gleam/otp/actor
import gleam/string

const call_timeout = 5000

/// A stateful consumer process subscribed to an OTP stage.
pub opaque type Consumer(state, event) {
  Consumer(
    subject: Subject(Message(state, event)),
    stage: otp.Stage(event),
    subscription_id: SubscriptionId,
  )
}

/// Failures from the consumer process or its Stage dependency.
pub type ConsumerError {
  Stage(otp.CallError)
  Timeout
  ConsumerUnavailable(ExitReason)
}

type Message(state, event) {
  StageMessage(runtime.ParticipantMessage(event))
  GetState(reply: Subject(state))
}

type State(state, event) {
  State(
    stage: otp.Stage(event),
    value: state,
    on_events: fn(state, List(event)) -> state,
  )
}

/// Starts a consumer in the default partition.
pub fn start(
  stage: otp.Stage(event),
  subscription_id: SubscriptionId,
  participant_id: ParticipantId,
  initial_state: state,
  on_events: fn(state, List(event)) -> state,
) -> actor.StartResult(Consumer(state, event)) {
  start_partitioned(
    stage,
    subscription_id,
    participant_id,
    0,
    initial_state,
    on_events,
  )
}

/// Starts a consumer and subscribes it to a specific partition.
pub fn start_partitioned(
  stage: otp.Stage(event),
  subscription_id: SubscriptionId,
  participant_id: ParticipantId,
  partition: Int,
  initial_state: state,
  on_events: fn(state, List(event)) -> state,
) -> actor.StartResult(Consumer(state, event)) {
  actor.new_with_initialiser(call_timeout, fn(subject) {
    let recipient = process.new_subject()
    case
      otp.subscribe(
        stage,
        subscription_id,
        participant_id,
        partition,
        recipient,
      )
    {
      Error(error) ->
        Error("could not subscribe consumer: " <> string.inspect(error))
      Ok(_) -> {
        let selector =
          process.new_selector()
          |> process.select(subject)
          |> process.select_map(recipient, StageMessage)
        actor.initialised(State(stage, initial_state, on_events))
        |> actor.selecting(selector)
        |> actor.returning(Consumer(subject, stage, subscription_id))
        |> Ok
      }
    }
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

/// Adds demand for this consumer's subscription.
pub fn ask(
  consumer: Consumer(state, event),
  amount: Int,
) -> Result(Nil, ConsumerError) {
  let Consumer(stage: stage, subscription_id: subscription_id, ..) = consumer
  otp.ask(stage, subscription_id, amount)
  |> map_stage_result
}

/// Returns the consumer's current application state.
pub fn state(consumer: Consumer(state, event)) -> Result(state, ConsumerError) {
  let Consumer(subject: subject, ..) = consumer
  call_consumer(subject, GetState)
}

/// Cancels the subscription and stops the consumer process.
pub fn stop(consumer: Consumer(state, event)) -> Result(Nil, ConsumerError) {
  let Consumer(subject: subject, stage: stage, subscription_id: subscription_id) =
    consumer
  case process.subject_owner(subject) {
    Error(_) -> Error(ConsumerUnavailable(process.Normal))
    Ok(pid) -> {
      let monitor = process.monitor(pid)
      case otp.cancel(stage, subscription_id) {
        Error(error) -> {
          process.demonitor_process(monitor)
          Error(Stage(error))
        }
        Ok(_) -> {
          let selector =
            process.new_selector()
            |> process.select_specific_monitor(monitor, fn(down) { down })
          let result = case process.selector_receive(selector, call_timeout) {
            Error(_) -> Error(Timeout)
            Ok(process.ProcessDown(..)) -> Ok(Nil)
            Ok(process.PortDown(..)) -> Error(Timeout)
          }
          process.demonitor_process(monitor)
          result
        }
      }
    }
  }
}

fn handle_message(
  state: State(state, event),
  message: Message(state, event),
) -> actor.Next(State(state, event), Message(state, event)) {
  case message {
    GetState(reply) -> {
      process.send(reply, state.value)
      actor.continue(state)
    }
    StageMessage(runtime.Events(subscription_id, events)) -> {
      let updated = state.on_events(state.value, events)
      otp.report_consumed(state.stage, subscription_id, list.length(events))
      actor.continue(State(..state, value: updated))
    }
    StageMessage(runtime.Cancelled(..)) -> actor.stop()
  }
}

fn map_stage_result(
  result: Result(Nil, otp.CallError),
) -> Result(Nil, ConsumerError) {
  case result {
    Error(error) -> Error(Stage(error))
    Ok(value) -> Ok(value)
  }
}

fn call_consumer(
  subject: Subject(Message(state, event)),
  make_message: fn(Subject(value)) -> Message(state, event),
) -> Result(value, ConsumerError) {
  case client.call(subject, call_timeout, make_message) {
    Error(client.Timeout) -> Error(Timeout)
    Error(client.StageUnavailable(reason)) -> Error(ConsumerUnavailable(reason))
    Ok(value) -> Ok(value)
  }
}
