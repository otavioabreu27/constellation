import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import stage/domains/dispatcher
import stage/runtime
import stage/value_objects/participant_id.{type ParticipantId}
import stage/value_objects/subscription_id.{type SubscriptionId}

const call_timeout = 5000

/// A running OTP stage. Its actor subject is intentionally hidden.
pub opaque type Stage(event) {
  Stage(Subject(Message(event)))
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
  Stop(reply: Subject(Nil))
}

/// Starts a stage using demand-based round-robin dispatching.
pub fn start() -> actor.StartResult(Stage(event)) {
  start_with_strategy(dispatcher.Demand)
}

/// Starts a stage actor with the supplied pure dispatch strategy.
pub fn start_with_strategy(
  strategy: dispatcher.Strategy(event),
) -> actor.StartResult(Stage(event)) {
  case
    actor.new(runtime.new_with_strategy(strategy))
    |> actor.on_message(handle_message)
    |> actor.start
  {
    Ok(actor.Started(pid: pid, data: subject)) ->
      Ok(actor.Started(pid: pid, data: Stage(subject)))
    Error(error) -> Error(error)
  }
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

/// Stops the stage actor after all earlier messages have been handled.
pub fn stop(stage: Stage(event)) -> Nil {
  let Stage(subject) = stage
  actor.call(subject, waiting: call_timeout, sending: Stop)
}

fn handle_message(
  state: runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
  message: Message(event),
) -> actor.Next(
  runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
  Message(event),
) {
  case message {
    Subscribe(id, participant_id, partition, recipient, reply) ->
      continue_with_result(
        state,
        runtime.subscribe(state, id, participant_id, partition, recipient),
        reply,
      )
    Ask(subscription_id, amount, reply) ->
      continue_with_result(
        state,
        runtime.ask(state, subscription_id, amount),
        reply,
      )
    Push(events, reply) ->
      continue_with_result(state, runtime.push(state, events), reply)
    Cancel(subscription_id, reply) ->
      continue_with_result(state, runtime.cancel(state, subscription_id), reply)
    Stop(reply) -> {
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

fn continue_with_result(
  current: runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
  result: Result(
    #(
      runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
      List(runtime.Outbound(event, Subject(runtime.ParticipantMessage(event)))),
    ),
    runtime.RuntimeError,
  ),
  reply: Subject(Result(Nil, runtime.RuntimeError)),
) -> actor.Next(
  runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
  Message(event),
) {
  case result {
    Error(error) -> {
      process.send(reply, Error(error))
      actor.continue(current)
    }
    Ok(#(updated, outbound)) -> {
      execute_outbound(outbound)
      process.send(reply, Ok(Nil))
      actor.continue(updated)
    }
  }
}

fn execute_outbound(
  outbound: List(
    runtime.Outbound(event, Subject(runtime.ParticipantMessage(event))),
  ),
) -> Nil {
  case outbound {
    [] -> Nil
    [runtime.Deliver(to: recipient, message: message), ..rest] -> {
      process.send(recipient, message)
      execute_outbound(rest)
    }
  }
}
