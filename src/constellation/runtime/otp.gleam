import constellation/domains/dispatcher
import constellation/runtime
import constellation/runtime/otp/client
import constellation/runtime/otp/model
import constellation/runtime/otp/server
import constellation/runtime/otp/telemetry
import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/erlang/process.{type ExitReason, type Subject}
import gleam/otp/actor
import gleam/string

/// A running OTP stage. Its actor subject is intentionally hidden.
pub opaque type Stage(event) {
  Stage(subject: Subject(model.Message(event)), call_timeout: Int)
}

/// Configuration for an OTP stage.
pub opaque type Config(event) {
  Config(model.Config(event))
}

/// Failures from the OTP transport or the pure stage runtime.
///
/// A timed-out operation may still complete because its message may already be
/// in the Stage mailbox. Callers must not assume that `Timeout` means the
/// operation was not applied.
pub type CallError {
  Runtime(runtime.RuntimeError)
  Timeout
  StageUnavailable(ExitReason)
}

pub type ConfigError {
  InvalidCallTimeout(Int)
  InvalidBufferCapacity(Int)
}

/// Returns the default demand-dispatch stage configuration.
pub fn config() -> Config(event) {
  Config(model.config())
}

/// Replaces the dispatch strategy in a stage configuration.
pub fn with_strategy(
  config: Config(event),
  strategy: dispatcher.Strategy(event),
) -> Config(event) {
  let Config(inner) = config
  Config(model.with_strategy(inner, strategy))
}

/// Enables lifecycle logging through the standard Erlang logger.
pub fn with_logging(config: Config(event)) -> Config(event) {
  with_reporter(config, telemetry.log)
}

/// Sends structured lifecycle events to a custom reporter.
///
/// The reporter runs inside the stage process and must return quickly.
pub fn with_reporter(
  config: Config(event),
  reporter: fn(telemetry.Event) -> Nil,
) -> Config(event) {
  let Config(inner) = config
  Config(model.with_reporter(inner, reporter))
}

/// Sets the maximum time for synchronous OTP calls.
///
/// This does not change the timeout used to start the Stage actor.
pub fn with_call_timeout(
  config: Config(event),
  milliseconds: Int,
) -> Result(Config(event), ConfigError) {
  case milliseconds > 0 {
    False -> Error(InvalidCallTimeout(milliseconds))
    True -> {
      let Config(inner) = config
      Ok(Config(model.with_call_timeout(inner, milliseconds)))
    }
  }
}

/// Sets the maximum number of events that may wait in the Stage buffer.
pub fn with_buffer_capacity(
  config: Config(event),
  capacity: Int,
) -> Result(Config(event), ConfigError) {
  case capacity > 0 {
    False -> Error(InvalidBufferCapacity(capacity))
    True -> {
      let Config(inner) = config
      Ok(Config(model.with_buffer_capacity(inner, capacity)))
    }
  }
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
  let Config(inner) = config
  server.start(inner, fn(subject) { Stage(subject, model.call_timeout(inner)) })
}

/// Registers a participant subject and creates its core subscription.
pub fn subscribe(
  stage: Stage(event),
  id: SubscriptionId,
  participant_id: ParticipantId,
  partition: Int,
  recipient: Subject(runtime.ParticipantMessage(event)),
) -> Result(Nil, CallError) {
  let Stage(subject, timeout) = stage
  call(subject, timeout, fn(reply) {
    model.Subscribe(id, participant_id, partition, recipient, reply)
  })
}

/// Adds downstream demand and synchronously returns validation errors.
pub fn ask(
  stage: Stage(event),
  subscription_id: SubscriptionId,
  amount: Int,
) -> Result(Nil, CallError) {
  let Stage(subject, timeout) = stage
  call(subject, timeout, fn(reply) { model.Ask(subscription_id, amount, reply) })
}

/// Pushes events and executes any resulting outbound deliveries.
pub fn push(
  stage: Stage(event),
  events: List(event),
) -> Result(Nil, CallError) {
  let Stage(subject, timeout) = stage
  call(subject, timeout, fn(reply) { model.Push(events, reply) })
}

/// Cancels a subscription and delivers its cancellation notification.
pub fn cancel(
  stage: Stage(event),
  subscription_id: SubscriptionId,
) -> Result(Nil, CallError) {
  let Stage(subject, timeout) = stage
  call(subject, timeout, fn(reply) { model.Cancel(subscription_id, reply) })
}

/// Reports that a consumer finished processing a previously received batch.
///
/// This is an observability signal, not a protocol acknowledgement. Consumers
/// call it only after their own message handler has completed its work.
pub fn report_consumed(
  stage: Stage(event),
  subscription_id: SubscriptionId,
  amount: Int,
) -> Nil {
  let Stage(subject, ..) = stage
  process.send(
    subject,
    model.ConsumptionReported(
      subscription_id,
      amount,
      string.inspect(process.self()),
    ),
  )
}

/// Stops the stage actor after all earlier messages have been handled.
pub fn stop(stage: Stage(event)) -> Result(Nil, CallError) {
  let Stage(subject, timeout) = stage
  call(subject, timeout, model.Stop)
}

fn call(
  subject: Subject(model.Message(event)),
  timeout: Int,
  make_message: fn(Subject(Result(Nil, runtime.RuntimeError))) ->
    model.Message(event),
) -> Result(Nil, CallError) {
  case client.call(subject, timeout, make_message) {
    Error(client.Timeout) -> Error(Timeout)
    Error(client.StageUnavailable(reason)) -> Error(StageUnavailable(reason))
    Ok(Error(error)) -> Error(Runtime(error))
    Ok(Ok(value)) -> Ok(value)
  }
}
