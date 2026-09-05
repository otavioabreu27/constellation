import constellation/domains/dispatcher
import constellation/runtime
import constellation/runtime/otp/notifier.{type Notifier}
import constellation/runtime/otp/participant_monitors
import constellation/runtime/otp/telemetry
import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/erlang/process.{type Subject}
import gleam/option.{type Option, None, Some}

@internal
pub const default_call_timeout = 5000

@internal
pub const default_start_timeout = 5000

@internal
pub opaque type Config(event) {
  Config(
    strategy: dispatcher.Strategy(event),
    reporter: Option(fn(telemetry.Event) -> Nil),
    call_timeout: Int,
    buffer_capacity: Option(Int),
  )
}

@internal
pub type Message(event) {
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
  ConsumptionReported(
    subscription_id: SubscriptionId,
    amount: Int,
    consumer_pid: String,
  )
  ParticipantWentDown(process.Down)
  Stop(reply: Subject(Result(Nil, runtime.RuntimeError)))
}

@internal
pub type State(event) {
  State(
    runtime: runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
    monitors: participant_monitors.Registry,
    reporter: Option(Notifier(telemetry.Event)),
    next_trace_id: Int,
  )
}

@internal
pub fn config() -> Config(event) {
  Config(
    strategy: dispatcher.demand_strategy(),
    reporter: None,
    call_timeout: default_call_timeout,
    buffer_capacity: None,
  )
}

@internal
pub fn with_strategy(
  config: Config(event),
  strategy: dispatcher.Strategy(event),
) -> Config(event) {
  Config(..config, strategy: strategy)
}

@internal
pub fn with_reporter(
  config: Config(event),
  reporter: fn(telemetry.Event) -> Nil,
) -> Config(event) {
  Config(..config, reporter: Some(reporter))
}

@internal
pub fn strategy(config: Config(event)) -> dispatcher.Strategy(event) {
  config.strategy
}

@internal
pub fn reporter(config: Config(event)) -> Option(fn(telemetry.Event) -> Nil) {
  config.reporter
}

@internal
pub fn with_call_timeout(config: Config(event), timeout: Int) -> Config(event) {
  Config(..config, call_timeout: timeout)
}

@internal
pub fn call_timeout(config: Config(event)) -> Int {
  config.call_timeout
}

@internal
pub fn with_buffer_capacity(
  config: Config(event),
  capacity: Int,
) -> Config(event) {
  Config(..config, buffer_capacity: Some(capacity))
}

@internal
pub fn buffer_capacity(config: Config(event)) -> Option(Int) {
  config.buffer_capacity
}
