//// High-level configuration and lifecycle API for Constellation OTP stages.

import constellation/domains/dispatcher
import constellation/runtime/otp
import constellation/runtime/otp/telemetry
import gleam/otp/actor

/// The high-level OTP event engine.
pub type Engine(event) =
  otp.Stage(event)

/// Configuration for an event engine.
pub type Config(event) =
  otp.Config(event)

/// Creates the default engine configuration.
pub fn config() -> Config(event) {
  otp.config()
}

/// Replaces the engine's pure dispatch strategy.
pub fn with_strategy(
  config: Config(event),
  strategy: dispatcher.Strategy(event),
) -> Config(event) {
  otp.with_strategy(config, strategy)
}

/// Enables the standard Erlang logger reporter.
pub fn with_logging(config: Config(event)) -> Config(event) {
  otp.with_logging(config)
}

/// Installs a custom structured telemetry reporter.
pub fn with_reporter(
  config: Config(event),
  reporter: fn(telemetry.Event) -> Nil,
) -> Config(event) {
  otp.with_reporter(config, reporter)
}

/// Sets the timeout used by synchronous engine calls.
pub fn with_call_timeout(
  config: Config(event),
  milliseconds: Int,
) -> Result(Config(event), otp.ConfigError) {
  otp.with_call_timeout(config, milliseconds)
}

/// Limits how many events may wait without downstream demand.
pub fn with_buffer_capacity(
  config: Config(event),
  capacity: Int,
) -> Result(Config(event), otp.ConfigError) {
  otp.with_buffer_capacity(config, capacity)
}

/// Starts an engine with default demand dispatching.
pub fn start() -> actor.StartResult(Engine(event)) {
  otp.start()
}

/// Starts an engine with explicit configuration.
pub fn start_with_config(
  config: Config(event),
) -> actor.StartResult(Engine(event)) {
  otp.start_with_config(config)
}

/// Pushes events into the engine.
pub fn push(
  engine: Engine(event),
  events: List(event),
) -> Result(Nil, otp.CallError) {
  otp.push(engine, events)
}

/// Gracefully cancels subscriptions and stops the engine.
pub fn stop(engine: Engine(event)) -> Result(Nil, otp.CallError) {
  otp.stop(engine)
}
