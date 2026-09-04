# constellation

A Gleam-native, demand-driven event pipeline for the BEAM, inspired by
[Elixir GenStage](https://github.com/elixir-lang/gen_stage).

Constellation is an early response to
[awesome-gleam issue #200](https://github.com/gleam-lang/awesome-gleam/issues/200),
which calls for GenStage- and Flow-like data processing libraries in Gleam. It
currently implements the GenStage foundation; Flow-like stage composition and
parallel processing remain future work.

The package is not published on Hex yet. The `constellation` name was available
when selected, but availability must be checked again before release.

```gleam
import constellation

pub fn main() -> Nil {
  let assert Ok(engine) = constellation.start()
  let assert Ok(Nil) = constellation.push(engine.data, [1, 2, 3])
  let assert Ok(Nil) = constellation.stop(engine.data)
}
```

## Execution logs

OTP runtime logging is opt-in:

```gleam
import logging
import constellation
import constellation/runtime/otp

logging.configure()
let assert Ok(started) =
  constellation.config()
  |> constellation.with_logging
  |> constellation.start_with_config
```

Logs identify the stage, caller and consumer PIDs and distinguish receiving a
command, dispatching a batch, enqueueing it in the consumer mailbox, and the
consumer reporting that processing finished. A consumer reports processing
after handling an `Events` message:

```gleam
otp.report_consumed(started.data, subscription_id, list.length(events))
```

`mailbox_enqueued` does not mean consumed. A `reported` record is an explicit
observability statement from the consumer, not a protocol-level guarantee.

The OTP adapter also monitors the process that owns each participant subject.
If that process exits, all of its subscriptions are cancelled automatically and
logging records a `participant_down` operation with the PID and exit reason.

## High-level OTP consumer

`constellation/runtime/otp/consumer` owns the actor, subject, selector, subscription,
consumption reporting, and cancellation lifecycle:

```gleam
import gleam/list
import constellation/runtime/otp/consumer

let assert Ok(started_consumer) =
  consumer.start(
    stage,
    subscription_id,
    participant_id,
    [],
    list.append,
  )

consumer.ask(started_consumer.data, 5)
let assert Ok(processed) = consumer.state(started_consumer.data)
```

The `on_events` function updates application state. Once it returns, the
consumer automatically reports the processed batch with `otp.report_consumed`.

OTP operations return `otp.CallError`, which distinguishes runtime validation,
timeouts, and an unavailable Stage process. Stopping an engine gracefully
cancels active subscriptions before the Stage actor exits. A timed-out command
may still complete if it was already queued, so retry only operations that are
safe for the application to apply more than once.

## Extension and limits

Dispatch is an opaque pure policy rather than a closed enum. Applications can
use `demand_strategy`, `broadcast_strategy`, `partition_strategy`, or provide a
new single-target selector with `dispatcher.custom_strategy` without modifying
the library. The library still owns demand accounting and rejects unknown or
full targets selected by custom code.

Producer-side buffering is unlimited by default. A capacity can be configured
to reject pushes whose undelivered remainder would exceed the limit:

```gleam
let assert Ok(config) =
  constellation.config()
  |> constellation.with_buffer_capacity(10_000)

let assert Ok(engine) = constellation.start_with_config(config)
```

Call timeouts are also explicit through `constellation.with_call_timeout`.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```

## Visual demand demo

The Mist dashboard runs as a separate example so HTTP concerns do not become
dependencies of the Constellation library.

```sh
cd examples/mist_dashboard
gleam deps download
gleam run
```

Open <http://localhost:4000> and start the 500,000-event run. The dashboard
shows live ingress and consumer throughput, the three OTP processes, FIFO
buffer pressure, rejected push retries, and completion progress. A preset run
fills the bounded buffer, activates real Stage backpressure, and drains it in
about ten seconds, making it suitable for a short screen recording.

The workload represents synthetic requests as Stage events. It runs inside OTP
actors after one HTTP command; it does not create 500,000 HTTP connections.

The example requires `rebar3` for Mist's Erlang dependencies. With `mise`, it
can be run without changing the global tool configuration:

```sh
mise x rebar@3.27.0 -- gleam run
```

For a smaller integration without a frontend, see `examples/mist_stage_api`.
It exposes only `/events/:value`, `/demand/:amount`, and `/consumed`.
