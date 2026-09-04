# stage

[![Package Version](https://img.shields.io/hexpm/v/stage)](https://hex.pm/packages/stage)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://stage.hexdocs.pm/)

```sh
gleam add stage@1
```
```gleam
import stage

pub fn main() -> Nil {
  // TODO: An example of the project in use
}
```

Further documentation can be found at <https://stage.hexdocs.pm/>.

## Execution logs

OTP runtime logging is opt-in:

```gleam
import logging
import stage/runtime/otp

logging.configure()
let assert Ok(started) =
  otp.config()
  |> otp.with_logging
  |> otp.start_with_config
```

Logs identify the stage, caller and consumer PIDs and distinguish receiving a
command, dispatching a batch, enqueueing it in the consumer mailbox, and the
consumer confirming that processing finished. A consumer confirms processing
after handling an `Events` message:

```gleam
otp.consumed(stage, subscription_id, list.length(events))
```

`mailbox_enqueued` does not mean consumed. Only the explicit `consumed` record
asserts that the consumer finished its work.

The OTP adapter also monitors the process that owns each participant subject.
If that process exits, all of its subscriptions are cancelled automatically and
logging records a `participant_down` operation with the PID and exit reason.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```

## Visual demand demo

The Mist dashboard runs as a separate example so HTTP concerns do not become
dependencies of the stage library.

```sh
cd examples/mist_dashboard
gleam deps download
gleam run
```

Open <http://localhost:4000>. Push a large event burst before requesting demand
to see the FIFO buffer grow, then release demand in smaller batches and observe
backpressure drain it without over-delivery.

The example requires `rebar3` for Mist's Erlang dependencies. With `mise`, it
can be run without changing the global tool configuration:

```sh
mise x rebar@3.27.0 -- gleam run
```
