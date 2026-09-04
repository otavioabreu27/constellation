# constellation

A Gleam-native, demand-driven event pipeline and resilient worker pool for the
BEAM, inspired by [Elixir GenStage](https://github.com/elixir-lang/gen_stage).

Constellation owns in-memory flow control: demand, dispatch, buffering, worker
capacity, and process replacement. Applications continue to own durable jobs,
leases, acknowledgements, retries, backoff, and dead-letter queues.

## Install

Constellation requires Gleam 1.18 or later and the Erlang target.

```sh
gleam add constellation
```

## Worker pool

The high-level pool starts and owns its Stage and workers. Each worker asks for
`prefetch` events, processes them in its own OTP process, and renews only the
capacity it has completed.

```gleam
import constellation/worker_pool

pub fn main() {
  let config = worker_pool.each(
    size: 4,
    prefetch: 8,
    initial_state: fn(_) { 0 },
    handle_event: fn(processed, _event) { processed + 1 },
  )
  let assert Ok(pool) = worker_pool.start(config)

  let assert Ok(Nil) = worker_pool.push(pool, [1, 2, 3, 4])
  let assert Ok(Nil) = worker_pool.stop(pool)
}
```

The maximum outstanding capacity is `workers * prefetch`. A worker that exits
is replaced in the same slot with a fresh monotonic `WorkerId`. A batch whose
handler crashes is not retried, so worker processing is at-most-once. Persist
and retry work before pushing it when stronger delivery semantics are required.

## Asynchronous source

A source receives callbacks only when downstream capacity is unreserved. It can
retain a grant while empty and supply it later without polling.

```gleam
import constellation/source
import constellation/worker_pool
import gleam/erlang/process

pub fn main() {
  let grants = process.new_subject()
  let config = worker_pool.each(
    size: 2,
    prefetch: 4,
    initial_state: fn(_) { 0 },
    handle_event: fn(total, event) { total + event },
  )
  let assert Ok(#(pool, attached_source)) =
    worker_pool.start_with_source(config, fn(event) {
      process.send(grants, event)
    })
  let assert Ok(source.DemandGranted(grant)) =
    process.receive(grants, within: 1000)

  let assert Ok(source.Accepted(..)) =
    source.supply(attached_source, grant, 0, [1, 2])
  let assert Ok(Nil) = worker_pool.stop(pool)
}
```

Supply is partial and offset-based. Exact retries return `Duplicate`, stale or
foreign grants are rejected, and shutdown revokes pending grants. Source and
reporter callbacks run outside the Stage process.

## Low-level Stage

Applications that need explicit subscriptions can use the OTP consumer:

```gleam
import constellation
import constellation/runtime/otp/consumer
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import gleam/list

pub fn main() {
  let assert Ok(engine) = constellation.start()
  let assert Ok(id) = subscription_id.new("example-consumer")
  let assert Ok(started) = consumer.start(
    engine.data,
    id,
    participant_id.new("example"),
    [],
    list.append,
  )

  let assert Ok(Nil) = consumer.ask(started.data, 3)
  let assert Ok(Nil) = constellation.push(engine.data, [1, 2, 3])
  let assert Ok([1, 2, 3]) = consumer.state(started.data)
  let assert Ok(Nil) = consumer.stop(started.data)
  let assert Ok(Nil) = constellation.stop(engine.data)
}
```

## Guarantees and scope

- Protocol transitions are deterministic and return effects; OTP modules own
  processes, mailboxes, monitoring, callbacks, and delivery.
- Events are delivered only against demand. Buffer overflow is rejected
  atomically after immediate dispatch.
- Demand, broadcast, partition, and safe custom dispatch strategies are built
  in. Source-backed pools use demand dispatch because other strategies cannot
  safely expose one scalar upstream capacity.
- OTP failures are typed. A timed-out command may still complete if it was
  already queued, so callers must only retry idempotent application operations.
- Worker-pool callbacks are isolated from the Stage. Low-level Stage telemetry
  reporters execute in the Stage process and must return quickly.
- State is ephemeral. Persistence, leases, durable ACKs, retries, backoff,
  dead-letter queues, and distributed coordination are application concerns.

## Configuration

The low-level Stage buffer is unlimited by default. A capacity can reject pushes
whose undelivered remainder would exceed the limit:

```gleam
let assert Ok(config) =
  constellation.config()
  |> constellation.with_buffer_capacity(10_000)

let assert Ok(engine) = constellation.start_with_config(config)
```

Call timeouts are explicit through `constellation.with_call_timeout` and
`worker_pool.with_timeout`.

## Examples

- [`examples/worker_pool`](https://github.com/otavioabreu27/constellation/tree/main/examples/worker_pool)
  is the smallest push-driven pool example.
- [`examples/async_source`](https://github.com/otavioabreu27/constellation/tree/main/examples/async_source)
  supplies one demand grant in two parts.
- [`examples/mist_dashboard`](https://github.com/otavioabreu27/constellation/tree/main/examples/mist_dashboard)
  visualizes a 500,000-event run, demand, buffer pressure, and throughput.
- [`examples/mist_stage_api`](https://github.com/otavioabreu27/constellation/tree/main/examples/mist_stage_api)
  exposes a smaller HTTP integration.
- [`examples/parallel_benchmark`](https://github.com/otavioabreu27/constellation/tree/main/examples/parallel_benchmark)
  compares deterministic sequential and OTP workloads at 25K, 100K, and 250K.

Run the dashboard with:

```sh
cd examples/mist_dashboard
gleam deps download
mise x rebar@3.27.0 -- gleam run
```

## Development

```sh
gleam deps download
gleam format --check src test examples
gleam test
gleam docs build
gleam export hex-tarball
```

See [CHANGELOG.md](CHANGELOG.md) for release notes and
[MIGRATION.md](MIGRATION.md) for compatibility guidance.
