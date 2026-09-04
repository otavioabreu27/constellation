import constellation/runtime/otp
import constellation/runtime/otp/consumer
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/string

const bar_width = 36

const work_rounds = 1500

type WorkResult {
  WorkResult(processed: Int, checksum: Int)
}

type Measurement {
  Measurement(result: WorkResult, milliseconds: Int)
}

pub fn main() -> Nil {
  let workers = int.max(1, int.min(4, schedulers_online()))

  io.println("============================================================")
  io.println(" CONSTELLATION // SEQUENTIAL vs OTP PARALLEL")
  io.println(" Same CPU work, same input, verified result")
  io.println("------------------------------------------------------------")
  io.println(
    " OTP workers: "
    <> int.to_string(workers)
    <> "  |  work rounds per event: "
    <> int.to_string(work_rounds),
  )
  io.println("============================================================")

  run_batteries([25_000, 100_000, 250_000], workers, 1)

  io.println("============================================================")
  io.println(" All three batteries produced identical verified results.")
  io.println(" Lower times are better. Results vary by CPU and scheduler.")
  io.println("============================================================")
}

fn run_batteries(sizes: List(Int), workers: Int, number: Int) -> Nil {
  case sizes {
    [] -> Nil
    [size, ..rest] -> {
      run_battery(size, workers, number)
      run_batteries(rest, workers, number + 1)
    }
  }
}

fn run_battery(event_count: Int, workers: Int, number: Int) -> Nil {
  let events = build_events(event_count)
  io.println("")
  io.println(
    "[ BATTERY "
    <> int.to_string(number)
    <> " / 3 ]  "
    <> int.to_string(event_count)
    <> " events",
  )
  io.println("  Running sequential workload...")

  let sequential = measure(fn() { process_events(WorkResult(0, 0), events) })
  io.println(
    "  Running OTP workload across "
    <> int.to_string(workers)
    <> " consumers...",
  )
  let parallel = run_parallel(events, workers)

  assert sequential.result == parallel.result
  render_comparison(sequential, parallel, workers)
  io.println(
    "  RESULT     "
    <> comparison(sequential.milliseconds, parallel.milliseconds)
    <> "  |  VERIFIED "
    <> int.to_string(parallel.result.processed)
    <> " events",
  )
  io.println("  CHECKSUM   " <> int.to_string(parallel.result.checksum))
}

fn build_events(event_count: Int) -> List(Int) {
  int.range(
    from: 1_000_000,
    to: 1_000_000 + event_count,
    with: [],
    run: fn(events, value) { [value, ..events] },
  )
  |> list.reverse
}

fn render_comparison(
  sequential: Measurement,
  parallel: Measurement,
  workers: Int,
) -> Nil {
  let longest = int.max(sequential.milliseconds, parallel.milliseconds)
  io.println(
    "  SEQUENTIAL "
    <> duration_bar(sequential.milliseconds, longest)
    <> " "
    <> int.to_string(sequential.milliseconds)
    <> " ms",
  )
  io.println(
    "  OTP x"
    <> string.pad_end(int.to_string(workers), to: 6, with: " ")
    <> duration_bar(parallel.milliseconds, longest)
    <> " "
    <> int.to_string(parallel.milliseconds)
    <> " ms",
  )
}

fn duration_bar(milliseconds: Int, longest: Int) -> String {
  let assert Ok(scaled) = int.divide(milliseconds * bar_width, by: longest)
  let width = int.max(1, scaled)
  "["
  <> string.repeat("#", times: width)
  <> string.repeat(".", times: bar_width - width)
  <> "]"
}

fn run_parallel(events: List(Int), worker_count: Int) -> Measurement {
  let assert Ok(started_stage) = otp.start()
  let workers = start_workers(started_stage.data, worker_count, [])
  request_demand(workers, list.length(events))

  let started_at = monotonic_milliseconds()
  let assert Ok(Nil) = otp.push(started_stage.data, events)
  let results = await_results(workers, list.length(events))
  let elapsed = monotonic_milliseconds() - started_at

  stop_workers(workers)
  let assert Ok(Nil) = otp.stop(started_stage.data)
  Measurement(aggregate(results), elapsed)
}

fn start_workers(
  stage: otp.Stage(Int),
  remaining: Int,
  workers: List(consumer.Consumer(WorkResult, Int)),
) -> List(consumer.Consumer(WorkResult, Int)) {
  case remaining {
    0 -> list.reverse(workers)
    number -> {
      let name = "benchmark-worker-" <> int.to_string(number)
      let assert Ok(id) = subscription_id.new(name)
      let assert Ok(started) =
        consumer.start(
          stage,
          id,
          participant_id.new(name),
          WorkResult(0, 0),
          process_events,
        )
      start_workers(stage, number - 1, [started.data, ..workers])
    }
  }
}

fn request_demand(
  workers: List(consumer.Consumer(WorkResult, Int)),
  amount: Int,
) -> Nil {
  list.each(workers, fn(worker) {
    let assert Ok(Nil) = consumer.ask(worker, amount)
    Nil
  })
}

fn await_results(
  workers: List(consumer.Consumer(WorkResult, Int)),
  expected: Int,
) -> List(WorkResult) {
  let results =
    list.map(workers, fn(worker) {
      let assert Ok(result) = consumer.state(worker)
      result
    })
  case processed_count(results) >= expected {
    True -> results
    False -> {
      process.sleep(2)
      await_results(workers, expected)
    }
  }
}

fn stop_workers(workers: List(consumer.Consumer(WorkResult, Int))) -> Nil {
  list.each(workers, fn(worker) {
    let assert Ok(Nil) = consumer.stop(worker)
    Nil
  })
}

fn process_events(state: WorkResult, events: List(Int)) -> WorkResult {
  list.fold(events, state, fn(state, event) {
    WorkResult(
      processed: state.processed + 1,
      checksum: state.checksum + cpu_work(event, work_rounds, event),
    )
  })
}

fn cpu_work(value: Int, remaining: Int, accumulator: Int) -> Int {
  case remaining {
    0 -> accumulator
    _ -> {
      let next = { accumulator * 48_271 + value + remaining } % 2_147_483_647
      cpu_work(value, remaining - 1, next)
    }
  }
}

fn aggregate(results: List(WorkResult)) -> WorkResult {
  list.fold(results, WorkResult(0, 0), fn(total, result) {
    WorkResult(
      processed: total.processed + result.processed,
      checksum: total.checksum + result.checksum,
    )
  })
}

fn processed_count(results: List(WorkResult)) -> Int {
  list.fold(results, 0, fn(total, result) { total + result.processed })
}

fn measure(run: fn() -> WorkResult) -> Measurement {
  let started_at = monotonic_milliseconds()
  let result = run()
  Measurement(result, monotonic_milliseconds() - started_at)
}

fn comparison(sequential: Int, parallel: Int) -> String {
  case parallel < sequential {
    True -> ratio(sequential, parallel) <> " faster with OTP"
    False ->
      case parallel > sequential {
        True -> ratio(parallel, sequential) <> " slower with OTP"
        False -> "same measured time"
      }
  }
}

fn ratio(numerator: Int, denominator: Int) -> String {
  case denominator > 0 {
    False -> "n/a"
    True -> {
      let assert Ok(hundredths) = int.divide(numerator * 100, by: denominator)
      let assert Ok(whole) = int.divide(hundredths, by: 100)
      let assert Ok(decimal) = int.remainder(hundredths, by: 100)
      int.to_string(whole)
      <> "."
      <> string.pad_start(int.to_string(decimal), to: 2, with: "0")
      <> "x"
    }
  }
}

@external(erlang, "parallel_benchmark_ffi", "monotonic_milliseconds")
fn monotonic_milliseconds() -> Int

@external(erlang, "parallel_benchmark_ffi", "schedulers_online")
fn schedulers_online() -> Int
