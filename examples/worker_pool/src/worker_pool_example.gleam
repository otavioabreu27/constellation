import constellation/worker_pool
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list

pub fn main() {
  let completed = process.new_subject()
  let config =
    worker_pool.new(
      size: 4,
      prefetch: 8,
      initial_state: fn(_) { 0 },
      handle_batch: fn(total, events) {
        process.send(completed, list.length(events))
        total + list.length(events)
      },
    )
  let assert Ok(pool) = worker_pool.start(config)

  let assert Ok(Nil) = worker_pool.push(pool, [1, 2, 3, 4, 5, 6, 7, 8])
  let assert Ok(Nil) = worker_pool.stop(pool)
  io.println(
    "processed " <> int.to_string(receive_total(completed, 8, 0)) <> " events",
  )
}

fn receive_total(subject, expected: Int, total: Int) -> Int {
  case total >= expected {
    True -> total
    False -> {
      let assert Ok(count) = process.receive(subject, within: 1000)
      receive_total(subject, expected, total + count)
    }
  }
}
