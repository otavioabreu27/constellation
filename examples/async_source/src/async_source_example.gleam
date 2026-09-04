import constellation/source
import constellation/worker_pool
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list

pub fn main() {
  let grants = process.new_subject()
  let completed = process.new_subject()
  let config =
    worker_pool.new(
      size: 2,
      prefetch: 4,
      initial_state: fn(_) { 0 },
      handle_batch: fn(total, events) {
        process.send(completed, list.length(events))
        total + list.length(events)
      },
    )
  let assert Ok(#(pool, attached_source)) =
    worker_pool.start_with_source(config, fn(event) {
      process.send(grants, event)
    })
  let assert Ok(source.DemandGranted(grant)) =
    process.receive(grants, within: 1000)

  let assert Ok(source.Accepted(next_offset: 2, remaining: 6)) =
    source.supply(attached_source, grant, 0, [1, 2])
  let assert Ok(source.Accepted(next_offset: 8, remaining: 0)) =
    source.supply(attached_source, grant, 2, [3, 4, 5, 6, 7, 8])
  let assert Ok(Nil) = worker_pool.stop(pool)
  let count = receive_total(completed, 8, 0)
  io.println("source delivered " <> int.to_string(count) <> " events")
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
