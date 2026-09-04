import constellation/source
import constellation/worker_pool
import gleam/erlang/process
import gleam/int
import gleam/list

pub fn invalid_pool_configuration_is_typed_test() {
  let config =
    worker_pool.new(
      size: 0,
      prefetch: 1,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, _) { state },
    )

  assert worker_pool.start(config)
    == Error(worker_pool.InvalidConfig(worker_pool.InvalidSize(0)))
}

pub fn source_reserves_exact_capacity_without_polling_test() {
  let notifications = process.new_subject()
  let config =
    worker_pool.new(
      size: 2,
      prefetch: 3,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, _) { state },
    )
  let assert Ok(#(pool, attached_source)) =
    worker_pool.start_with_source(config, fn(event) {
      process.send(notifications, event)
    })

  let assert Ok(source.DemandGranted(grant)) =
    process.receive(notifications, within: 1000)
  assert source.grant_amount(grant) == 6
  assert process.receive(notifications, within: 30) == Error(Nil)

  source.unavailable(attached_source)
  let assert Ok(source.GrantRevoked(revoked)) =
    process.receive(notifications, within: 1000)
  assert source.grant_amount(revoked) == 6

  source.available(attached_source)
  let assert Ok(source.DemandGranted(regranted)) =
    process.receive(notifications, within: 1000)
  assert source.grant_amount(regranted) == 6
  assert worker_pool.stop(pool) == Ok(Nil)
  assert source.supply(attached_source, regranted, 0, [])
    == Error(source.SourceUnavailable)
  assert worker_pool.snapshot(pool) == Error(worker_pool.PoolUnavailable)
}

pub fn source_accepts_partial_and_idempotent_supply_test() {
  let notifications = process.new_subject()
  let processed = process.new_subject()
  let config =
    worker_pool.new(
      size: 2,
      prefetch: 2,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, events) {
        process.send(processed, events)
        state
      },
    )
  let assert Ok(#(pool, attached_source)) =
    worker_pool.start_with_source(config, fn(event) {
      process.send(notifications, event)
    })
  let assert Ok(source.DemandGranted(grant)) =
    process.receive(notifications, within: 1000)

  assert source.supply(attached_source, grant, 0, [])
    == Ok(source.Accepted(next_offset: 0, remaining: 4))
  assert source.supply(attached_source, grant, 0, [1])
    == Ok(source.Accepted(next_offset: 1, remaining: 3))
  assert source.supply(attached_source, grant, 0, [1]) == Ok(source.Duplicate)
  assert source.supply(attached_source, grant, 1, [2, 3, 4])
    == Ok(source.Accepted(next_offset: 4, remaining: 0))

  assert receive_event_count(processed, 4, 0) == 4
  assert worker_pool.stop(pool) == Ok(Nil)
}

pub fn grant_cannot_be_supplied_to_another_source_test() {
  let first_notifications = process.new_subject()
  let second_notifications = process.new_subject()
  let config =
    worker_pool.new(
      size: 1,
      prefetch: 1,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, _) { state },
    )
  let assert Ok(#(first_pool, _)) =
    worker_pool.start_with_source(config, fn(event) {
      process.send(first_notifications, event)
    })
  let assert Ok(#(second_pool, second_source)) =
    worker_pool.start_with_source(config, fn(event) {
      process.send(second_notifications, event)
    })
  let assert Ok(source.DemandGranted(first_grant)) =
    process.receive(first_notifications, within: 1000)
  let assert Ok(source.DemandGranted(_)) =
    process.receive(second_notifications, within: 1000)

  assert source.supply(second_source, first_grant, 0, [1])
    == Error(source.SourceMismatch)
  assert worker_pool.stop(first_pool) == Ok(Nil)
  assert worker_pool.stop(second_pool) == Ok(Nil)
}

pub fn failed_source_callback_stops_pool_instead_of_starving_test() {
  let config =
    worker_pool.new(
      size: 1,
      prefetch: 1,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, _) { state },
    )
  let assert Ok(#(pool, _)) =
    worker_pool.start_with_source(config, fn(_) {
      panic as "expected source callback failure"
    })

  assert wait_until_unavailable(pool, 50) == worker_pool.PoolUnavailable
}

pub fn slow_source_callback_does_not_block_pool_test() {
  let config =
    worker_pool.new(
      size: 1,
      prefetch: 1,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, _) { state },
    )
    |> worker_pool.with_timeout(100)
  let assert Ok(#(pool, _)) =
    worker_pool.start_with_source(config, fn(_) { process.sleep(500) })

  let assert Ok(worker_pool.Snapshot(workers: [_], ..)) =
    worker_pool.snapshot(pool)
  assert worker_pool.stop(pool) == Ok(Nil)
}

pub fn worker_renews_demand_only_after_handler_completion_test() {
  let events = process.new_subject()
  let config =
    worker_pool.new(
      size: 1,
      prefetch: 2,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, _) {
        process.sleep(80)
        state
      },
    )
    |> worker_pool.with_reporter(fn(event) { process.send(events, event) })
  let assert Ok(pool) = worker_pool.start(config)
  assert worker_pool.push(pool, [1, 2, 3, 4]) == Ok(Nil)
  let assert Ok(worker_pool.BatchStarted(_, _)) =
    receive_matching_batch_event(events)
  let assert Ok(worker_pool.Snapshot(buffered_events: 2, ..)) =
    worker_pool.snapshot(pool)

  assert worker_pool.stop(pool) == Ok(Nil)
}

pub fn failed_handler_is_replaced_with_a_new_identity_test() {
  let processed = process.new_subject()
  let config =
    worker_pool.new(
      size: 2,
      prefetch: 1,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, events) {
        case events {
          [0, ..] -> panic as "expected worker failure"
          _ -> {
            process.send(processed, list.length(events))
            state
          }
        }
      },
    )
  let assert Ok(pool) = worker_pool.start(config)
  let assert Ok(worker_pool.Snapshot(workers: workers, ..)) =
    worker_pool.snapshot(pool)
  assert list.length(workers) == 2

  assert worker_pool.push(pool, [0]) == Ok(Nil)
  let replacement = wait_for_replacement(pool, workers, 50)
  assert list.contains(workers, replacement) == False
  assert worker_pool.push(pool, [1]) == Ok(Nil)
  assert process.receive(processed, within: 1000) == Ok(1)
  assert worker_pool.stop(pool) == Ok(Nil)
}

pub fn graceful_shutdown_drains_buffered_work_test() {
  let processed = process.new_subject()
  let config =
    worker_pool.new(
      size: 2,
      prefetch: 2,
      initial_state: fn(_) { Nil },
      handle_batch: fn(state, events) {
        process.send(processed, list.length(events))
        state
      },
    )
  let assert Ok(pool) = worker_pool.start(config)
  let events =
    int.range(from: 1, to: 1001, with: [], run: fn(events, value) {
      [value, ..events]
    })

  assert worker_pool.push(pool, events) == Ok(Nil)
  assert worker_pool.stop(pool) == Ok(Nil)
  assert receive_count(processed, 1000, 0) == 1000
}

fn receive_event_count(subject, expected: Int, total: Int) -> Int {
  case total >= expected {
    True -> total
    False -> {
      let assert Ok(events) = process.receive(subject, within: 1000)
      receive_event_count(subject, expected, total + list.length(events))
    }
  }
}

fn receive_count(subject, expected: Int, total: Int) -> Int {
  case total >= expected {
    True -> total
    False -> {
      let assert Ok(count) = process.receive(subject, within: 1000)
      receive_count(subject, expected, total + count)
    }
  }
}

fn receive_matching_batch_event(subject) {
  case process.receive(subject, within: 1000) {
    Ok(worker_pool.BatchStarted(id, count)) ->
      Ok(worker_pool.BatchStarted(id, count))
    Ok(_) -> receive_matching_batch_event(subject)
    Error(error) -> Error(error)
  }
}

fn wait_for_replacement(pool, previous, attempts: Int) {
  let assert Ok(worker_pool.Snapshot(workers: workers, ..)) =
    worker_pool.snapshot(pool)
  case list.find(workers, fn(id) { !list.contains(previous, id) }) {
    Ok(id) -> id
    Error(_) -> {
      assert attempts > 0
      process.sleep(10)
      wait_for_replacement(pool, previous, attempts - 1)
    }
  }
}

fn wait_until_unavailable(pool, attempts: Int) {
  case worker_pool.snapshot(pool) {
    Error(error) -> error
    Ok(_) -> {
      assert attempts > 0
      process.sleep(10)
      wait_until_unavailable(pool, attempts - 1)
    }
  }
}
