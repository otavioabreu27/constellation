import constellation/core
import constellation/domains/command
import constellation/domains/dispatcher
import constellation/source
import constellation/source/core as source_core
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
import constellation/worker_pool
import constellation/worker_pool/internal/protocol
import constellation/worker_pool/types
import gleam/erlang/process
import gleam/erlang/reference
import gleam/list
import gleam/option.{Some}
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result

pub fn supervisor_shutdown_uses_configured_timeout_test() {
  let config =
    worker_pool.each(1, 1, fn(_) { Nil }, fn(_, _) { Nil })
    |> worker_pool.with_timeout(1234)
  let assert Ok(child) = worker_pool.supervised(config)
  assert child.child_type == supervision.Worker(1234)
}

pub fn killed_pool_reporter_stops_owner_instead_of_silently_disabling_telemetry_test() {
  let reports = process.new_subject()
  let assert Ok(pool) =
    worker_pool.each(1, 1, fn(_) { Nil }, fn(_, _) { Nil })
    |> worker_pool.with_reporter(fn(_) { process.send(reports, process.self()) })
    |> worker_pool.start
  let assert Ok(pid) = process.receive(reports, within: 1000)
  process.kill(pid)
  assert wait_for_unavailable(pool, 50) == Error(types.PoolUnavailable)
}

fn wait_for_unavailable(pool, attempts: Int) {
  case worker_pool.snapshot(pool), attempts {
    Ok(_), remaining if remaining > 0 -> {
      process.sleep(10)
      wait_for_unavailable(pool, remaining - 1)
    }
    outcome, _ -> outcome
  }
}

pub fn revoked_grant_cannot_deliver_work_or_consume_new_capacity_test() {
  let reports = process.new_subject()
  let processed = process.new_subject()
  let config =
    worker_pool.each(1, 1, fn(_) { Nil }, fn(_, event) {
      process.send(processed, event)
    })
  let assert Ok(#(pool, attached)) =
    worker_pool.start_with_source(config, fn(event) {
      process.send(reports, event)
    })
  let assert Ok(source.DemandGranted(old)) =
    process.receive(reports, within: 1000)
  source.unavailable(attached)
  let assert Ok(source.GrantRevoked(_)) = process.receive(reports, within: 1000)
  assert source.supply(attached, old, 0, [42]) == Ok(source.StaleGrant)
  source.available(attached)
  let assert Ok(source.DemandGranted(fresh)) =
    process.receive(reports, within: 1000)
  assert source.supply(attached, old, 0, [43]) == Ok(source.StaleGrant)
  assert source.supply(attached, fresh, 0, [7]) == Ok(source.Accepted(1, 0))
  assert worker_pool.stop(pool) == Ok(Nil)
  assert process.receive(processed, within: 1000) == Ok(7)
  assert process.receive(processed, within: 0) == Error(Nil)
}

pub fn rejected_stage_push_does_not_consume_reservation_test() {
  let stage = core.new_configured(dispatcher.demand_strategy(), Some(1))
  let assert #(reservations, [source_core.GrantCapacity(id, _)]) =
    source_core.reconcile(source_core.new(), 2)
  let grant = source.new_grant(reference.new(), id, 2)
  assert protocol.supply(stage, reservations, grant, 0, [1, 2])
    == Error(source.SourceUnavailable)
  let assert Ok(#(stage, reservations, [], source.Accepted(1, 1))) =
    protocol.supply(stage, reservations, grant, 0, [1])
  assert core.buffered_events(stage) == [1]
  assert source_core.pending(reservations) == 1
  let assert Ok(subscription) = subscription_id.new("worker")
  let assert Ok(#(stage, _)) =
    core.update(
      stage,
      command.Subscribe(subscription, participant_id.new("worker"), 0),
    )
  let assert Ok(#(stage, _)) = core.update(stage, command.Ask(subscription, 1))
  let assert Ok(#(stage, _, _, source.Accepted(2, 0))) =
    protocol.supply(stage, reservations, grant, 1, [2])
  assert core.buffered_events(stage) == [2]
}

pub fn supervised_handle_survives_restart_without_old_worker_completion_test() {
  let starts = process.new_subject()
  let gates = process.new_subject()
  let processed = process.new_subject()
  let config =
    worker_pool.each(1, 1, fn(_) { Nil }, fn(_, event) {
      case event < 2 {
        True -> {
          let gate = process.new_subject()
          process.send(gates, #(event, gate, process.self()))
          let assert Ok(Nil) = process.receive(gate, within: 3000)
          Nil
        }
        False -> Nil
      }
      process.send(processed, event)
    })
    |> worker_pool.with_buffer_capacity(1)
  let assert Ok(child) = worker_pool.supervised(config)
  let watched =
    supervision.ChildSpecification(..child, start: fn() {
      child.start()
      |> result.map(fn(started) {
        process.send(starts, started)
        started
      })
    })
  let assert Ok(supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(watched)
    |> static_supervisor.start
  process.unlink(supervisor.pid)
  let assert Ok(first) = process.receive(starts, within: 1000)
  let handle = first.data
  assert worker_pool.push(handle, [0]) == Ok(Nil)
  let assert Ok(#(0, old_gate, old_worker)) =
    process.receive(gates, within: 1000)
  let monitor = process.monitor(old_worker)
  process.kill(first.pid)
  let assert Ok(second) = process.receive(starts, within: 2000)
  assert first.pid != second.pid
  assert worker_pool.push(handle, [1]) == Ok(Nil)
  let assert Ok(#(1, new_gate, _)) = process.receive(gates, within: 1000)
  assert worker_pool.push(handle, [2]) == Ok(Nil)
  process.send(old_gate, Nil)
  let assert Ok(_) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(1000)
  let assert Ok(types.Snapshot(buffered_events: 1, ..)) =
    worker_pool.snapshot(handle)
  process.send(new_gate, Nil)
  let events =
    list.map([1, 2, 3], fn(_) {
      let assert Ok(value) = process.receive(processed, within: 1000)
      value
    })
  assert events == [0, 1, 2]
  process.kill(supervisor.pid)
}

pub fn pool_reporter_panic_does_not_disable_later_events_test() {
  let reports = process.new_subject()
  let config =
    worker_pool.each(1, 1, fn(_) { Nil }, fn(_, _) { Nil })
    |> worker_pool.with_reporter(fn(event) {
      case event {
        types.WorkerStarted(..) -> panic as "telemetry only"
        types.BatchCompleted(_, count) -> process.send(reports, count)
        _ -> Nil
      }
    })
  let assert Ok(pool) = worker_pool.start(config)
  assert worker_pool.push(pool, [1]) == Ok(Nil)
  assert process.receive(reports, within: 1000) == Ok(1)
  assert worker_pool.push(pool, [2]) == Ok(Nil)
  assert process.receive(reports, within: 1000) == Ok(1)
  assert worker_pool.stop(pool) == Ok(Nil)
}
