import constellation/source
import constellation/source/core

pub fn grants_only_unreserved_capacity_test() {
  let #(state, first) = core.reconcile(core.new(), 5)
  let #(state, second) = core.reconcile(state, 8)
  let #(state, third) = core.reconcile(state, 8)

  assert first == [core.GrantCapacity(1, 5)]
  assert second == [core.GrantCapacity(2, 3)]
  assert third == []
  assert core.pending(state) == 8
}

pub fn partial_supply_keeps_remaining_capacity_reserved_test() {
  let #(state, _) = core.reconcile(core.new(), 5)
  let assert Ok(#(state, result)) = core.supply(state, 1, 0, 2)
  let #(state, actions) = core.reconcile(state, 3)

  assert result == source.Accepted(next_offset: 2, remaining: 3)
  assert actions == []
  assert core.pending(state) == 3
}

pub fn duplicate_supply_offset_is_idempotent_test() {
  let #(state, _) = core.reconcile(core.new(), 5)
  let assert Ok(#(state, _)) = core.supply(state, 1, 0, 2)
  let assert Ok(#(unchanged, result)) = core.supply(state, 1, 0, 2)

  assert result == source.Duplicate
  assert core.pending(unchanged) == 3
}

pub fn partially_overlapping_retry_is_rejected_test() {
  let #(state, _) = core.reconcile(core.new(), 5)
  let assert Ok(#(state, _)) = core.supply(state, 1, 0, 2)

  assert core.supply(state, 1, 1, 2) == Error(source.OffsetOverlap(2, 3))
  assert core.pending(state) == 3
}

pub fn completed_grant_retry_is_idempotent_test() {
  let #(state, _) = core.reconcile(core.new(), 2)
  let assert Ok(#(state, _)) = core.supply(state, 1, 0, 2)
  let assert Ok(#(state, result)) = core.supply(state, 1, 0, 2)

  assert result == source.Duplicate
  assert core.pending(state) == 0
}

pub fn supply_rejects_offset_gaps_and_overproduction_test() {
  let #(state, _) = core.reconcile(core.new(), 5)

  assert core.supply(state, 1, 1, 1) == Error(source.OffsetGap(0, 1))
  assert core.supply(state, 1, 0, 6) == Error(source.GrantExceeded(5, 6))
  assert core.pending(state) == 5
}

pub fn empty_source_can_retain_and_supply_grant_later_test() {
  let #(state, _) = core.reconcile(core.new(), 3)
  let assert Ok(#(state, result)) = core.supply(state, 1, 0, 0)
  let assert Ok(#(state, later)) = core.supply(state, 1, 0, 3)

  assert result == source.Accepted(next_offset: 0, remaining: 3)
  assert later == source.Accepted(next_offset: 3, remaining: 0)
  assert core.pending(state) == 0
}

pub fn reduced_capacity_revokes_and_regrants_exact_amount_test() {
  let #(state, _) = core.reconcile(core.new(), 10)
  let #(state, actions) = core.reconcile(state, 4)

  assert actions == [core.RevokeGrant(1, 10), core.GrantCapacity(2, 4)]
  assert core.pending(state) == 4
}

pub fn unavailable_source_revokes_and_available_source_regrants_test() {
  let #(state, _) = core.reconcile(core.new(), 5)
  let #(state, revoked) = core.unavailable(state)
  let #(state, unavailable_actions) = core.reconcile(state, 8)
  let #(state, available_actions) = core.available(state, 8)

  assert revoked == [core.RevokeGrant(1, 5)]
  assert unavailable_actions == []
  assert available_actions == [core.GrantCapacity(2, 8)]
  assert core.pending(state) == 8
}

pub fn shutdown_invalidates_grants_and_stops_source_test() {
  let #(state, _) = core.reconcile(core.new(), 5)
  let #(state, actions) = core.shutdown(state)
  let assert Ok(#(_, result)) = core.supply(state, 1, 0, 1)
  let #(_, later) = core.available(state, 5)

  assert actions == [core.RevokeGrant(1, 5), core.StopSource]
  assert result == source.StaleGrant
  assert later == []
}
