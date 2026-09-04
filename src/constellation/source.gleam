//// Demand grants used by asynchronous sources.
////
//// A grant reserves downstream capacity. Sources may supply it in parts and
//// may retain it while temporarily empty, without polling the Stage.

/// Capacity reserved for one asynchronous source request.
pub opaque type DemandGrant {
  DemandGrant(id: Int, amount: Int)
}

/// Notifications emitted to an attached asynchronous source.
pub type Event {
  DemandGranted(DemandGrant)
  GrantRevoked(DemandGrant)
  SourceStopped
}

/// Result of supplying part of a demand grant.
pub type SupplyResult {
  Accepted(next_offset: Int, remaining: Int)
  Duplicate
  StaleGrant
}

/// Protocol errors returned while supplying a demand grant.
pub type SupplyError {
  OffsetGap(expected: Int, provided: Int)
  GrantExceeded(remaining: Int, provided: Int)
}

/// Returns the maximum number of events reserved by a grant.
pub fn grant_amount(grant: DemandGrant) -> Int {
  grant.amount
}

@internal
pub fn new_grant(id: Int, amount: Int) -> DemandGrant {
  DemandGrant(id, amount)
}

@internal
pub fn grant_id(grant: DemandGrant) -> Int {
  grant.id
}
