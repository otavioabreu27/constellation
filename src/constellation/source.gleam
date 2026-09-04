//// Demand grants used by asynchronous sources.
////
//// A grant reserves downstream capacity. Sources may supply it in parts and
//// may retain it while temporarily empty, without polling the Stage.

import constellation/runtime/otp/client
import gleam/erlang/process.{type Subject}
import gleam/erlang/reference.{type Reference}

/// Capacity reserved for one asynchronous source request.
pub opaque type DemandGrant {
  DemandGrant(source_id: Reference, id: Int, amount: Int)
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
  OffsetOverlap(accepted_until: Int, provided_until: Int)
  GrantExceeded(remaining: Int, provided: Int)
}

/// Errors returned by a source attached to an OTP worker pool.
pub type SourceError {
  SupplyProtocol(SupplyError)
  SourceMismatch
  SourceTimeout
  SourceUnavailable
}

/// A typed capability used to supply events for demand grants.
pub opaque type Source(event) {
  Source(source_id: Reference, subject: Subject(Request(event)), timeout: Int)
}

@internal
pub type Request(event) {
  Supply(
    grant: DemandGrant,
    offset: Int,
    events: List(event),
    reply: Subject(Result(SupplyResult, SourceError)),
  )
  SetAvailable(Bool)
}

/// Returns the maximum number of events reserved by a grant.
pub fn grant_amount(grant: DemandGrant) -> Int {
  grant.amount
}

/// Supplies the next contiguous portion of a demand grant.
///
/// Retrying an already accepted range is safe and returns `Duplicate` while
/// its grant remains in the source's bounded recent-completion window.
pub fn supply(
  source: Source(event),
  grant: DemandGrant,
  offset: Int,
  events: List(event),
) -> Result(SupplyResult, SourceError) {
  case grant.source_id == source.source_id {
    False -> Error(SourceMismatch)
    True ->
      case
        client.call(source.subject, source.timeout, Supply(
          grant,
          offset,
          events,
          _,
        ))
      {
        Ok(result) -> result
        Error(client.Timeout) -> Error(SourceTimeout)
        Error(client.StageUnavailable(_)) -> Error(SourceUnavailable)
      }
  }
}

/// Suspends grants and revokes all capacity currently reserved by this source.
pub fn unavailable(source: Source(event)) -> Nil {
  process.send(source.subject, SetAvailable(False))
}

/// Resumes grant notifications for current downstream capacity.
pub fn available(source: Source(event)) -> Nil {
  process.send(source.subject, SetAvailable(True))
}

@internal
pub fn new_grant(source_id: Reference, id: Int, amount: Int) -> DemandGrant {
  DemandGrant(source_id, id, amount)
}

@internal
pub fn grant_id(grant: DemandGrant) -> Int {
  grant.id
}

@internal
pub fn new_source(
  source_id: Reference,
  subject: Subject(Request(event)),
  timeout: Int,
) -> Source(event) {
  Source(source_id, subject, timeout)
}

@internal
pub fn subject(source: Source(event)) -> Subject(Request(event)) {
  source.subject
}
