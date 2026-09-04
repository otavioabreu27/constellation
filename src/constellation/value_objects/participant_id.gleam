//// Stable participant identities used across subscriptions.

/// Identifies one participant that may own multiple subscriptions.
pub opaque type ParticipantId {
  ParticipantId(String)
}

/// Creates a participant identity.
pub fn new(id: String) -> ParticipantId {
  ParticipantId(id)
}

/// Returns the identity's string representation.
pub fn to_string(id: ParticipantId) -> String {
  let ParticipantId(value) = id
  value
}
