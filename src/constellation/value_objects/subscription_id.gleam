//// Validated subscription identities.

/// Identifies one subscription for its entire lifecycle.
pub opaque type SubscriptionId {
  SubscriptionId(String)
}

/// Errors returned while validating a subscription identity.
pub type SubscriptionIdError {
  Empty
}

/// Creates a non-empty subscription identity.
pub fn new(id: String) -> Result(SubscriptionId, SubscriptionIdError) {
  case id {
    "" -> Error(Empty)
    value -> Ok(SubscriptionId(value))
  }
}

/// Returns the identity's string representation.
pub fn to_string(id: SubscriptionId) -> String {
  let SubscriptionId(value) = id
  value
}
