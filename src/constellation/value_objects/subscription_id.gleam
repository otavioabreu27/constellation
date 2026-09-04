pub opaque type SubscriptionId {
  SubscriptionId(String)
}

pub type SubscriptionIdError {
  Empty
}

pub fn new(id: String) -> Result(SubscriptionId, SubscriptionIdError) {
  case id {
    "" -> Error(Empty)
    value -> Ok(SubscriptionId(value))
  }
}

pub fn to_string(id: SubscriptionId) -> String {
  let SubscriptionId(value) = id
  value
}
