pub opaque type ParticipantId {
  ParticipantId(String)
}

pub fn new(id: String) -> ParticipantId {
  ParticipantId(id)
}

pub fn to_string(id: ParticipantId) -> String {
  let ParticipantId(value) = id
  value
}
