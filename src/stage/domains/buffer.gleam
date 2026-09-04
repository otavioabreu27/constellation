import gleam/list

/// A FIFO buffer with amortized constant-time appends.
pub opaque type Buffer(event) {
  Buffer(front: List(event), back: List(event))
}

/// Creates an empty event buffer.
pub fn new() -> Buffer(event) {
  Buffer(front: [], back: [])
}

/// Builds a buffer while preserving the order of the supplied events.
pub fn from_list(events: List(event)) -> Buffer(event) {
  Buffer(front: events, back: [])
}

/// Appends a batch without traversing events already buffered.
pub fn push(buffer: Buffer(event), events: List(event)) -> Buffer(event) {
  let Buffer(front: front, back: back) = buffer
  let updated_back = list.fold(events, back, fn(acc, event) { [event, ..acc] })
  Buffer(front: front, back: updated_back)
}

/// Reports whether no events are currently buffered.
pub fn is_empty(buffer: Buffer(event)) -> Bool {
  let Buffer(front: front, back: back) = buffer
  list.is_empty(front) && list.is_empty(back)
}

/// Returns buffered events in FIFO order.
pub fn to_list(buffer: Buffer(event)) -> List(event) {
  let Buffer(front: front, back: back) = buffer
  list.append(front, list.reverse(back))
}
