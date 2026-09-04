import gleam/list

/// A FIFO buffer with amortized constant-time appends.
pub opaque type Buffer(event) {
  Buffer(front: List(event), back: List(event), size: Int)
}

/// Creates an empty event buffer.
pub fn new() -> Buffer(event) {
  Buffer(front: [], back: [], size: 0)
}

/// Builds a buffer while preserving the order of the supplied events.
pub fn from_list(events: List(event)) -> Buffer(event) {
  Buffer(front: events, back: [], size: list.length(events))
}

/// Appends a batch without traversing events already buffered.
pub fn push(buffer: Buffer(event), events: List(event)) -> Buffer(event) {
  let Buffer(front: front, back: back, size: size) = buffer
  let updated_back = list.fold(events, back, fn(acc, event) { [event, ..acc] })
  Buffer(front: front, back: updated_back, size: size + list.length(events))
}

/// Reports whether no events are currently buffered.
pub fn is_empty(buffer: Buffer(event)) -> Bool {
  buffer.size == 0
}

/// Returns buffered events in FIFO order.
pub fn to_list(buffer: Buffer(event)) -> List(event) {
  let Buffer(front: front, back: back, ..) = buffer
  list.append(front, list.reverse(back))
}

/// Returns the number of buffered events.
pub fn size(buffer: Buffer(event)) -> Int {
  buffer.size
}
