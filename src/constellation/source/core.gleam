//// Pure capacity reservation state for asynchronous sources.

import constellation/source.{type SupplyError, type SupplyResult}
import gleam/list

type Grant {
  Grant(id: Int, amount: Int, supplied: Int)
}

type Status {
  Available
  Unavailable
  Stopped
}

@internal
pub opaque type State {
  State(
    status: Status,
    grants: List(Grant),
    completed: List(#(Int, Int)),
    next_id: Int,
  )
}

@internal
pub type Action {
  GrantCapacity(id: Int, amount: Int)
  RevokeGrant(id: Int, amount: Int)
  StopSource
}

@internal
pub fn new() -> State {
  State(status: Available, grants: [], completed: [], next_id: 1)
}

@internal
pub fn reconcile(state: State, capacity: Int) -> #(State, List(Action)) {
  case state.status {
    Unavailable | Stopped -> #(state, [])
    Available ->
      reconcile_available(state, case capacity < 0 {
        True -> 0
        False -> capacity
      })
  }
}

@internal
pub fn supply(
  state: State,
  id: Int,
  offset: Int,
  event_count: Int,
) -> Result(#(State, SupplyResult), SupplyError) {
  case take_grant(state.grants, id, []) {
    Error(_) ->
      case list.key_find(state.completed, id) {
        Ok(_) -> Ok(#(state, source.Duplicate))
        Error(_) -> Ok(#(state, source.StaleGrant))
      }
    Ok(#(grant, rest)) -> supply_grant(state, grant, rest, offset, event_count)
  }
}

@internal
pub fn unavailable(state: State) -> #(State, List(Action)) {
  let actions = revoke_actions(state.grants)
  #(State(..state, status: Unavailable, grants: []), actions)
}

@internal
pub fn available(state: State, capacity: Int) -> #(State, List(Action)) {
  case state.status {
    Stopped -> #(state, [])
    Available | Unavailable ->
      reconcile(State(..state, status: Available), capacity)
  }
}

@internal
pub fn shutdown(state: State) -> #(State, List(Action)) {
  let actions = list.append(revoke_actions(state.grants), [StopSource])
  #(State(..state, status: Stopped, grants: []), actions)
}

@internal
pub fn pending(state: State) -> Int {
  list.fold(state.grants, 0, fn(total, grant) {
    total + grant.amount - grant.supplied
  })
}

fn reconcile_available(state: State, capacity: Int) -> #(State, List(Action)) {
  let reserved = pending(state)
  case capacity == reserved {
    True -> #(state, [])
    False ->
      case capacity > reserved {
        True -> grant(state, capacity - reserved, [])
        False -> {
          let actions = revoke_actions(state.grants)
          let cleared = State(..state, grants: [])
          case capacity {
            0 -> #(cleared, actions)
            _ -> grant(cleared, capacity, actions)
          }
        }
      }
  }
}

fn grant(
  state: State,
  amount: Int,
  actions: List(Action),
) -> #(State, List(Action)) {
  let id = state.next_id
  #(
    State(
      ..state,
      grants: list.append(state.grants, [Grant(id, amount, 0)]),
      next_id: id + 1,
    ),
    list.append(actions, [GrantCapacity(id, amount)]),
  )
}

fn supply_grant(
  state: State,
  grant: Grant,
  rest: List(Grant),
  offset: Int,
  event_count: Int,
) -> Result(#(State, SupplyResult), SupplyError) {
  case offset < grant.supplied {
    True ->
      case offset + event_count <= grant.supplied {
        True -> Ok(#(state, source.Duplicate))
        False ->
          Error(source.OffsetOverlap(grant.supplied, offset + event_count))
      }
    False ->
      case offset > grant.supplied {
        True -> Error(source.OffsetGap(grant.supplied, offset))
        False -> accept_supply(state, grant, rest, event_count)
      }
  }
}

fn accept_supply(
  state: State,
  grant: Grant,
  rest: List(Grant),
  event_count: Int,
) -> Result(#(State, SupplyResult), SupplyError) {
  let remaining = grant.amount - grant.supplied
  case event_count > remaining {
    True -> Error(source.GrantExceeded(remaining, event_count))
    False -> {
      let supplied = grant.supplied + event_count
      let grants = case supplied == grant.amount {
        True -> rest
        False -> insert_grant(rest, Grant(..grant, supplied: supplied))
      }
      let completed = case supplied == grant.amount {
        True -> remember_completed(state.completed, grant.id, grant.amount)
        False -> state.completed
      }
      Ok(#(
        State(..state, grants: grants, completed: completed),
        source.Accepted(
          next_offset: supplied,
          remaining: grant.amount - supplied,
        ),
      ))
    }
  }
}

fn remember_completed(
  completed: List(#(Int, Int)),
  id: Int,
  amount: Int,
) -> List(#(Int, Int)) {
  [#(id, amount), ..completed]
  |> list.take(64)
}

fn take_grant(
  grants: List(Grant),
  id: Int,
  before: List(Grant),
) -> Result(#(Grant, List(Grant)), Nil) {
  case grants {
    [] -> Error(Nil)
    [grant, ..rest] ->
      case grant.id == id {
        True -> Ok(#(grant, list.append(list.reverse(before), rest)))
        False -> take_grant(rest, id, [grant, ..before])
      }
  }
}

fn insert_grant(grants: List(Grant), updated: Grant) -> List(Grant) {
  case grants {
    [] -> [updated]
    [grant, ..rest] ->
      case updated.id < grant.id {
        True -> [updated, grant, ..rest]
        False -> [grant, ..insert_grant(rest, updated)]
      }
  }
}

fn revoke_actions(grants: List(Grant)) -> List(Action) {
  list.map(grants, fn(grant) {
    RevokeGrant(grant.id, grant.amount - grant.supplied)
  })
}
