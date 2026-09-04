import constellation/domains/dispatcher/common.{type Dispatch, type Target}
import gleam/int
import gleam/list

@internal
pub fn dispatch(targets: List(Target), events: List(event)) -> Dispatch(event) {
  let count = broadcast_count(targets, list.length(events))
  let delivered = list.take(events, count)

  common.Dispatch(
    targets: list.map(targets, fn(target) { common.decrease_by(target, count) }),
    deliveries: list.map(targets, fn(target) {
      common.Delivery(
        subscription_id: common.subscription_id(target),
        events: delivered,
      )
    }),
    remaining: list.drop(events, count),
  )
}

// Strict broadcast can only cross the minimum-demand barrier of all targets.
fn broadcast_count(targets: List(Target), event_count: Int) -> Int {
  case targets {
    [] -> 0
    [first, ..rest] ->
      list.fold(
        rest,
        int.min(common.demand(first), event_count),
        fn(acc, target) { int.min(acc, common.demand(target)) },
      )
  }
}
