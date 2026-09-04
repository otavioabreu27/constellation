import constellation/domains/dispatcher/common.{
  type Delivery, type Dispatch, type Target,
}
import gleam/list

@internal
pub fn dispatch(targets: List(Target), events: List(event)) -> Dispatch(event) {
  loop(targets, events, [], [])
}

fn loop(
  targets: List(Target),
  events: List(event),
  deliveries: List(Delivery(event)),
  remaining: List(event),
) -> Dispatch(event) {
  case events {
    [] ->
      common.Dispatch(
        targets: targets,
        deliveries: common.normalize_deliveries(deliveries),
        remaining: list.reverse(remaining),
      )
    [event, ..rest] ->
      case select_target(targets, []) {
        Error(_) ->
          common.Dispatch(
            targets: targets,
            deliveries: common.normalize_deliveries(deliveries),
            remaining: list.append(list.reverse(remaining), [event, ..rest]),
          )
        Ok(#(selected, reordered)) -> {
          let updated_targets =
            list.append(reordered, [common.decrease_demand(selected)])
          let next_deliveries =
            common.add_delivery(
              deliveries,
              common.subscription_id(selected),
              event,
            )
          loop(updated_targets, rest, next_deliveries, remaining)
        }
      }
  }
}

// Moving each selected target to the end preserves round-robin target order.
fn select_target(
  targets: List(Target),
  before: List(Target),
) -> Result(#(Target, List(Target)), Nil) {
  case targets {
    [] -> Error(Nil)
    [first, ..rest] ->
      case common.demand(first) > 0 {
        True -> Ok(#(first, list.append(rest, list.reverse(before))))
        False -> select_target(rest, [first, ..before])
      }
  }
}
