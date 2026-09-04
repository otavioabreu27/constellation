import constellation/domains/dispatcher/common.{
  type Delivery, type Dispatch, type Target,
}
import gleam/list

@internal
pub fn dispatch(
  targets: List(Target),
  events: List(event),
  key: fn(event) -> Int,
) -> Dispatch(event) {
  loop(targets, events, key, [], [])
}

fn loop(
  targets: List(Target),
  events: List(event),
  key: fn(event) -> Int,
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
      case take_target(targets, key(event), []) {
        Error(_) -> loop(targets, rest, key, deliveries, [event, ..remaining])
        Ok(#(selected, reordered)) -> {
          let updated_targets =
            list.append(reordered, [common.decrease_demand(selected)])
          let next_deliveries =
            common.add_delivery(
              deliveries,
              common.subscription_id(selected),
              event,
            )
          loop(updated_targets, rest, key, next_deliveries, remaining)
        }
      }
  }
}

// Rotating matching targets independently preserves fairness per partition.
fn take_target(
  targets: List(Target),
  partition: Int,
  before: List(Target),
) -> Result(#(Target, List(Target)), Nil) {
  case targets {
    [] -> Error(Nil)
    [first, ..rest] ->
      case common.partition(first) == partition && common.demand(first) > 0 {
        True -> Ok(#(first, list.append(rest, list.reverse(before))))
        False -> take_target(rest, partition, [first, ..before])
      }
  }
}
