import constellation/domains/dispatcher/common
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/list
import gleam/option.{type Option, None, Some}

@internal
pub fn dispatch(
  targets: List(common.Target),
  events: List(event),
  select: fn(List(common.Target), event) -> Option(SubscriptionId),
) -> common.Dispatch(event) {
  dispatch_loop(targets, events, select, [], [])
}

fn dispatch_loop(
  targets: List(common.Target),
  events: List(event),
  select: fn(List(common.Target), event) -> Option(SubscriptionId),
  deliveries: List(common.Delivery(event)),
  remaining: List(event),
) -> common.Dispatch(event) {
  case events {
    [] ->
      common.Dispatch(
        targets: targets,
        deliveries: common.normalize_deliveries(deliveries),
        remaining: list.reverse(remaining),
      )
    [event, ..rest] ->
      case select(targets, event) {
        None ->
          dispatch_loop(targets, rest, select, deliveries, [event, ..remaining])
        Some(id) ->
          case consume_target(targets, id) {
            Error(_) ->
              dispatch_loop(targets, rest, select, deliveries, [
                event,
                ..remaining
              ])
            Ok(updated) ->
              dispatch_loop(
                updated,
                rest,
                select,
                common.add_delivery(deliveries, id, event),
                remaining,
              )
          }
      }
  }
}

fn consume_target(
  targets: List(common.Target),
  id: SubscriptionId,
) -> Result(List(common.Target), Nil) {
  case targets {
    [] -> Error(Nil)
    [target, ..rest] ->
      case common.subscription_id(target) == id && common.demand(target) > 0 {
        True -> Ok([common.decrease_demand(target), ..rest])
        False ->
          case consume_target(rest, id) {
            Error(_) -> Error(Nil)
            Ok(updated) -> Ok([target, ..updated])
          }
      }
  }
}
