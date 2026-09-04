import constellation/domains/dispatcher
import constellation/value_objects/subscription_id
import gleam/int
import gleam/list
import gleam/option.{Some}

fn id(value: String) {
  let assert Ok(value) = subscription_id.new(value)
  value
}

fn demand_target(value: String, demand: Int, partition: Int) {
  let target = dispatcher.target(id(value), partition)
  let assert Ok(target) = dispatcher.with_demand(target, demand)
  target
}

pub fn demand_dispatches_round_robin_test() {
  let first = demand_target("a", 2, 0)
  let second = demand_target("b", 2, 0)
  let result =
    dispatcher.dispatch(dispatcher.demand_strategy(), [first, second], [
      1,
      2,
      3,
      4,
      5,
    ])

  assert result
    == dispatcher.DispatchResult(
      targets: [demand_target("a", 0, 0), demand_target("b", 0, 0)],
      deliveries: [
        dispatcher.Delivery(subscription_id: id("a"), events: [1, 3]),
        dispatcher.Delivery(subscription_id: id("b"), events: [2, 4]),
      ],
      remaining: [5],
    )
}

pub fn broadcast_requires_capacity_from_all_targets_test() {
  let first = demand_target("a", 3, 0)
  let second = demand_target("b", 1, 0)
  let result =
    dispatcher.dispatch(dispatcher.broadcast_strategy(), [first, second], [1, 2])

  assert result
    == dispatcher.DispatchResult(
      targets: [demand_target("a", 2, 0), demand_target("b", 0, 0)],
      deliveries: [
        dispatcher.Delivery(subscription_id: id("a"), events: [1]),
        dispatcher.Delivery(subscription_id: id("b"), events: [1]),
      ],
      remaining: [2],
    )
}

pub fn partition_sends_events_to_matching_partition_test() {
  let first = demand_target("a", 2, 0)
  let second = demand_target("b", 2, 1)
  let result =
    dispatcher.dispatch(
      dispatcher.partition_strategy(fn(value) { value % 2 }),
      [first, second],
      [0, 1, 2, 3, 4],
    )

  assert result
    == dispatcher.DispatchResult(
      targets: [demand_target("a", 0, 0), demand_target("b", 0, 1)],
      deliveries: [
        dispatcher.Delivery(subscription_id: id("a"), events: [0, 2]),
        dispatcher.Delivery(subscription_id: id("b"), events: [1, 3]),
      ],
      remaining: [4],
    )
}

pub fn target_rejects_negative_demand_test() {
  let target = dispatcher.target(id("a"), 0)
  assert dispatcher.with_demand(target, -1)
    == Error(dispatcher.InvalidDemand(-1))
}

pub fn demand_preserves_all_events_when_capacity_ends_test() {
  let result =
    dispatcher.dispatch(
      dispatcher.demand_strategy(),
      [demand_target("a", 2, 0)],
      [
        1,
        2,
        3,
        4,
      ],
    )

  assert result.remaining == [3, 4]
  assert result.deliveries
    == [
      dispatcher.Delivery(subscription_id: id("a"), events: [1, 2]),
    ]
}

pub fn demand_dispatches_large_batch_without_losing_events_test() {
  let events =
    int.range(from: 0, to: 10_000, with: [], run: fn(acc, event) {
      [event, ..acc]
    })
    |> list.reverse
  let result =
    dispatcher.dispatch(
      dispatcher.demand_strategy(),
      [demand_target("a", 10_000, 0)],
      events,
    )
  let assert [dispatcher.Delivery(events: delivered, ..)] = result.deliveries

  assert delivered == events
  assert result.remaining == []
}

pub fn custom_strategy_can_extend_dispatch_without_library_changes_test() {
  let target = demand_target("a", 2, 0)
  let strategy = dispatcher.custom_strategy(fn(_, _) { Some(id("a")) })

  assert dispatcher.has_capacity(strategy, [target]) == True
  assert dispatcher.dispatch(strategy, [target], [1, 2])
    == dispatcher.DispatchResult(
      targets: [demand_target("a", 0, 0)],
      deliveries: [
        dispatcher.Delivery(subscription_id: id("a"), events: [1, 2]),
      ],
      remaining: [],
    )
}

pub fn custom_strategy_cannot_deliver_to_unknown_target_test() {
  let target = demand_target("a", 1, 0)
  let strategy =
    dispatcher.custom_strategy(fn(_, _) { Some(id("not-registered")) })

  assert dispatcher.dispatch(strategy, [target], [1])
    == dispatcher.DispatchResult(targets: [target], deliveries: [], remaining: [
      1,
    ])
}
