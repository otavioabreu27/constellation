import gleam/dict
import gleam/int
import gleam/list
import stage/domains/command.{
  type Command, Ask, Cancel, ParticipantDown, Push, Subscribe,
}
import stage/domains/effect.{type Effect, NotifyCancelled, SendEvents}
import stage/domains/stage_error.{
  type StageError, DuplicateSubscription, InvalidDemand,
  MultipleSubscriptionsNotSupported, SubscriptionCancelled, UnknownSubscription,
}
import stage/domains/stage_state.{type StageState, StageState}
import stage/domains/subscription
import stage/value_objects/subscription_id.{type SubscriptionId}

pub fn new() -> StageState(event) {
  StageState(subscriptions: dict.new(), buffer: [])
}

pub fn update(
  state: StageState(event),
  command: Command(event),
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let StageState(subscriptions: subscriptions, buffer: buffer) = state

  case command {
    Subscribe(id: id, participant_id: participant) ->
      case dict.get(subscriptions, id) {
        Ok(_) -> Error(DuplicateSubscription(id))
        Error(_) ->
          case has_active_subscription(subscriptions) {
            False ->
              Ok(
                #(
                  StageState(
                    subscriptions: dict.insert(
                      subscriptions,
                      id,
                      subscription.new(id, participant),
                    ),
                    buffer: buffer,
                  ),
                  [],
                ),
              )
            True -> Error(MultipleSubscriptionsNotSupported)
          }
      }

    Ask(subscription_id: id, amount: amount) ->
      case dict.get(subscriptions, id) {
        Error(_) -> Error(UnknownSubscription(id))
        Ok(value) ->
          case subscription.add_demand(value, amount) {
            Error(subscription.InvalidDemand(invalid)) ->
              Error(InvalidDemand(invalid))
            Error(subscription.SubscriptionCancelled) ->
              Error(SubscriptionCancelled(id))
            Error(subscription.InsufficientDemand) ->
              Error(InvalidDemand(amount))
            Ok(updated) -> apply_ask(subscriptions, buffer, id, updated)
          }
      }

    Push(events: events) ->
      case sole_subscription(subscriptions) {
        Error(_) ->
          Ok(
            #(
              StageState(
                subscriptions: subscriptions,
                buffer: list.append(buffer, events),
              ),
              [],
            ),
          )
        Ok(#(id, value)) -> apply_push(subscriptions, buffer, events, id, value)
      }

    Cancel(subscription_id: id) ->
      case dict.get(subscriptions, id) {
        Error(_) -> Error(UnknownSubscription(id))
        Ok(value) -> cancel_subscription(subscriptions, buffer, id, value)
      }

    ParticipantDown(participant_id: participant_id) ->
      case sole_subscription(subscriptions) {
        Error(_) -> Ok(#(state, []))
        Ok(#(id, value)) ->
          case subscription.participant_id(value) == participant_id {
            True -> cancel_subscription(subscriptions, buffer, id, value)
            False -> Ok(#(state, []))
          }
      }
  }
}

fn sole_subscription(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
) -> Result(#(SubscriptionId, subscription.Subscription), Nil) {
  case
    list.filter(dict.to_list(subscriptions), fn(pair) {
      let #(_, value) = pair
      subscription.status(value) == subscription.Active
    })
  {
    [value] -> Ok(value)
    _ -> Error(Nil)
  }
}

fn has_active_subscription(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
) -> Bool {
  list.any(dict.to_list(subscriptions), fn(pair) {
    let #(_, value) = pair
    subscription.status(value) == subscription.Active
  })
}

fn cancel_subscription(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  buffer: List(event),
  id: SubscriptionId,
  value: subscription.Subscription,
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  case subscription.status(value) {
    subscription.Cancelled ->
      Ok(#(StageState(subscriptions: subscriptions, buffer: buffer), []))
    subscription.Active ->
      Ok(
        #(
          StageState(
            subscriptions: dict.insert(
              subscriptions,
              id,
              subscription.cancel(value),
            ),
            buffer: buffer,
          ),
          [NotifyCancelled(subscription_id: id)],
        ),
      )
  }
}

fn apply_ask(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  buffer: List(event),
  id: SubscriptionId,
  value: subscription.Subscription,
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let #(updated, emitted, remaining) = drain(value, buffer)
  Ok(#(
    StageState(
      subscriptions: dict.insert(subscriptions, id, updated),
      buffer: remaining,
    ),
    effects_for(id, emitted),
  ))
}

fn apply_push(
  subscriptions: dict.Dict(SubscriptionId, subscription.Subscription),
  buffer: List(event),
  events: List(event),
  id: SubscriptionId,
  value: subscription.Subscription,
) -> Result(#(StageState(event), List(Effect(event))), StageError) {
  let #(updated, emitted, remaining) = drain(value, list.append(buffer, events))
  Ok(#(
    StageState(
      subscriptions: dict.insert(subscriptions, id, updated),
      buffer: remaining,
    ),
    effects_for(id, emitted),
  ))
}

fn drain(
  value: subscription.Subscription,
  events: List(event),
) -> #(subscription.Subscription, List(event), List(event)) {
  let count = int.min(subscription.demand(value), list.length(events))
  case count {
    0 -> #(value, [], events)
    _ -> drain_available(value, events, count)
  }
}

fn drain_available(
  value: subscription.Subscription,
  events: List(event),
  count: Int,
) -> #(subscription.Subscription, List(event), List(event)) {
  let emitted = list.take(events, count)
  let remaining = list.drop(events, count)
  let assert Ok(updated) = subscription.consume_demand(value, count)
  #(updated, emitted, remaining)
}

fn effects_for(id: SubscriptionId, events: List(event)) -> List(Effect(event)) {
  case events {
    [] -> []
    _ -> [SendEvents(subscription_id: id, events: events)]
  }
}
