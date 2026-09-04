import constellation/core/dispatch
import constellation/core/model
import constellation/domains/effect.{type Effect, NotifyCancelled}
import constellation/domains/stage_error.{type StageError, UnknownSubscription}
import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/list

@internal
pub fn cancel(
  state: model.State(event),
  id: SubscriptionId,
) -> Result(#(model.State(event), List(Effect(event))), StageError) {
  case model.subscription(state, id) {
    Ok(_) -> cancel_active(state, id)
    Error(_) ->
      case model.is_cancelled(state, id) {
        True -> Ok(#(state, []))
        False -> Error(UnknownSubscription(id))
      }
  }
}

fn cancel_active(
  state: model.State(event),
  id: SubscriptionId,
) -> Result(#(model.State(event), List(Effect(event))), StageError) {
  let updated = model.remove_subscriptions(state, [id])
  let #(dispatched, effects) = dispatch.dispatch_buffer(updated)
  Ok(#(dispatched, [NotifyCancelled(subscription_id: id), ..effects]))
}

@internal
pub fn participant_down(
  state: model.State(event),
  participant_id: ParticipantId,
) -> Result(#(model.State(event), List(Effect(event))), StageError) {
  let ids = model.subscriptions_for_participant(state, participant_id)
  let updated = model.remove_subscriptions(state, ids)
  let #(dispatched, effects) = dispatch.dispatch_buffer(updated)
  let notifications =
    list.map(ids, fn(id) { NotifyCancelled(subscription_id: id) })
  Ok(#(dispatched, list.append(notifications, effects)))
}

@internal
pub fn shutdown(
  state: model.State(event),
) -> #(model.State(event), List(Effect(event))) {
  let ids = model.subscription_ids(state)
  let updated = model.remove_subscriptions(state, ids)
  let notifications =
    list.map(ids, fn(id) { NotifyCancelled(subscription_id: id) })
  #(updated, notifications)
}
