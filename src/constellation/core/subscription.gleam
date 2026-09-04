import constellation/core/dispatch
import constellation/core/model
import constellation/domains/effect.{type Effect}
import constellation/domains/stage_error.{
  type StageError, DuplicateSubscription, InvalidDemand,
}
import constellation/domains/subscription
import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}

@internal
pub fn subscribe(
  state: model.State(event),
  id: SubscriptionId,
  participant: ParticipantId,
  partition: Int,
) -> Result(#(model.State(event), List(Effect(event))), StageError) {
  case model.has_subscription_id(state, id) {
    True -> Error(DuplicateSubscription(id))
    False ->
      Ok(#(model.add_subscription(state, id, participant, partition), []))
  }
}

@internal
pub fn ask(
  state: model.State(event),
  id: SubscriptionId,
  amount: Int,
) -> Result(#(model.State(event), List(Effect(event))), StageError) {
  case model.subscription(state, id) {
    Error(error) -> Error(error)
    Ok(value) ->
      case subscription.add_demand(value, amount) {
        Error(subscription.InvalidDemand(invalid)) ->
          Error(InvalidDemand(invalid))
        Error(subscription.InsufficientDemand) -> Error(InvalidDemand(amount))
        Ok(updated) ->
          Ok(
            dispatch.dispatch_buffer(model.put_subscription(state, id, updated)),
          )
      }
  }
}
