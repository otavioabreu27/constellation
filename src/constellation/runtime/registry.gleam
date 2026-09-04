import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/dict
import gleam/list
import gleam/result

@internal
pub opaque type Registry(participant) {
  Registry(
    participants: dict.Dict(ParticipantId, participant),
    subscriptions: dict.Dict(SubscriptionId, ParticipantId),
  )
}

@internal
pub type RegistryError {
  ParticipantAlreadyRegistered(ParticipantId)
  MissingSubscriptionRoute(SubscriptionId)
  MissingParticipantRoute(ParticipantId)
}

@internal
pub fn new() -> Registry(participant) {
  Registry(participants: dict.new(), subscriptions: dict.new())
}

@internal
pub fn register(
  registry: Registry(participant),
  subscription_id: SubscriptionId,
  participant_id: ParticipantId,
  participant: participant,
) -> Result(Registry(participant), RegistryError) {
  let Registry(participants: participants, subscriptions: subscriptions) =
    registry
  case dict.get(participants, participant_id) {
    Ok(registered) if registered != participant ->
      Error(ParticipantAlreadyRegistered(participant_id))
    _ ->
      Ok(Registry(
        participants: dict.insert(participants, participant_id, participant),
        subscriptions: dict.insert(
          subscriptions,
          subscription_id,
          participant_id,
        ),
      ))
  }
}

@internal
pub fn participant_for(
  registry: Registry(participant),
  subscription_id: SubscriptionId,
) -> Result(participant, RegistryError) {
  let Registry(participants: participants, subscriptions: subscriptions) =
    registry
  use participant_id <- result.try(
    dict.get(subscriptions, subscription_id)
    |> result.map_error(fn(_) { MissingSubscriptionRoute(subscription_id) }),
  )
  dict.get(participants, participant_id)
  |> result.map_error(fn(_) { MissingParticipantRoute(participant_id) })
}

@internal
pub fn remove_subscription(
  registry: Registry(participant),
  subscription_id: SubscriptionId,
) -> Registry(participant) {
  let Registry(participants: participants, subscriptions: subscriptions) =
    registry
  case dict.get(subscriptions, subscription_id) {
    Error(_) -> registry
    Ok(participant_id) -> {
      let subscriptions = dict.delete(subscriptions, subscription_id)
      let participants = case
        participant_is_registered(subscriptions, participant_id)
      {
        True -> participants
        False -> dict.delete(participants, participant_id)
      }
      Registry(participants: participants, subscriptions: subscriptions)
    }
  }
}

@internal
pub fn has_participant(
  registry: Registry(participant),
  participant_id: ParticipantId,
) -> Bool {
  let Registry(participants: participants, ..) = registry
  dict.has_key(participants, participant_id)
}

@internal
pub fn has_subscription(
  registry: Registry(participant),
  subscription_id: SubscriptionId,
) -> Bool {
  let Registry(subscriptions: subscriptions, ..) = registry
  dict.has_key(subscriptions, subscription_id)
}

fn participant_is_registered(
  subscriptions: dict.Dict(SubscriptionId, ParticipantId),
  participant_id: ParticipantId,
) -> Bool {
  list.any(dict.to_list(subscriptions), fn(pair) {
    let #(_, registered) = pair
    registered == participant_id
  })
}
