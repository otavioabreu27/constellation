import constellation/domains/effect.{type Effect}
import constellation/runtime/registry.{type Registry, type RegistryError}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/list
import gleam/result

@internal
pub type RoutedEffect(event, participant) {
  EventsRouted(
    to: participant,
    subscription_id: SubscriptionId,
    events: List(event),
  )
  CancellationRouted(to: participant, subscription_id: SubscriptionId)
}

@internal
pub fn resolve(
  registry: Registry(participant),
  effects: List(Effect(event)),
) -> Result(
  #(Registry(participant), List(RoutedEffect(event, participant))),
  RegistryError,
) {
  resolve_loop(registry, effects, [])
}

fn resolve_loop(
  registry: Registry(participant),
  effects: List(Effect(event)),
  routed: List(RoutedEffect(event, participant)),
) -> Result(
  #(Registry(participant), List(RoutedEffect(event, participant))),
  RegistryError,
) {
  case effects {
    [] -> Ok(#(registry, list.reverse(routed)))
    [first, ..rest] -> {
      use #(updated, routed_effect) <- result.try(resolve_one(registry, first))
      resolve_loop(updated, rest, [routed_effect, ..routed])
    }
  }
}

fn resolve_one(
  registry: Registry(participant),
  routed_effect: Effect(event),
) -> Result(
  #(Registry(participant), RoutedEffect(event, participant)),
  RegistryError,
) {
  case routed_effect {
    effect.SendEvents(subscription_id: id, events: events) -> {
      use participant <- result.try(registry.participant_for(registry, id))
      Ok(#(
        registry,
        EventsRouted(to: participant, subscription_id: id, events: events),
      ))
    }
    effect.NotifyCancelled(subscription_id: id) -> {
      use participant <- result.try(registry.participant_for(registry, id))
      Ok(#(
        registry.remove_subscription(registry, id),
        CancellationRouted(to: participant, subscription_id: id),
      ))
    }
  }
}
