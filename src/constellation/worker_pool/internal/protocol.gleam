//// Atomic boundary between source reservations and the pure Stage.
//// No messages are sent here. The caller commits both states and then emits
//// effects, only when both transitions accept the supplied batch.

import constellation/core
import constellation/domains/command
import constellation/domains/effect
import constellation/source
import constellation/source/core as source_core
import gleam/list
import gleam/result

pub fn supply(
  stage: core.StageState(event),
  reservations: source_core.State,
  grant: source.DemandGrant,
  offset: Int,
  events: List(event),
) -> Result(
  #(
    core.StageState(event),
    source_core.State,
    List(effect.Effect(event)),
    source.SupplyResult,
  ),
  source.SourceError,
) {
  use supplied <- result.try(
    source_core.supply(
      reservations,
      source.grant_id(grant),
      offset,
      list.length(events),
    )
    |> result.map_error(source.SupplyProtocol),
  )
  let #(updated_reservations, outcome) = supplied
  case outcome {
    source.Duplicate | source.StaleGrant ->
      Ok(#(stage, reservations, [], outcome))
    source.Accepted(..) -> {
      use dispatched <- result.try(
        core.update(stage, command.Push(events))
        |> result.map_error(fn(_) { source.SourceUnavailable }),
      )
      let #(updated_stage, effects) = dispatched
      Ok(#(updated_stage, updated_reservations, effects, outcome))
    }
  }
}
