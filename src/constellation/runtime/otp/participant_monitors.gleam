import constellation/runtime
import constellation/value_objects/participant_id.{type ParticipantId}
import gleam/erlang/process.{type Monitor, type Subject}
import gleam/list

type ParticipantMonitor {
  ParticipantMonitor(participant_id: ParticipantId, monitor: Monitor)
}

@internal
pub opaque type Registry {
  Registry(monitors: List(ParticipantMonitor))
}

@internal
pub fn new() -> Registry {
  Registry([])
}

@internal
pub fn ensure(
  registry: Registry,
  participant_id: ParticipantId,
  recipient: Subject(runtime.ParticipantMessage(event)),
) -> Registry {
  let Registry(monitors) = registry
  case
    list.any(monitors, fn(registered) {
      registered.participant_id == participant_id
    })
  {
    True -> registry
    False ->
      case process.subject_owner(recipient) {
        Error(_) -> registry
        Ok(pid) ->
          Registry([
            ParticipantMonitor(participant_id, process.monitor(pid)),
            ..monitors
          ])
      }
  }
}

@internal
pub fn remove_unused(
  registry: Registry,
  updated: runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
) -> Registry {
  let Registry(monitors) = registry
  Registry(remove_unused_monitors(monitors, updated))
}

@internal
pub fn take_down(
  registry: Registry,
  monitor: Monitor,
) -> Result(#(Registry, ParticipantId), Nil) {
  let Registry(monitors) = registry
  case list.find(monitors, fn(registered) { registered.monitor == monitor }) {
    Error(_) -> Error(Nil)
    Ok(registered) ->
      Ok(#(
        Registry(
          list.filter(monitors, fn(candidate) { candidate.monitor != monitor }),
        ),
        registered.participant_id,
      ))
  }
}

fn remove_unused_monitors(
  monitors: List(ParticipantMonitor),
  updated: runtime.Runtime(event, Subject(runtime.ParticipantMessage(event))),
) -> List(ParticipantMonitor) {
  case monitors {
    [] -> []
    [registered, ..rest] ->
      case runtime.has_participant(updated, registered.participant_id) {
        True -> [registered, ..remove_unused_monitors(rest, updated)]
        False -> {
          process.demonitor_process(registered.monitor)
          remove_unused_monitors(rest, updated)
        }
      }
  }
}
