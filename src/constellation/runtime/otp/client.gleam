import gleam/erlang/process.{type ExitReason, type Subject}

@internal
pub type Failure {
  Timeout
  StageUnavailable(ExitReason)
}

type Response(reply) {
  Reply(reply)
  Down(process.Down)
}

@internal
pub fn call(
  subject: Subject(message),
  timeout: Int,
  make_message: fn(Subject(reply)) -> message,
) -> Result(reply, Failure) {
  case process.subject_owner(subject) {
    Error(_) -> Error(StageUnavailable(process.Normal))
    Ok(pid) -> {
      let monitor = process.monitor(pid)
      let reply = process.new_subject()
      let selector =
        process.new_selector()
        |> process.select_map(reply, Reply)
        |> process.select_specific_monitor(monitor, Down)
      process.send(subject, make_message(reply))
      let result = case process.selector_receive(selector, timeout) {
        Error(_) -> Error(Timeout)
        Ok(Reply(value)) -> Ok(value)
        Ok(Down(process.ProcessDown(reason: reason, ..))) ->
          Error(StageUnavailable(reason))
        Ok(Down(process.PortDown(reason: reason, ..))) ->
          Error(StageUnavailable(reason))
      }
      process.demonitor_process(monitor)
      result
    }
  }
}
