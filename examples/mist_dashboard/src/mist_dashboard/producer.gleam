import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor
import stage/runtime
import stage/runtime/otp

const call_timeout = 5000

pub opaque type Producer {
  Producer(Subject(Message))
}

type Message {
  Generate(amount: Int, reply: Subject(Result(Nil, runtime.RuntimeError)))
}

type State {
  State(stage: otp.Stage(Int), next_event: Int)
}

/// Starts a producer process that owns integer event generation.
pub fn start(stage: otp.Stage(Int)) -> actor.StartResult(Producer) {
  case
    actor.new(State(stage: stage, next_event: 1))
    |> actor.on_message(handle_message)
    |> actor.start
  {
    Ok(actor.Started(pid: pid, data: subject)) ->
      Ok(actor.Started(pid: pid, data: Producer(subject)))
    Error(error) -> Error(error)
  }
}

/// Generates a batch inside the producer process and pushes it to the stage.
pub fn generate(
  producer: Producer,
  amount: Int,
) -> Result(Nil, runtime.RuntimeError) {
  let Producer(subject) = producer
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    Generate(amount: amount, reply: reply)
  })
}

fn handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    Generate(amount, reply) -> generate_batch(state, amount, reply)
  }
}

fn generate_batch(
  state: State,
  amount: Int,
  reply: Subject(Result(Nil, runtime.RuntimeError)),
) -> actor.Next(State, Message) {
  let events =
    int.range(
      from: state.next_event,
      to: state.next_event + amount,
      with: [],
      run: fn(acc, event) { [event, ..acc] },
    )
    |> list.reverse
  case otp.push(state.stage, events) {
    Error(error) -> {
      process.send(reply, Error(error))
      actor.continue(state)
    }
    Ok(_) -> {
      process.send(reply, Ok(Nil))
      actor.continue(State(..state, next_event: state.next_event + amount))
    }
  }
}
