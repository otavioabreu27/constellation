import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/string
import mist_dashboard/producer.{type Producer}
import stage/runtime
import stage/runtime/otp
import stage/value_objects/participant_id.{type ParticipantId}
import stage/value_objects/subscription_id.{type SubscriptionId}

const call_timeout = 5000

const producer_name = "counter-producer"

const consumer_name = "dashboard-consumer"

pub type Activity {
  Activity(
    sequence: Int,
    kind: String,
    source: String,
    target: String,
    amount: Int,
  )
}

pub type Snapshot {
  Snapshot(
    revision: Int,
    pushed: Int,
    received: Int,
    outstanding_demand: Int,
    buffered: Int,
    last_events: List(Int),
    activities: List(Activity),
    producer_pid: String,
    stage_pid: String,
    consumer_pid: String,
    active: Bool,
  )
}

pub opaque type Dashboard {
  Dashboard(Subject(Message))
}

type Message {
  Ask(amount: Int, reply: Subject(Result(Snapshot, String)))
  Push(amount: Int, reply: Subject(Result(Snapshot, String)))
  GetSnapshot(reply: Subject(Snapshot))
  StageMessage(runtime.ParticipantMessage(Int))
}

type State {
  State(
    stage: otp.Stage(Int),
    producer: Producer,
    subscription_id: SubscriptionId,
    revision: Int,
    pushed: Int,
    received: Int,
    outstanding_demand: Int,
    last_events: List(Int),
    activities: List(Activity),
    producer_pid: String,
    stage_pid: String,
    consumer_pid: String,
    active: Bool,
  )
}

pub fn start(
  stage: otp.Stage(Int),
  producer: Producer,
  subscription_id: SubscriptionId,
  participant_id: ParticipantId,
  producer_pid: String,
  stage_pid: String,
) -> actor.StartResult(Dashboard) {
  actor.new_with_initialiser(1000, fn(subject) {
    initialise(
      stage,
      producer,
      subscription_id,
      participant_id,
      producer_pid,
      stage_pid,
      subject,
    )
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

pub fn ask(dashboard: Dashboard, amount: Int) -> Result(Snapshot, String) {
  let Dashboard(subject) = dashboard
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    Ask(amount: amount, reply: reply)
  })
}

pub fn push(dashboard: Dashboard, amount: Int) -> Result(Snapshot, String) {
  let Dashboard(subject) = dashboard
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    Push(amount: amount, reply: reply)
  })
}

pub fn snapshot(dashboard: Dashboard) -> Snapshot {
  let Dashboard(subject) = dashboard
  actor.call(subject, waiting: call_timeout, sending: GetSnapshot)
}

fn initialise(
  stage: otp.Stage(Int),
  producer: Producer,
  subscription_id: SubscriptionId,
  participant_id: ParticipantId,
  producer_pid: String,
  stage_pid: String,
  subject: Subject(Message),
) {
  let recipient = process.new_subject()
  case otp.subscribe(stage, subscription_id, participant_id, 0, recipient) {
    Error(_) -> Error("could not subscribe dashboard consumer")
    Ok(_) -> {
      let selector =
        process.new_selector()
        |> process.select(subject)
        |> process.select_map(recipient, StageMessage)
      let state =
        State(
          stage: stage,
          producer: producer,
          subscription_id: subscription_id,
          revision: 3,
          pushed: 0,
          received: 0,
          outstanding_demand: 0,
          last_events: [],
          activities: [
            Activity(3, "PROCESS", consumer_name, "BEAM scheduler", 1),
            Activity(2, "PROCESS", "stage-actor", "BEAM scheduler", 1),
            Activity(1, "PROCESS", producer_name, "BEAM scheduler", 1),
          ],
          producer_pid: producer_pid,
          stage_pid: stage_pid,
          consumer_pid: string.inspect(process.self()),
          active: True,
        )
      actor.initialised(state)
      |> actor.selecting(selector)
      |> actor.returning(Dashboard(subject))
      |> Ok
    }
  }
}

fn handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    Ask(amount, reply) -> handle_ask(state, amount, reply)
    Push(amount, reply) -> handle_push(state, amount, reply)
    GetSnapshot(reply) -> {
      process.send(reply, to_snapshot(state))
      actor.continue(state)
    }
    StageMessage(message) -> handle_stage_message(state, message)
  }
}

fn handle_ask(
  state: State,
  amount: Int,
  reply: Subject(Result(Snapshot, String)),
) -> actor.Next(State, Message) {
  case amount > 0 && state.active {
    False -> reply_error(state, reply, "demand must be positive")
    True ->
      case otp.ask(state.stage, state.subscription_id, amount) {
        Error(_) -> reply_error(state, reply, "stage rejected demand")
        Ok(_) -> {
          let updated =
            State(
              ..state,
              revision: state.revision + 1,
              outstanding_demand: state.outstanding_demand + amount,
              activities: add_activity(
                state,
                "DEMAND",
                consumer_name,
                "stage-actor",
                amount,
              ),
            )
          process.send(reply, Ok(to_snapshot(updated)))
          actor.continue(updated)
        }
      }
  }
}

fn handle_push(
  state: State,
  amount: Int,
  reply: Subject(Result(Snapshot, String)),
) -> actor.Next(State, Message) {
  case amount > 0 {
    False -> reply_error(state, reply, "batch size must be positive")
    True -> push_batch(state, amount, reply)
  }
}

fn push_batch(
  state: State,
  amount: Int,
  reply: Subject(Result(Snapshot, String)),
) -> actor.Next(State, Message) {
  case producer.generate(state.producer, amount) {
    Error(_) -> reply_error(state, reply, "stage rejected event batch")
    Ok(_) -> {
      let updated =
        State(
          ..state,
          revision: state.revision + 1,
          pushed: state.pushed + amount,
          activities: add_activity(
            state,
            "PUSH",
            producer_name,
            "stage-buffer",
            amount,
          ),
        )
      process.send(reply, Ok(to_snapshot(updated)))
      actor.continue(updated)
    }
  }
}

fn handle_stage_message(
  state: State,
  message: runtime.ParticipantMessage(Int),
) -> actor.Next(State, Message) {
  case message {
    runtime.Events(subscription_id: subscription, events: events) -> {
      let count = list.length(events)
      let updated =
        State(
          ..state,
          revision: state.revision + 1,
          received: state.received + count,
          outstanding_demand: int.max(0, state.outstanding_demand - count),
          last_events: keep_latest(state.last_events, events),
          activities: add_activity(
            state,
            "DELIVER",
            "stage-dispatcher",
            consumer_name,
            count,
          ),
        )
      otp.consumed(state.stage, subscription, count)
      actor.continue(updated)
    }
    runtime.Cancelled(..) ->
      actor.continue(
        State(
          ..state,
          revision: state.revision + 1,
          activities: add_activity(
            state,
            "CANCEL",
            "stage-runtime",
            consumer_name,
            1,
          ),
          active: False,
        ),
      )
  }
}

fn keep_latest(current: List(Int), events: List(Int)) -> List(Int) {
  list.append(current, events)
  |> list.reverse
  |> list.take(18)
  |> list.reverse
}

fn add_activity(
  state: State,
  kind: String,
  source: String,
  target: String,
  amount: Int,
) -> List(Activity) {
  [
    Activity(
      sequence: state.revision + 1,
      kind: kind,
      source: source,
      target: target,
      amount: amount,
    ),
    ..state.activities
  ]
  |> list.take(24)
}

fn reply_error(
  state: State,
  reply: Subject(Result(Snapshot, String)),
  reason: String,
) -> actor.Next(State, Message) {
  process.send(reply, Error(reason))
  actor.continue(state)
}

fn to_snapshot(state: State) -> Snapshot {
  Snapshot(
    revision: state.revision,
    pushed: state.pushed,
    received: state.received,
    outstanding_demand: state.outstanding_demand,
    buffered: state.pushed - state.received,
    last_events: state.last_events,
    activities: state.activities,
    producer_pid: state.producer_pid,
    stage_pid: state.stage_pid,
    consumer_pid: state.consumer_pid,
    active: state.active,
  )
}
