import constellation/runtime
import constellation/runtime/otp
import constellation/value_objects/participant_id.{type ParticipantId}
import constellation/value_objects/subscription_id.{type SubscriptionId}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/string
import mist_dashboard/producer.{type Producer}

const call_timeout = 5000

const producer_name = "counter-producer"

const consumer_name = "dashboard-consumer"

const run_tick_milliseconds = 70

const run_push_batch = 10_000

const run_demand_batch = 5000

pub const buffer_capacity = 80_000

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
    running: Bool,
    run_total: Int,
    run_pushed: Int,
    run_received: Int,
    rejected_pushes: Int,
    backpressured: Bool,
  )
}

pub opaque type Dashboard {
  Dashboard(Subject(Message))
}

type Message {
  Ask(amount: Int, reply: Subject(Result(Snapshot, String)))
  Push(amount: Int, reply: Subject(Result(Snapshot, String)))
  StartRun(total: Int, reply: Subject(Result(Snapshot, String)))
  StopRun(reply: Subject(Snapshot))
  RunTick
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
    subject: Subject(Message),
    running: Bool,
    run_total: Int,
    run_pushed: Int,
    run_received: Int,
    rejected_pushes: Int,
    backpressured: Bool,
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

pub fn start_run(dashboard: Dashboard, total: Int) -> Result(Snapshot, String) {
  let Dashboard(subject) = dashboard
  actor.call(subject, waiting: call_timeout, sending: fn(reply) {
    StartRun(total: total, reply: reply)
  })
}

pub fn stop_run(dashboard: Dashboard) -> Snapshot {
  let Dashboard(subject) = dashboard
  actor.call(subject, waiting: call_timeout, sending: StopRun)
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
          subject: subject,
          running: False,
          run_total: 0,
          run_pushed: 0,
          run_received: 0,
          rejected_pushes: 0,
          backpressured: False,
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
    StartRun(total, reply) -> handle_start_run(state, total, reply)
    StopRun(reply) -> {
      let updated = State(..state, running: False, backpressured: False)
      process.send(reply, to_snapshot(updated))
      actor.continue(updated)
    }
    RunTick -> handle_run_tick(state)
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
  case amount > 0 && state.active && !state.running {
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
  case amount > 0 && !state.running {
    False -> reply_error(state, reply, "batch size must be positive")
    True -> push_batch(state, amount, reply)
  }
}

fn handle_start_run(
  state: State,
  total: Int,
  reply: Subject(Result(Snapshot, String)),
) -> actor.Next(State, Message) {
  case total > 0 && !state.running && buffered(state) == 0 {
    False -> reply_error(state, reply, "finish or drain the current run first")
    True -> {
      let updated =
        State(
          ..state,
          revision: state.revision + 1,
          running: True,
          run_total: total,
          run_pushed: 0,
          run_received: 0,
          rejected_pushes: 0,
          backpressured: False,
          activities: add_activity(
            state,
            "RUN",
            "load-generator",
            "stage-actor",
            total,
          ),
        )
      let _ = process.send_after(updated.subject, 10, RunTick)
      process.send(reply, Ok(to_snapshot(updated)))
      actor.continue(updated)
    }
  }
}

fn handle_run_tick(state: State) -> actor.Next(State, Message) {
  case state.running {
    False -> actor.continue(state)
    True -> {
      let updated = state |> push_run_batch |> request_run_demand
      let _ =
        process.send_after(updated.subject, run_tick_milliseconds, RunTick)
      actor.continue(updated)
    }
  }
}

fn push_run_batch(state: State) -> State {
  let amount = int.min(run_push_batch, state.run_total - state.run_pushed)
  case amount <= 0 {
    True -> state
    False ->
      case producer.generate(state.producer, amount) {
        Error(_) ->
          State(
            ..state,
            revision: state.revision + 1,
            rejected_pushes: state.rejected_pushes + 1,
            backpressured: True,
            activities: add_activity(
              state,
              "PRESSURE",
              "stage-buffer",
              producer_name,
              buffered(state),
            ),
          )
        Ok(_) ->
          State(
            ..state,
            revision: state.revision + 1,
            pushed: state.pushed + amount,
            run_pushed: state.run_pushed + amount,
            backpressured: False,
            activities: add_activity(
              state,
              "PUSH",
              producer_name,
              "stage-buffer",
              amount,
            ),
          )
      }
  }
}

fn request_run_demand(state: State) -> State {
  let available =
    state.run_pushed - state.run_received - state.outstanding_demand
  let amount = int.min(run_demand_batch, int.max(0, available))
  case amount <= 0 {
    True -> state
    False ->
      case otp.ask(state.stage, state.subscription_id, amount) {
        Error(_) -> state
        Ok(_) ->
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
      }
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
          run_received: case state.running {
            True -> state.run_received + count
            False -> state.run_received
          },
          activities: add_activity(
            state,
            "DELIVER",
            "stage-dispatcher",
            consumer_name,
            count,
          ),
        )
      otp.report_consumed(state.stage, subscription, count)
      actor.continue(complete_run(updated))
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

fn complete_run(state: State) -> State {
  case state.running && state.run_received >= state.run_total {
    False -> state
    True ->
      State(
        ..state,
        revision: state.revision + 1,
        running: False,
        backpressured: False,
        activities: add_activity(
          state,
          "COMPLETE",
          consumer_name,
          "run-complete",
          state.run_received,
        ),
      )
  }
}

fn buffered(state: State) -> Int {
  state.pushed - state.received
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
    buffered: buffered(state),
    last_events: state.last_events,
    activities: state.activities,
    producer_pid: state.producer_pid,
    stage_pid: state.stage_pid,
    consumer_pid: state.consumer_pid,
    active: state.active,
    running: state.running,
    run_total: state.run_total,
    run_pushed: state.run_pushed,
    run_received: state.run_received,
    rejected_pushes: state.rejected_pushes,
    backpressured: state.backpressured,
  )
}
