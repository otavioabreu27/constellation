import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/io
import gleam/list
import gleam/string
import logging
import mist.{type Connection, type ResponseData}
import mist_dashboard/dashboard
import mist_dashboard/page
import mist_dashboard/producer
import stage/runtime/otp
import stage/value_objects/participant_id
import stage/value_objects/subscription_id

pub fn main() -> Nil {
  logging.configure()
  let assert Ok(id) = subscription_id.new("dashboard-consumer")
  let assert Ok(stage) = otp.start_with_config(otp.config() |> otp.with_logging)
  let assert Ok(producer) = producer.start(stage.data)
  let assert Ok(control) =
    dashboard.start(
      stage.data,
      producer.data,
      id,
      participant_id.new("dashboard"),
      string.inspect(producer.pid),
      string.inspect(stage.pid),
    )
  let handler = fn(request) { handle_request(request, control.data) }
  let assert Ok(_) =
    handler
    |> mist.new
    |> mist.bind("127.0.0.1")
    |> mist.port(4000)
    |> mist.start

  io.println("Demand Lab running at http://localhost:4000")
  process.sleep_forever()
}

fn handle_request(
  request: Request(Connection),
  control: dashboard.Dashboard,
) -> Response(ResponseData) {
  case request.path_segments(request) {
    [] -> html_response(page.html)
    ["api", "state"] ->
      json_response(snapshot_json(dashboard.snapshot(control)))
    ["api", "ask", amount] -> action_response(amount, dashboard.ask, control)
    ["api", "push", amount] -> action_response(amount, dashboard.push, control)
    _ -> text_response(404, "not found")
  }
}

fn action_response(
  amount: String,
  action: fn(dashboard.Dashboard, Int) -> Result(dashboard.Snapshot, String),
  control: dashboard.Dashboard,
) -> Response(ResponseData) {
  case int.parse(amount) {
    Error(_) -> text_response(400, "invalid amount")
    Ok(value) ->
      case action(control, value) {
        Error(reason) -> text_response(400, reason)
        Ok(snapshot) -> json_response(snapshot_json(snapshot))
      }
  }
}

fn snapshot_json(snapshot: dashboard.Snapshot) -> String {
  let events =
    snapshot.last_events
    |> list.map(int.to_string)
    |> string.join(",")
  "{\"revision\":"
  <> int.to_string(snapshot.revision)
  <> ",\"pushed\":"
  <> int.to_string(snapshot.pushed)
  <> ",\"received\":"
  <> int.to_string(snapshot.received)
  <> ",\"outstanding_demand\":"
  <> int.to_string(snapshot.outstanding_demand)
  <> ",\"buffered\":"
  <> int.to_string(snapshot.buffered)
  <> ",\"last_events\":["
  <> events
  <> "],\"activities\":["
  <> activities_json(snapshot.activities)
  <> "],\"producer\":\"counter-producer\",\"consumer\":\"dashboard-consumer\",\"subscription\":\"dashboard-consumer\",\"producer_pid\":\""
  <> snapshot.producer_pid
  <> "\",\"stage_pid\":\""
  <> snapshot.stage_pid
  <> "\",\"consumer_pid\":\""
  <> snapshot.consumer_pid
  <> "\",\"active\":"
  <> bool_json(snapshot.active)
  <> "}"
}

fn activities_json(activities: List(dashboard.Activity)) -> String {
  activities
  |> list.map(fn(activity) {
    "{\"sequence\":"
    <> int.to_string(activity.sequence)
    <> ",\"kind\":\""
    <> activity.kind
    <> "\",\"source\":\""
    <> activity.source
    <> "\",\"target\":\""
    <> activity.target
    <> "\",\"amount\":"
    <> int.to_string(activity.amount)
    <> "}"
  })
  |> string.join(",")
}

fn bool_json(value: Bool) -> String {
  case value {
    True -> "true"
    False -> "false"
  }
}

fn html_response(body: String) -> Response(ResponseData) {
  build_response(200, "text/html; charset=utf-8", body)
}

fn json_response(body: String) -> Response(ResponseData) {
  build_response(200, "application/json", body)
}

fn text_response(status: Int, body: String) -> Response(ResponseData) {
  build_response(status, "text/plain; charset=utf-8", body)
}

fn build_response(
  status: Int,
  content_type: String,
  body: String,
) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", content_type)
  |> response.set_header("cache-control", "no-store")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
}
