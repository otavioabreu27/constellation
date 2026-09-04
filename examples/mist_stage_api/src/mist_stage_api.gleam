import constellation
import constellation/runtime/otp/consumer
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id
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

pub fn main() -> Nil {
  logging.configure()

  let assert Ok(subscription) = subscription_id.new("api-consumer")

  let assert Ok(started_stage) =
    constellation.config()
    |> constellation.with_logging
    |> constellation.start_with_config

  let assert Ok(started_consumer) =
    consumer.start(
      started_stage.data,
      subscription,
      participant_id.new("api-consumer"),
      [],
      list.append,
    )

  let handler = fn(request) {
    handle_request(request, started_stage.data, started_consumer.data)
  }

  let assert Ok(_) =
    handler
    |> mist.new
    |> mist.bind("127.0.0.1")
    |> mist.port(4001)
    |> mist.start

  io.println("Mist + Constellation API running at http://localhost:4001")
  process.sleep_forever()
}

fn handle_request(
  request: Request(Connection),
  engine: constellation.Engine(Int),
  event_consumer: consumer.Consumer(List(Int), Int),
) -> Response(ResponseData) {
  case request.path_segments(request) {
    ["events", value] ->
      with_integer(value, fn(event) {
        case constellation.push(engine, [event]) {
          Ok(_) ->
            json_response(202, "{\"queued\":" <> int.to_string(event) <> "}")
          Error(error) -> error_response(string.inspect(error))
        }
      })
    ["demand", value] ->
      with_integer(value, fn(amount) {
        case consumer.ask(event_consumer, amount) {
          Ok(_) ->
            json_response(202, "{\"demand\":" <> int.to_string(amount) <> "}")
          Error(error) -> error_response(string.inspect(error))
        }
      })
    ["consumed"] -> {
      case consumer.state(event_consumer) {
        Error(error) -> error_response(string.inspect(error))
        Ok(consumed) -> {
          let events =
            consumed
            |> list.map(int.to_string)
            |> string.join(",")
          json_response(200, "{\"events\":[" <> events <> "]}")
        }
      }
    }
    _ -> text_response(404, "not found")
  }
}

fn with_integer(
  value: String,
  handler: fn(Int) -> Response(ResponseData),
) -> Response(ResponseData) {
  case int.parse(value) {
    Ok(parsed) -> handler(parsed)
    Error(_) -> text_response(400, "expected an integer")
  }
}

fn error_response(error: String) -> Response(ResponseData) {
  text_response(400, error)
}

fn json_response(status: Int, body: String) -> Response(ResponseData) {
  build_response(status, "application/json", body)
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
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
}
