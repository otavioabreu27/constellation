# Mist + Constellation OTP

A minimal HTTP API using Mist as the web server, `constellation/runtime/otp` as the
event engine, and `constellation/runtime/otp/consumer` for the consumer process. There
is no frontend, manual subject, selector, or consumption-reporting code.

```sh
mise x rebar@3.27.0 -- gleam run
```

Queue an integer event, request one event, then inspect the consumer:

```sh
curl -X POST http://localhost:4001/events/42
curl -X POST http://localhost:4001/demand/1
curl http://localhost:4001/consumed
```

Expected response:

```json
{"events":[42]}
```
