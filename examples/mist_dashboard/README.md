# BEAM Traffic Observatory

A visual high-load demonstration of `constellation` running on OTP. The workload
passes through independent producer, Stage, and consumer actors while the page
polls only aggregate metrics.

```sh
mise x rebar@3.27.0 -- gleam run
```

Open <http://localhost:4000> and select **Run 500,000 events**. The preset uses:

- 10,000-event producer batches;
- 5,000-event demand batches;
- an 80,000-event bounded Stage buffer;
- controlled retries when backpressure rejects a push.

The chart, counters, buffer gauge, and protocol timeline remain bounded. No DOM
node or JSON record is created for every event, so the visualization stays
responsive while the OTP pipeline processes the full workload.

The events model synthetic requests. One HTTP request starts the server-side
run; the demo does not claim to benchmark hundreds of thousands of HTTP
connections.
