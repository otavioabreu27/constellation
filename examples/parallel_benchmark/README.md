# Sequential vs OTP parallel benchmark

This example runs three increasingly large batteries of 25,000, 100,000, and
250,000 integers. Each battery applies the same deterministic CPU-bound
function in two ways:

1. sequentially with `list.fold`;
2. through one Constellation Stage and up to eight OTP consumer actors.

The parallel measurement includes Constellation dispatch and mailbox delivery.
Stage and consumer startup and shutdown are outside the measured interval. Both
runs must produce the same processed count and checksum. The terminal prints
proportional duration bars and the measured speedup for each battery.

```sh
gleam run
```

Run it on an otherwise idle machine and repeat it several times. Results depend
on CPU topology, available BEAM schedulers, thermal state, and background work.
This is a demonstration of parallel CPU execution, not a comprehensive library
benchmark. Small or I/O-bound tasks may be slower through OTP because message
passing and dispatch have a cost.

With one scheduler and one consumer, OTP is expected to be slightly slower than
the sequential run. Small apparent wins can occur from measurement noise, CPU
frequency changes, cache warming, or garbage collection.
