import constellation
import constellation/runtime/otp
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn facade_starts_pushes_and_stops_engine_test() {
  let assert Ok(engine) = constellation.start()
  assert constellation.push(engine.data, [1, 2, 3]) == Ok(Nil)
  assert constellation.stop(engine.data) == Ok(Nil)
}

pub fn facade_rejects_invalid_call_timeout_test() {
  assert constellation.with_call_timeout(constellation.config(), 0)
    == Error(otp.InvalidCallTimeout(0))
}

pub fn facade_rejects_invalid_buffer_capacity_test() {
  assert constellation.with_buffer_capacity(constellation.config(), 0)
    == Error(otp.InvalidBufferCapacity(0))
}
