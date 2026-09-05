//// Configuration validation without starting processes.

import constellation/worker_pool/internal/model.{type Config}
import constellation/worker_pool/types.{
  type ConfigError, InvalidBufferCapacity, InvalidPrefetch, InvalidSize,
  InvalidTimeout,
}
import gleam/option.{Some}

pub fn validate(config: Config(event, state)) -> Result(Nil, ConfigError) {
  case config.size <= 0, config.prefetch <= 0, config.timeout <= 0 {
    True, _, _ -> Error(InvalidSize(config.size))
    _, True, _ -> Error(InvalidPrefetch(config.prefetch))
    _, _, True -> Error(InvalidTimeout(config.timeout))
    False, False, False ->
      case config.buffer_capacity {
        Some(capacity) if capacity <= 0 -> Error(InvalidBufferCapacity(capacity))
        _ -> Ok(Nil)
      }
  }
}
