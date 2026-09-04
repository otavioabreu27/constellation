import gleam/dict.{type Dict}
import stage/domains/subscription.{type Subscription}
import stage/value_objects/subscription_id.{type SubscriptionId}

pub type StageState(event) {
  StageState(
    subscriptions: Dict(SubscriptionId, Subscription),
    buffer: List(event),
  )
}
