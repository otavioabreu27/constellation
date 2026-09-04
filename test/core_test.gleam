import stage/core
import stage/domains/command.{Ask, Cancel, ParticipantDown, Push, Subscribe}
import stage/domains/dispatcher
import stage/domains/effect.{NotifyCancelled, SendEvents}
import stage/domains/stage_error
import stage/domains/subscription
import stage/value_objects/participant_id
import stage/value_objects/subscription_id

fn subscription_id() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  id
}

fn participant_id() {
  participant_id.new("consumer-1")
}

pub fn subscribe_creates_active_subscription_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, effects)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )

  assert effects == []
  let assert Ok(value) = core.subscription(state, id)
  assert subscription.demand(value) == 0
}

pub fn ask_rejects_non_positive_amount_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )

  assert core.update(state, Ask(subscription_id: id, amount: 0))
    == Error(stage_error.InvalidDemand(0))
}

pub fn ask_rejects_negative_amount_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )

  assert core.update(state, Ask(subscription_id: id, amount: -1))
    == Error(stage_error.InvalidDemand(-1))
}

pub fn ask_rejects_unknown_subscription_test() {
  let id = subscription_id()

  assert core.update(core.new(), Ask(subscription_id: id, amount: 1))
    == Error(stage_error.UnknownSubscription(id))
}

pub fn push_without_subscriptions_buffers_all_events_test() {
  let assert Ok(#(state, effects)) =
    core.update(core.new(), Push(events: [1, 2, 3]))

  assert effects == []
  assert core.buffered_events(state) == [1, 2, 3]
}

pub fn push_emits_only_requested_events_and_buffers_excess_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: id, amount: 3))
  let assert Ok(#(state, effects)) =
    core.update(state, Push(events: [1, 2, 3, 4, 5]))

  assert effects == [SendEvents(subscription_id: id, events: [1, 2, 3])]
  assert core.buffered_events(state) == [4, 5]
}

pub fn ask_drains_buffer_in_fifo_order_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: id, amount: 3))
  let assert Ok(#(state, _)) = core.update(state, Push(events: [1, 2, 3, 4, 5]))
  let assert Ok(#(state, effects)) =
    core.update(state, Ask(subscription_id: id, amount: 2))

  assert effects == [SendEvents(subscription_id: id, events: [4, 5])]
  assert core.buffered_events(state) == []
}

pub fn cancel_invalidates_subscription_and_discards_demand_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: id, amount: 3))
  let assert Ok(#(state, effects)) =
    core.update(state, Cancel(subscription_id: id))

  assert effects == [NotifyCancelled(subscription_id: id)]
  assert core.is_cancelled(state, id)
  assert core.subscription(state, id)
    == Error(stage_error.SubscriptionCancelled(id))
  assert core.update(state, Ask(subscription_id: id, amount: 1))
    == Error(stage_error.SubscriptionCancelled(id))
}

pub fn cancel_is_idempotent_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) = core.update(state, Cancel(subscription_id: id))
  let assert Ok(#(state, effects)) =
    core.update(state, Cancel(subscription_id: id))

  assert effects == []
  assert core.is_cancelled(state, id)
}

pub fn participant_down_cancels_associated_subscription_test() {
  let id = subscription_id()
  let participant = participant_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant, partition: 0),
    )
  let assert Ok(#(state, effects)) =
    core.update(state, ParticipantDown(participant_id: participant))

  assert effects == [NotifyCancelled(subscription_id: id)]
  assert core.is_cancelled(state, id)
}

pub fn core_uses_demand_dispatcher_for_multiple_subscriptions_test() {
  let first_id = subscription_id()
  let assert Ok(second_id) = subscription_id.new("subscription-uuid-2")
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: first_id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(
        id: second_id,
        participant_id: participant_id.new("consumer-2"),
        partition: 0,
      ),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: first_id, amount: 2))
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: second_id, amount: 2))
  let assert Ok(#(state, effects)) =
    core.update(state, Push(events: [1, 2, 3, 4, 5]))

  assert effects
    == [
      SendEvents(subscription_id: first_id, events: [1, 3]),
      SendEvents(subscription_id: second_id, events: [2, 4]),
    ]
  assert core.buffered_events(state) == [5]
}

pub fn core_uses_broadcast_dispatcher_test() {
  let first_id = subscription_id()
  let assert Ok(second_id) = subscription_id.new("subscription-uuid-2")
  let state = core.new_with_strategy(dispatcher.Broadcast)
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: first_id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(
        id: second_id,
        participant_id: participant_id.new("consumer-2"),
        partition: 0,
      ),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: first_id, amount: 2))
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: second_id, amount: 2))
  let assert Ok(#(state, effects)) = core.update(state, Push(events: [1, 2, 3]))

  assert effects
    == [
      SendEvents(subscription_id: first_id, events: [1, 2]),
      SendEvents(subscription_id: second_id, events: [1, 2]),
    ]
  assert core.buffered_events(state) == [3]
}

pub fn core_uses_partition_dispatcher_test() {
  let first_id = subscription_id()
  let assert Ok(second_id) = subscription_id.new("subscription-uuid-2")
  let state =
    core.new_with_strategy(dispatcher.Partition(fn(value) { value % 2 }))
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: first_id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(
        id: second_id,
        participant_id: participant_id.new("consumer-2"),
        partition: 1,
      ),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: first_id, amount: 2))
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: second_id, amount: 2))
  let assert Ok(#(state, effects)) =
    core.update(state, Push(events: [0, 1, 2, 3, 4]))

  assert effects
    == [
      SendEvents(subscription_id: first_id, events: [0, 2]),
      SendEvents(subscription_id: second_id, events: [1, 3]),
    ]
  assert core.buffered_events(state) == [4]
}

pub fn demand_dispatcher_keeps_round_robin_between_pushes_test() {
  let first_id = subscription_id()
  let assert Ok(second_id) = subscription_id.new("subscription-uuid-2")
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: first_id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(
        id: second_id,
        participant_id: participant_id.new("consumer-2"),
        partition: 0,
      ),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: first_id, amount: 3))
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: second_id, amount: 3))
  let assert Ok(#(state, first_effects)) = core.update(state, Push(events: [1]))
  let assert Ok(#(state, second_effects)) =
    core.update(state, Push(events: [2]))
  let assert Ok(#(_, third_effects)) = core.update(state, Push(events: [3]))

  assert first_effects == [SendEvents(subscription_id: first_id, events: [1])]
  assert second_effects == [SendEvents(subscription_id: second_id, events: [2])]
  assert third_effects == [SendEvents(subscription_id: first_id, events: [3])]
}

pub fn cancelling_subscriber_unblocks_strict_broadcast_test() {
  let first_id = subscription_id()
  let assert Ok(second_id) = subscription_id.new("subscription-uuid-2")
  let state = core.new_with_strategy(dispatcher.Broadcast)
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: first_id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(
        id: second_id,
        participant_id: participant_id.new("consumer-2"),
        partition: 0,
      ),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: first_id, amount: 2))
  let assert Ok(#(state, effects)) = core.update(state, Push(events: [1, 2]))
  assert effects == []
  assert core.buffered_events(state) == [1, 2]

  let assert Ok(#(state, effects)) =
    core.update(state, Cancel(subscription_id: second_id))
  assert effects
    == [
      NotifyCancelled(subscription_id: second_id),
      SendEvents(subscription_id: first_id, events: [1, 2]),
    ]
  assert core.buffered_events(state) == []
}

pub fn participant_down_only_cancels_owned_subscriptions_test() {
  let participant = participant_id()
  let assert Ok(first_id) = subscription_id.new("subscription-1")
  let assert Ok(second_id) = subscription_id.new("subscription-2")
  let assert Ok(other_id) = subscription_id.new("subscription-3")
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: first_id, participant_id: participant, partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: second_id, participant_id: participant, partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(
        id: other_id,
        participant_id: participant_id.new("other-consumer"),
        partition: 0,
      ),
    )
  let assert Ok(#(state, effects)) =
    core.update(state, ParticipantDown(participant_id: participant))

  assert effects
    == [
      NotifyCancelled(subscription_id: first_id),
      NotifyCancelled(subscription_id: second_id),
    ]
  assert core.is_cancelled(state, first_id)
  assert core.is_cancelled(state, second_id)
  let assert Ok(_) = core.subscription(state, other_id)
}

pub fn partition_dispatcher_keeps_fairness_between_pushes_test() {
  let assert Ok(first_id) = subscription_id.new("subscription-1")
  let assert Ok(second_id) = subscription_id.new("subscription-2")
  let state =
    core.new_with_strategy(dispatcher.Partition(fn(value) { value % 2 }))
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: first_id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(
        id: second_id,
        participant_id: participant_id.new("consumer-2"),
        partition: 0,
      ),
    )
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: first_id, amount: 2))
  let assert Ok(#(state, _)) =
    core.update(state, Ask(subscription_id: second_id, amount: 2))
  let assert Ok(#(state, first_effects)) = core.update(state, Push(events: [0]))
  let assert Ok(#(_, second_effects)) = core.update(state, Push(events: [2]))

  assert first_effects
    == [
      SendEvents(subscription_id: first_id, events: [0]),
    ]
  assert second_effects
    == [
      SendEvents(subscription_id: second_id, events: [2]),
    ]
}

pub fn cancelled_subscription_id_cannot_be_reused_test() {
  let id = subscription_id()
  let state = core.new()
  let assert Ok(#(state, _)) =
    core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )
  let assert Ok(#(state, _)) = core.update(state, Cancel(subscription_id: id))

  assert core.update(
      state,
      Subscribe(id: id, participant_id: participant_id(), partition: 0),
    )
    == Error(stage_error.DuplicateSubscription(id))
}
