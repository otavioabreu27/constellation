import constellation/core
import constellation/domains/command.{
  Ask, Cancel, ParticipantDown, Push, Subscribe,
}
import constellation/domains/dispatcher
import constellation/domains/effect.{NotifyCancelled, SendEvents}
import constellation/domains/stage_error
import constellation/value_objects/participant_id
import constellation/value_objects/subscription_id

fn subscription_id() {
  let assert Ok(id) = subscription_id.new("subscription-uuid")
  id
}

fn participant_id() {
  participant_id.new("consumer-1")
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

pub fn cancelling_subscriber_unblocks_strict_broadcast_test() {
  let first_id = subscription_id()
  let assert Ok(second_id) = subscription_id.new("subscription-uuid-2")
  let state = core.new_with_strategy(dispatcher.broadcast_strategy())
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
