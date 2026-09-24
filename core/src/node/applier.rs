use crate::entity::{RemoteTrxEntity, StateApplyResult};
use crate::error::{ApplyError, ApplyErrorItem};
use crate::internal::prelude::*;
use crate::reactor::ChangeNotification;
use crate::retrieval::{CachedEventGetter, LocalStateGetter, SuspenseEvents};
use crate::storage::StorageTransaction;
use crate::util::ready_chunks::ReadyChunks;
use futures::stream::StreamExt;
use proto::Attested;
use std::sync::{atomic::AtomicBool, Arc};

/// Consolidates all logic for applying remote updates to a node
/// Handles both SubscriptionUpdateItem (streaming updates) and EntityDelta (initial Fetch/QuerySubscribed)
pub struct NodeApplier;

impl NodeApplier {
    /// Similar to commit_transaction, except that we check event attestations instead of checking write permissions
    /// we also don't need to fan events out to peers because we're receiving them from a peer
    pub(crate) async fn apply_updates<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        items: Vec<proto::SubscriptionUpdateItem>,
    ) -> Result<(), ApplyError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        tracing::debug!("received subscription update for {} items", items.len());

        // In theory, if initialized_predicate is specified, we could potentially narrow it down to just the context for that predicate
        // but this feels brittle, because failure to apply this event would affect the other contexts on this node.
        let Some(relay) = &node.subscription_relay else {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates without a subscription relay").into());
        };
        // A peer may push only what we asked it for, so a push is admissible
        // only against a standing query of ours with that peer.
        if !relay.has_subscription_with_peer(from_peer_id) {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates from a peer we hold no subscription with").into());
        }
        let cdata = relay.get_contexts_for_peer(from_peer_id);

        // Apply all updates. One bad item must not poison the batch: failures
        // are collected per item, the remaining items still apply, and the
        // reactor is notified for the successfully applied subset.
        let mut changes = Vec::new();
        let mut errors: Vec<ApplyErrorItem> = Vec::new();
        for update in items {
            let entity_id = update.entity_id;
            let result = async {
                let event_getter = CachedEventGetter::new(node, &cdata);
                let state_getter = LocalStateGetter::new(node.storage.clone());
                Self::apply_update(node, from_peer_id, update, &event_getter, &state_getter, &mut changes, &mut ()).await
            }
            .await;
            if let Err(cause) = result {
                tracing::warn!("failed to apply update for {}: {}", entity_id, cause);
                errors.push(ApplyErrorItem { entity_id, cause });
            }
        }

        node.reactor.notify_change(changes).await;

        if !errors.is_empty() {
            return Err(ApplyError::Items(errors));
        }
        Ok(())
    }

    /// Validate each event fragment structurally and against policy, then
    /// stage it for BFS discovery. Shared by every update arm that carries
    /// events, so it is the one place a subscription update's events are
    /// admitted.
    fn validate_and_stage<SE, PA, E>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        entity_id: proto::EntityId,
        event_fragments: Vec<proto::EventFragment>,
        event_getter: &E,
    ) -> Result<Vec<Attested<proto::Event>>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
    {
        let mut attested_events = Vec::new();
        for fragment in event_fragments {
            let attested_event: Attested<proto::Event> = (entity_id, fragment).into();
            // A genesis whose id the entity it claims does not derive, or a
            // parent clock that disagrees with the body, is refused before
            // the DAG ever sees it: the sender's framing of the entity id is
            // what the fragment supplies, and a genesis must derive its own.
            attested_event.payload.validate_structure()?;
            super::event_admissibility::check_genesis_membership(&attested_event.payload)?;
            node.policy_agent.validate_received_event(node, from_peer_id, &attested_event)?;
            event_getter.stage_event(attested_event.payload.clone());
            attested_events.push(attested_event);
        }
        Ok(attested_events)
    }

    async fn apply_update<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        update: proto::SubscriptionUpdateItem,
        event_getter: &E,
        state_getter: &S,
        changes: &mut Vec<EntityChange>,
        entities: &mut impl Pushable<crate::entity::Entity>,
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        // TODO: do we actually need predicate_relevance?
        let proto::SubscriptionUpdateItem { entity_id, content, predicate_relevance: _ } = update;

        match content {
            // EventOnly: equivalent to old SubscriptionItem::Change
            proto::UpdateContent::EventOnly(event_fragments) => {
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, event_fragments, event_getter)?;
                // Wire order is untrusted for every multi-event shape, not
                // just bridges: a child applied before its staged parent
                // gap-jumps the head and drops the parent's operations (V4).
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;

                // We did not receive an entity fragment, so we need to retrieve it from local storage or a remote peer
                let Some(first) = attested_events.first() else { return Ok(()) };
                let candidate = RemoteTrxEntity::for_event(
                    &node.entities, state_getter, event_getter, &first.payload, Arc::new(AtomicBool::new(true)),
                ).await?;

                let mut applied_events = Vec::new();
                let mut failure: Option<MutationError> = None;
                for mut event in attested_events {
                    // Events should always be appliable sequentially
                    let applied = match candidate.apply_event(event_getter, &mut event, |_| Ok(None)).await {
                        Ok(applied) => applied,
                        Err(e) => {
                            failure = Some(e);
                            break;
                        }
                    };
                    if applied {
                        applied_events.push(event);
                    }
                }

                // Anything applied before a failure is real progress; notify it.
                if !applied_events.is_empty() {
                    // Rebuild only the accepted events: a failing operation may
                    // have partially changed the fork.
                    if let Some(change) = Self::save_events(node, entity_id, &applied_events, event_getter).await? {
                        changes.push(change);
                    }
                }

                if let Some(entity) = node.entities.get(&entity_id) { entities.push(entity); }
                if let Some(e) = failure { return Err(e); }
            }

            // StateAndEvent: equivalent to old SubscriptionItem::Add
            proto::UpdateContent::StateAndEvent(state_fragment, event_fragments) => {
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, event_fragments, event_getter)?;
                // Sorted for the same reason as the EventOnly arm: the
                // fallback below applies event by event.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;

                let state: Attested<proto::EntityState> = (entity_id, state_fragment.clone()).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &state)?;

                if let Some(entity) = Self::save_new_entity(node, &state.payload, &attested_events, event_getter, state_getter).await? {
                    entities.push(entity.clone());
                    changes.push(EntityChange::new(entity, attested_events)?);
                    return Ok(());
                }
                let entity = node.entities.get_or_retrieve(state_getter, event_getter, &entity_id).await?.ok_or(RetrievalError::EntityNotFound(entity_id))?;
                entities.push(entity.clone());
                let candidate = RemoteTrxEntity::edit(&entity, Arc::new(AtomicBool::new(true)))?;
                let changed = matches!(candidate.apply_state(event_getter, &state.payload.state).await?, StateApplyResult::Applied);

                if changed {
                    // State applied successfully (new entity or strictly descends)
                    Self::save_state(node, &entity, candidate.to_state()?, &attested_events, event_getter).await?;
                    changes.push(EntityChange::new(entity, attested_events)?);
                } else {
                    // State not applied (divergence or older) - fall back to event-by-event application
                    // This handles DivergedSince where we need to merge concurrent branches
                    let mut applied_events = Vec::new();
                    for mut event in attested_events {
                        if candidate.apply_event(event_getter, &mut event, |_| Ok(None)).await? {
                            applied_events.push(event);
                        }
                    }
                    if !applied_events.is_empty() {
                        if let Some(change) = Self::save_events(node, entity_id, &applied_events, event_getter).await? {
                            changes.push(change);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Persist a first snapshot before constructing its resident. Existing entities use the forked update path.
    async fn save_new_entity<SE, PA, E, S>(
        node: &Node<SE, PA>,
        state: &proto::EntityState,
        events: &[Attested<proto::Event>],
        event_getter: &E,
        state_getter: &S,
    ) -> Result<Option<Entity>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        if node.entities.get_or_retrieve(state_getter, event_getter, &state.entity_id).await?.is_some() {
            return Ok(None);
        }
        // Reject undecodable snapshots before persisting them or attaching a resident.
        crate::entity::TemporaryEntity::new(state.entity_id, &state.state)?;
        let attestation = node.policy_agent.attest_state(node, state);
        let mut storage_trx = node.storage.transaction();
        storage_trx.set_state(&proto::Clock::default(), &Attested::opt(state.clone(), attestation)).await?;
        storage_trx.add_events(events).await?;
        let _publication = node.commit_publication_lock.lock().await;
        match storage_trx.commit().await? {
            crate::storage::StorageCommitOutcome::Conflict { .. } => return Ok(None),
            crate::storage::StorageCommitOutcome::Committed(_) => {}
        }
        let (_, entity) = node.entities.with_state(state_getter, event_getter, state.entity_id, state.state.clone()).await?;
        Ok(Some(entity))
    }

    /// Merge on an isolated fork and persist before updating the resident entity. A storage conflict
    /// restarts the merge from the winner's resident state rather than publishing the stale candidate.
    async fn save_state<SE, PA, E>(
        node: &Node<SE, PA>,
        entity: &crate::entity::Entity,
        state: proto::State,
        events: &[Attested<proto::Event>],
        event_getter: &E,
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
    {
        crate::util::retry::retry_on!(MutationError::WriteConflict, {
            let candidate = RemoteTrxEntity::edit(&entity, Arc::new(AtomicBool::new(true)))?;
            let expected_head = candidate.head();
            if let StateApplyResult::DivergedRequiresEvents = candidate.apply_state(event_getter, &state).await? {
                for event_id in state.head.iter() {
                    candidate.apply_event(event_getter, &mut Attested::from(event_getter.get_event(event_id).await?), |_| Ok(None)).await?;
                }
            }
            let state = proto::EntityState { entity_id: entity.id(), state: candidate.to_state()? };
            let attestation = node.policy_agent.attest_state(node, &state);
            let state = Attested::opt(state, attestation);
            let mut storage_trx = node.storage.transaction();
            storage_trx.set_state(&expected_head, &state).await?;
            storage_trx.add_events(events).await?;
            let _publication = node.commit_publication_lock.lock().await;
            storage_trx.commit().await?.committed()?;
            node.entities.with_state(
                &LocalStateGetter::new(node.storage.clone()), event_getter, entity.id(), state.payload.state,
            ).await?;
            Ok(())
        })
    }

    /// Replay only accepted events, discarding any partial mutations from a failed application.
    /// Each storage retry starts with a fresh fork; publication follows successful persistence.
    async fn save_events<SE, PA, E>(
        node: &Node<SE, PA>,
        entity_id: proto::EntityId,
        events: &[Attested<proto::Event>],
        event_getter: &E,
    ) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
    {
        let Some(first) = events.first() else { return Ok(None) };
        let state_getter = LocalStateGetter::new(node.storage.clone());
        crate::util::retry::retry_on!(MutationError::WriteConflict, {
            let candidate = RemoteTrxEntity::for_event(
                &node.entities, &state_getter, event_getter, &first.payload, Arc::new(AtomicBool::new(true)),
            ).await?;
            let expected_head = candidate.head();
            for event in events {
                candidate.apply_event(event_getter, &mut event.clone(), |_| Ok(None)).await?;
            }
            let state = proto::EntityState { entity_id, state: candidate.to_state()? };
            let attestation = node.policy_agent.attest_state(node, &state);
            let mut storage_trx = node.storage.transaction();
            storage_trx.set_state(&expected_head, &Attested::opt(state, attestation)).await?;
            storage_trx.add_events(events).await?;
            let _publication = node.commit_publication_lock.lock().await;
            storage_trx.commit().await?.committed()?;
            let change = candidate.commit(&node.entities, event_getter).await?;
            Ok((!change.events().is_empty()).then_some(change))
        })
    }

    /// Apply multiple EntityDeltas in parallel with batched reactor notification
    /// Drains all ready futures per wake and calls reactor.notify_change for each batch
    /// Collects all errors and returns them at the end - caller decides whether to fail or log
    pub(crate) async fn apply_deltas<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        deltas: Vec<proto::EntityDelta>,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<(), ApplyError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        // do not wait for all apply_delta futures to complete - we need to apply all updates in a timely fashion
        // if there are stragglers, they will be picked up on the next wake
        // this should in theory be deterministic for eventbridge cases where all events are immediately available
        let mut ready_chunks =
            ReadyChunks::new(deltas.into_iter().map(|delta| Self::apply_delta(node, from_peer_id, delta, event_getter, state_getter)));

        let mut all_errors = Vec::new();

        while let Some(results) = ready_chunks.next().await {
            let mut batch = Vec::new();

            for result in results {
                match result {
                    Ok(Some(change)) => batch.push(change),
                    Ok(None) => {} // No change, continue
                    Err(error_item) => {
                        all_errors.push(error_item);
                    }
                }
            }

            if !batch.is_empty() {
                node.reactor.notify_change(batch).await;
            }
        }

        if !all_errors.is_empty() {
            return Err(ApplyError::Items(all_errors));
        }

        Ok(())
    }

    /// Apply EntityDelta from Fetch or QuerySubscribed responses
    /// Returns Some(EntityChange) if the delta resulted in a change, None otherwise
    async fn apply_delta<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        delta: proto::EntityDelta,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<Option<EntityChange>, ApplyErrorItem>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        let entity_id = delta.entity_id;

        let result = Self::apply_delta_inner(node, from_peer_id, delta, event_getter, state_getter).await;
        result.map_err(|cause| ApplyErrorItem { entity_id, cause })
    }

    async fn apply_delta_inner<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        delta: proto::EntityDelta,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        match delta.content {
            proto::DeltaContent::StateSnapshot { state } => {
                let attested_state: Attested<proto::EntityState> = (delta.entity_id, state).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &attested_state)?;

                if let Some(entity) = Self::save_new_entity(node, &attested_state.payload, &[], event_getter, state_getter).await? {
                    return Ok(Some(EntityChange::new(entity, Vec::new())?));
                }
                let entity = node.entities.get_or_retrieve(state_getter, event_getter, &delta.entity_id).await?.ok_or(RetrievalError::EntityNotFound(delta.entity_id))?;
                let candidate = RemoteTrxEntity::edit(&entity, Arc::new(AtomicBool::new(true)))?;
                let changed = matches!(candidate.apply_state(event_getter, &attested_state.payload.state).await?, StateApplyResult::Applied);

                // Save state to storage
                Self::save_state(node, &entity, candidate.to_state()?, &[], event_getter).await?;

                // Only notify if the snapshot actually advanced the entity. The
                // candidate is unchanged when the state did not apply (the entity is
                // already resident at this head, or the snapshot is older). Emitting a
                // change here would be spurious: notify_change is global across every
                // subscription on the node, so a no-op snapshot for one subscribing
                // query surfaces on ANOTHER already-established query - which holds the
                // same entity - as an empty-events ItemChange::Update. That is the
                // subscription-notification race behind the intermittent
                // server_edits_subscription failure. Freshly created and advanced
                // entities are real changes and still notify.
                if !changed {
                    return Ok(None);
                }

                // Snapshots carry no events, so the change reports an empty events list.
                Ok(Some(EntityChange::new(entity, Vec::new())?))
            }

            proto::DeltaContent::EventBridge { events } => {
                // Bridge events pass the same policy gate as subscription
                // updates; transport must not decide trust.
                let attested_events = Self::validate_and_stage(node, from_peer_id, delta.entity_id, events, event_getter)?;

                // Apply events parents-first. Wire order is untrusted: applying
                // a child before its staged parent gap-jumps the head past the
                // parent, whose operations are then dropped as StrictAscends
                // (V4). The producer also sorts, but receivers must not rely
                // on sender ordering.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;
                let change = Self::save_events(node, delta.entity_id, &attested_events, event_getter).await?;

                // Only notify if the bridge actually advanced the entity. If every
                // event was already applied (apply_event returned false for all), the
                // entity did not change and emitting an EntityChange would surface a
                // spurious empty-events Update on other subscriptions holding this
                // entity (see the StateSnapshot arm above). This mirrors the streaming
                // StateAndEvent fallback, which also only notifies when events applied.
                let Some(change) = change else { return Ok(None) };
                let (entity, _) = change.into_parts();

                // Bridges carry no events on the change itself; the events were applied
                // above, so the change reports an empty events list.
                Ok(Some(EntityChange::new(entity, Vec::new())?))
            }

            proto::DeltaContent::StateAndRelation { .. } => Err(MutationError::InvalidUpdate("StateAndRelation not yet implemented")),
        }
    }
}

trait Pushable<T> {
    fn push(&mut self, value: T);
}
impl<T> Pushable<T> for Vec<T> {
    fn push(&mut self, value: T) { self.push(value); }
}
impl<T> Pushable<T> for &mut Vec<T> {
    fn push(&mut self, value: T) { (*self).push(value); }
}
impl<T> Pushable<T> for () {
    fn push(&mut self, _: T) {
        // do nothing
    }
}

#[cfg(test)]
mod tests;
