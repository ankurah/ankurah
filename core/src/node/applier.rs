use crate::entity::RemoteTrxEntity;
use crate::error::{ApplyError, ApplyErrorItem};
use crate::internal::prelude::*;
use crate::reactor::ChangeNotification;
use crate::retrieval::{CachedEventGetter, LocalStateGetter, SuspenseEvents};
use crate::storage::StorageTransaction;
use crate::util::ready_chunks::ReadyChunks;
use futures::stream::StreamExt;
use proto::{Attested, EventStructureError};

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
            let event_getter = CachedEventGetter::new(node, &cdata);
            let state_getter = LocalStateGetter::new(node.storage.clone());
            match Self::apply_update(node, from_peer_id, update, &event_getter, &state_getter).await {
                Ok(Some(change)) => changes.push(change),
                Ok(None) => {}
                Err(cause) => {
                    tracing::warn!("failed to apply update for {}: {}", entity_id, cause);
                    errors.push(ApplyErrorItem { entity_id, cause });
                }
            }
        }

        node.reactor.notify_change(changes).await;

        if !errors.is_empty() {
            return Err(ApplyError::Items(errors));
        }
        Ok(())
    }

    /// Validate each event fragment structurally and against policy, then
    /// stage it for causal comparison. Application checks its lineage later.
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
    ) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        // TODO: do we actually need predicate_relevance?
        let proto::SubscriptionUpdateItem { entity_id, content, predicate_relevance: _ } = update;

        match content {
            proto::UpdateContent::EventOnly(event_fragments) => {
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, event_fragments, event_getter)?;
                // Batch preparation requires parents before their children; wire order is untrusted.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;
                Self::apply_and_commit_events(node, &attested_events, event_getter).await
            }

            proto::UpdateContent::StateAndEvent(state_fragment, event_fragments) => {
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, event_fragments, event_getter)?;
                // Batch preparation requires parents before their children.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;

                let state: Attested<proto::EntityState> = (entity_id, state_fragment.clone()).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &state)?;

                if let Some(entity) = Self::save_new_entity(node, &state.payload, &attested_events, event_getter, state_getter).await? {
                    return Ok(Some(EntityChange::new(entity, attested_events)));
                }
                let entity = node
                    .entities
                    .get_or_retrieve(state_getter, event_getter, &entity_id)
                    .await?
                    .ok_or(RetrievalError::EntityNotFound(entity_id))?;
                Self::apply_and_commit_state(node, &entity, &state.payload, &attested_events, event_getter).await
            }
        }
    }

    /// Before adopting a snapshot, check that its events lead to its head through the ordered batch,
    /// then check known parent generations. Missing history is never fetched from a peer for these checks.
    async fn check_snapshot_events<SE, PA, E>(
        node: &Node<SE, PA>,
        snapshot: &proto::EntityState,
        known_head: &proto::Clock,
        events: &[Attested<proto::Event>],
        event_getter: &E,
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
    {
        if events.is_empty() {
            return Ok(());
        }
        for (i, event) in events.iter().enumerate() {
            if event.payload.entity_id != snapshot.entity_id {
                return Err(MutationError::InvalidEvent);
            }
            let id = event.payload.id();
            let in_head = snapshot.state.head.contains(&id);
            let superseded_in_batch = events[i + 1..].iter().any(|later| later.payload.parent.contains(&id));
            if !in_head && !superseded_in_batch {
                return Err(MutationError::InvalidEvent);
            }
        }
        let mut generations: std::collections::BTreeMap<_, _> =
            known_head.entries().iter().map(|(generation, id)| (id.clone(), *generation)).collect();
        // Carried payloads take precedence over resident head annotations.
        generations.extend(events.iter().map(|event| (event.payload.id(), event.payload.generation())));
        let missing: std::collections::BTreeSet<_> =
            events.iter().flat_map(|event| event.payload.parent.ids()).filter(|id| !generations.contains_key(*id)).cloned().collect();
        if !missing.is_empty() {
            for event in node.storage.get_events(missing.into_iter().collect()).await? {
                generations.insert(event.payload.id(), event.payload.generation());
                // Publication or a retry can reuse these local reads.
                event_getter.stage_event(event.payload);
            }
        }
        for event in events {
            for &(claimed, ref parent) in &event.payload.parent {
                let Some(known) = generations.get(parent).copied() else { continue };
                if claimed != known {
                    return Err(EventStructureError::GenerationMismatch { event: parent.clone(), claimed, known }.into());
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
        Self::check_snapshot_events(node, state, &proto::Clock::default(), events, event_getter).await?;
        let attestation = node.policy_agent.attest_state(node, state);
        let mut storage_trx = node.storage.transaction();
        storage_trx.set_state(&proto::Clock::default(), &Attested::opt(state.clone(), attestation)).await?;
        storage_trx.add_events(events).await?;
        let mut source = node.reactor.begin_commit().await;
        match source.record_storage_outcome(storage_trx.commit().await)? {
            crate::storage::StorageCommitOutcome::Conflict { .. } => return Ok(None),
            crate::storage::StorageCommitOutcome::Committed(_) => {}
        }
        let (_, entity) = node.entities.with_state(state_getter, event_getter, state.entity_id, state.state.clone()).await?;
        source.publish([&entity]);
        Ok(Some(entity))
    }

    /// Adopt an extending snapshot, or merge its accompanying events. Persist both before publishing.
    /// Each storage conflict retries that decision from a fresh fork; already-applied events do not notify again.
    /// A concurrent snapshot without accompanying events leaves the resident unchanged.
    async fn apply_and_commit_state<SE, PA, E>(
        node: &Node<SE, PA>,
        entity: &crate::entity::Entity,
        snapshot: &proto::EntityState,
        events: &[Attested<proto::Event>],
        event_getter: &E,
    ) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
    {
        crate::util::retry::retry_on!(MutationError::WriteConflict, {
            let candidate = RemoteTrxEntity::edit(entity)?;
            let expected_head = candidate.head();
            let adopted = candidate.apply_state(event_getter, &snapshot.state).await?;
            if !adopted && events.is_empty() {
                return Ok(None);
            }
            let mut events = events.to_vec();
            if adopted {
                Self::check_snapshot_events(node, snapshot, &expected_head, &events, event_getter).await?;
            } else {
                candidate.apply_events(event_getter, &mut events, |_| Ok(None)).await?;
            }
            let state = proto::EntityState { entity_id: entity.id(), state: candidate.to_state()? };
            let attestation = node.policy_agent.attest_state(node, &state);
            let state = Attested::opt(state, attestation);
            let mut storage_trx = node.storage.transaction();
            storage_trx.set_state(&expected_head, &state).await?;
            storage_trx.add_events(&events).await?;
            let mut source = node.reactor.begin_commit().await;
            source.record_storage_outcome(storage_trx.commit().await)?.committed()?;
            if adopted {
                let (_, entity) = node
                    .entities
                    .with_state(&LocalStateGetter::new(node.storage.clone()), event_getter, entity.id(), state.payload.state)
                    .await?;
                source.publish([&entity]);
                Ok(Some(EntityChange::new(entity, events)))
            } else {
                let change = candidate.commit(&node.entities, event_getter).await?;
                source.publish([change.entity()]);
                Ok((!change.events().is_empty()).then_some(change))
            }
        })
    }

    /// Apply and persist the whole batch before publishing. Any application failure discards the fork.
    /// Storage conflicts retry from a fresh fork.
    async fn apply_and_commit_events<SE, PA, E>(
        node: &Node<SE, PA>,
        events: &[Attested<proto::Event>],
        event_getter: &E,
    ) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
    {
        let Some(first) = events.first() else { return Ok(None) };
        let entity_id = first.payload.entity_id;
        let state_getter = LocalStateGetter::new(node.storage.clone());
        crate::util::retry::retry_on!(MutationError::WriteConflict, {
            let candidate = RemoteTrxEntity::for_event(&node.entities, &state_getter, event_getter, &first.payload).await?;
            let expected_head = candidate.head();
            let mut events = events.to_vec();
            candidate.apply_events(event_getter, &mut events, |_| Ok(None)).await?;
            let state = proto::EntityState { entity_id, state: candidate.to_state()? };
            let attestation = node.policy_agent.attest_state(node, &state);
            let mut storage_trx = node.storage.transaction();
            storage_trx.set_state(&expected_head, &Attested::opt(state, attestation)).await?;
            storage_trx.add_events(&events).await?;
            let mut source = node.reactor.begin_commit().await;
            source.record_storage_outcome(storage_trx.commit().await)?.committed()?;
            let change = candidate.commit(&node.entities, event_getter).await?;
            source.publish([change.entity()]);
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
        let mut ready_chunks = ReadyChunks::new(deltas.into_iter().map(|delta| {
            let entity_id = delta.entity_id;
            async move {
                Self::apply_delta(node, from_peer_id, delta, event_getter, state_getter)
                    .await
                    .map_err(|cause| ApplyErrorItem { entity_id, cause })
            }
        }));

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
                    return Ok(Some(EntityChange::new(entity, Vec::new())));
                }
                let entity = node
                    .entities
                    .get_or_retrieve(state_getter, event_getter, &delta.entity_id)
                    .await?
                    .ok_or(RetrievalError::EntityNotFound(delta.entity_id))?;
                Self::apply_and_commit_state(node, &entity, &attested_state.payload, &[], event_getter).await
            }

            proto::DeltaContent::EventBridge { events } => {
                // Bridge events pass the same policy gate as subscription
                // updates; transport must not decide trust.
                let attested_events = Self::validate_and_stage(node, from_peer_id, delta.entity_id, events, event_getter)?;

                // Batch preparation requires parents before their children; wire order is untrusted.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;
                let change = Self::apply_and_commit_events(node, &attested_events, event_getter).await?;

                // An already-applied bridge changes nothing. Avoid emitting an empty
                // Update to subscriptions that already hold this entity.
                let Some(change) = change else { return Ok(None) };
                let (entity, _) = change.into_parts();

                // Bridges carry no events on the change itself; the events were applied
                // above, so the change reports an empty events list.
                Ok(Some(EntityChange::new(entity, Vec::new())))
            }

            proto::DeltaContent::StateAndRelation { .. } => Err(MutationError::InvalidUpdate("StateAndRelation not yet implemented")),
        }
    }
}

#[cfg(test)]
mod tests;
