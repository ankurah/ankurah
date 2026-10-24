use crate::entity::RemoteTrxEntity;
use crate::error::{ApplyError, ApplyErrorItem};
use crate::internal::prelude::*;
use crate::reactor::ChangeNotification;
use crate::retrieval::{LocalStateGetter, SuspenseEvents};
use crate::storage::StorageTransaction;
use crate::util::ready_chunks::ReadyChunks;
use futures::stream::StreamExt;
use proto::{Attested, EventStructureError};

/// Applies entity deltas received from a peer, whether they arrive as initial
/// transfer (Fetch, QuerySubscribed, repair) or as live subscription updates.
/// One path handles every `DeltaContent`; the delivery decides only what the
/// reactor change publishes. Each delta is applied atomically.
pub struct NodeApplier;

/// How a delta reached this node.
///
/// Initial transfer publishes no events on its reactor changes: a repair for
/// one subscribing query must not surface on another established query holding
/// the same entity as an event update. Live delivery also returns validated events
/// independently of state advancement; the reactor tracks each subscriber's
/// event delivery so a prior initial snapshot cannot swallow a live update.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Delivery {
    Initial,
    Live,
}

/// What applying one delta did.
///
/// `change` is set only when the entity advanced and carries what this
/// delivery publishes. A failed delta returns an error without persisting a prefix.
#[derive(Debug)]
pub(crate) struct DeltaOutcome {
    pub entity: Option<Entity>,
    pub change: Option<EntityChange>,
    /// Validated live events remain delivery obligations even when an earlier
    /// initial snapshot already advanced the entity through them.
    pub live_events: Vec<Attested<proto::Event>>,
}

impl DeltaOutcome {
    fn unchanged(entity: Option<Entity>) -> Self { Self { entity, change: None, live_events: Vec::new() } }
}

impl NodeApplier {
    /// Apply initial-transfer deltas concurrently, notifying the reactor for each
    /// batch that completes together. A concurrent bare snapshot may leave the
    /// local entity unchanged; callers needing causal coverage use apply_delta
    /// and check coverage against the resulting entity head.
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
        // Stragglers do not hold back the rest: every batch of futures that is
        // ready together is notified together.
        let mut ready_chunks = ReadyChunks::new(deltas.into_iter().map(|delta| async move {
            let entity_id = delta.entity_id;
            (entity_id, Self::apply_delta(node, from_peer_id, delta, Delivery::Initial, event_getter, state_getter).await)
        }));

        let mut errors = Vec::new();
        while let Some(results) = ready_chunks.next().await {
            let mut batch = Vec::new();
            for (entity_id, result) in results {
                match result {
                    Ok(outcome) => {
                        batch.extend(outcome.change);
                    }
                    Err(cause) => errors.push(ApplyErrorItem { entity_id, cause }),
                }
            }
            if !batch.is_empty() {
                let failures = node.reactor.notify_change(batch).await;
                node.live_queries.fail_notifications(&failures);
            }
        }

        if !errors.is_empty() {
            return Err(ApplyError::Items(errors));
        }
        Ok(())
    }

    /// Apply one delta of any content. Errors before anything is persisted are
    /// returned as `Err`; everything that applied is described by the outcome.
    pub(crate) async fn apply_delta<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        delta: proto::EntityDelta,
        delivery: Delivery,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<DeltaOutcome, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        let entity_id = delta.entity_id;
        match delta.content {
            proto::DeltaContent::StateSnapshot { state } => {
                Self::apply_snapshot(node, from_peer_id, entity_id, state, Vec::new(), delivery, event_getter, state_getter).await
            }
            proto::DeltaContent::StateAndEvents { state, events } => {
                Self::apply_snapshot(node, from_peer_id, entity_id, state, events, delivery, event_getter, state_getter).await
            }
            proto::DeltaContent::EventBridge { events } => {
                Self::apply_bridge(node, from_peer_id, entity_id, events, delivery, event_getter, state_getter).await
            }
            proto::DeltaContent::StateAndRelation { .. } => Err(MutationError::InvalidUpdate("StateAndRelation is not supported")),
        }
    }

    /// Validate each event fragment structurally and against policy, then
    /// stage it for causal comparison. Every delta that carries events admits them
    /// here and nowhere else.
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
        // Wire order is untrusted for every multi-event shape: a child applied
        // before its staged parent gap-jumps the head and drops the parent's
        // operations. The producer also sorts; receivers must not rely on it.
        crate::event_dag::ordering::topo_sort_events(attested_events)
    }

    /// What a delivery publishes for an advanced entity.
    fn publish(entity: Entity, events: Vec<Attested<proto::Event>>, delivery: Delivery) -> EntityChange {
        match delivery {
            Delivery::Initial => EntityChange::new(entity, Vec::new()),
            Delivery::Live => EntityChange::new(entity, events),
        }
    }

    /// Apply a snapshot, with any connecting events carried beside it.
    ///
    /// A snapshot that strictly advances the entity replaces its state. Otherwise
    /// the carried events are applied as one atomic batch on a fork. Each storage
    /// retry decides again against the current state. The state-stream owner
    /// checks whether the resulting entity covers its outstanding targets.
    #[allow(clippy::too_many_arguments)]
    async fn apply_snapshot<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        entity_id: proto::EntityId,
        fragment: proto::StateFragment,
        carried: Vec<proto::EventFragment>,
        delivery: Delivery,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<DeltaOutcome, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        let events = Self::validate_and_stage(node, from_peer_id, entity_id, carried, event_getter)?;
        let live_events = if delivery == Delivery::Live { events.clone() } else { Vec::new() };
        let state: Attested<proto::EntityState> = (entity_id, fragment).into();
        node.policy_agent.validate_received_state(node, from_peer_id, &state)?;

        if let Some(entity) = Self::save_new_entity(node, &state.payload, &events, delivery, event_getter, state_getter).await? {
            let change = Self::publish(entity.clone(), events, delivery);
            return Ok(DeltaOutcome { entity: Some(entity), change: Some(change), live_events });
        }
        let entity = node
            .entities
            .get_or_retrieve(state_getter, event_getter, &entity_id)
            .await?
            .ok_or(RetrievalError::EntityNotFound(entity_id))?;

        let change = Self::apply_and_commit_state(node, &entity, &state.payload, &events, delivery, event_getter).await?;
        let change = change.map(|change| {
            let (entity, events) = change.into_parts();
            Self::publish(entity, events, delivery)
        });
        Ok(DeltaOutcome { entity: Some(entity), change, live_events })
    }

    /// Apply a bridge as one atomic batch for both initial and live delivery.
    async fn apply_bridge<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        entity_id: proto::EntityId,
        fragments: Vec<proto::EventFragment>,
        delivery: Delivery,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<DeltaOutcome, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: crate::retrieval::GetState + Send + Sync,
    {
        let events = Self::validate_and_stage(node, from_peer_id, entity_id, fragments, event_getter)?;
        if events.is_empty() {
            return Ok(DeltaOutcome::unchanged(node.entities.get(&entity_id)));
        }
        let live_events = if delivery == Delivery::Live { events.clone() } else { Vec::new() };
        let change = Self::apply_and_commit_events(node, &events, delivery, event_getter).await?.map(|change| {
            let (entity, events) = change.into_parts();
            Self::publish(entity, events, delivery)
        });
        let entity = match &change {
            Some(change) => Some(change.entity().clone()),
            None => node.entities.get_or_retrieve(state_getter, event_getter, &entity_id).await?,
        };
        Ok(DeltaOutcome { entity, change, live_events })
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
        delivery: Delivery,
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
        let mut publication = node.reactor.begin_commit().await;
        match publication.record_storage_outcome(storage_trx.commit().await)? {
            crate::storage::StorageCommitOutcome::Conflict { .. } => return Ok(None),
            crate::storage::StorageCommitOutcome::Committed(_) => {}
        }
        let (_, entity) = node.entities.with_state(state_getter, event_getter, state.entity_id, state.state.clone()).await?;
        publication.publish(&[EntityChange::new(entity.clone(), events.to_vec())], delivery == Delivery::Live);
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
        delivery: Delivery,
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
            let mut events = events.to_vec();
            if adopted {
                Self::check_snapshot_events(node, snapshot, &expected_head, &events, event_getter).await?;
            } else {
                candidate.apply_events(event_getter, &mut events, |_| Ok(None)).await?;
            }
            if !adopted && events.is_empty() {
                return Ok(None);
            }
            let state = proto::EntityState { entity_id: entity.id(), state: candidate.to_state()? };
            let attestation = node.policy_agent.attest_state(node, &state);
            let state = Attested::opt(state, attestation);
            let mut storage_trx = node.storage.transaction();
            storage_trx.set_state(&expected_head, &state).await?;
            storage_trx.add_events(&events).await?;
            let mut publication = node.reactor.begin_commit().await;
            publication.record_storage_outcome(storage_trx.commit().await)?.committed()?;
            if adopted {
                let (_, entity) = node
                    .entities
                    .with_state(&LocalStateGetter::new(node.storage.clone()), event_getter, entity.id(), state.payload.state)
                    .await?;
                let change = EntityChange::new(entity, events);
                publication.publish(std::slice::from_ref(&change), delivery == Delivery::Live);
                Ok(Some(change))
            } else {
                let change = candidate.commit(&node.entities, event_getter).await?;
                publication.publish(std::slice::from_ref(&change), delivery == Delivery::Live);
                Ok((!change.events().is_empty()).then_some(change))
            }
        })
    }

    /// Apply and persist the whole batch before publishing. Any application failure discards the fork.
    /// Storage conflicts retry from a fresh fork.
    async fn apply_and_commit_events<SE, PA, E>(
        node: &Node<SE, PA>,
        events: &[Attested<proto::Event>],
        delivery: Delivery,
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
            let mut publication = node.reactor.begin_commit().await;
            publication.record_storage_outcome(storage_trx.commit().await)?.committed()?;
            let change = candidate.commit(&node.entities, event_getter).await?;
            publication.publish(std::slice::from_ref(&change), delivery == Delivery::Live);
            Ok((!change.events().is_empty()).then_some(change))
        })
    }
}

#[cfg(test)]
mod tests;
