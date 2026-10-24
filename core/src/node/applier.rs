use crate::entity::RemoteTrxEntity;
use crate::error::{ApplyError, ApplyErrorItem};
use crate::internal::prelude::*;
use crate::reactor::ChangeNotification;
use crate::retrieval::{LocalStateGetter, SuspenseEvents};
use crate::util::ready_chunks::ReadyChunks;
use entity_write::{EntityWrite, ResidentAdvance};
use futures::stream::StreamExt;
use proto::{Attested, EventStructureError};

mod entity_write;

/// Applies entity deltas received from a peer, whether they arrive as initial
/// transfer (Fetch, QuerySubscribed, repair) or as live subscription updates.
/// One path handles every `DeltaContent`; the delivery decides only what the
/// reactor change publishes. Each delta is applied atomically, and a delta whose
/// write reached storage reaches the resident entity even if its caller is dropped.
pub struct NodeApplier;

/// How a delta reached this node.
///
/// Initial transfer publishes no events on its reactor changes: a repair for
/// one subscribing query must not surface on another established query holding
/// the same entity as an event update. Live delivery publishes every event it
/// validated, whether or not the entity advanced; the reactor tracks each
/// subscriber's event delivery so a prior initial snapshot cannot swallow a live update.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Delivery {
    Initial,
    Live,
}

/// What applying one delta did.
#[derive(Debug)]
pub(crate) struct DeltaOutcome {
    /// The entity the delta applied to; an empty bridge reports it only if it is resident.
    pub entity: Option<Entity>,
    /// The reactor change this delivery publishes, if any.
    pub change: Option<EntityChange>,
}

impl NodeApplier {
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

    /// The reactor change a delivery publishes for one applied delta. Initial transfer publishes
    /// an advanced entity without its events. Live delivery publishes every validated event, even
    /// when an earlier initial snapshot already advanced the entity through them, and publishes an
    /// advanced entity even when no events came with it.
    fn change_for_delivery(
        entity: &Entity,
        advanced: bool,
        events: Vec<Attested<proto::Event>>,
        delivery: Delivery,
    ) -> Option<EntityChange> {
        match delivery {
            Delivery::Initial => advanced.then(|| EntityChange::new(entity.clone(), Vec::new())),
            Delivery::Live => (advanced || !events.is_empty()).then(|| EntityChange::new(entity.clone(), events)),
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
        let state: Attested<proto::EntityState> = (entity_id, fragment).into();
        node.policy_agent.validate_received_state(node, from_peer_id, &state)?;

        if let Some(entity) = Self::save_new_entity(node, &state.payload, &events, delivery, event_getter, state_getter).await? {
            let change = Self::change_for_delivery(&entity, true, events, delivery);
            return Ok(DeltaOutcome { entity: Some(entity), change });
        }
        let entity = node
            .entities
            .get_or_retrieve(state_getter, event_getter, &entity_id)
            .await?
            .ok_or(RetrievalError::EntityNotFound(entity_id))?;

        let advanced = Self::apply_and_commit_state(node, &entity, &state.payload, &events, delivery, event_getter).await?.is_some();
        let change = Self::change_for_delivery(&entity, advanced, events, delivery);
        Ok(DeltaOutcome { entity: Some(entity), change })
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
            return Ok(DeltaOutcome { entity: node.entities.get(&entity_id), change: None });
        }
        let advanced = Self::apply_and_commit_events(node, &events, delivery, event_getter).await?;
        let entity = match &advanced {
            Some(change) => change.entity().clone(),
            // The bridge committed, so storage holds the entity even if no event was new.
            None => node
                .entities
                .get_or_retrieve(state_getter, event_getter, &entity_id)
                .await?
                .ok_or(RetrievalError::EntityNotFound(entity_id))?,
        };
        let change = Self::change_for_delivery(&entity, advanced.is_some(), events, delivery);
        Ok(DeltaOutcome { entity: Some(entity), change })
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
        let write = EntityWrite {
            expected_head: proto::Clock::default(),
            state: Attested::opt(state.clone(), attestation),
            events: events.to_vec(),
            resident: ResidentAdvance::Create,
        };
        // A refused write means another delivery stored the entity first.
        Ok(write.commit(node, delivery).await?.map(|change| change.into_parts().0))
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
            let resident = if adopted { ResidentAdvance::Adopt(candidate) } else { ResidentAdvance::Apply(candidate) };
            let write = EntityWrite { expected_head, state: Attested::opt(state, attestation), events, resident };
            let change = write.commit(node, delivery).await?.ok_or(MutationError::WriteConflict)?;
            Ok((adopted || !change.events().is_empty()).then_some(change))
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
            let write = EntityWrite {
                expected_head,
                state: Attested::opt(state, attestation),
                events,
                resident: ResidentAdvance::Apply(candidate),
            };
            let change = write.commit(node, delivery).await?.ok_or(MutationError::WriteConflict)?;
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
                Self::apply_delta(node, from_peer_id, delta, Delivery::Initial, event_getter, state_getter)
                    .await
                    .map_err(|cause| ApplyErrorItem { entity_id, cause })
            }
        }));

        let mut all_errors = Vec::new();

        while let Some(results) = ready_chunks.next().await {
            let mut batch = Vec::new();

            for result in results {
                match result {
                    Ok(outcome) => batch.extend(outcome.change),
                    Err(error_item) => {
                        all_errors.push(error_item);
                    }
                }
            }

            if !batch.is_empty() {
                node.notify_reactor(batch).await;
            }
        }

        if !all_errors.is_empty() {
            return Err(ApplyError::Items(all_errors));
        }

        Ok(())
    }

    /// Apply one delta of any content and describe what it did. A refused delta changes nothing:
    /// structure, policy and lineage checks run before anything is written, and the delta's
    /// state and events commit in one storage transaction. An error from that commit or after it
    /// is returned as well, but it does not prove that nothing persisted.
    ///
    /// The caller may drop this future at any point. Before the delta's write is handed to
    /// storage, dropping it applies nothing; after, the commit, the resident entity's advance and
    /// the publication to resident maintenance finish without the caller. A failed state stream
    /// can therefore drop its applications: it starts nothing new and finishes what is committing.
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

            proto::DeltaContent::StateAndRelation { .. } => Err(MutationError::InvalidUpdate("StateAndRelation not yet implemented")),
        }
    }
}

#[cfg(test)]
mod tests;
