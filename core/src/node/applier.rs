use crate::entity::RemoteTrxEntity;
use crate::error::{ApplyError, ApplyErrorItem};
use crate::internal::prelude::*;
use crate::reactor::ChangeNotification;
use crate::retrieval::{CachedEventGetter, LocalStateGetter, SuspenseEvents};
use crate::storage::StorageTransaction;
use crate::util::ready_chunks::ReadyChunks;
use crate::util::Iterable;
use futures::stream::StreamExt;
use proto::{Attested, EventStructureError};
use std::collections::HashMap;

/// Applies entity deltas received from a peer, whether they arrive as initial
/// transfer (Fetch, QuerySubscribed, repair) or as live subscription updates.
/// One path handles every `DeltaContent`; the delivery decides only what the
/// reactor change publishes. Each delta is applied atomically.
pub struct NodeApplier;

/// How a delta reached this node.
///
/// Initial transfer publishes no events on its reactor changes: a repair for
/// one subscribing query must not surface on another established query holding
/// the same entity as an event update. Live delivery publishes the events it
/// applied, which is what subscribers are listening for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Delivery {
    Initial,
    Live,
}

/// A snapshot the local entity did not incorporate: the entity stands at
/// `actual`, which neither equals nor descends from the offered `target`, and
/// nothing supplied with the snapshot connected the two. The caller decides
/// whether to ask the sender for connecting history; the applier never treats
/// an unchanged entity as coverage of the target.
#[derive(Debug, Clone)]
pub(crate) struct Divergence {
    pub entity_id: proto::EntityId,
    pub target: proto::Clock,
    pub actual: proto::Clock,
}

/// What applying one delta did.
///
/// `change` is set only when the entity advanced and carries what this
/// delivery publishes. `divergence` is set when a snapshot target remains
/// unincorporated. A failed delta returns an error without persisting a prefix.
#[derive(Debug)]
pub(crate) struct DeltaOutcome {
    pub entity: Option<Entity>,
    pub change: Option<EntityChange>,
    pub divergence: Option<Divergence>,
}

/// Heads observed after successful application, with any divergences.
/// These heads are coverage evidence at observation time, without retaining entities.
#[derive(Debug, Default)]
pub(crate) struct AppliedDeltas {
    heads: HashMap<proto::EntityId, proto::Clock>,
    divergences: Vec<Divergence>,
}

impl AppliedDeltas {
    /// The committed head observed while collecting a successful outcome.
    pub(crate) fn observed_head(&self, entity_id: &proto::EntityId) -> Option<proto::Clock> { self.heads.get(entity_id).cloned() }

    pub(crate) fn diverged(&self, entity_id: &proto::EntityId) -> bool { self.divergences.iter().any(|gap| &gap.entity_id == entity_id) }
}

impl DeltaOutcome {
    fn unchanged(entity: Option<Entity>) -> Self { Self { entity, change: None, divergence: None } }
}

impl NodeApplier {
    /// Apply a batch of live updates pushed outside any sequenced stream, from a
    /// peer we hold a standing query with. Such a push names no single query,
    /// so it runs under the union of every standing query's credentials with
    /// that peer.
    pub(crate) async fn apply_updates<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        items: Vec<proto::SubscriptionUpdateItem>,
    ) -> Result<(), ApplyError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let Some(relay) = &node.subscription_relay else {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates without a subscription relay").into());
        };
        // A peer may push only what we asked it for, so a push is admissible
        // only against a standing query of ours with that peer.
        if !relay.has_subscription_with_peer(from_peer_id) {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates from a peer we hold no subscription with").into());
        }
        let credentials = relay.get_contexts_for_peer(from_peer_id);
        Self::apply_stream_updates(node, from_peer_id, &credentials, items).await
    }

    /// Apply a batch of live updates from one query's sequenced state stream,
    /// under the credentials captured when that query was opened with the peer.
    /// The stream owner has already checked that the query is current, so the
    /// stream is its own authorization and no standing-subscription gate runs.
    /// One bad item does not poison the batch: every item is attempted, the
    /// reactor is notified for the applied subset, and the failures are
    /// returned together.
    pub(crate) async fn apply_stream_updates<SE, PA, C>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        credentials: &C,
        items: Vec<proto::SubscriptionUpdateItem>,
    ) -> Result<(), ApplyError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        C: Iterable<PA::ContextData> + Send + Sync,
    {
        tracing::debug!("received subscription update for {} items", items.len());
        let event_getter = CachedEventGetter::new(node, credentials);
        let state_getter = LocalStateGetter::new(node.storage.clone());

        let mut changes = Vec::new();
        let mut memberships = Vec::new();
        let mut errors: Vec<ApplyErrorItem> = Vec::new();
        for item in items {
            let entity_id = item.delta.entity_id;
            match Self::apply_delta(node, from_peer_id, item.delta, Delivery::Live, &event_getter, &state_getter).await {
                Ok(outcome) => {
                    changes.extend(outcome.change);
                    let failure = outcome.divergence.map(Self::live_divergence_failure);
                    match failure {
                        Some(cause) => {
                            tracing::warn!("failed to apply update for {}: {}", entity_id, cause);
                            errors.push(ApplyErrorItem { entity_id, cause });
                        }
                        None => {
                            if let Some(entity) = outcome.entity {
                                memberships.extend(item.predicate_relevance.into_iter().map(|(query, _)| (query, entity.clone())));
                            }
                        }
                    }
                }
                Err(cause) => {
                    tracing::warn!("failed to apply update for {}: {}", entity_id, cause);
                    errors.push(ApplyErrorItem { entity_id, cause });
                }
            }
        }

        node.reactor.notify_change(changes).await;
        for (query, entity) in memberships {
            if let Some(query) = node.live_queries.get(query) {
                if let Err(error) = query.refresh_membership(entity).await {
                    tracing::debug!(%error, "query disappeared during membership delivery");
                }
            }
        }

        if !errors.is_empty() {
            return Err(ApplyError::Items(errors));
        }
        Ok(())
    }

    /// A live update carries the sender's own commit with the events that connect
    /// it, so an entity that still diverges afterwards is missing an earlier
    /// update. That is a broken stream, not progress: the batch reports it as a
    /// failure so the stream owner can restart from a known generation.
    fn live_divergence_failure(divergence: Divergence) -> MutationError {
        tracing::warn!(entity = %divergence.entity_id, target = %divergence.target, actual = %divergence.actual, "live update left local state divergent");
        MutationError::InvalidUpdate("live update diverged from local state and its events did not connect them")
    }

    /// Apply initial-transfer deltas concurrently, notifying the reactor for each
    /// batch that completes together. Snapshots the local entity could not
    /// incorporate are returned as divergences for the caller to repair; they are
    /// not errors, and they are never reported as applied.
    pub(crate) async fn apply_deltas<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        deltas: Vec<proto::EntityDelta>,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<AppliedDeltas, ApplyError>
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

        let mut applied = AppliedDeltas::default();
        let mut errors = Vec::new();
        while let Some(results) = ready_chunks.next().await {
            let mut batch = Vec::new();
            for (entity_id, result) in results {
                match result {
                    Ok(outcome) => {
                        batch.extend(outcome.change);
                        applied.divergences.extend(outcome.divergence);
                        if let Some(entity) = outcome.entity {
                            applied.heads.insert(entity_id, entity.head());
                        }
                    }
                    Err(cause) => errors.push(ApplyErrorItem { entity_id, cause }),
                }
            }
            if !batch.is_empty() {
                node.reactor.notify_change(batch).await;
            }
        }

        if !errors.is_empty() {
            return Err(ApplyError::Items(errors));
        }
        Ok(applied)
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
        // operations (V4). The producer also sorts; receivers must not rely on it.
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
    /// retry decides again against the current state. An entity that
    /// still neither equals nor descends from the target is reported as a
    /// divergence; it is never reported as covered.
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
        let target = &state.payload.state;

        if let Some(entity) = Self::save_new_entity(node, &state.payload, &events, event_getter, state_getter).await? {
            let change = Self::publish(entity.clone(), events, delivery);
            return Ok(DeltaOutcome { entity: Some(entity), change: Some(change), divergence: None });
        }
        let entity = node
            .entities
            .get_or_retrieve(state_getter, event_getter, &entity_id)
            .await?
            .ok_or(RetrievalError::EntityNotFound(entity_id))?;

        let (change, covered) = Self::apply_and_commit_state(node, &entity, &state.payload, &events, event_getter).await?;
        let change = change.map(|change| {
            let (entity, events) = change.into_parts();
            Self::publish(entity, events, delivery)
        });
        let actual = entity.head();
        let divergence = (!covered).then(|| Divergence { entity_id, target: target.head.clone(), actual });
        Ok(DeltaOutcome { entity: Some(entity), change, divergence })
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
        let change = Self::apply_and_commit_events(node, &events, event_getter).await?.map(|change| {
            let (entity, events) = change.into_parts();
            Self::publish(entity, events, delivery)
        });
        let entity = match &change {
            Some(change) => Some(change.entity().clone()),
            None => node.entities.get_or_retrieve(state_getter, event_getter, &entity_id).await?,
        };
        Ok(DeltaOutcome { entity, change, divergence: None })
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
        let _publication = node.commit_publication_lock.lock().await;
        match storage_trx.commit().await? {
            crate::storage::StorageCommitOutcome::Conflict { .. } => return Ok(None),
            crate::storage::StorageCommitOutcome::Committed(_) => {}
        }
        let (_, entity) = node.entities.with_state(state_getter, event_getter, state.entity_id, state.state.clone()).await?;
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
    ) -> Result<(Option<EntityChange>, bool), MutationError>
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
            let actual = candidate.head();
            // Determine coverage on the unpublished fork: a failed comparison must
            // not discard a notification after state has already been committed.
            let covered = if adopted || actual == snapshot.state.head {
                true
            } else {
                let comparison =
                    crate::event_dag::compare(event_getter, &actual, &snapshot.state.head, crate::event_dag::DEFAULT_BUDGET).await?;
                matches!(
                    comparison.relation,
                    crate::event_dag::AbstractCausalRelation::StrictDescends { .. } | crate::event_dag::AbstractCausalRelation::Equal
                )
            };
            if !adopted && events.is_empty() {
                return Ok((None, covered));
            }
            let state = proto::EntityState { entity_id: entity.id(), state: candidate.to_state()? };
            let attestation = node.policy_agent.attest_state(node, &state);
            let state = Attested::opt(state, attestation);
            let mut storage_trx = node.storage.transaction();
            storage_trx.set_state(&expected_head, &state).await?;
            storage_trx.add_events(&events).await?;
            let _publication = node.commit_publication_lock.lock().await;
            storage_trx.commit().await?.committed()?;
            if adopted {
                let (advanced, entity) = node
                    .entities
                    .with_state(&LocalStateGetter::new(node.storage.clone()), event_getter, entity.id(), state.payload.state)
                    .await?;
                Ok(((advanced == Some(true)).then(|| EntityChange::new(entity, events)), covered))
            } else {
                let change = candidate.commit(&node.entities, event_getter).await?;
                Ok(((!change.events().is_empty()).then_some(change), covered))
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
            let _publication = node.commit_publication_lock.lock().await;
            storage_trx.commit().await?.committed()?;
            let change = candidate.commit(&node.entities, event_getter).await?;
            Ok((!change.events().is_empty()).then_some(change))
        })
    }
}

#[cfg(test)]
mod tests;
