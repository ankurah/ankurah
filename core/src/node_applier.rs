use crate::{
    changes::EntityChange,
    error::{ApplyError, ApplyErrorItem, MutationError},
    ingest::{self, StagingArea},
    node::Node,
    policy::PolicyAgent,
    retrieval::{CachedEventGetter, GetState, LocalStateGetter, SuspenseEvents},
    storage::StorageEngine,
    util::ready_chunks::ReadyChunks,
};
use ankurah_proto::{self as proto};
use futures::stream::StreamExt;
use proto::Attested;

/// Persistence adapter for ingest. It supplies the node's PolicyAgent for
/// state attestation and centralizes marker-safe resident persistence for
/// apply, Get response, remote commit, and local commit lanes.
pub(crate) struct NodePersist<'a, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) node: &'a Node<SE, PA>,
    pub(crate) collection: &'a crate::storage::StorageCollectionWrapper,
}

#[async_trait::async_trait]
impl<'a, SE, PA> ingest::PersistState for NodePersist<'a, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Persist the canonical resident's current state. The completed-persist
    /// marker elides the write only when it names the resident's exact current
    /// head in the current reset epoch.
    ///
    /// The reset fence orders the whole operation before or after a hard
    /// reset. Inside that fence, the per-entity persist span serializes the
    /// canonical check, marker check, snapshot, storage write, and marker
    /// stamp. This prevents sibling lanes from landing snapshots out of order
    /// and prevents a stale duplicate resident from overwriting the canonical
    /// state. The marker records the head returned by `save_state`, never a
    /// later re-read.
    ///
    /// Lock order is reset fence first, then the per-entity persist span.
    /// Callers that already hold the reset fence use `persist_fenced` to avoid
    /// recursively acquiring the fair read lock.
    async fn persist(&self, entity: &crate::entity::Entity) -> Result<(), MutationError> {
        let _fence = self.node.entities.reset_fence_read().await;
        self.persist_fenced(entity).await
    }

    async fn persist_fenced(&self, entity: &crate::entity::Entity) -> Result<(), MutationError> {
        let span = self.node.entities.persist_span(entity.id());
        let _guard = span.lock().await;
        // Markers and snapshots belong to one Entity instance. Refuse a
        // non-canonical live instance before consulting its marker so it
        // cannot overwrite or claim durability for the canonical resident.
        // An absent map entry is allowed because this is then the only live
        // instance known to the node. A refused delivery is retryable; its
        // redelivery resolves to the canonical resident.
        if let Some(canonical) = self.node.entities.get(&entity.id()) {
            if canonical != *entity {
                return Err(MutationError::StaleInstancePersistRefused(entity.id()));
            }
        }
        let epoch = self.node.entities.reset_epoch();
        if entity.persist_marker_current(epoch) {
            return Ok(());
        }
        let persisted_head = NodeApplier::save_state(self.node, entity, self.collection).await?;
        entity.stamp_persist_marker(epoch, persisted_head);
        Ok(())
    }
}

/// Applies streaming subscription updates and initial Fetch/QuerySubscribed
/// deltas received from a peer.
pub struct NodeApplier;

impl NodeApplier {
    /// Apply authenticated remote updates without relaying them back to peers.
    pub(crate) async fn apply_updates_at_epoch<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        items: Vec<proto::SubscriptionUpdateItem>,
        expected_epoch: u64,
    ) -> Result<(), ApplyError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        tracing::debug!("received subscription update for {} items", items.len());

        // Validate against every context registered for this peer. Narrowing
        // by one predicate would hide an apply failure from the peer's other
        // active contexts on this node.
        let Some(relay) = &node.subscription_relay else {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates without a subscription relay").into());
        };
        let cdata = relay.get_contexts_for_peer(from_peer_id);
        if cdata.is_empty() {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates without at least predicate context").into());
        }

        // Apply all updates. One bad item must not poison the batch: failures
        // are collected per item, the remaining items still apply, and the
        // reactor is notified for the successfully applied subset.
        let mut changes = Vec::new();
        let mut errors: Vec<ApplyErrorItem> = Vec::new();
        for update in items {
            let entity_id = update.entity_id;
            let item_model = update.model;
            let result = async {
                // Resolve the wire model id to a local collection through the
                // well-known map or catalog, rejecting an unknown model (#330).
                let collection_id = node.resolve_model_wait_at_epoch(&update.model, expected_epoch).await?;
                let collection = node.collections.get(&collection_id).await?;
                // Node-held staging keeps unapplied events across deliveries.
                // The getter shares it so lineage discovery and pipeline
                // scheduling observe the same retained ancestors.
                let staging = node.staging_for(&collection_id);
                let event_getter = CachedEventGetter::with_staging_at_epoch(
                    collection_id,
                    collection.clone(),
                    node,
                    &cdata,
                    staging.clone(),
                    expected_epoch,
                );
                let state_getter = LocalStateGetter::new(collection);
                Self::apply_update(
                    node,
                    from_peer_id,
                    update,
                    &staging,
                    &event_getter,
                    &state_getter,
                    &mut changes,
                    &mut (),
                    expected_epoch,
                )
                .await
            }
            .await;
            if let Err(cause) = result {
                tracing::warn!("failed to apply update for model {}/{}: {}", item_model.to_base64_short(), entity_id, cause);
                errors.push(ApplyErrorItem { entity_id, model: item_model, cause });
            }
        }

        node.reactor.notify_change(changes).await;

        if !errors.is_empty() {
            return Err(ApplyError::Items(errors));
        }
        Ok(())
    }

    /// TEST ONLY: drive the subscription-update applier directly, returning
    /// the aggregate per-item error that handle_message otherwise folds into
    /// the update ack where tests cannot inspect it. Typed-error pins match
    /// on the per-item causes this returns.
    #[cfg(feature = "test-helpers")]
    pub async fn apply_updates_for_test<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        items: Vec<proto::SubscriptionUpdateItem>,
    ) -> Result<(), ApplyError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let expected_epoch = node.entities.reset_epoch();
        Self::apply_updates_at_epoch(node, from_peer_id, items, expected_epoch).await
    }

    /// Validate each event fragment against policy and stage it for BFS
    /// discovery. Shared by every update arm that carries events. Stages the
    /// ATTESTED event so a buffered or scheduled event can later be
    /// committed with the attestations it arrived with. Validation runs for
    /// the WHOLE batch before anything stages: with node-held staging, a
    /// rejected item must leave nothing behind (rejection is not buffering).
    fn validate_and_stage<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        entity_id: proto::EntityId,
        model: proto::EntityId,
        event_fragments: Vec<proto::EventFragment>,
        staging: &StagingArea,
    ) -> Result<Vec<Attested<proto::Event>>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut attested_events = Vec::new();
        for fragment in event_fragments {
            let attested_event: Attested<proto::Event> = (entity_id, model, fragment).into();
            // Relayed catalog events are trusted from the serving peer the
            // way every other served event is (RFC section 4 in
            // specs/model-property-metadata/rfc.md). The
            // structural write ban covers the transaction paths
            // (CommitTransaction and local commits); this ingest path has
            // no allocator-identity check -- in the single-allocator
            // topology the serving durable IS the allocator, and
            // allocator-identity enforcement for multi-peer topologies is
            // #309's routing work. validate_received_event is the
            // per-agent hook if a deployment wants to gate this earlier.
            node.policy_agent.validate_received_event(node, from_peer_id, &attested_event)?;
            attested_events.push(attested_event);
        }
        // Collapse duplicates by id to their first occurrence. Staging is
        // id-keyed and the planner dedups anyway; this protects the batch
        // returned here, which the StateAndEvent fast path echoes into its
        // EntityChange. The wire is untrusted, so any sender may repeat a
        // fragment.
        let mut seen = std::collections::BTreeSet::new();
        attested_events.retain(|event| seen.insert(event.payload.id()));
        staging.try_stage_batch(attested_events.iter().cloned())?;
        Ok(attested_events)
    }

    /// Unstage a batch whose item failed between staging and execution
    /// (entity retrieval or planning): with the node-held area those events
    /// must not linger as if buffered; the sender's retry re-stages them.
    /// Retention for events that reach execution is the executor's sweep.
    fn unstage_batch(staging: &StagingArea, batch: &[proto::EventId]) {
        for id in batch {
            staging.remove(id);
        }
    }

    async fn apply_update<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        update: proto::SubscriptionUpdateItem,
        staging: &StagingArea,
        event_getter: &E,
        state_getter: &S,
        changes: &mut Vec<EntityChange>,
        entities: &mut impl Pushable<crate::entity::Entity>,
        expected_epoch: u64,
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: GetState + Send + Sync,
    {
        // TODO: do we actually need predicate_relevance?
        let proto::SubscriptionUpdateItem { entity_id, model, content, predicate_relevance: _, source_queries: _ } = update;
        let collection_id = node.resolve_model_wait_at_epoch(&model, expected_epoch).await?;
        let collection = node.collections.get(&collection_id).await?;

        match content {
            // EventOnly: equivalent to old SubscriptionItem::Change
            proto::UpdateContent::EventOnly(event_fragments) => {
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, model, event_fragments, staging)?;
                let batch: Vec<proto::EventId> = attested_events.iter().map(|e| e.payload.id()).collect();

                // Second arm on the ingest pipeline (D1 M3). The planner
                // orders the staged closure parents-first (wire order is
                // untrusted for every multi-event shape, V4) and the executor
                // owns apply-then-commit, uniform state persistence, phantom
                // eviction, and the applied-prefix containment this arm
                // pioneered (C4-11). The entity comes from local storage or
                // a remote peer, since this shape carries no fragment.
                let planned = async {
                    let entity = node.entities.get_retrieve_or_create(state_getter, event_getter, &collection_id, &entity_id).await?;
                    let plan = ingest::plan_entity_for(entity.id(), &entity.head(), &batch, staging, event_getter).await?;
                    Ok::<_, MutationError>((entity, plan))
                }
                .await;
                let (entity, plan) = match planned {
                    Ok(v) => v,
                    Err(e) => {
                        Self::unstage_batch(staging, &batch);
                        return Err(e);
                    }
                };
                entities.push(entity.clone());

                let persist = NodePersist { node, collection: &collection };
                let outcome =
                    ingest::execute_plan_at_epoch(plan, &entity, &node.entities, staging, event_getter, &persist, expected_epoch).await;

                // Anything applied before a failure is real progress; notify
                // it. Streaming updates carry the applied events on the change.
                if let Some(change) = outcome.change.clone() {
                    changes.push(change);
                }

                if let Some(failure) = outcome.failure {
                    return Err(failure);
                }
                // Per-item error surface: NeedsState is the typed empty-head
                // lineage rejection. NeedsEvents retains the event locally
                // but remains retryable so the sender holds a lossless lease
                // while atomic capacity admission applies backpressure.
                if let Some(e) = outcome.needs_state_error() {
                    return Err(e);
                }
                if let Some(e) = outcome.needs_events_error() {
                    return Err(e);
                }
            }

            // StateAndEvent: equivalent to old SubscriptionItem::Add
            proto::UpdateContent::StateAndEvent(state_fragment, event_fragments) => {
                // State validation runs before anything stages so a rejected
                // item leaves nothing in the node-held area.
                let state: Attested<proto::EntityState> = (entity_id, model, state_fragment.clone()).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &state)?;
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, model, event_fragments, staging)?;
                let batch: Vec<proto::EventId> = attested_events.iter().map(|e| e.payload.id()).collect();

                // Fast path through the shared state-apply: fresh adoption or
                // strict descent takes the snapshot wholesale, committing the
                // accompanying events before the buffer persists.
                let persist = NodePersist { node, collection: &collection };
                let applied = match ingest::apply_state_feed_at_epoch(
                    &node.entities,
                    state_getter,
                    event_getter,
                    staging,
                    entity_id,
                    model,
                    collection_id.clone(),
                    state.payload.state,
                    &attested_events,
                    &persist,
                    expected_epoch,
                )
                .await
                {
                    Ok(applied) => applied,
                    Err(e) => {
                        Self::unstage_batch(staging, &batch);
                        return Err(e);
                    }
                };
                let entity = applied.entity.clone();
                entities.push(applied.entity);

                if let Some(change) = applied.change {
                    changes.push(change);
                } else {
                    // State not applied (divergence or older): fall back to
                    // event-by-event application through the pipeline, which
                    // handles DivergedSince by merging concurrent branches.
                    // The events are already staged; the planner orders them.
                    let plan = match ingest::plan_entity_for(entity.id(), &entity.head(), &batch, staging, event_getter).await {
                        Ok(plan) => plan,
                        Err(e) => {
                            Self::unstage_batch(staging, &batch);
                            return Err(e);
                        }
                    };
                    let outcome =
                        ingest::execute_plan_at_epoch(plan, &entity, &node.entities, staging, event_getter, &persist, expected_epoch).await;

                    if let Some(change) = outcome.change.clone() {
                        changes.push(change);
                    }
                    if let Some(failure) = outcome.failure {
                        return Err(failure);
                    }
                    // Same per-item error surface as the EventOnly arm.
                    if let Some(e) = outcome.needs_state_error() {
                        return Err(e);
                    }
                    if let Some(e) = outcome.needs_events_error() {
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Serialize, attest, and persist the resident's current state.
    /// Returns the HEAD the completed set_state wrote (the snapshot
    /// to_state read under the entity lock, which may lag a concurrent
    /// advance): the persist-currency marker must stamp exactly what was
    /// persisted, never a re-read.
    async fn save_state<SE, PA>(
        node: &Node<SE, PA>,
        entity: &crate::entity::Entity,
        collection_wrapper: &crate::storage::StorageCollectionWrapper,
    ) -> Result<proto::Clock, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let state = entity.to_state()?;
        let persisted_head = state.head.clone();
        let entity_state = proto::EntityState { entity_id: entity.id(), model: entity.model_id()?, state };
        let attestation = node.policy_agent.attest_state(node, &entity_state);
        let attested = Attested::opt(entity_state, attestation);
        collection_wrapper.set_state(attested).await?;
        Ok(persisted_head)
    }

    /// Apply multiple EntityDeltas in parallel with batched reactor notification
    /// Drains all ready futures per wake and calls reactor.notify_change for each batch
    /// Collects all errors and returns them at the end - caller decides whether to fail or log
    pub(crate) async fn apply_deltas_at_epoch<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        deltas: Vec<proto::EntityDelta>,
        staging: &StagingArea,
        event_getter: &E,
        state_getter: &S,
        expected_epoch: u64,
    ) -> Result<(), ApplyError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: GetState + Send + Sync,
    {
        // do not wait for all apply_delta futures to complete - we need to apply all updates in a timely fashion
        // if there are stragglers, they will be picked up on the next wake
        // this should in theory be deterministic for eventbridge cases where all events are immediately available
        let mut ready_chunks = ReadyChunks::new(
            deltas
                .into_iter()
                .map(|delta| Self::apply_delta(node, from_peer_id, delta, staging, event_getter, state_getter, expected_epoch)),
        );

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
        staging: &StagingArea,
        event_getter: &E,
        state_getter: &S,
        expected_epoch: u64,
    ) -> Result<Option<EntityChange>, ApplyErrorItem>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: GetState + Send + Sync,
    {
        let entity_id = delta.entity_id;
        let model = delta.model;

        let result = Self::apply_delta_inner(node, from_peer_id, delta, staging, event_getter, state_getter, expected_epoch).await;
        result.map_err(|cause| ApplyErrorItem { entity_id, model, cause })
    }

    async fn apply_delta_inner<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        delta: proto::EntityDelta,
        staging: &StagingArea,
        event_getter: &E,
        state_getter: &S,
        expected_epoch: u64,
    ) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: GetState + Send + Sync,
    {
        // INGRESS (#330): resolve the wire model id to the local collection
        // (well-knowns, then catalog) or reject this delta.
        let collection_id = node.resolve_model_wait_at_epoch(&delta.model, expected_epoch).await?;
        let collection = node.collections.get(&collection_id).await?;

        match delta.content {
            proto::DeltaContent::StateSnapshot { state } => {
                let attested_state = (delta.entity_id, delta.model, state).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &attested_state)?;

                // Shared state-apply: advance-gated persistence and the
                // advance-only change. A no-op snapshot (the entity is
                // already resident at this head, or the snapshot is older)
                // must not notify: notify_change is global across every
                // subscription on the node, so a no-op snapshot for one
                // subscribing query surfaces on ANOTHER already-established
                // query, which holds the same entity, as an empty-events
                // ItemChange::Update. That is the subscription-notification
                // race behind the intermittent server_edits_subscription
                // failure. Fresh adoption and strict descent are real
                // changes and still notify.
                let persist = NodePersist { node, collection: &collection };
                let applied = ingest::apply_state_feed_at_epoch(
                    &node.entities,
                    state_getter,
                    event_getter,
                    staging,
                    delta.entity_id,
                    delta.model,
                    collection_id,
                    attested_state.payload.state,
                    &[],
                    &persist,
                    expected_epoch,
                )
                .await?;

                if !applied.advanced {
                    return Ok(None);
                }

                // A state-only snapshot can still re-drive buffered
                // descendants; those events belong on the one resulting
                // change so live queries observe the final resident state.
                Ok(applied.change)
            }

            proto::DeltaContent::EventBridge { events } => {
                // Bridge events pass the same policy gate as subscription
                // updates; transport must not decide trust.
                let attested_events = Self::validate_and_stage(node, from_peer_id, delta.entity_id, delta.model, events, staging)?;
                let batch: Vec<proto::EventId> = attested_events.iter().map(|e| e.payload.id()).collect();

                // First arm on the ingest pipeline (D1 M2). The planner
                // orders parents-first over the staging closure (wire order
                // untrusted, V4) and the executor owns apply-then-commit,
                // back-fill of integrated-but-unstored events, advance-gated
                // state persistence, and phantom eviction on failure.
                let planned = async {
                    let entity = node.entities.get_retrieve_or_create(state_getter, event_getter, &collection_id, &delta.entity_id).await?;
                    let plan = ingest::plan_entity_for(entity.id(), &entity.head(), &batch, staging, event_getter).await?;
                    Ok::<_, MutationError>((entity, plan))
                }
                .await;
                let (entity, plan) = match planned {
                    Ok(v) => v,
                    Err(e) => {
                        Self::unstage_batch(staging, &batch);
                        return Err(e);
                    }
                };
                let persist = NodePersist { node, collection: &collection };
                let outcome =
                    ingest::execute_plan_at_epoch(plan, &entity, &node.entities, staging, event_getter, &persist, expected_epoch).await;

                if let Some(failure) = outcome.failure {
                    return Err(failure);
                }
                // Per-item error surface, typed at M5. NeedsState should not
                // arise on bridges (they include genesis); if it does, the
                // typed empty-head rejection says so honestly.
                if let Some(e) = outcome.needs_state_error() {
                    return Err(e);
                }
                if let Some(e) = outcome.needs_events_error() {
                    return Err(e);
                }

                // Advance-only notification, exactly as before: a bridge whose
                // every event was already integrated emits nothing (the
                // spurious empty-events Update class, see the StateSnapshot
                // arm above).
                if !outcome.advanced() {
                    return Ok(None);
                }

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
