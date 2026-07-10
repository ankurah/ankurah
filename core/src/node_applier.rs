use crate::{
    changes::EntityChange,
    error::{ApplyError, ApplyErrorItem, MutationError},
    node::Node,
    policy::PolicyAgent,
    retrieval::{CachedEventGetter, GetState, LocalStateGetter, SuspenseEvents},
    storage::StorageEngine,
    util::ready_chunks::ReadyChunks,
};
use ankurah_proto::{self as proto};
use futures::stream::StreamExt;
use proto::Attested;

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
                // INGRESS (#330): resolve the wire model id to the local
                // collection (well-knowns, then catalog) or reject this item.
                let collection_id = node.resolve_model_wait(&update.model).await?;
                let collection = node.collections.get(&collection_id).await?;
                let event_getter = CachedEventGetter::new(collection_id, collection.clone(), node, &cdata);
                let state_getter = LocalStateGetter::new(collection);
                Self::apply_update(node, from_peer_id, update, &event_getter, &state_getter, &mut changes, &mut ()).await
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

    /// Validate each event fragment against policy and stage it for BFS
    /// discovery. Shared by every update arm that carries events.
    fn validate_and_stage<SE, PA, E>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        entity_id: proto::EntityId,
        model: proto::EntityId,
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
            let attested_event: Attested<proto::Event> = (entity_id, model, fragment).into();
            // Relayed catalog events are trusted FROM THE SERVING PEER the
            // way every other served event is (RFC section 4 in specs/model-property-metadata/rfc.md, rev 4). The
            // structural write ban covers the transaction paths
            // (CommitTransaction and local commits); this ingest path has
            // no allocator-identity check -- in the single-allocator
            // topology the serving durable IS the allocator, and
            // allocator-identity enforcement for multi-peer topologies is
            // #309's routing work. validate_received_event is the
            // per-agent hook if a deployment wants to gate this earlier.
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
        S: GetState + Send + Sync,
    {
        // TODO: do we actually need predicate_relevance?
        let proto::SubscriptionUpdateItem { entity_id, model, content, predicate_relevance: _ } = update;
        let collection_id = node.resolve_model_wait(&model).await?;
        let collection = node.collections.get(&collection_id).await?;

        match content {
            // EventOnly: equivalent to old SubscriptionItem::Change
            proto::UpdateContent::EventOnly(event_fragments) => {
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, model, event_fragments, event_getter)?;
                // Wire order is untrusted for every multi-event shape, not
                // just bridges: a child applied before its staged parent
                // gap-jumps the head and drops the parent's operations (V4).
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;

                // We did not receive an entity fragment, so we need to retrieve it from local storage or a remote peer
                // (assembly binds it to the id-keyed contract before we apply).
                let entity = node.entities.get_retrieve_or_create(state_getter, event_getter, &collection_id, &entity_id).await?;
                entities.push(entity.clone());

                let mut applied_events = Vec::new();
                let mut failure: Option<MutationError> = None;
                for event in attested_events {
                    // Events should always be appliable sequentially
                    let applied = match entity.apply_event(event_getter, &event.payload).await {
                        Ok(applied) => applied,
                        Err(e) => {
                            failure = Some(e);
                            break;
                        }
                    };
                    if applied {
                        if let Err(e) = event_getter.commit_event(&event).await {
                            failure = Some(e);
                            break;
                        }
                        applied_events.push(event);
                    }
                }

                // Anything applied before a failure is real progress; notify it.
                if !applied_events.is_empty() {
                    changes.push(EntityChange::new(entity.clone(), applied_events)?);
                }

                if let Some(e) = failure {
                    // get_retrieve_or_create may have materialized an empty-head
                    // resident for an entity we know nothing about (e.g. a
                    // non-creation event for an entity that was never received).
                    // Evict it, or the entity appears to exist with no state.
                    // Recovering by requesting state from the peer is tracked as
                    // a follow-up.
                    node.entities.remove_if_phantom(&entity_id);
                    return Err(e);
                }
            }

            // StateAndEvent: equivalent to old SubscriptionItem::Add
            proto::UpdateContent::StateAndEvent(state_fragment, event_fragments) => {
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, model, event_fragments, event_getter)?;
                // Sorted for the same reason as the EventOnly arm: the
                // fallback below applies event by event.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;

                let state = (entity_id, model, state_fragment.clone()).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &state)?;

                // with_state only updates the in-memory entity, it does NOT persist to storage
                let (changed, entity) =
                    node.entities.with_state(state_getter, event_getter, entity_id, collection_id.clone(), state.payload.state).await?;
                entities.push(entity.clone());

                if matches!(changed, Some(true) | None) {
                    // State applied successfully (new entity or strictly descends)
                    // Commit all staged events
                    for event in &attested_events {
                        event_getter.commit_event(event).await?;
                    }
                    Self::save_state(node, &entity, &collection).await?;
                    changes.push(EntityChange::new(entity, attested_events)?);
                } else {
                    // State not applied (divergence or older) - fall back to event-by-event application
                    // This handles DivergedSince where we need to merge concurrent branches
                    let mut applied_events = Vec::new();
                    for event in attested_events {
                        if entity.apply_event(event_getter, &event.payload).await? {
                            event_getter.commit_event(&event).await?;
                            applied_events.push(event);
                        }
                    }
                    if !applied_events.is_empty() {
                        Self::save_state(node, &entity, &collection).await?;
                        changes.push(EntityChange::new(entity, applied_events)?);
                    }
                }
            }
        }

        Ok(())
    }

    async fn save_state<SE, PA>(
        node: &Node<SE, PA>,
        entity: &crate::entity::Entity,
        collection_wrapper: &crate::storage::StorageCollectionWrapper,
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let state = entity.to_state()?;
        let entity_state = proto::EntityState { entity_id: entity.id(), model: entity.model_id()?, state };
        let attestation = node.policy_agent.attest_state(node, &entity_state);
        let attested = Attested::opt(entity_state, attestation);
        collection_wrapper.set_state(attested).await?;
        Ok(())
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
        S: GetState + Send + Sync,
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
        S: GetState + Send + Sync,
    {
        let entity_id = delta.entity_id;
        let model = delta.model;

        let result = Self::apply_delta_inner(node, from_peer_id, delta, event_getter, state_getter).await;
        result.map_err(|cause| ApplyErrorItem { entity_id, model, cause })
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
        S: GetState + Send + Sync,
    {
        // INGRESS (#330): resolve the wire model id to the local collection
        // (well-knowns, then catalog) or reject this delta.
        let collection_id = node.resolve_model_wait(&delta.model).await?;
        let collection = node.collections.get(&collection_id).await?;

        match delta.content {
            proto::DeltaContent::StateSnapshot { state } => {
                let attested_state = (delta.entity_id, delta.model, state).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &attested_state)?;

                let (changed, entity) = node
                    .entities
                    .with_state(state_getter, event_getter, delta.entity_id, collection_id, attested_state.payload.state)
                    .await?;

                // Save state to storage
                Self::save_state(node, &entity, &collection).await?;

                // Only notify if the snapshot actually advanced the entity. with_state
                // returns Some(false) when the state did not apply (the entity is
                // already resident at this head, or the snapshot is older). Emitting a
                // change here would be spurious: notify_change is global across every
                // subscription on the node, so a no-op snapshot for one subscribing
                // query surfaces on ANOTHER already-established query - which holds the
                // same entity - as an empty-events ItemChange::Update. That is the
                // subscription-notification race behind the intermittent
                // server_edits_subscription failure. None (freshly created) and
                // Some(true) (advanced) are real changes and still notify.
                if matches!(changed, Some(false)) {
                    return Ok(None);
                }

                // Snapshots carry no events, so the change reports an empty events list.
                Ok(Some(EntityChange::new(entity, Vec::new())?))
            }

            proto::DeltaContent::EventBridge { events } => {
                // Bridge events pass the same policy gate as subscription
                // updates; transport must not decide trust.
                let attested_events = Self::validate_and_stage(node, from_peer_id, delta.entity_id, delta.model, events, event_getter)?;

                // Get or create entity
                let entity = node.entities.get_retrieve_or_create(state_getter, event_getter, &collection_id, &delta.entity_id).await?;

                // Apply events parents-first. Wire order is untrusted: applying
                // a child before its staged parent gap-jumps the head past the
                // parent, whose operations are then dropped as StrictAscends
                // (V4). The producer also sorts, but receivers must not rely
                // on sender ordering.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;
                let mut applied_any = false;
                for event in attested_events.into_iter() {
                    if entity.apply_event(event_getter, &event.payload).await? {
                        applied_any = true;
                    }
                    event_getter.commit_event(&event).await?;
                }

                // Save updated state
                Self::save_state(node, &entity, &collection).await?;

                // Only notify if the bridge actually advanced the entity. If every
                // event was already applied (apply_event returned false for all), the
                // entity did not change and emitting an EntityChange would surface a
                // spurious empty-events Update on other subscriptions holding this
                // entity (see the StateSnapshot arm above). This mirrors the streaming
                // StateAndEvent fallback, which also only notifies when events applied.
                if !applied_any {
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
