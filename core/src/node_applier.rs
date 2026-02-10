use crate::{
    changes::EntityChange,
    error::{ApplyError, ApplyErrorItem, MutationError},
    node::Node,
    policy::PolicyAgent,
    retrieval::{GetState, SuspenseEvents, CachedEventGetter, LocalStateGetter},
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
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        tracing::debug!("received subscription update for {} items", items.len());

        // In theory, if initialized_predicate is specified, we could potentially narrow it down to just the context for that predicate
        // but this feels brittle, because failure to apply this event would affect the other contexts on this node.
        let Some(relay) = &node.subscription_relay else {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates without a subscription relay"));
        };
        let cdata = relay.get_contexts_for_peer(from_peer_id);
        if cdata.is_empty() {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates without at least predicate context"));
        }

        // Apply all updates and notify reactor
        let mut changes = Vec::new();
        for update in items {
            let collection = node.collections.get(&update.collection).await?;
            let event_getter = CachedEventGetter::new(update.collection.clone(), collection.clone(), node, &cdata);
            let state_getter = LocalStateGetter::new(collection);
            Self::apply_update(node, from_peer_id, update, &event_getter, &state_getter, &mut changes, &mut ()).await?;
        }

        node.reactor.notify_change(changes).await;

        Ok(())
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
        let proto::SubscriptionUpdateItem { entity_id, collection: collection_id, content, predicate_relevance: _ } = update;
        let collection = node.collections.get(&collection_id).await?;

        match content {
            // EventOnly: equivalent to old SubscriptionItem::Change
            proto::UpdateContent::EventOnly(event_fragments) => {
                let mut attested_events = Vec::new();
                for fragment in event_fragments {
                    let attested_event: Attested<proto::Event> = (entity_id, collection_id.clone(), fragment).into();
                    node.policy_agent.validate_received_event(node, from_peer_id, &attested_event)?;
                    event_getter.stage_event(attested_event.payload.clone());
                    attested_events.push(attested_event);
                }

                // We did not receive an entity fragment, so we need to retrieve it from local storage or a remote peer
                let entity = node.entities.get_retrieve_or_create(state_getter, event_getter, &collection_id, &entity_id).await?;
                entities.push(entity.clone());

                let mut applied_events = Vec::new();
                for event in attested_events {
                    // Events should always be appliable sequentially
                    if entity.apply_event(event_getter, &event.payload).await? {
                        event_getter.commit_event(&event).await?;
                        applied_events.push(event);
                    }
                }

                if !applied_events.is_empty() {
                    changes.push(EntityChange::new(entity, applied_events)?);
                }
            }

            // StateAndEvent: equivalent to old SubscriptionItem::Add
            proto::UpdateContent::StateAndEvent(state_fragment, event_fragments) => {
                let mut attested_events = Vec::new();
                for fragment in event_fragments {
                    let attested_event: Attested<proto::Event> = (entity_id, collection_id.clone(), fragment).into();
                    node.policy_agent.validate_received_event(node, from_peer_id, &attested_event)?;
                    event_getter.stage_event(attested_event.payload.clone());
                    attested_events.push(attested_event);
                }

                let state = (entity_id, collection_id.clone(), state_fragment.clone()).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &state)?;

                // with_state only updates the in-memory entity, it does NOT persist to storage
                let (changed, entity) = node.entities.with_state(state_getter, event_getter, entity_id, collection_id.clone(), state.payload.state).await?;
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
        let entity_state = proto::EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
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
        let mut ready_chunks = ReadyChunks::new(deltas.into_iter().map(|delta| Self::apply_delta(node, from_peer_id, delta, event_getter, state_getter)));

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
        let collection = delta.collection.clone();

        let result = Self::apply_delta_inner(node, from_peer_id, delta, event_getter, state_getter).await;
        result.map_err(|cause| ApplyErrorItem { entity_id, collection, cause })
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
        let collection = node.collections.get(&delta.collection).await?;

        match delta.content {
            proto::DeltaContent::StateSnapshot { state } => {
                let attested_state = (delta.entity_id, delta.collection.clone(), state).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &attested_state)?;

                let (_, entity) =
                    node.entities.with_state(state_getter, event_getter, delta.entity_id, delta.collection, attested_state.payload.state).await?;

                // Save state to storage
                Self::save_state(node, &entity, &collection).await?;

                // Phase 1: Return EntityChange with empty events
                Ok(Some(EntityChange::new(entity, Vec::new())?))
            }

            proto::DeltaContent::EventBridge { events } => {
                let attested_events: Vec<Attested<proto::Event>> =
                    events.into_iter().map(|f| (delta.entity_id, delta.collection.clone(), f).into()).collect();

                // Stage events for BFS discovery
                for event in &attested_events {
                    event_getter.stage_event(event.payload.clone());
                }

                // Get or create entity
                let entity = node.entities.get_retrieve_or_create(state_getter, event_getter, &delta.collection, &delta.entity_id).await?;

                // Apply events in forward (causal) order - oldest first
                // Events in EventBridge are already in causal order from the server
                for event in attested_events.into_iter() {
                    entity.apply_event(event_getter, &event.payload).await?;
                    event_getter.commit_event(&event).await?;
                }

                // Save updated state
                Self::save_state(node, &entity, &collection).await?;

                // Phase 1: Return EntityChange with empty events
                Ok(Some(EntityChange::new(entity, Vec::new())?))
            }

            proto::DeltaContent::StateAndRelation { .. } => {
                Err(MutationError::InvalidUpdate("StateAndRelation not yet implemented"))
            }
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
