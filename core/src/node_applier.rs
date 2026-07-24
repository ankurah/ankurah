use crate::{
    changes::EntityChange,
    error::{ApplyError, ApplyErrorItem, MutationError},
    node::Node,
    policy::PolicyAgent,
    retrieval::{CachedEventGetter, GetState, LocalStateGetter, SuspenseEvents},
    storage::StorageEngine,
    storage_commit::ResidentWriteIntent,
    util::ready_chunks::ReadyChunks,
};
use ankurah_proto::{self as proto};
use futures::stream::StreamExt;
use proto::Attested;

/// Consolidates all logic for applying remote updates to a node
/// Handles both SubscriptionUpdateItem (streaming updates) and EntityDelta (initial Fetch/QuerySubscribed)
pub struct NodeApplier;

struct SavedState {
    models: Vec<proto::ModelId>,
    canonical_changed: bool,
}

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
            let item_model = update.model.clone();
            let result = async {
                // INGRESS: resolve the wire model address (validated system
                // name or allocated catalog id) or reject this item.
                let collection_id = node.prepare_model_for_ingress(&update.model).await?;
                let event_getter = CachedEventGetter::new(collection_id, node, &cdata);
                let state_getter = LocalStateGetter::new(node.storage.clone());
                Self::apply_update(node, from_peer_id, update, &event_getter, &state_getter, &mut changes, &mut ()).await
            }
            .await;
            if let Err(cause) = result {
                tracing::warn!("failed to apply update for model {}/{}: {}", item_model, entity_id, cause);
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
        model: proto::ModelId,
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
            node.policy_agent.validate_received_event(node, from_peer_id, &model, &attested_event)?;
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
        let proto::SubscriptionUpdateItem { entity_id, model, content, predicate_relevance: _, source_queries: _ } = update;
        let collection_id = node.prepare_model_for_ingress(&model).await?;

        match content {
            // EventOnly: equivalent to old SubscriptionItem::Change
            proto::UpdateContent::EventOnly(event_fragments) => {
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, model, event_fragments, event_getter)?;
                // Wire order is untrusted for every multi-event shape, not
                // just bridges: a child applied before its staged parent
                // gap-jumps the head and drops the parent's operations (V4).
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;
                node.storage.append_events(&attested_events).await?;

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
                        applied_events.push(event);
                    }
                }

                // Anything applied before a failure is real progress; notify it.
                if !applied_events.is_empty() {
                    let saved = Self::save_state(node, collection_id, &entity, applied_events.clone(), event_getter).await?;
                    changes.extend(Self::changes_after_save(&entity, applied_events, saved)?);
                } else if failure.is_none() {
                    // Even an idempotent/older event forms the explicit
                    // entity-model association through which it was received.
                    let saved = Self::save_state(node, collection_id, &entity, Vec::new(), event_getter).await?;
                    changes.extend(Self::changes_after_save(&entity, Vec::new(), saved)?);
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
                let attested_events =
                    Self::validate_and_stage(node, from_peer_id, entity_id, model.clone(), event_fragments, event_getter)?;
                // Sorted for the same reason as the EventOnly arm: the
                // fallback below applies event by event.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;

                let state = (entity_id, state_fragment.clone()).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &model, &state)?;
                node.storage.append_events(&attested_events).await?;

                // with_state only updates the in-memory entity, it does NOT persist to storage
                let (changed, entity) =
                    node.entities.with_state(state_getter, event_getter, entity_id, collection_id.clone(), state.payload.state).await?;
                entities.push(entity.clone());

                if matches!(changed, Some(true) | None) {
                    // State applied successfully (new entity or strictly
                    // descends). Events were blindly appended before state.
                    let saved = Self::save_state(node, collection_id, &entity, attested_events.clone(), event_getter).await?;
                    changes.extend(Self::changes_after_save(&entity, attested_events, saved)?);
                } else {
                    // State not applied (divergence or older) - fall back to event-by-event application
                    // This handles DivergedSince where we need to merge concurrent branches
                    let mut applied_events = Vec::new();
                    for event in attested_events {
                        if entity.apply_event(event_getter, &event.payload).await? {
                            applied_events.push(event);
                        }
                    }
                    let saved = Self::save_state(node, collection_id, &entity, applied_events.clone(), event_getter).await?;
                    changes.extend(Self::changes_after_save(&entity, applied_events, saved)?);
                }
            }
        }

        Ok(())
    }

    async fn save_state<SE, PA, E>(
        node: &Node<SE, PA>,
        accessed_as: proto::ModelId,
        entity: &crate::entity::Entity,
        events: Vec<Attested<proto::Event>>,
        event_getter: &E,
    ) -> Result<SavedState, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: crate::retrieval::GetEvents + Send + Sync,
    {
        let result = node
            .commit_resident_writes(
                vec![ResidentWriteIntent::from_resident_state(entity.clone(), accessed_as, events)?],
                None,
                event_getter,
            )
            .await?;
        let entity_result = result
            .entities
            .into_iter()
            .next()
            .ok_or_else(|| MutationError::General(format!("storage omitted commit result for entity {}", entity.id()).into()))?;
        let models = if entity_result.canonical_changed { entity_result.materialized_as } else { entity_result.associations_added };
        Ok(SavedState { models, canonical_changed: entity_result.canonical_changed })
    }

    /// Build reactor changes for the durable effects of a state commit.
    ///
    /// A canonical no-op can still add a model association, but the input
    /// events are then historical rather than newly applied and must not be
    /// reported as the cause of the association-only notification.
    fn changes_after_save(
        entity: &crate::entity::Entity,
        events: Vec<Attested<proto::Event>>,
        saved: SavedState,
    ) -> Result<Vec<EntityChange>, MutationError> {
        let events = if saved.canonical_changed { events } else { Vec::new() };
        Self::changes_for_models(entity, events, saved.models)
    }

    /// Build one reactor change per refreshed model materialization while
    /// sharing the entity's canonical in-memory state.
    fn changes_for_models(
        entity: &crate::entity::Entity,
        events: Vec<Attested<proto::Event>>,
        models: Vec<proto::ModelId>,
    ) -> Result<Vec<EntityChange>, MutationError> {
        models.into_iter().map(|model| EntityChange::new(entity.with_model_context(model), events.clone())).collect()
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
                    Ok(changes) => batch.extend(changes),
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
    /// Returns one change per refreshed model materialization.
    async fn apply_delta<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        delta: proto::EntityDelta,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<Vec<EntityChange>, ApplyErrorItem>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: GetState + Send + Sync,
    {
        let entity_id = delta.entity_id;
        let model = delta.model.clone();

        let result = Self::apply_delta_inner(node, from_peer_id, delta, event_getter, state_getter).await;
        result.map_err(|cause| ApplyErrorItem { entity_id, model, cause })
    }

    async fn apply_delta_inner<SE, PA, E, S>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        delta: proto::EntityDelta,
        event_getter: &E,
        state_getter: &S,
    ) -> Result<Vec<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        E: SuspenseEvents + Send + Sync,
        S: GetState + Send + Sync,
    {
        // INGRESS: resolve the wire model address to the local collection or
        // reject this delta. System-name arms are validated at this boundary.
        let proto::EntityDelta { entity_id, model, content } = delta;
        let collection_id = node.prepare_model_for_ingress(&model).await?;

        match content {
            proto::DeltaContent::StateSnapshot { state } => {
                let attested_state = (entity_id, state).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &collection_id, &attested_state)?;

                let (changed, entity) =
                    node.entities.with_state(state_getter, event_getter, entity_id, collection_id, attested_state.payload.state).await?;

                // Save state to storage
                let saved = Self::save_state(node, collection_id, &entity, Vec::new(), event_getter).await?;

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
                if matches!(changed, Some(false)) && saved.models.is_empty() {
                    return Ok(Vec::new());
                }

                // Snapshots carry no events, so the change reports an empty events list.
                Self::changes_after_save(&entity, Vec::new(), saved)
            }

            proto::DeltaContent::EventBridge { events } => {
                // Bridge events pass the same policy gate as subscription
                // updates; transport must not decide trust.
                let attested_events = Self::validate_and_stage(node, from_peer_id, entity_id, model, events, event_getter)?;

                // Get or create entity
                let entity = node.entities.get_retrieve_or_create(state_getter, event_getter, &collection_id, &entity_id).await?;

                // Apply events parents-first. Wire order is untrusted: applying
                // a child before its staged parent gap-jumps the head past the
                // parent, whose operations are then dropped as StrictAscends
                // (V4). The producer also sorts, but receivers must not rely
                // on sender ordering.
                let attested_events = crate::event_dag::ordering::topo_sort_events(attested_events)?;
                node.storage.append_events(&attested_events).await?;
                let mut applied_any = false;
                for event in &attested_events {
                    if entity.apply_event(event_getter, &event.payload).await? {
                        applied_any = true;
                    }
                }

                // Save updated state
                let saved = Self::save_state(node, collection_id, &entity, attested_events, event_getter).await?;

                // Only notify if the bridge actually advanced the entity. If every
                // event was already applied (apply_event returned false for all), the
                // entity did not change and emitting an EntityChange would surface a
                // spurious empty-events Update on other subscriptions holding this
                // entity (see the StateSnapshot arm above). This mirrors the streaming
                // StateAndEvent fallback, which also only notifies when events applied.
                if !applied_any && saved.models.is_empty() {
                    return Ok(Vec::new());
                }

                // Bridges carry no events on the change itself; the events were applied
                // above, so the change reports an empty events list.
                Self::changes_after_save(&entity, Vec::new(), saved)
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
