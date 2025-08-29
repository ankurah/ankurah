use crate::{
    action_debug, action_warn, changes::EntityChange, context::NodeAndContext, error::MutationError, node::Node, policy::PolicyAgent,
    storage::StorageEngine,
};
use ankurah_proto::{self as proto};
use tracing::{debug, error};

pub struct UpdateApplier;

impl UpdateApplier {
    pub(crate) async fn apply_updates<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        items: Vec<proto::SubscriptionUpdateItem>,
        initialized_predicate: Option<proto::PredicateId>,
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // TODO check if this is a valid subscription
        action_debug!(node, "received subscription update for {} items", "{}", items.len());

        if let Some(predicate_id) = initialized_predicate {
            if let Some(cdata) = node.predicate_context.get(&predicate_id) {
                let nodeandcontext = NodeAndContext { node: node.clone(), cdata };

                // Signal any pending subscription waiting for first update
                if let Some(tx) = node.pending_predicate_subs.remove(&predicate_id) {
                    let initial_states = items.iter().cloned().filter_map(|item| item.try_into().ok()).collect();
                    Self::apply_subscription_updates(node, from_peer_id, items, nodeandcontext).await?;
                    let _ = tx.send(initial_states); // Ignore if receiver was dropped
                } else {
                    Self::apply_subscription_updates(node, from_peer_id, items, nodeandcontext).await?;
                }
            } else {
                error!("Received subscription update for unknown predicate {}", predicate_id);
                return Err(MutationError::FailedStep(
                    "apply_updates",
                    format!("Received subscription update for unknown predicate {}", predicate_id),
                ));
            }
        }

        Ok(())
    }

    // Similar to commit_transaction, except that we check event attestations instead of checking write permissions
    // we also don't need to fan events out to peers because we're receiving them from a peer
    pub async fn apply_subscription_updates<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        updates: Vec<proto::SubscriptionUpdateItem>,
        nodeandcontext: NodeAndContext<SE, PA>,
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut changes = Vec::new();
        for update in updates {
            match Self::apply_subscription_update(node, from_peer_id, update, &nodeandcontext).await {
                Ok(Some(change)) => {
                    changes.push(change);
                }
                Ok(None) => {
                    continue;
                }
                Err(e) => {
                    action_warn!(node, "received invalid update from peer", "{}: {}", from_peer_id.to_base64_short(), e);
                }
            }
        }
        debug!("{node} notifying reactor of {} changes", changes.len());
        node.reactor.notify_change(changes);
        Ok(())
    }

    // FIXME: This needs a full audit versus the old code - because of the criticality of this workflow
    pub async fn apply_subscription_update<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        update: proto::SubscriptionUpdateItem,
        nodeandcontext: &NodeAndContext<SE, PA>,
    ) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let proto::SubscriptionUpdateItem { entity_id, collection, content, predicate_relevance: _, entity_subscribed: _ } = update;

        let collection_wrapper = node.collections.get(&collection).await?;
        let getter = (collection.clone(), nodeandcontext);
        let (state_fragment, event_fragments) = content.into_parts();

        let mut applied_events = Vec::new();
        let mut entity_changed = false;
        let mut entity_opt = None;

        // Apply events if present - store them first for lineage comparison
        if let Some(event_fragments) = event_fragments {
            for event_fragment in event_fragments {
                let attested_event = (entity_id, collection.clone(), event_fragment).into();
                node.policy_agent.validate_received_event(node, from_peer_id, &attested_event)?;

                // Store the validated event in case we need it for lineage comparison
                collection_wrapper.add_event(&attested_event).await?;
                applied_events.push(attested_event);
            }

            // Get or create entity and apply events
            let entity = node.entities.get_retrieve_or_create(&getter, &collection, &entity_id).await?;
            for event in applied_events.iter() {
                if entity.apply_event(&getter, &event.payload).await? {
                    entity_changed = true;
                }
            }
            entity_opt = Some(entity);
        }

        // Apply state if present using with_state (preserves original semantics)
        if let Some(state_fragment) = state_fragment {
            let attested_state = (entity_id, collection.clone(), state_fragment).into();
            node.policy_agent.validate_received_state(node, from_peer_id, &attested_state)?;

            match node.entities.with_state(&getter, entity_id, collection.clone(), attested_state.payload.state).await? {
                (Some(true), entity) => {
                    // State was newer and applied successfully
                    entity_changed = true;
                    entity_opt = Some(entity);
                }
                (Some(false), entity) => {
                    // State was not newer, but we still have the entity
                    if entity_opt.is_none() {
                        entity_opt = Some(entity);
                    }
                }
                (None, entity) => {
                    // Entity was created with this state
                    entity_changed = true;
                    entity_opt = Some(entity);
                }
            }
        }

        // Reproject and re-attest state if anything changed, following original pattern
        if entity_changed {
            if let Some(entity) = &entity_opt {
                let new_state = entity.to_state()?;
                let entity_state = proto::EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state: new_state };
                let attestation = node.policy_agent.attest_state(node, &entity_state);
                let attested = proto::Attested::opt(entity_state, attestation);
                collection_wrapper.set_state(attested).await?;
            }
        }

        // Return EntityChange if the entity was modified
        if entity_changed {
            if let Some(entity) = entity_opt {
                Ok(Some(EntityChange::new(entity, applied_events)?))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
