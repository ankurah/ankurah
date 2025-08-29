use crate::{
    action_debug, action_warn, context::NodeAndContext, error::MutationError, node::Node, policy::PolicyAgent, storage::StorageEngine,
};
use ankurah_proto::{self as proto, EntityId};
use anyhow::anyhow;
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
                return Err(anyhow!("Received subscription update for unknown predicate {}", predicate_id));
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
        todo!("Update this to reflect the changes to SubscriptionUpdateItem")
        match update {
            proto::SubscriptionUpdateItem::Initial { entity_id, collection, state } => {
        //         let state = (entity_id, collection, state).into();
        //         // validate that we trust the state given to us
        //         self.policy_agent.validate_received_state(self, from_peer_id, &state)?;

        //         let payload = state.payload;
        //         let collection = self.collections.get(&payload.collection).await?;
        //         let getter = (payload.collection.clone(), nodeandcontext);

        //         match self.entities.with_state(&getter, payload.entity_id, payload.collection, payload.state).await? {
        //             (Some(true), entity) => {
        //                 // We had the entity already, and this state is newer than the one we have, so save it to the collection
        //                 // Not sure if we should reproject the state - discuss
        //                 // however, if we do reproject, we need to re-attest the state
        //                 let state = entity.to_state()?;
        //                 let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
        //                 let attestation = self.policy_agent.attest_state(self, &entity_state);
        //                 let attested = Attested::opt(entity_state, attestation);
        //                 collection.set_state(attested).await?;
        //                 Ok(Some(EntityChange::new(entity, vec![])?))
        //             }
        //             (Some(false), _entity) => {
        //                 // We had the entity already, and this state is not newer than the one we have so we drop it to the floor
        //                 Ok(None)
        //             }
        //             (None, entity) => {
        //                 // We did not have the entity yet, so we created it, so save it to the collection
        //                 // see notes as above regarding reprojecting and re-attesting the state
        //                 let state = entity.to_state()?;
        //                 let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
        //                 let attestation = self.policy_agent.attest_state(self, &entity_state);
        //                 let attested = Attested::opt(entity_state, attestation);
        //                 collection.set_state(attested).await?;
        //                 Ok(Some(EntityChange::new(entity, vec![])?))
        //             }
        //         }
        //     }
        //     proto::SubscriptionUpdateItem::Add { entity_id, collection: collection_id, state, events } => {
        //         let collection = self.collections.get(&collection_id).await?;

        //         // validate and store the events, in case we need them for lineage comparison
        //         let mut attested_events = Vec::new();
        //         for event in events.iter() {
        //             let event: Attested<ankurah_proto::Event> = (entity_id, collection_id.clone(), event.clone()).into();
        //             self.policy_agent.validate_received_event(self, from_peer_id, &event)?;
        //             // store the validated event in case we need it for lineage comparison
        //             collection.add_event(&event).await?;
        //             attested_events.push(event);
        //         }

        //         let state = (entity_id, collection_id.clone(), state).into();
        //         self.policy_agent.validate_received_state(self, from_peer_id, &state)?;

        //         match self
        //             .entities
        //             .with_state(&(collection_id.clone(), nodeandcontext), entity_id, collection_id, state.payload.state)
        //             .await?
        //         {
        //             (Some(false), _entity) => {
        //                 // had it already, and the state is not newer than the one we have
        //                 Ok(None)
        //             }
        //             (Some(true), entity) => {
        //                 // had it already, and the state is newer than the one we have, and was applied successfully, so save it and return the change
        //                 // reduce the probability of error by reprojecting the state - is this necessary if we've validated the attestation?
        //                 // See notes above regarding reprojecting and re-attesting the state
        //                 let state = entity.to_state()?;
        //                 let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
        //                 let attestation = self.policy_agent.attest_state(self, &entity_state);
        //                 let attested = Attested::opt(entity_state, attestation);
        //                 collection.set_state(attested).await?;
        //                 Ok(Some(EntityChange::new(entity, attested_events)?))
        //             }

        //             (None, entity) => {
        //                 // did not have it, so we created it, so save it and return the change
        //                 // See notes above regarding attestation/reprojection
        //                 let state = entity.to_state()?;
        //                 let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
        //                 let attestation = self.policy_agent.attest_state(self, &entity_state);
        //                 let attested = Attested::opt(entity_state, attestation);
        //                 collection.set_state(attested).await?;
        //                 Ok(Some(EntityChange::new(entity, attested_events)?))
        //             }
        //         }
        //     }
        //     proto::SubscriptionUpdateItem::Change { entity_id, collection: collection_id, events } => {
        //         let collection = self.collections.get(&collection_id).await?;
        //         let mut attested_events = Vec::new();
        //         for event in events.iter() {
        //             let event = (entity_id, collection_id.clone(), event.clone()).into();

        //             self.policy_agent.validate_received_event(self, from_peer_id, &event)?;
        //             // store the validated event in case we need it for lineage comparison
        //             collection.add_event(&event).await?;
        //             attested_events.push(event);
        //         }

        //         let entity =
        //             self.entities.get_retrieve_or_create(&(collection_id.clone(), nodeandcontext), &collection_id, &entity_id).await?;

        //         let mut changed = false;
        //         for event in attested_events.iter() {
        //             changed = entity.apply_event(&(collection_id.clone(), nodeandcontext), &event.payload).await?;
        //         }
        //         if changed {
        //             Ok(Some(EntityChange::new(entity, attested_events)?))
        //         } else {
        //             Ok(None)
        //         }
            }
        }
    }
}
