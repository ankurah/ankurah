use crate::{
    action_info, changes::EntityChange, error::MutationError, lineage::Retrieve, node::Node, policy::PolicyAgent, storage::StorageEngine,
};
use ankurah_proto::{self as proto, Event, EventId};

pub struct UpdateApplier;

impl UpdateApplier {
    /// Similar to commit_transaction, except that we check event attestations instead of checking write permissions
    /// we also don't need to fan events out to peers because we're receiving them from a peer
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
        action_info!(node, "received subscription update for {} items", "{}", items.len());

        // In theory, if initialized_predicate is specified, we could potentially narrow it down to just the context for that predicate
        // but this feels brittle, because failure to apply this event would affect the other contexts on this node.
        let Some(relay) = &node.subscription_relay else {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates without a subscription relay"));
        };
        let cdata = relay.get_contexts_for_peer(from_peer_id);
        if cdata.is_empty() {
            return Err(MutationError::InvalidUpdate("Should not be receiving updates without at least predicate context"));
        }

        if let Some(tx) = initialized_predicate.and_then(|p| node.pending_predicate_subs.remove(&p)) {
            let mut changes = Vec::new();
            let mut entities = Vec::new();
            for update in items {
                let retriever = crate::retrieval::EphemeralNodeRetriever::new(update.collection.clone(), node, &cdata);
                Self::apply_update(node, from_peer_id, update, &retriever, &mut changes, &mut entities).await?;
            }

            // Important to notify the reactor before sending the initial entities
            node.reactor.notify_change(changes);
            let _ = tx.send(entities); // Ignore if receiver was dropped
        } else {
            // Use unit type to avoid accumulation
            let mut changes = Vec::new();
            for update in items {
                let retriever = crate::retrieval::EphemeralNodeRetriever::new(update.collection.clone(), node, &cdata);
                Self::apply_update(node, from_peer_id, update, &retriever, &mut changes, &mut ()).await?;
            }

            node.reactor.notify_change(changes);
        }

        Ok(())
    }

    async fn apply_update<SE, PA, R>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        update: proto::SubscriptionUpdateItem,
        retriever: &R,
        changes: &mut Vec<EntityChange>,
        entities: &mut impl Pushable<crate::entity::Entity>,
    ) -> Result<(), MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        R: Retrieve<Id = EventId, Event = Event> + Send + Sync,
    {
        // TODO: do we actually need predicate_relevance and entity_subscribed?
        let proto::SubscriptionUpdateItem { entity_id, collection: collection_id, content, predicate_relevance: _, entity_subscribed: _ } =
            update;
        let collection = node.collections.get(&collection_id).await?;

        match content {
            // StateOnly: equivalent to old SubscriptionItem::Initial
            proto::UpdateContent::StateOnly(state_fragment) => {
                let state = (entity_id, collection_id, state_fragment).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &state)?;

                let (changed, entity) =
                    node.entities.with_state(retriever, state.payload.entity_id, state.payload.collection, state.payload.state).await?;
                entities.push(entity.clone());

                if matches!(changed, Some(true) | None) {
                    Self::save_state(node, &entity, &collection).await?;
                    changes.push(EntityChange::new(entity, vec![])?);
                }
            }

            // EventOnly: equivalent to old SubscriptionItem::Change
            proto::UpdateContent::EventOnly(event_fragments) => {
                let events = Self::save_events(node, from_peer_id, entity_id, &collection_id, event_fragments, &collection).await?;
                // We did not receive an entity fragment, so we need to retrieve it from local storage or a remote peer
                // TODO update the retriever to support bulk retrieval for multiple entities at once
                // this will require a queuing phase and a batch retrieval phase
                let entity = node.entities.get_retrieve_or_create(retriever, &collection_id, &entity_id).await?;
                entities.push(entity.clone());

                let mut applied_events = Vec::new();
                for event in events {
                    // Events should always be appliable sequentially
                    if entity.apply_event(retriever, &event.payload).await? {
                        applied_events.push(event);
                    }
                }

                if !applied_events.is_empty() {
                    changes.push(EntityChange::new(entity, applied_events)?);
                }
            }

            // StateAndEvent: equivalent to old SubscriptionItem::Add
            proto::UpdateContent::StateAndEvent(state_fragment, event_fragments) => {
                let events = Self::save_events(node, from_peer_id, entity_id, &collection_id, event_fragments, &collection).await?;
                let state = (entity_id, collection_id.clone(), state_fragment.clone()).into();
                node.policy_agent.validate_received_state(node, from_peer_id, &state)?;

                // with_state only updates the in-memory entity, it does NOT persist to storage
                let (changed, entity) = node.entities.with_state(retriever, entity_id, collection_id, state.payload.state).await?;
                entities.push(entity.clone());

                // TODO: get the list of events that where actually applied - don't just pass them all through blindly
                if matches!(changed, Some(true) | None) {
                    Self::save_state(node, &entity, &collection).await?;
                    changes.push(EntityChange::new(entity, events)?);
                }
            }
        }

        Ok(())
    }

    // Helper to process events: validate, store, and return attested events
    async fn save_events<SE, PA>(
        node: &Node<SE, PA>,
        from_peer_id: &proto::EntityId,
        entity_id: proto::EntityId,
        collection_id: &proto::CollectionId,
        fragments: Vec<proto::EventFragment>,
        collection: &crate::storage::StorageCollectionWrapper,
    ) -> Result<Vec<proto::Attested<proto::Event>>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut attested_events = Vec::new();
        for fragment in fragments {
            let attested_event = (entity_id, collection_id.clone(), fragment).into();
            node.policy_agent.validate_received_event(node, from_peer_id, &attested_event)?;
            // TODO - add a suspense set of events which the retriever can draw from. Then add events to the collection only when the entity.add_event is successful
            //        this way, we can quickly determine that a given event is descended merely by nature of being in the collection. This will be essential in peer-aided descent tests via attestation.
            //        which should dramatically accelerate the lineage test
            collection.add_event(&attested_event).await?;
            attested_events.push(attested_event);
        }
        Ok(attested_events)
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
        let attested = proto::Attested::opt(entity_state, attestation);
        collection_wrapper.set_state(attested).await?;
        Ok(())
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
