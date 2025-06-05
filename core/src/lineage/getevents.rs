use std::collections::HashMap;

use ankurah_proto::{self as proto, Attested, Clock, CollectionId, Event, EventId};

use crate::{context::NodeAndContext, error::RetrievalError, policy::PolicyAgent, storage::StorageEngine};
use async_trait::async_trait;

/// Event retrieval trait exclusively for lineage comparisons
#[async_trait]
pub trait GetEvents {
    type Id: Eq + PartialEq + Clone + std::fmt::Debug + Send + Sync;
    type Event: TEvent<Id = Self::Id> + std::fmt::Display;

    /// Estimate the budget cost for retrieving a batch of events
    /// This allows different implementations to model their cost structure
    fn estimate_cost(&self, _batch_size: usize) -> usize {
        // Default implementation: fixed cost of 1 per batch
        1
    }

    /// retrieve the events from the store OR the remote peer
    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<Self::Id>,
    ) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError>;
}

/// a trait for events and eventlike things that can be descended
pub trait TEvent: std::fmt::Display {
    type Id: Eq + PartialEq + Clone;
    type Parent: TClock<Id = Self::Id>;

    fn id(&self) -> Self::Id;
    fn parent(&self) -> &Self::Parent;
}

pub trait TClock {
    type Id: Eq + PartialEq + Clone;
    fn members(&self) -> &[Self::Id];
}

impl TClock for Clock {
    type Id = EventId;
    fn members(&self) -> &[Self::Id] { self.as_slice() }
}

impl TEvent for ankurah_proto::Event {
    type Id = ankurah_proto::EventId;
    type Parent = Clock;

    fn id(&self) -> EventId { self.id() }
    fn parent(&self) -> &Clock { &self.parent }
}

pub struct LocalEventGetter<SE>(SE);

impl<SE> LocalEventGetter<SE> {
    pub fn new(collection: SE) -> Self { Self(collection) }
}

#[async_trait]
impl<SE> GetEvents for LocalEventGetter<SE>
where SE: StorageEngine + Send + Sync + 'static
{
    type Id = EventId;
    type Event = ankurah_proto::Event;

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<Self::Id>,
    ) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
        println!("StorageCollectionWrapper.GetEvents {:?}", event_ids);
        // TODO: push the consumption figure to the store, because its not necessarily the same for all stores
        Ok((1, self.0.collection(collection_id).await?.get_events(event_ids).await?))
    }
}

/// Retrieve events from a durable peer
pub struct LocalOrRemoteEventGetter<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> {
    nodeandcontext: NodeAndContext<SE, PA>,
}

impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> LocalOrRemoteEventGetter<SE, PA> {
    pub fn new(nodeandcontext: NodeAndContext<SE, PA>) -> Self { Self { nodeandcontext } }
}

#[async_trait]
impl<SE, PA> GetEvents for LocalOrRemoteEventGetter<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    type Id = EventId;
    type Event = Event;

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<Self::Id>,
    ) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
        let cost = 1; // Cost for local retrieval

        // try to get the events from the local storage first
        let collection = self.nodeandcontext.node.collections.get(collection_id).await?;
        let mut events: HashMap<EventId, Attested<Event>> =
            collection.get_events(event_ids.clone()).await?.into_iter().map(|event| (event.id(), event)).collect();

        // collect the missing event IDs
        let missing_event_ids: Vec<EventId> = event_ids.iter().filter(|id| !events.contains_key(id)).cloned().collect();

        if missing_event_ids.is_empty() {
            // all events found locally - build result directly
            let result_events: Vec<_> = event_ids.into_iter().map(|event_id| events.remove(&event_id).expect("sanity error")).collect();
            Ok((cost, result_events))
        } else {
            let peer_id =
                self.nodeandcontext.node.get_durable_peer_random().ok_or(RetrievalError::StorageError("No durable peer found".into()))?;

            match self
                .nodeandcontext
                .node
                .request(
                    peer_id,
                    &self.nodeandcontext.cdata,
                    proto::NodeRequestBody::GetEvents { collection: collection_id.clone(), event_ids: missing_event_ids.clone() },
                )
                .await?
            {
                proto::NodeResponseBody::GetEvents(peer_events) => {
                    // merge remote events into local events
                    for event in peer_events {
                        events.insert(event.id(), event);
                    }

                    // build result vector in the requested order, validating each event exists
                    let mut result_events = Vec::with_capacity(event_ids.len());
                    for event_id in event_ids {
                        match events.remove(&event_id) {
                            Some(event) => result_events.push(event),
                            None => return Err(RetrievalError::StorageError(format!("Failed to retrieve event: {:?}", event_id).into())),
                        }
                    }

                    Ok((cost, result_events))
                }
                proto::NodeResponseBody::Error(e) => Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into())),
                _ => Err(RetrievalError::StorageError("Unexpected response type from peer".into())),
            }
        }
    }
}
