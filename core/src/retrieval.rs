//! Implements event and state retrieval from local and remote sources.

use std::sync::Arc;

use crate::{
    error::RetrievalError,
    policy::PolicyAgent,
    storage::{StorageCollectionWrapper, StorageEngine},
    util::Iterable,
    Node,
};
use ankurah_proto::{self as proto, EntityId, Event, EventId};
use async_trait::async_trait;

#[async_trait]
pub trait Retrieve {
    // Each implementation of Retrieve determines whether to use local or remote storage
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<proto::Attested<proto::EntityState>>, RetrievalError>;

    /// Retrieve a single event by ID from storage.
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError>;

    /// Check whether an event exists in storage without fetching the full body.
    /// Default implementation delegates to get_event, but implementors should
    /// override with a more efficient storage-level check when possible.
    async fn event_exists(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        match self.get_event(event_id).await {
            Ok(_) => Ok(true),
            Err(RetrievalError::EventNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// Blanket implementation for shared references - enables passing `&retriever` to functions
/// that take `R: Retrieve`.
#[async_trait]
impl<R: Retrieve + Send + Sync + ?Sized> Retrieve for &R {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<proto::Attested<proto::EntityState>>, RetrievalError> {
        (*self).get_state(entity_id).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        (*self).get_event(event_id).await
    }

    async fn event_exists(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        (*self).event_exists(event_id).await
    }
}

/// Durable node retriever - retrieves everything locally from storage
#[derive(Clone)]
pub struct LocalRetriever(Arc<LocalRetrieverInner>);
struct LocalRetrieverInner {
    collection: StorageCollectionWrapper,
}

impl LocalRetriever {
    pub fn new(collection: StorageCollectionWrapper) -> Self {
        Self(Arc::new(LocalRetrieverInner { collection }))
    }
}

#[async_trait]
impl Retrieve for LocalRetriever {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<proto::Attested<proto::EntityState>>, RetrievalError> {
        match self.0.collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        let events = self.0.collection.get_events(vec![event_id.clone()]).await?;
        events.into_iter().next().map(|e| e.payload).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }
}

/// Ephemeral node retriever - retrieves events remotely, states locally, with multiple contexts for authentication
pub struct EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    pub collection: proto::CollectionId,
    pub node: &'a Node<SE, PA>,
    pub cdata: &'a C,
}

impl<'a, SE, PA, C> EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    pub fn new(collection: proto::CollectionId, node: &'a Node<SE, PA>, cdata: &'a C) -> Self {
        Self { collection, node, cdata }
    }
}

#[async_trait]
impl<'a, SE, PA, C> Retrieve for EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<proto::Attested<proto::EntityState>>, RetrievalError> {
        let collection = self.node.collections.get(&self.collection).await?;
        match collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        // Try local storage
        let collection = self.node.collections.get(&self.collection).await?;
        let events = collection.get_events(vec![event_id.clone()]).await?;
        if let Some(event) = events.into_iter().next() {
            return Ok(event.payload);
        }

        // Try remote peer
        let Some(peer_id) = self.node.get_durable_peer_random() else {
            return Err(RetrievalError::EventNotFound(event_id.clone()));
        };

        match self
            .node
            .request(
                peer_id,
                self.cdata,
                proto::NodeRequestBody::GetEvents {
                    collection: self.collection.clone(),
                    event_ids: vec![event_id.clone()],
                },
            )
            .await?
        {
            proto::NodeResponseBody::GetEvents(peer_events) => {
                // Store locally for future access
                for event in peer_events.iter() {
                    collection.add_event(event).await?;
                }
                peer_events
                    .into_iter()
                    .next()
                    .map(|e| e.payload)
                    .ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
            }
            proto::NodeResponseBody::Error(e) => {
                Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into()))
            }
            _ => Err(RetrievalError::StorageError("Unexpected response type from peer".into())),
        }
    }
}
