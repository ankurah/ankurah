use ankurah_core::selection::filter::Filterable;
use ankurah_storage_common::filtering::{HasEntityId, ValueSetStream};
use ankurah_storage_common::traits::EntityIdStream;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Sled-specific entity state lookup that hydrates EntityIds into full EntityStates
pub struct SledEntityLookup<S> {
    pub entities_tree: sled::Tree,
    /// The model address written on reconstructed envelopes. The bucket
    /// captures the resolution OUTCOME at stream construction and this
    /// lookup checks it only when a row actually hydrates, so an empty
    /// result set never demands a model id (a cold catalog must not fail a
    /// query that returns nothing -- e.g. the ephemeral known_matches
    /// pre-fetch against a collection this node has never stored).
    pub model: Result<ankurah_proto::ModelId, String>,
    pub stream: S,
}

impl<S: EntityIdStream> SledEntityLookup<S> {
    pub fn new(entities_tree: &sled::Tree, model: Result<ankurah_proto::ModelId, String>, stream: S) -> Self {
        Self { entities_tree: entities_tree.clone(), model, stream }
    }
}

impl<S: Unpin> Unpin for SledEntityLookup<S> {}

impl<S: EntityIdStream> Stream for SledEntityLookup<S> {
    type Item = Result<ankurah_proto::Attested<ankurah_proto::EntityState>, ankurah_core::error::RetrievalError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Get next EntityId from upstream stream
        let entity_id = match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(id))) => id,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        // Lookup entity state from entities tree
        let key = entity_id.to_bytes();
        let value_bytes = match self.entities_tree.get(key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Poll::Ready(Some(Err(ankurah_core::error::RetrievalError::StorageError("Entity not found".to_string().into()))))
            }
            Err(e) => return Poll::Ready(Some(Err(ankurah_core::error::RetrievalError::StorageError(e.to_string().into())))),
        };

        // Decode StateFragment
        let state_fragment: ankurah_proto::StateFragment = match bincode::deserialize(&value_bytes) {
            Ok(fragment) => fragment,
            Err(e) => return Poll::Ready(Some(Err(ankurah_core::error::RetrievalError::StorageError(e.to_string().into())))),
        };

        // A row exists, so the envelope needs its model id NOW: an
        // unresolved model is only an error once there is an envelope to fill in.
        let model = match &self.model {
            Ok(model) => model.clone(),
            Err(e) => return Poll::Ready(Some(Err(ankurah_core::error::RetrievalError::Other(e.clone())))),
        };

        // Create Attested<EntityState> from parts
        let attested = ankurah_proto::Attested::<ankurah_proto::EntityState>::from_parts(entity_id, model, state_fragment);

        Poll::Ready(Some(Ok(attested)))
    }
}

// EntityStateStream is automatically implemented via blanket impl since SledEntityLookup emits Result<Attested<EntityState>, RetrievalError>

/// Trait that provides a convenient `.entities()` combinator for EntityId streams
pub trait SledEntityExt: EntityIdStream + Sized {
    /// Hydrate EntityIds into EntityStates using the sled entities tree.
    /// `model` is the model-id resolution outcome written on reconstructed
    /// envelopes (#330), checked lazily on the first hydrated row.
    fn entities(self, entities_tree: &sled::Tree, model: Result<ankurah_proto::ModelId, String>) -> SledEntityLookup<Self> {
        SledEntityLookup::new(entities_tree, model, self)
    }
}

/// Trait that provides a convenient `.entities()` combinator for materialized value streams
pub trait SledEntityExtFromMats: ValueSetStream + Sized
where Self::Item: HasEntityId + Filterable
{
    /// Extract EntityIds and hydrate into EntityStates using the sled entities tree.
    /// `model` is the model-id resolution outcome written on reconstructed
    /// envelopes (#330), checked lazily on the first hydrated row.
    fn entities(self, entities_tree: &sled::Tree, model: Result<ankurah_proto::ModelId, String>) -> SledEntityLookup<impl EntityIdStream> {
        let ids = self.extract_ids();
        SledEntityLookup::new(entities_tree, model, ids)
    }
}

// Blanket implementation for all EntityId streams
impl<S: EntityIdStream> SledEntityExt for S {}

// Blanket implementation for all streams with HasEntityId + Filterable items
impl<S> SledEntityExtFromMats for S
where
    S: Stream + Unpin + ValueSetStream,
    S::Item: HasEntityId + Filterable,
{
}
