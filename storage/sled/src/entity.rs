use ankurah_core::selection::filter::Filterable;
use ankurah_proto::CollectionId;
use ankurah_storage_common::filtering::{HasEntityId, ValueSetStream};
use ankurah_storage_common::traits::EntityIdStream;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Sled-specific entity state lookup that hydrates EntityIds into full EntityStates
pub struct SledEntityLookup<S> {
    pub entities_tree: sled::Tree,
    pub collection_id: CollectionId,
    pub stream: S,
}

impl<S: EntityIdStream> SledEntityLookup<S> {
    pub fn new(entities_tree: &sled::Tree, collection_id: &CollectionId, stream: S) -> Self {
        Self { entities_tree: entities_tree.clone(), collection_id: collection_id.clone(), stream }
    }
}

impl<S: Unpin> Unpin for SledEntityLookup<S> {}

impl<S: EntityIdStream> Stream for SledEntityLookup<S> {
    type Item = Result<ankurah_proto::Attested<ankurah_proto::EntityState>, ankurah_core::error::StorageError>;

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
                return Poll::Ready(Some(Err(ankurah_core::error::StorageError::EntityNotFound(entity_id))))
            }
            Err(e) => return Poll::Ready(Some(Err(ankurah_core::error::StorageError::BackendError(Box::new(e))))),
        };

        // Decode StateFragment
        let state_fragment: ankurah_proto::StateFragment = match bincode::deserialize(&value_bytes) {
            Ok(fragment) => fragment,
            Err(e) => return Poll::Ready(Some(Err(ankurah_core::error::StorageError::SerializationError(Box::new(e))))),
        };

        // Create Attested<EntityState> from parts
        let attested =
            ankurah_proto::Attested::<ankurah_proto::EntityState>::from_parts(entity_id, self.collection_id.clone(), state_fragment);

        Poll::Ready(Some(Ok(attested)))
    }
}

// EntityStateStream is automatically implemented via blanket impl since SledEntityLookup emits Result<Attested<EntityState>, StorageError>

/// Trait that provides a convenient `.entities()` combinator for EntityId streams
pub trait SledEntityExt: EntityIdStream + Sized {
    /// Hydrate EntityIds into EntityStates using the sled entities tree
    fn entities(self, entities_tree: &sled::Tree, collection_id: &CollectionId) -> SledEntityLookup<Self> {
        SledEntityLookup::new(entities_tree, collection_id, self)
    }
}

/// Trait that provides a convenient `.entities()` combinator for materialized value streams
pub trait SledEntityExtFromMats: ValueSetStream + Sized
where Self::Item: HasEntityId + Filterable
{
    /// Extract EntityIds and hydrate into EntityStates using the sled entities tree
    fn entities(self, entities_tree: &sled::Tree, collection_id: &CollectionId) -> SledEntityLookup<impl EntityIdStream> {
        let ids = self.extract_ids();
        SledEntityLookup::new(entities_tree, collection_id, ids)
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
