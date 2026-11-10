use ankurah_core::selection::filter::ValueLookup;
use ankurah_proto::{EntityId, EntityState, State};
use ankurah_storage_common::filtering::{HasEntityId, ValueSetStream};
use ankurah_storage_common::traits::EntityIdStream;
use ankurah_storage_common::EngineColumns;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The bytes sled stores for an entity's state.
pub(crate) fn encode_state(state: &State) -> bincode::Result<Vec<u8>> { bincode::serialize(state) }

/// The state of `entity_id` that sled stored as `bytes`.
pub(crate) fn decode_state(entity_id: EntityId, bytes: &[u8]) -> bincode::Result<EntityState> {
    Ok(EntityState { entity_id, state: bincode::deserialize(bytes)? })
}

/// Sled-specific entity state lookup that hydrates EntityIds into full EntityStates
pub struct SledEntityLookup<S> {
    pub entities_tree: sled::Tree,
    pub stream: S,
}

impl<S: EntityIdStream> SledEntityLookup<S> {
    pub fn new(entities_tree: &sled::Tree, stream: S) -> Self { Self { entities_tree: entities_tree.clone(), stream } }
}

impl<S: Unpin> Unpin for SledEntityLookup<S> {}

impl<S: EntityIdStream> Stream for SledEntityLookup<S> {
    type Item = Result<EntityState, ankurah_core::error::RetrievalError>;

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
            Ok(None) => return Poll::Ready(Some(Err(ankurah_core::error::RetrievalError::EntityNotFound(entity_id)))),
            Err(e) => return Poll::Ready(Some(Err(ankurah_core::error::RetrievalError::storage(e)))),
        };

        Poll::Ready(Some(decode_state(entity_id, &value_bytes).map_err(ankurah_core::error::RetrievalError::storage)))
    }
}

// EntityStateStream is automatically implemented via blanket impl since SledEntityLookup emits Result<EntityState, RetrievalError>

/// Trait that provides a convenient `.entities()` combinator for EntityId streams
pub trait SledEntityExt: EntityIdStream + Sized {
    /// Hydrate EntityIds into EntityStates using the sled entities tree.
    fn entities(self, entities_tree: &sled::Tree) -> SledEntityLookup<Self> { SledEntityLookup::new(entities_tree, self) }
}

/// Trait that provides a convenient `.entities()` combinator for materialized value streams
pub trait SledEntityExtFromMats: ValueSetStream + Sized
where Self::Item: HasEntityId + ValueLookup<EngineColumns>
{
    /// Extract EntityIds and hydrate into EntityStates using the sled entities tree.
    fn entities(self, entities_tree: &sled::Tree) -> SledEntityLookup<impl EntityIdStream> {
        let ids = self.extract_ids();
        SledEntityLookup::new(entities_tree, ids)
    }
}

// Blanket implementation for all EntityId streams
impl<S: EntityIdStream> SledEntityExt for S {}

// Blanket implementation for all streams with HasEntityId + ValueLookup<EngineColumns> items
impl<S> SledEntityExtFromMats for S
where
    S: Stream + Unpin + ValueSetStream,
    S::Item: HasEntityId + ValueLookup<EngineColumns>,
{
}
