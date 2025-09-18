use ankurah_proto::CollectionId;
use ankurah_storage_common::filtering::{GetPropertyValueStream, HasEntityId};
use ankurah_storage_common::traits::{EntityIdStream, EntityStateStream};

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

impl<S: EntityIdStream> Iterator for SledEntityLookup<S> {
    type Item = Result<ankurah_proto::Attested<ankurah_proto::EntityState>, ankurah_core::error::RetrievalError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next EntityId from upstream stream
        let entity_id = match self.stream.next()? {
            Ok(id) => id,
            Err(e) => return Some(Err(e)),
        };

        // Lookup entity state from entities tree
        let key = entity_id.to_bytes();
        let value_bytes = match self.entities_tree.get(&key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Some(Err(ankurah_core::error::RetrievalError::StorageError("Entity not found".to_string().into()))),
            Err(e) => return Some(Err(ankurah_core::error::RetrievalError::StorageError(e.to_string().into()))),
        };

        // Decode StateFragment
        let state_fragment: ankurah_proto::StateFragment = match bincode::deserialize(&value_bytes) {
            Ok(fragment) => fragment,
            Err(e) => return Some(Err(ankurah_core::error::RetrievalError::StorageError(e.to_string().into()))),
        };

        // Create Attested<EntityState> from parts
        let attested =
            ankurah_proto::Attested::<ankurah_proto::EntityState>::from_parts(entity_id, self.collection_id.clone(), state_fragment);

        Some(Ok(attested))
    }
}

// EntityStateStream is automatically implemented via blanket impl since SledEntityLookup emits Result<Attested<EntityState>, RetrievalError>

/// Trait that provides a convenient `.entities()` combinator for EntityId streams
pub trait SledEntityExt: EntityIdStream + Sized {
    /// Hydrate EntityIds into EntityStates using the sled entities tree
    fn entities(self, entities_tree: &sled::Tree, collection_id: &CollectionId) -> SledEntityLookup<Self> {
        SledEntityLookup::new(entities_tree, collection_id, self)
    }
}

/// Trait that provides a convenient `.entities()` combinator for materialized value streams
pub trait SledEntityExtFromMats: GetPropertyValueStream + Sized
where Self::Item: HasEntityId + ankql::selection::filter::Filterable
{
    /// Extract EntityIds and hydrate into EntityStates using the sled entities tree
    fn entities(self, entities_tree: &sled::Tree, collection_id: &CollectionId) -> SledEntityLookup<impl EntityIdStream> {
        let ids = self.extract_ids();
        SledEntityLookup::new(entities_tree, collection_id, ids)
    }
}

// Blanket implementation for all EntityId streams
impl<S: EntityIdStream> SledEntityExt for S {}

// Blanket implementation for all iterators with HasEntityId + Filterable items
impl<S> SledEntityExtFromMats for S
where
    S: Iterator + GetPropertyValueStream,
    S::Item: HasEntityId + ankql::selection::filter::Filterable,
{
}
