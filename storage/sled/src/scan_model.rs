use crate::error::IndexError;
use crate::planner_integration::{key_bounds_to_sled_range, SledRangeBounds};
use crate::property::PropertyManager;
use ankurah_core::indexing::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah_core::value::ValueType;
use ankurah_core::{error::RetrievalError, EntityId};
use ankurah_storage_common::{traits::EntityIdStream, KeyBounds, ScanDirection};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Scanner over a model-materialization tree yielding `EntityId` directly.
pub struct SledMaterializationKeyScanner {
    iter: SledMaterializationIter,
}

enum SledMaterializationIter {
    Forward(sled::Iter),
    Reverse(std::iter::Rev<sled::Iter>),
}

impl Iterator for SledMaterializationIter {
    type Item = Result<(sled::IVec, sled::IVec), sled::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SledMaterializationIter::Forward(iter) => iter.next(),
            SledMaterializationIter::Reverse(iter) => iter.next(),
        }
    }
}

impl SledMaterializationKeyScanner {
    /// Create an entity-id scanner over the requested materialization range.
    pub fn new(tree: &sled::Tree, bounds: &KeyBounds, direction: ScanDirection) -> Result<Self, IndexError> {
        // Setup iterator immediately in constructor
        // Create a primary key spec (single ascending EntityId component)
        let primary_key_spec = KeySpec {
            keyparts: vec![IndexKeyPart {
                key: "id".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,  // Primary keys are always ascending
                value_type: ValueType::EntityId, // EntityId type
                nulls: None,
                collation: None,
            }],
        };

        let SledRangeBounds { start: start_key, end: end_key_opt, .. } = key_bounds_to_sled_range(bounds, &primary_key_spec)?;

        let iter = match direction {
            ScanDirection::Forward => match &end_key_opt {
                Some(end_key) => SledMaterializationIter::Forward(tree.range(start_key.clone()..end_key.clone())),
                None => SledMaterializationIter::Forward(tree.range(start_key.clone()..)),
            },
            ScanDirection::Reverse => match &end_key_opt {
                Some(end_key) => SledMaterializationIter::Reverse(tree.range(start_key.clone()..end_key.clone()).rev()),
                None => SledMaterializationIter::Reverse(tree.range(start_key.clone()..).rev()),
            },
        };

        Ok(Self { iter })
    }
}

impl Unpin for SledMaterializationKeyScanner {}

impl Stream for SledMaterializationKeyScanner {
    type Item = Result<EntityId, RetrievalError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Synchronous implementation - always returns Ready
        let (key_bytes, _value_bytes) = match self.iter.next() {
            Some(Ok(kv)) => kv,
            Some(Err(e)) => return Poll::Ready(Some(Err(RetrievalError::StorageError(e.to_string().into())))),
            None => return Poll::Ready(None),
        };

        // Materialization-tree keys are the raw EntityId bytes.
        match key_bytes.as_ref().try_into() {
            Ok(bytes) => Poll::Ready(Some(Ok(EntityId::from_bytes(bytes)))),
            Err(_) => Poll::Ready(None),
        }
    }
}

/// Scanner over a model-materialization tree yielding projected values.
pub struct SledMaterializationScanner<'a> {
    property_manager: &'a PropertyManager,
    iter: SledMaterializationIter,
}

impl<'a> SledMaterializationScanner<'a> {
    /// Create a projected-value scanner over the requested materialization
    /// range.
    pub fn new(
        tree: &'a sled::Tree,
        bounds: &'a KeyBounds,
        direction: ScanDirection,
        property_manager: &'a PropertyManager,
    ) -> Result<Self, IndexError> {
        // Set up the iterator immediately, as in
        // `SledMaterializationKeyScanner`.
        // Create a primary key spec (single ascending EntityId component)
        let primary_key_spec = KeySpec {
            keyparts: vec![IndexKeyPart {
                key: "id".to_string(),
                sub_path: None,
                direction: IndexDirection::Asc,  // Primary keys are always ascending
                value_type: ValueType::EntityId, // EntityId type
                nulls: None,
                collation: None,
            }],
        };

        let SledRangeBounds { start: start_key, end: end_key_opt, .. } = key_bounds_to_sled_range(bounds, &primary_key_spec)?;
        let iter = match direction {
            ScanDirection::Forward => match &end_key_opt {
                Some(end_key) => SledMaterializationIter::Forward(tree.range(start_key.clone()..end_key.clone())),
                None => SledMaterializationIter::Forward(tree.range(start_key.clone()..)),
            },
            ScanDirection::Reverse => match &end_key_opt {
                Some(end_key) => SledMaterializationIter::Reverse(tree.range(start_key.clone()..end_key.clone()).rev()),
                None => SledMaterializationIter::Reverse(tree.range(start_key.clone()..).rev()),
            },
        };

        Ok(Self { property_manager, iter })
    }
}

impl Unpin for SledMaterializationScanner<'_> {}

impl Stream for SledMaterializationScanner<'_> {
    type Item = crate::materialization::ProjectedEntity;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Synchronous implementation - always returns Ready
        let (key_bytes, value_bytes) = match self.iter.next() {
            Some(Ok(kv)) => kv,
            Some(Err(_e)) => return Poll::Ready(None), // Skip errors for now - TODO: proper error handling
            None => return Poll::Ready(None),
        };

        // Extract EntityId from key
        let entity_id = match key_bytes.as_ref().try_into() {
            Ok(bytes) => EntityId::from_bytes(bytes),
            Err(_) => return Poll::Ready(None),
        };

        // Decode materialized values from value
        let property_values: Vec<(u32, ankurah_core::value::Value)> = match bincode::deserialize(&value_bytes) {
            Ok(values) => values,
            Err(_e) => return Poll::Ready(None), // Skip errors for now - TODO: proper error handling
        };

        // Convert to ProjectedEntity - reverse each numeric slot to its durable
        // PropertyId so the projection is addressed by identity.
        let mut map = std::collections::BTreeMap::new();
        for (slot, value) in property_values {
            if let Ok(Some(property_id)) = self.property_manager.property_for_slot(slot) {
                map.insert(property_id, value);
            }
        }

        Poll::Ready(Some(crate::materialization::ProjectedEntity { id: entity_id, map }))
    }
}

/// Sled-specific materialized value lookup iterator (for index scans that need materialization)
pub struct SledMaterializationLookup<S> {
    /// Materialization tree containing projected property slots.
    pub tree: sled::Tree,
    /// Durable property-id to compact-slot registry.
    pub property_manager: PropertyManager,
    /// Upstream entity-id stream to hydrate from the materialization.
    pub stream: S,
}

impl<S: EntityIdStream> SledMaterializationLookup<S> {
    /// Attach projected-value lookup to an entity-id stream.
    pub fn new(tree: &sled::Tree, property_manager: &PropertyManager, stream: S) -> Self {
        Self { tree: tree.clone(), property_manager: property_manager.clone(), stream }
    }
}

impl<S: Unpin> Unpin for SledMaterializationLookup<S> {}

impl<S: EntityIdStream> Stream for SledMaterializationLookup<S> {
    type Item = crate::materialization::ProjectedEntity;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Get next EntityId from upstream stream
        let entity_id = match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(id))) => id,
            Poll::Ready(Some(Err(_e))) => return Poll::Ready(None), // Skip errors for now
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        // Look up projected values from the model materialization.
        let key = entity_id.to_bytes();
        let value_bytes = match self.tree.get(key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Poll::Ready(None), // Entity not found
            Err(_e) => return Poll::Ready(None),  // Skip errors for now - TODO: proper error handling
        };

        // Decode materialized values
        let property_values: Vec<(u32, ankurah_core::value::Value)> = match bincode::deserialize(&value_bytes) {
            Ok(values) => values,
            Err(_e) => return Poll::Ready(None), // Skip errors for now - TODO: proper error handling
        };

        // Convert to ProjectedEntity - reverse each numeric slot to its durable
        // PropertyId so the projection is addressed by identity.
        let mut map = std::collections::BTreeMap::new();
        for (slot, value) in property_values {
            if let Ok(Some(property_id)) = self.property_manager.property_for_slot(slot) {
                map.insert(property_id, value);
            }
        }

        Poll::Ready(Some(crate::materialization::ProjectedEntity { id: entity_id, map }))
    }
}
