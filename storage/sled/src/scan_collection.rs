use crate::error::IndexError;
use crate::planner_integration::{key_bounds_to_sled_range, SledRangeBounds};
use crate::property::PropertyManager;
use ankurah_core::indexing::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah_core::value::ValueType;
use ankurah_core::{error::StorageError, EntityId};
use ankurah_storage_common::{traits::EntityIdStream, KeyBounds, ScanDirection};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Scanner over a materialized collection tree yielding EntityId directly (for ID-only queries)
pub struct SledCollectionKeyScanner<'a> {
    pub tree: &'a sled::Tree,
    pub bounds: &'a KeyBounds,
    pub direction: ScanDirection,
    // Iterator state
    iter: SledCollectionIter,
}

enum SledCollectionIter {
    Forward(sled::Iter),
    Reverse(std::iter::Rev<sled::Iter>),
}

impl Iterator for SledCollectionIter {
    type Item = Result<(sled::IVec, sled::IVec), sled::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SledCollectionIter::Forward(iter) => iter.next(),
            SledCollectionIter::Reverse(iter) => iter.next(),
        }
    }
}

impl<'a> SledCollectionKeyScanner<'a> {
    pub fn new(tree: &'a sled::Tree, bounds: &'a KeyBounds, direction: ScanDirection) -> Result<Self, IndexError> {
        // Setup iterator immediately in constructor
        // Create a primary key spec (single ascending EntityId component)
        let primary_key_spec = KeySpec {
            keyparts: vec![IndexKeyPart {
                column: "id".to_string(),
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
                Some(end_key) => SledCollectionIter::Forward(tree.range(start_key.clone()..end_key.clone())),
                None => SledCollectionIter::Forward(tree.range(start_key.clone()..)),
            },
            ScanDirection::Reverse => match &end_key_opt {
                Some(end_key) => SledCollectionIter::Reverse(tree.range(start_key.clone()..end_key.clone()).rev()),
                None => SledCollectionIter::Reverse(tree.range(start_key.clone()..).rev()),
            },
        };

        Ok(Self { tree, bounds, direction, iter })
    }
}

impl Unpin for SledCollectionKeyScanner<'_> {}

impl Stream for SledCollectionKeyScanner<'_> {
    type Item = Result<EntityId, StorageError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Synchronous implementation - always returns Ready
        let (key_bytes, _value_bytes) = match self.iter.next() {
            Some(Ok(kv)) => kv,
            Some(Err(e)) => return Poll::Ready(Some(Err(StorageError::BackendError(Box::new(e))))),
            None => return Poll::Ready(None),
        };

        // For collection trees, the key is directly the EntityId bytes
        match key_bytes.as_ref().try_into() {
            Ok(bytes) => Poll::Ready(Some(Ok(EntityId::from_bytes(bytes)))),
            Err(_) => Poll::Ready(None),
        }
    }
}

/// Scanner over a materialized collection tree yielding materialized values directly
pub struct SledCollectionScanner<'a> {
    pub tree: &'a sled::Tree,
    pub bounds: &'a KeyBounds,
    pub direction: ScanDirection,
    pub property_manager: &'a PropertyManager,
    iter: SledCollectionIter,
}

impl<'a> SledCollectionScanner<'a> {
    pub fn new(
        tree: &'a sled::Tree,
        bounds: &'a KeyBounds,
        direction: ScanDirection,
        property_manager: &'a PropertyManager,
    ) -> Result<Self, IndexError> {
        // Setup iterator immediately in constructor (same logic as SledCollectionKeyScanner)
        // Create a primary key spec (single ascending EntityId component)
        let primary_key_spec = KeySpec {
            keyparts: vec![IndexKeyPart {
                column: "id".to_string(),
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
                Some(end_key) => SledCollectionIter::Forward(tree.range(start_key.clone()..end_key.clone())),
                None => SledCollectionIter::Forward(tree.range(start_key.clone()..)),
            },
            ScanDirection::Reverse => match &end_key_opt {
                Some(end_key) => SledCollectionIter::Reverse(tree.range(start_key.clone()..end_key.clone()).rev()),
                None => SledCollectionIter::Reverse(tree.range(start_key.clone()..).rev()),
            },
        };

        Ok(Self { tree, bounds, direction, property_manager, iter })
    }
}

impl Unpin for SledCollectionScanner<'_> {}

impl Stream for SledCollectionScanner<'_> {
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

        // Convert to ProjectedEntity - store by property name (not ID)
        let mut map = std::collections::BTreeMap::new();
        for (property_id, value) in property_values {
            if let Some(property_name) = self.property_manager.get_property_name(property_id) {
                map.insert(property_name, value);
            }
        }

        Poll::Ready(Some(crate::materialization::ProjectedEntity {
            id: entity_id,
            collection: ankurah_proto::CollectionId::from("unknown"), // TODO: get from context
            map,
        }))
    }
}

/// Sled-specific materialized value lookup iterator (for index scans that need materialization)
pub struct SledCollectionLookup<S> {
    pub tree: sled::Tree,
    pub property_manager: PropertyManager,
    pub stream: S,
}

impl<S: EntityIdStream> SledCollectionLookup<S> {
    pub fn new(tree: &sled::Tree, property_manager: &PropertyManager, stream: S) -> Self {
        Self { tree: tree.clone(), property_manager: property_manager.clone(), stream }
    }
}

impl<S: Unpin> Unpin for SledCollectionLookup<S> {}

impl<S: EntityIdStream> Stream for SledCollectionLookup<S> {
    type Item = crate::materialization::ProjectedEntity;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Get next EntityId from upstream stream
        let entity_id = match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(id))) => id,
            Poll::Ready(Some(Err(_e))) => return Poll::Ready(None), // Skip errors for now
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        // Lookup materialized values from collection tree
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

        // Convert to ProjectedEntity - store by property name
        let mut map = std::collections::BTreeMap::new();
        for (property_id, value) in property_values {
            if let Some(property_name) = self.property_manager.get_property_name(property_id) {
                map.insert(property_name, value);
            }
        }

        Poll::Ready(Some(crate::materialization::ProjectedEntity {
            id: entity_id,
            collection: ankurah_proto::CollectionId::from("unknown"), // TODO: get from context
            map,
        }))
    }
}
