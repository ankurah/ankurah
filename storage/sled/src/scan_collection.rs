use crate::error::IndexError;
use crate::planner_integration::{key_bounds_to_sled_range, SledRangeBounds};
use crate::property::PropertyManager;
use ankurah_core::{error::RetrievalError, EntityId};
use ankurah_storage_common::{traits::EntityIdStream, IndexDirection, IndexKeyPart, KeyBounds, KeySpec, ScanDirection, ValueType};

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
                direction: IndexDirection::Asc, // Primary keys are always ascending
                value_type: ValueType::Binary,  // EntityId is 16-byte binary
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

impl<'a> Iterator for SledCollectionKeyScanner<'a> {
    type Item = Result<EntityId, RetrievalError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next item from iterator
        let (key_bytes, _value_bytes) = match self.iter.next()? {
            Ok(kv) => kv,
            Err(e) => return Some(Err(RetrievalError::StorageError(e.to_string().into()))),
        };

        // For collection trees, the key is directly the EntityId bytes
        match EntityId::from_bytes(key_bytes.as_ref().try_into().ok()?) {
            entity_id => Some(Ok(entity_id)),
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
                direction: IndexDirection::Asc, // Primary keys are always ascending
                value_type: ValueType::Binary,  // EntityId is 16-byte binary
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

impl<'a> Iterator for SledCollectionScanner<'a> {
    type Item = crate::materialization::MatRow;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next item from iterator
        let (key_bytes, value_bytes) = match self.iter.next()? {
            Ok(kv) => kv,
            Err(_e) => return None, // Skip errors for now - TODO: proper error handling
        };

        // Extract EntityId from key
        let entity_id = EntityId::from_bytes(key_bytes.as_ref().try_into().ok()?);

        // Decode materialized values from value
        let property_values: Vec<(u32, ankurah_core::property::PropertyValue)> = match bincode::deserialize(&value_bytes) {
            Ok(values) => values,
            Err(_e) => return None, // Skip errors for now - TODO: proper error handling
        };

        // Convert to MatEntity - store by property name (not ID)
        let mut map = std::collections::BTreeMap::new();
        for (property_id, value) in property_values {
            if let Some(property_name) = self.property_manager.get_property_name(property_id) {
                map.insert(property_name, value);
            }
        }

        let mat_entity = crate::materialization::MatEntity {
            id: entity_id,
            collection: ankurah_proto::CollectionId::from("unknown"), // TODO: get from context
            map,
        };

        Some(crate::materialization::MatRow { id: entity_id, mat: mat_entity })
    }
}

/// Sled-specific materialized value lookup iterator (for index scans that need materialization)
pub struct SledMaterializeIter<S> {
    pub tree: sled::Tree,
    pub property_manager: PropertyManager,
    pub stream: S,
}

impl<S: EntityIdStream> SledMaterializeIter<S> {
    pub fn new(tree: &sled::Tree, property_manager: &PropertyManager, stream: S) -> Self {
        Self { tree: tree.clone(), property_manager: property_manager.clone(), stream }
    }
}

impl<S: EntityIdStream> Iterator for SledMaterializeIter<S> {
    type Item = crate::materialization::MatRow;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next EntityId from upstream stream
        let entity_id = match self.stream.next()? {
            Ok(id) => id,
            Err(_e) => return None, // Skip errors for now - TODO: proper error handling
        };

        // Lookup materialized values from collection tree
        let key = entity_id.to_bytes();
        let value_bytes = match self.tree.get(key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return None, // Entity not found
            Err(_e) => return None,  // Skip errors for now - TODO: proper error handling
        };

        // Decode materialized values
        let property_values: Vec<(u32, ankurah_core::property::PropertyValue)> = match bincode::deserialize(&value_bytes) {
            Ok(values) => values,
            Err(_e) => return None, // Skip errors for now - TODO: proper error handling
        };

        // Convert to MatEntity - store by property ID for now
        let mut map = std::collections::BTreeMap::new();
        for (property_id, value) in property_values {
            // For now, use property_id as string key - we can optimize this later
            // do it:
            let property_name = self.property_manager.get_property_name(property_id);
            if let Some(property_name) = property_name {
                map.insert(property_name, value);
            }
        }

        let mat_entity = crate::materialization::MatEntity {
            id: entity_id,
            collection: ankurah_proto::CollectionId::from("unknown"), // TODO: get from context
            map,
        };

        Some(crate::materialization::MatRow { id: entity_id, mat: mat_entity })
    }
}

// Implement GetPropertyValueStream for SledMaterializeIter so it can be used with filtering/sorting
// impl<S: EntityIdStream> GetPropertyValueStream for SledMaterializeIter<S> {}
