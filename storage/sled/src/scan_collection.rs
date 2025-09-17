use crate::planner_integration::{bounds_to_sled_range, normalize};
use crate::property::PropertyManager;
use ankurah_core::{error::RetrievalError, EntityId};
use ankurah_storage_common::{filtering::GetPropertyValueStream, traits::EntityIdStream, IndexBounds, ScanDirection};

/// Scanner over a materialized collection tree yielding EntityId directly (for ID-only queries)
pub struct SledCollectionKeyScanner<'a> {
    pub tree: &'a sled::Tree,
    pub bounds: &'a IndexBounds,
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
    pub fn new(tree: &'a sled::Tree, bounds: &'a IndexBounds, direction: ScanDirection) -> Self {
        // Setup iterator immediately in constructor
        let (canonical, _eq_prefix_len, _eq_prefix_values) = normalize(bounds);

        // Extract EntityId range from canonical bounds
        let (start_key, end_key_opt) = match (&canonical.lower, &canonical.upper) {
            (Some((lower_vals, _)), Some((upper_vals, _))) if !lower_vals.is_empty() && !upper_vals.is_empty() => {
                // Both bounds present - extract EntityIds
                let start_id = entity_id_from_property_value(&lower_vals[0]).unwrap_or_else(|| EntityId::from_bytes([0u8; 16]));
                let end_id = entity_id_from_property_value(&upper_vals[0]).unwrap_or_else(|| EntityId::from_bytes([0xFFu8; 16]));
                (start_id.to_bytes().to_vec(), Some(end_id.to_bytes().to_vec()))
            }
            (Some((lower_vals, _)), None) if !lower_vals.is_empty() => {
                // Only lower bound
                let start_id = entity_id_from_property_value(&lower_vals[0]).unwrap_or_else(|| EntityId::from_bytes([0u8; 16]));
                (start_id.to_bytes().to_vec(), None)
            }
            (None, Some((upper_vals, _))) if !upper_vals.is_empty() => {
                // Only upper bound - start from beginning
                let end_id = entity_id_from_property_value(&upper_vals[0]).unwrap_or_else(|| EntityId::from_bytes([0xFFu8; 16]));
                (vec![0u8; 16], Some(end_id.to_bytes().to_vec()))
            }
            _ => {
                // No bounds or empty bounds - scan everything
                (vec![0u8; 16], None)
            }
        };

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

        Self { tree, bounds, direction, iter }
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
    pub bounds: &'a IndexBounds,
    pub direction: ScanDirection,
    pub property_manager: &'a PropertyManager,
    iter: SledCollectionIter,
}

impl<'a> SledCollectionScanner<'a> {
    pub fn new(tree: &'a sled::Tree, bounds: &'a IndexBounds, direction: ScanDirection, property_manager: &'a PropertyManager) -> Self {
        // Setup iterator immediately in constructor (same logic as SledCollectionKeyScanner)
        let (canonical, _eq_prefix_len, _eq_prefix_values) = normalize(bounds);

        let (start_key, end_key_opt) = match (&canonical.lower, &canonical.upper) {
            (Some((lower_vals, _)), Some((upper_vals, _))) if !lower_vals.is_empty() && !upper_vals.is_empty() => {
                let start_id = entity_id_from_property_value(&lower_vals[0]).unwrap_or_else(|| EntityId::from_bytes([0u8; 16]));
                let end_id = entity_id_from_property_value(&upper_vals[0]).unwrap_or_else(|| EntityId::from_bytes([0xFFu8; 16]));
                (start_id.to_bytes().to_vec(), Some(end_id.to_bytes().to_vec()))
            }
            (Some((lower_vals, _)), None) if !lower_vals.is_empty() => {
                let start_id = entity_id_from_property_value(&lower_vals[0]).unwrap_or_else(|| EntityId::from_bytes([0u8; 16]));
                (start_id.to_bytes().to_vec(), None)
            }
            (None, Some((upper_vals, _))) if !upper_vals.is_empty() => {
                let end_id = entity_id_from_property_value(&upper_vals[0]).unwrap_or_else(|| EntityId::from_bytes([0xFFu8; 16]));
                (vec![0u8; 16], Some(end_id.to_bytes().to_vec()))
            }
            _ => (vec![0u8; 16], None),
        };

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

        Self { tree, bounds, direction, property_manager, iter }
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

// Implement GetPropertyValueStream for SledCollectionScanner so it can be used with filtering/sorting
// impl<'a> GetPropertyValueStream for SledCollectionScanner<'a> {}

/// Helper function to extract EntityId from PropertyValue (for primary key bounds)
fn entity_id_from_property_value(value: &ankurah_core::property::PropertyValue) -> Option<EntityId> {
    match value {
        ankurah_core::property::PropertyValue::String(s) => EntityId::from_base64(s).ok(),
        ankurah_core::property::PropertyValue::Binary(bytes) if bytes.len() == 16 => {
            let mut array = [0u8; 16];
            array.copy_from_slice(bytes);
            Some(EntityId::from_bytes(array))
        }
        _ => None,
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
        let value_bytes = match self.tree.get(&key) {
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
