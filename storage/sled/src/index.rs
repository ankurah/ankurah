use ankurah_core::error::{MutationError, RetrievalError};
use ankurah_core::indexing::IndexSpecMatch;
use ankurah_proto::EntityId;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::HashMap;
// use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};

use crate::{error::IndexError, planner_integration::encode_tuple_values_with_key_spec, property::PropertyManager};

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum BuildStatus {
    NotBuilt,
    Building,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexRecord {
    pub id: u32,
    pub collection: String,
    pub name: String,
    pub spec: ankurah_core::indexing::KeySpec,
    pub created_at_unix_ms: i64,
    pub build_status: BuildStatus,
}

#[derive(Clone)]
pub struct Index(Arc<IndexInner>);

// Do not expose IndexInner via Deref to avoid leaking private type
struct IndexInner {
    pub id: u32,
    pub collection: String,
    pub name: String,
    pub spec: ankurah_core::indexing::KeySpec,
    pub created_at_unix_ms: i64,
    build_status: Mutex<BuildStatus>,
    pub build_lock: Mutex<()>,
    pub tree: Tree,
    pub index_config_tree: Tree,
    pub property_manager: PropertyManager,
}

pub struct IndexManager {
    pub index_config_tree: Tree,
    pub indexes: RwLock<HashMap<u32, Index>>,
}

impl IndexManager {
    pub fn open(index_config_tree: Tree, db: &Db, property_manager: PropertyManager) -> Result<Self, IndexError> {
        let mut indexes = HashMap::new();
        for item in index_config_tree.iter() {
            let (key, bytes) = item?;
            let key = u32::from_be_bytes(key.as_ref().try_into().map_err(|_| IndexError::InvalidKeyLength)?);
            if let Ok(mut rec) = bincode::deserialize::<IndexRecord>(&bytes) {
                // Trust key as source of truth for id
                rec.id = key;
                indexes.insert(key, Index::from_record(rec, db, index_config_tree.clone(), property_manager.clone())?);
            }
        }
        Ok(Self { index_config_tree, indexes: RwLock::new(indexes) })
    }

    pub fn next_index_id(&self) -> Result<u32, IndexError> {
        if let Some((k, _)) = self.index_config_tree.last()? {
            let arr: [u8; 4] = k.as_ref().try_into().map_err(|_| IndexError::InvalidKeyLength)?;
            Ok(u32::from_be_bytes(arr) + 1)
        } else {
            Ok(0)
        }
    }

    pub fn assure_index_exists(
        &self,
        collection: &str,
        spec: &ankurah_core::indexing::KeySpec,
        db: &Db,
        property_manager: &PropertyManager,
    ) -> Result<(Index, IndexSpecMatch), RetrievalError> {
        // Try existing matching index
        if let Some((_, existing, match_type)) = self.indexes.read().unwrap().iter().find_map(|(id, idx)| {
            if idx.collection() == collection {
                if let Some(match_result) = spec.matches(idx.spec()) {
                    return Some((*id, idx.clone(), match_result));
                }
            }
            None
        }) {
            existing.build_if_needed(db)?;
            return Ok((existing, match_type));
        }

        // Create a fresh index rather than mutating an incompatible persisted
        // tree in place. `KeySpec::matches` includes the encoding value type,
        // so an upgraded database with a legacy String-collated numeric index
        // reaches this path and is backfilled under a new id. Keeping the old
        // tree intact is safe for scans that already hold a handle to it.
        let index = {
            let id = self.next_index_id()?;
            let mut w = self.indexes.write().unwrap();
            let index = Index::new_from_spec(collection, spec.clone(), db, id, self.index_config_tree.clone(), property_manager.clone())?;
            w.insert(id, index.clone());
            index
        };

        index.build_if_needed(db)?;
        Ok((index, ankurah_core::indexing::IndexSpecMatch::Match))
    }
}

impl Index {
    pub fn tree(&self) -> &sled::Tree { &self.0.tree }
    pub fn id(&self) -> u32 { self.0.id }
    pub fn collection(&self) -> &str { &self.0.collection }
    pub fn name(&self) -> &str { &self.0.name }
    pub fn spec(&self) -> &ankurah_core::indexing::KeySpec { &self.0.spec }
    pub fn created_at_unix_ms(&self) -> i64 { self.0.created_at_unix_ms }
    /// Build the index key for an entity given a materialized property map.
    /// Returns Ok(None) if any required key part is missing and the entity should not be indexed.
    pub fn build_key(&self, eid: &EntityId, properties: &[(u32, ankurah_core::value::Value)]) -> Result<Option<Vec<u8>>, IndexError> {
        use std::collections::BTreeMap;
        let map: BTreeMap<_, _> = properties.iter().cloned().collect();
        self.build_key_from_map(eid, &map)
    }

    /// Internal helper to build index key from a property map.
    /// Shared between build_key and backfill to avoid duplication.
    fn build_key_from_map(
        &self,
        eid: &EntityId,
        property_map: &std::collections::BTreeMap<u32, ankurah_core::value::Value>,
    ) -> Result<Option<Vec<u8>>, IndexError> {
        // Resolve pids using PropertyManager
        let mut pids: Vec<u32> = Vec::with_capacity(self.0.spec.keyparts.len());
        for kp in &self.0.spec.keyparts {
            match self.0.property_manager.get_property_id(&kp.column) {
                Ok(id) => pids.push(id),
                Err(_e) => return Err(IndexError::PropertyNotFound(kp.column.clone())),
            }
        }

        // Build composite key using per-keypart direction from spec
        let mut tuple_values: Vec<ankurah_core::value::Value> = Vec::with_capacity(self.0.spec.keyparts.len());
        for (pid, kp) in pids.iter().zip(self.0.spec.keyparts.iter()) {
            if let Some(val) = property_map.get(pid).cloned() {
                // If keypart has a sub_path, extract the value at that path
                let extracted = match &kp.sub_path {
                    None => Some(val),
                    Some(path) => val.extract_at_path(path),
                };
                match extracted {
                    Some(v) => tuple_values.push(v),
                    None => {
                        // Missing sub_path value - don't index this entity
                        return Ok(None);
                    }
                }
            } else {
                // Missing required property for this index
                return Ok(None);
            }
        }

        let mut key = encode_tuple_values_with_key_spec(&tuple_values, &self.0.spec)?;
        // No separator needed - KeySpec provides structure info for parsing
        key.extend_from_slice(&eid.to_bytes());
        Ok(Some(key))
    }
    pub fn from_record(rec: IndexRecord, db: &Db, index_config_tree: Tree, property_manager: PropertyManager) -> Result<Self, IndexError> {
        Ok(Self(Arc::new(IndexInner {
            id: rec.id,
            collection: rec.collection,
            name: rec.name,
            spec: rec.spec,
            created_at_unix_ms: rec.created_at_unix_ms,
            build_status: Mutex::new(rec.build_status),
            build_lock: Mutex::new(()),
            tree: db.open_tree(format!("index_{}", rec.id))?,
            index_config_tree,
            property_manager,
        })))
    }

    pub fn new_from_spec(
        collection: &str,
        spec: ankurah_core::indexing::KeySpec,
        db: &Db,
        id: u32,
        index_config_tree: Tree,
        property_manager: PropertyManager,
    ) -> Result<Self, IndexError> {
        Ok(Self(Arc::new(IndexInner {
            id,
            collection: collection.to_string(),
            name: spec.name_with("", "__"),
            spec,
            created_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            build_status: Mutex::new(BuildStatus::NotBuilt),
            build_lock: Mutex::new(()),
            tree: db.open_tree(format!("index_{}", id))?,
            index_config_tree,
            property_manager,
        })))
    }

    pub fn backfill(&self, db: &Db) -> Result<(), IndexError> {
        let coll_tree = db.open_tree(format!("collection_{}", &self.0.collection))?;

        for item in coll_tree.iter() {
            let (k, v) = item?;
            let eid = EntityId::from_bytes(k.as_ref().try_into().map_err(|_| IndexError::InvalidKeyLength)?);
            let mat: Vec<(u32, ankurah_core::value::Value)> = bincode::deserialize(&v)?;
            let map: std::collections::BTreeMap<_, _> = mat.into_iter().collect();

            // Use shared key building logic
            if let Some(key) = self.build_key_from_map(&eid, &map)? {
                self.0.tree.insert(key, &[])?;
            }
        }
        Ok(())
    }

    pub fn status(&self) -> BuildStatus { *self.0.build_status.lock().unwrap() }
    pub fn set_status(&self, status: BuildStatus) { *self.0.build_status.lock().unwrap() = status; }

    pub fn persist_snapshot(&self) -> Result<(), IndexError> {
        let bytes = bincode::serialize(&IndexRecord {
            id: self.0.id,
            collection: self.0.collection.clone(),
            name: self.0.name.clone(),
            spec: self.0.spec.clone(),
            created_at_unix_ms: self.0.created_at_unix_ms,
            build_status: self.status(),
        })?;
        self.0.index_config_tree.insert(self.0.id.to_be_bytes(), bytes)?;
        Ok(())
    }
    pub fn build_if_needed(&self, db: &Db) -> Result<(), RetrievalError> {
        let _guard = self.0.build_lock.lock().unwrap();
        if matches!(self.status(), BuildStatus::Ready) {
            return Ok(());
        }

        self.set_status(BuildStatus::Building);
        self.persist_snapshot()?;
        self.backfill(db)?;
        self.set_status(BuildStatus::Ready);
        self.persist_snapshot()?;
        Ok(())
    }
}

impl IndexManager {
    /// Update all indexes for a collection given an entity's old and new materializations.
    /// - Removes old index entries that no longer apply
    /// - Inserts new index entries that now apply
    pub fn update_indexes_for_entity(
        &self,
        collection: &str,
        eid: &EntityId,
        old_mat: Option<&[(u32, ankurah_core::value::Value)]>,
        new_mat: &[(u32, ankurah_core::value::Value)],
    ) -> Result<(), MutationError> {
        // Snapshot matching indexes
        let indexes: Vec<Index> = {
            let guard = self.indexes.read().unwrap();
            guard.values().filter(|idx| idx.collection() == collection).cloned().collect()
        };

        for index in indexes.iter() {
            let old_key = match old_mat {
                Some(mat) => index.build_key(eid, mat)?,
                None => None,
            };
            let new_key = index.build_key(eid, new_mat)?;

            match (old_key.as_ref(), new_key.as_ref()) {
                (Some(ok), Some(nk)) if ok == nk => {
                    // No change for this index
                }
                (Some(ok), Some(nk)) => {
                    // Key changed: remove old, insert new
                    index.tree().remove(ok).map_err(IndexError::from)?;
                    index.tree().insert(nk, &[]).map_err(IndexError::from)?;
                }
                (Some(ok), None) => {
                    // No longer matches index
                    index.tree().remove(ok).map_err(IndexError::from)?;
                }
                (None, Some(nk)) => {
                    // Newly matches index
                    index.tree().insert(nk, &[]).map_err(IndexError::from)?;
                }
                (None, None) => {}
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use ankurah_core::{
        indexing::{IndexKeyPart, KeySpec},
        value::{Value, ValueType},
    };

    fn entity_order(index: &Index) -> anyhow::Result<Vec<EntityId>> {
        index
            .tree()
            .iter()
            .map(|entry| {
                let (key, _) = entry?;
                let id_offset = key.len().checked_sub(32).ok_or_else(|| anyhow::anyhow!("index key is shorter than an entity id"))?;
                let bytes: [u8; 32] = key[id_offset..].try_into()?;
                Ok(EntityId::from_bytes(bytes))
            })
            .collect()
    }

    #[test]
    fn reopened_database_rebuilds_legacy_string_collated_numeric_index() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let price_two = EntityId::from_bytes([2; 32]);
        let price_ten = EntityId::from_bytes([10; 32]);
        let legacy_index_id;

        {
            let db = sled::open(directory.path())?;
            let database = Database::open(db.clone())?;
            let price_property = database.property_manager.get_property_id("price")?;
            let products = db.open_tree("collection_product")?;
            products.insert(price_two.to_bytes(), bincode::serialize(&vec![(price_property, Value::I64(2))])?)?;
            products.insert(price_ten.to_bytes(), bincode::serialize(&vec![(price_property, Value::I64(10))])?)?;

            // This is the pre-epoch persisted shape: numeric payloads were
            // indexed using String collation, yielding "10" before "2".
            let legacy_spec = KeySpec::new(vec![IndexKeyPart::asc("price", ValueType::String)]);
            let (legacy_index, _) =
                database.index_manager.assure_index_exists("product", &legacy_spec, &database.db, &database.property_manager)?;
            legacy_index_id = legacy_index.id();
            assert_eq!(entity_order(&legacy_index)?, vec![price_ten, price_two]);
            db.flush()?;
        }

        {
            let db = sled::open(directory.path())?;
            let database = Database::open(db)?;
            assert!(database.index_manager.indexes.read().unwrap().contains_key(&legacy_index_id));

            let canonical_spec = KeySpec::new(vec![IndexKeyPart::asc("price", ValueType::I64)]);
            let (canonical_index, _) =
                database.index_manager.assure_index_exists("product", &canonical_spec, &database.db, &database.property_manager)?;

            assert_ne!(canonical_index.id(), legacy_index_id, "the legacy encoder must not be reused");
            assert_eq!(canonical_index.spec(), &canonical_spec);
            assert_eq!(entity_order(&canonical_index)?, vec![price_two, price_ten]);
        }

        Ok(())
    }
}
