use ankurah_core::error::RetrievalError;
use ankurah_proto::EntityId;
use ankurah_storage_common::IndexSpecMatch;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::HashMap;
// use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};

use crate::{error::IndexError, planner_integration::encode_tuple_for_sled, property::PropertyManager};

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
    pub spec: ankurah_storage_common::IndexSpec,
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
    pub spec: ankurah_storage_common::IndexSpec,
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
        spec: &ankurah_storage_common::IndexSpec,
        db: &Db,
        property_manager: &PropertyManager,
    ) -> Result<(Index, IndexSpecMatch), RetrievalError> {
        // Try existing matching index
        if let Some((_, existing, match_type)) = self.indexes.read().unwrap().iter().find_map(|(id, idx)| {
            if idx.collection() == collection {
                if let Some(match_result) = spec.matches(idx.spec()) {
                    return Some((id.clone(), idx.clone(), match_result));
                }
            }
            None
        }) {
            existing.build_if_needed(db)?;
            return Ok((existing, match_type));
        }

        // Create new index
        let index = {
            let id = self.next_index_id()?;
            let mut w = self.indexes.write().unwrap();
            let index =
                Index::new_from_spec(collection, spec.clone(), db, id, self.index_config_tree.clone(), property_manager.clone()).unwrap();
            w.insert(id, index.clone());
            index
        };

        index.build_if_needed(db)?;
        Ok((index, ankurah_storage_common::index_spec::IndexSpecMatch::Match))
    }
}

impl Index {
    pub fn tree(&self) -> &sled::Tree { &self.0.tree }
    pub fn id(&self) -> u32 { self.0.id }
    pub fn collection(&self) -> &str { &self.0.collection }
    pub fn name(&self) -> &str { &self.0.name }
    pub fn spec(&self) -> &ankurah_storage_common::IndexSpec { &self.0.spec }
    pub fn created_at_unix_ms(&self) -> i64 { self.0.created_at_unix_ms }
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
        spec: ankurah_storage_common::IndexSpec,
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

        // Resolve pids using PropertyManager
        let mut pids: Vec<u32> = Vec::with_capacity(self.0.spec.keyparts.len());
        for kp in &self.0.spec.keyparts {
            match self.0.property_manager.get_property_id(&kp.column) {
                Ok(id) => pids.push(id),
                Err(_e) => return Err(IndexError::PropertyNotFound(kp.column.clone())),
            }
        }

        for item in coll_tree.iter() {
            let (k, v) = item?;
            let eid = EntityId::from_bytes(k.as_ref().try_into().map_err(|_| IndexError::InvalidKeyLength)?);
            let mat: Vec<(u32, ankurah_core::property::PropertyValue)> = bincode::deserialize(&v)?;
            let map: std::collections::BTreeMap<_, _> = mat.into_iter().collect();

            let tuple_parts: Vec<_> = pids.iter().filter_map(|pid| map.get(pid).cloned()).collect();
            if tuple_parts.len() == pids.len() {
                let mut key = encode_tuple_for_sled(&tuple_parts);
                key.push(0);
                key.extend_from_slice(&eid.to_bytes());
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
        self.0.index_config_tree.insert(&self.0.id.to_be_bytes(), bytes)?;
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
