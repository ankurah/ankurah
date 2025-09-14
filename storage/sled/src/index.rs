use ankurah_core::error::RetrievalError;
use ankurah_proto::EntityId;
use ankurah_storage_common::IndexSpecMatch;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};

use crate::{database::Database, error::IndexError, planner_integration::encode_tuple_for_sled, property::PropertyManager};

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum BuildStatus {
    NotBuilt,
    Building,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexRecord {
    pub collection: String,
    pub name: String,
    pub spec: ankurah_storage_common::IndexSpec,
    pub created_at_unix_ms: i64,
    pub build_status: BuildStatus,
}

#[derive(Clone)]
pub struct Index(Arc<IndexInner>);

impl Deref for Index {
    type Target = IndexInner;
    fn deref(&self) -> &Self::Target { &self.0 }
}
struct IndexInner {
    pub collection: String,
    pub name: String,
    pub spec: ankurah_storage_common::IndexSpec,
    pub created_at_unix_ms: i64,
    build_status: Mutex<BuildStatus>,
    pub build_lock: Mutex<()>,
    pub tree: Tree,
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
            if let Ok(rec) = bincode::deserialize::<IndexRecord>(&bytes) {
                indexes.insert(key, Index::from_record(rec, db, property_manager.clone())?);
            }
        }
        Ok(Self { index_config_tree, indexes: RwLock::new(indexes) })
    }

    pub fn next_index_id(&self) -> Result<u32, IndexError> {
        Ok(self
            .index_config_tree
            .last()?
            .map(|(k, _)| u32::from_be_bytes(k.as_ref().try_into().map_err(|_| IndexError::InvalidKeyLength)?) + 1)
            .unwrap_or(0))
    }

    pub fn assure_index_exists(
        &self,
        collection: &str,
        spec: &ankurah_storage_common::IndexSpec,
        db: &Db,
        property_manager: &PropertyManager,
    ) -> Result<(Index, IndexSpecMatch), RetrievalError> {
        // Try existing matching index
        if let Some((id, existing, match_type)) = self.indexes.read().unwrap().iter().find_map(|(id, idx)| {
            if idx.collection == collection {
                if let Some(match_result) = spec.matches(&idx.spec) {
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
            let index = Index::new_from_spec(collection, spec.clone(), db, id, property_manager.clone()).unwrap();
            w.insert(id, index.clone());
            index
        };

        index.build_if_needed(db)?;
        Ok((index, ankurah_storage_common::index_spec::IndexSpecMatch::Match))
    }
}

impl Index {
    pub fn from_record(rec: IndexRecord, db: &Db, property_manager: PropertyManager) -> Result<Self, IndexError> {
        Ok(Self(Arc::new(IndexInner {
            collection: rec.collection,
            name: rec.name,
            spec: rec.spec,
            created_at_unix_ms: rec.created_at_unix_ms,
            build_status: Mutex::new(rec.build_status),
            build_lock: Mutex::new(()),
            tree: db.open_tree(format!("index_{}", rec.id))?,
            property_manager,
        })))
    }

    pub fn new_from_spec(
        collection: &str,
        spec: ankurah_storage_common::IndexSpec,
        db: &Db,
        id: u32,
        property_manager: PropertyManager,
    ) -> Result<Self, IndexError> {
        Ok(Self(Arc::new(IndexInner {
            collection: collection.to_string(),
            name: spec.name_with("", "__"),
            spec,
            created_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            build_status: Mutex::new(BuildStatus::NotBuilt),
            build_lock: Mutex::new(()),
            tree: db.open_tree(format!("index_{}", id))?,
            property_manager,
        })))
    }

    pub fn backfill(&self, db: &Db) -> Result<(), IndexError> {
        let coll_tree = db.open_tree(format!("collection_{}", &self.collection))?;

        // Resolve pids using PropertyManager
        let pids: Vec<u32> = self.spec.keyparts.iter().map(|kp| self.property_manager.get_property_id(&kp.column)).collect()?;

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
                self.tree.insert(key, &[])?;
            }
        }
        Ok(())
    }

    pub fn status(&self) -> BuildStatus { *self.build_status.lock().unwrap() }
    pub fn set_status(&self, status: BuildStatus) { *self.build_status.lock().unwrap() = status; }

    pub fn persist_snapshot(&self, db: &Dbtr) -> Result<(), IndexError> {
        let bytes = bincode::serialize(&IndexRecord {
            collection: self.collection.clone(),
            name: self.name.clone(),
            spec: self.spec.clone(),
            created_at_unix_ms: self.created_at_unix_ms,
            build_status: self.status(),
        })?;
        db.open_tree("indexes")?.insert(id.as_bytes(), bytes)?;
        Ok(())
    }
    pub fn build_if_needed(&self, db: &Db) -> Result<(), RetrievalError> {
        let _guard = self.build_lock.lock().unwrap();
        if matches!(self.status(), BuildStatus::Ready) {
            return Ok(());
        }

        self.set_status(BuildStatus::Building);
        self.persist_snapshot(db)?;
        self.backfill(db)?;
        self.set_status(BuildStatus::Ready);
        self.persist_snapshot(db)?;
        Ok(())
    }
}
