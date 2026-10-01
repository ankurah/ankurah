use ankurah_core::error::RetrievalError;
use ankurah_core::indexing::IndexSpecMatch;
use ankurah_proto::{EntityId, ModelId};
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::HashMap;
// use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};

use crate::{error::IndexError, planner_integration::encode_tuple_values_with_key_spec, property::slot_from_planner_column};

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum BuildStatus {
    NotBuilt,
    Building,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexRecord {
    pub id: u32,
    pub name: String,
    pub spec: ankurah_core::indexing::KeySpec<String>,
    pub created_at_unix_ms: i64,
    pub build_status: BuildStatus,
}

#[derive(Clone)]
pub struct Index(Arc<IndexInner>);

// Do not expose IndexInner via Deref to avoid leaking private type
struct IndexInner {
    pub id: u32,
    pub name: String,
    pub spec: ankurah_core::indexing::KeySpec<String>,
    pub created_at_unix_ms: i64,
    build_status: Mutex<BuildStatus>,
    pub build_lock: Mutex<()>,
    pub tree: Tree,
    pub index_config_tree: Tree,
}

pub struct IndexManager {
    pub index_config_tree: Tree,
    pub indexes: RwLock<HashMap<u32, Index>>,
    /// Serializes index creation/backfill with atomic entity/materialization
    /// commits. Sled databases are single-process; this closes the only window
    /// in which a newly published index could miss a concurrent projection.
    pub(crate) mutation_lock: Mutex<()>,
}

impl IndexManager {
    pub fn open(index_config_tree: Tree, db: &Db) -> Result<Self, IndexError> {
        let mut indexes = HashMap::new();
        for item in index_config_tree.iter() {
            let (key, bytes) = item?;
            let key = u32::from_be_bytes(key.as_ref().try_into().map_err(|_| IndexError::InvalidKeyLength)?);
            if let Ok(mut rec) = bincode::deserialize::<IndexRecord>(&bytes) {
                // Trust key as source of truth for id
                rec.id = key;
                indexes.insert(key, Index::from_record(rec, db, index_config_tree.clone())?);
            }
        }
        Ok(Self { index_config_tree, indexes: RwLock::new(indexes), mutation_lock: Mutex::new(()) })
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
        spec: &ankurah_core::indexing::KeySpec<String>,
        db: &Db,
    ) -> Result<(Index, IndexSpecMatch), RetrievalError> {
        let _mutation_guard = self.mutation_lock.lock().unwrap();
        // Try existing matching index
        if let Some((_, existing, match_type)) = self.indexes.read().unwrap().iter().find_map(|(id, idx)| {
            if let Some(match_result) = spec.matches(idx.spec()) {
                // A shared index may omit entities missing a trailing property.
                // Only total suffix keys are safe when reusing a shorter prefix.
                if idx.spec().keyparts[spec.keyparts.len()..].iter().any(|part| {
                    part.key != "id" && part.key != crate::materialization::MATERIALIZATION_COLUMN
                }) { return None; }
                return Some((*id, idx.clone(), match_result));
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
            let index = Index::new_from_spec(spec.clone(), db, id, self.index_config_tree.clone())?;
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
    pub fn name(&self) -> &str { &self.0.name }
    pub fn spec(&self) -> &ankurah_core::indexing::KeySpec<String> { &self.0.spec }
    pub fn created_at_unix_ms(&self) -> i64 { self.0.created_at_unix_ms }
    /// Build the index key for an entity given a materialized property map.
    /// Returns Ok(None) if any required key part is missing and the entity should not be indexed.
    pub fn build_key(
        &self,
        eid: &EntityId,
        materialization: &ModelId,
        properties: &[(u32, ankurah_core::value::Value)],
    ) -> Result<Option<Vec<u8>>, IndexError> {
        use std::collections::BTreeMap;
        let map: BTreeMap<_, _> = properties.iter().cloned().collect();
        self.build_key_from_map(eid, materialization, &map)
    }

    /// Internal helper to build index key from a property map.
    /// Shared between build_key and backfill to avoid duplication.
    fn build_key_from_map(
        &self,
        eid: &EntityId,
        materialization: &ModelId,
        property_map: &std::collections::BTreeMap<u32, ankurah_core::value::Value>,
    ) -> Result<Option<Vec<u8>>, IndexError> {
        // The planner's string key is only a transient encoding of sled's
        // numeric property slot. Decode it directly; there is deliberately no
        // per-model PropertyId-to-column-name map in this engine.
        // Build composite key using per-keypart direction from spec
        let mut tuple_values: Vec<ankurah_core::value::Value> = Vec::with_capacity(self.0.spec.keyparts.len());
        for kp in &self.0.spec.keyparts {
            if kp.key == crate::materialization::MATERIALIZATION_COLUMN {
                tuple_values.push(ankurah_core::value::Value::String(materialization.to_string()));
                continue;
            }
            if kp.key == "id" {
                tuple_values.push(ankurah_core::value::Value::EntityId(*eid));
                continue;
            }
            let pid = slot_from_planner_column(&kp.key).ok_or_else(|| IndexError::PropertyNotFound(kp.key.clone()))?;
            if let Some(val) = property_map.get(&pid).cloned() {
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
    pub fn from_record(rec: IndexRecord, db: &Db, index_config_tree: Tree) -> Result<Self, IndexError> {
        Ok(Self(Arc::new(IndexInner {
            id: rec.id,
            name: rec.name,
            spec: rec.spec,
            created_at_unix_ms: rec.created_at_unix_ms,
            build_status: Mutex::new(rec.build_status),
            build_lock: Mutex::new(()),
            tree: db.open_tree(format!("index_{}", rec.id))?,
            index_config_tree,
        })))
    }

    pub fn new_from_spec(
        spec: ankurah_core::indexing::KeySpec<String>,
        db: &Db,
        id: u32,
        index_config_tree: Tree,
    ) -> Result<Self, IndexError> {
        Ok(Self(Arc::new(IndexInner {
            id,
            name: spec.name_with("", "__"),
            spec,
            created_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            build_status: Mutex::new(BuildStatus::NotBuilt),
            build_lock: Mutex::new(()),
            tree: db.open_tree(format!("index_{}", id))?,
            index_config_tree,
        })))
    }

    pub fn backfill(&self, db: &Db) -> Result<(), IndexError> {
        for name in db.tree_names() {
            let Some(model) = crate::database::model_from_tree_name(&name) else { continue };
            let tree = db.open_tree(name)?;
            for item in tree.iter() {
                let (k, v) = item?;
                let eid = EntityId::from_bytes(k.as_ref().try_into().map_err(|_| IndexError::InvalidKeyLength)?);
                let mat: Vec<(u32, ankurah_core::value::Value)> = bincode::deserialize(&v)?;
                let map: std::collections::BTreeMap<_, _> = mat.into_iter().collect();

                // Use shared key building logic
                if let Some(key) = self.build_key_from_map(&eid, &model, &map)? {
                    self.0.tree.insert(key, &[])?;
                }
            }
        }
        Ok(())
    }

    pub fn status(&self) -> BuildStatus { *self.0.build_status.lock().unwrap() }
    pub fn set_status(&self, status: BuildStatus) { *self.0.build_status.lock().unwrap() = status; }

    pub fn persist_snapshot(&self) -> Result<(), IndexError> {
        let bytes = bincode::serialize(&IndexRecord {
            id: self.0.id,
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
