use crate::error::sled_error;
use ankurah_core::error::RetrievalError;
use ankurah_core::property::{PropertyKey, PropertyResolver};
use ankurah_core::storage::naming;
use ankurah_proto::EntityId;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tracing::warn;

#[derive(Clone)]
pub struct PropertyManager(Arc<Inner>);

struct Inner {
    property_config_tree: sled::Tree,
    next_property_id: AtomicU32,
    /// The engine-owned durable id-to-column map: key =
    /// `{collection}\0{property id bytes}`, value = the assigned column name
    /// (which then feeds the name-to-slot `property_config_tree` exactly like
    /// a system field's name does). Dedup scope is per collection, the
    /// ratified naming rule.
    property_columns_tree: sled::Tree,
    /// Serializes column ASSIGNMENT (first sight of a property id) so two
    /// concurrent writers cannot both claim one column name for different
    /// ids -- sled has per-key CAS but no cross-key uniqueness constraint.
    /// Assignment is rare (once per property per database); reads skip this.
    assign_lock: Mutex<()>,
}

/// `ProjectedEntity::value` special-cases `id`; a property column may not
/// shadow it.
const RESERVED_COLUMNS: [&str; 1] = ["id"];

fn column_map_key(collection: &str, id: &EntityId) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + 1 + 32);
    key.extend_from_slice(collection.as_bytes());
    key.push(0);
    key.extend_from_slice(&id.to_bytes());
    key
}

impl PropertyManager {
    pub fn open(property_config_tree: sled::Tree, property_columns_tree: sled::Tree) -> anyhow::Result<Self> {
        // Scan property_config_tree to find max property ID
        let mut max_property_id = 0u32;
        for (_key, value_bytes) in property_config_tree.iter().flatten() {
            if value_bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&value_bytes);
                let id = u32::from_be_bytes(arr);
                max_property_id = max_property_id.max(id);
            }
        }

        Ok(Self(Arc::new(Inner {
            property_config_tree,
            next_property_id: AtomicU32::new(max_property_id + 1),
            property_columns_tree,
            assign_lock: Mutex::new(()),
        })))
    }

    /// The materialized column name for a property key in `collection`.
    ///
    /// `Name` keys (system/catalog collections, legacy residue) use the name
    /// directly -- which also collapses a legacy `Name("title")` residue and
    /// its re-registered `Id` successor into one column, the
    /// materialized-layer form of the legacy fallback. `Id` keys resolve
    /// through the durable per-collection map; a miss assigns a column NOW:
    /// seed from the catalog resolver (sanitized), dedupe
    /// `{name}_{trailing id chars}` against this collection's other
    /// assignments, or fall back to a synthetic `p_{trailing id chars}` when
    /// the resolver cannot name the id (logged loudly; should effectively
    /// never fire).
    pub fn column_for_key(
        &self,
        collection: &str,
        key: &PropertyKey,
        resolver: Option<&dyn PropertyResolver>,
    ) -> Result<String, RetrievalError> {
        let id = match key {
            PropertyKey::Name(name) => return Ok(name.clone()),
            PropertyKey::Id(id) => *id,
        };
        let map_key = column_map_key(collection, &id);
        if let Some(ivec) = self.0.property_columns_tree.get(&map_key).map_err(sled_error)? {
            return Ok(String::from_utf8(ivec.to_vec()).map_err(|e| RetrievalError::Other(format!("corrupt column map entry: {e}")))?);
        }

        // Assignment path, serialized so the taken-set check and the insert
        // are atomic across property ids.
        let _guard = self.0.assign_lock.lock().unwrap();
        // Re-check under the lock (another thread may have assigned).
        if let Some(ivec) = self.0.property_columns_tree.get(&map_key).map_err(sled_error)? {
            return Ok(String::from_utf8(ivec.to_vec()).map_err(|e| RetrievalError::Other(format!("corrupt column map entry: {e}")))?);
        }

        // This collection's already-assigned column names (the taken set).
        let mut prefix = collection.as_bytes().to_vec();
        prefix.push(0);
        let mut assigned = std::collections::HashSet::new();
        for entry in self.0.property_columns_tree.scan_prefix(&prefix) {
            let (_, value) = entry.map_err(sled_error)?;
            if let Ok(name) = String::from_utf8(value.to_vec()) {
                assigned.insert(name);
            }
        }
        let is_taken = |candidate: &str| RESERVED_COLUMNS.contains(&candidate) || assigned.contains(candidate);

        let column = match resolver.and_then(|r| r.name_for(&id)).map(|name| naming::sanitize(&name)) {
            Some(seed) => naming::dedupe(&seed, &id, is_taken),
            None => {
                warn!(
                    "sled: catalog cannot name property {} in '{collection}'; assigning fallback column (descriptor race?)",
                    id.to_base64()
                );
                naming::fallback("p", &id, is_taken)
            }
        };
        self.0.property_columns_tree.insert(&map_key, column.as_bytes()).map_err(sled_error)?;
        Ok(column)
    }

    /// The column assigned to `id` in `collection`, if any (read-only map
    /// lookup: never assigns). The read path uses this to translate resolved
    /// property ids into column space; a miss means this engine never
    /// materialized the property.
    pub fn column_of(&self, collection: &str, id: &EntityId) -> Option<String> {
        let ivec = self.0.property_columns_tree.get(column_map_key(collection, id)).ok()??;
        String::from_utf8(ivec.to_vec()).ok()
    }

    /// Gets or creates a property ID for the given property name.
    /// Uses compare_and_swap for atomic insert-if-absent to avoid race conditions.
    pub fn get_property_id(&self, name: &str) -> Result<u32, RetrievalError> {
        // Fast path: check if already exists
        if let Some(ivec) = self.0.property_config_tree.get(name.as_bytes()).map_err(sled_error)? {
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&ivec);
            return Ok(u32::from_be_bytes(arr));
        }

        // Slow path: try to insert atomically
        loop {
            let new_id = self.0.next_property_id.fetch_add(1, Ordering::Relaxed);
            let new_id_bytes = new_id.to_be_bytes();

            // Atomically insert only if key doesn't exist (expected = None)
            match self.0.property_config_tree.compare_and_swap(name.as_bytes(), None::<&[u8]>, Some(&new_id_bytes[..])) {
                Ok(Ok(())) => {
                    // Successfully inserted our new ID
                    return Ok(new_id);
                }
                Ok(Err(cas_error)) => {
                    // Another thread inserted first - use their value
                    let existing = cas_error.current.expect("CAS failed but no current value");
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&existing);
                    return Ok(u32::from_be_bytes(arr));
                }
                Err(e) => {
                    return Err(sled_error(e));
                }
            }
        }
    }

    /// Gets the property name for the given property ID
    pub fn get_property_name(&self, id: u32) -> Option<String> {
        // Scan the property_config_tree to find the name for this ID
        for (key_bytes, value_bytes) in self.0.property_config_tree.iter().flatten() {
            if value_bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&value_bytes);
                let property_id = u32::from_be_bytes(arr);
                if property_id == id {
                    return String::from_utf8(key_bytes.to_vec()).ok();
                }
            }
        }
        None
    }
}
