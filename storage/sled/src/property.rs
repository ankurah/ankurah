use crate::error::sled_error;
use ankql::ast::PropertyId;
use ankurah_core::error::RetrievalError;
use ankurah_core::schema::CatalogResolver;
use ankurah_core::storage::naming;
use ankurah_proto::EntityId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tracing::warn;

#[derive(Clone)]
pub struct PropertyManager(Arc<Inner>);

struct Inner {
    /// The numeric-slot map: key = serialized [`PropertyId`], value = the compact
    /// `u32` slot the per-entity materialization is keyed by. Keyed by durable
    /// identity (NOT the physical column), so a system property -- the same
    /// identity in every collection -- shares one slot, and a registered
    /// property's slot is stable across a rename.
    property_config_tree: sled::Tree,
    next_property_id: AtomicU32,
    /// The engine-owned durable identity-to-column map: key =
    /// `{collection}\0{serialized PropertyId}`, value = the assigned column name.
    /// Keyed by durable [`PropertyId`] (NOT `EntityId`, NOT the column) so the
    /// write side (a backend's `property_values()` id) and the read side (a
    /// `PropertyId` off the resolved AST) address the same row. Dedup scope is
    /// per collection, the ratified naming rule.
    property_columns_tree: sled::Tree,
    /// Serializes column ASSIGNMENT (first sight of a property id) so two
    /// concurrent writers cannot both claim one column name for different
    /// ids -- sled has per-key CAS but no cross-key uniqueness constraint.
    /// Assignment is rare (once per property per database); reads skip this.
    assign_lock: Mutex<()>,
}

/// `ProjectedEntity::value` special-cases `id`, and every state table pins the
/// `id` pseudo-property to the reserved `"id"` primary-key column; a property
/// column may not shadow it.
const RESERVED_COLUMNS: [&str; 1] = ["id"];

/// The durable, serialized address of a property: the bytes a [`PropertyId`]
/// serializes to. The write side (the id a backend yields) and the read side (an
/// id straight off the resolved AST) both go through this, so a column/slot
/// assigned on write is found by the byte-identical key on read.
fn property_id_bytes(id: &PropertyId) -> Vec<u8> { bincode::serialize(id).expect("PropertyId always serializes") }

/// The per-collection durable-map key for a property identity:
/// `{collection}\0{serialized PropertyId}`. Collection names are sane (no NUL),
/// so the NUL cleanly separates the collection scope from the identity suffix.
fn column_map_key(collection: &str, id: &PropertyId) -> Vec<u8> {
    let id_bytes = property_id_bytes(id);
    let mut key = Vec::with_capacity(collection.len() + 1 + id_bytes.len());
    key.extend_from_slice(collection.as_bytes());
    key.push(0);
    key.extend_from_slice(&id_bytes);
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

    /// The materialized column name for a property identity in `collection`.
    ///
    /// Both arms route through the durable map (there is no name short-circuit),
    /// so a system property is addressed by identity on reads exactly like a
    /// registered one. A map miss assigns a column NOW, mirroring the sqlite
    /// engine:
    /// - a **system** property (`System { name }`) is a globally-unique catalog
    ///   field name, so its sanitized name is the column directly -- no id-suffix
    ///   dedupe (there is no id, and catalog names do not collide);
    /// - a **registered** property (`EntityId`) seeds from the catalog resolver's
    ///   display name (sanitized), deduped `{name}_{trailing id chars}` against
    ///   this collection's other assignments, or -- when the resolver cannot name
    ///   the id (the intra-node descriptor race; should effectively never fire)
    ///   -- a synthetic `p_{trailing id chars}` name, logged loudly;
    /// - the **`id`** pseudo-property is the primary key, never a stored value, so
    ///   it never reaches column assignment (it is pinned to the `"id"` column in
    ///   [`Self::column_of`]).
    pub fn column_for_key(
        &self,
        collection: &str,
        key: &PropertyId,
        resolver: Option<&dyn CatalogResolver>,
    ) -> Result<String, RetrievalError> {
        let map_key = column_map_key(collection, key);
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

        let column = match key {
            // System property: its sanitized name IS the column. It carries no id
            // to suffix-dedupe with, so a candidate already held by a DIFFERENT
            // identity (or a reserved column) cannot be widened out of the way --
            // it must hard-error, or two distinct identities would silently share
            // one column. Re-presenting the SAME identity is idempotent, not a
            // collision, and the map re-check above already returns in that case.
            PropertyId::System { name } => {
                let candidate = naming::sanitize(name);
                if RESERVED_COLUMNS.contains(&candidate.as_str()) {
                    return Err(RetrievalError::Other(format!(
                        "sled: system property {key:?} sanitizes to reserved column {candidate:?} in collection '{collection}'; rename the property"
                    )));
                }
                let key_bytes = property_id_bytes(key);
                let mut prefix = collection.as_bytes().to_vec();
                prefix.push(0);
                for entry in self.0.property_columns_tree.scan_prefix(&prefix) {
                    let (existing_key, existing_value) = entry.map_err(sled_error)?;
                    if existing_value.as_ref() != candidate.as_bytes() {
                        continue;
                    }
                    let existing_id_bytes = &existing_key[prefix.len()..];
                    if existing_id_bytes != key_bytes.as_slice() {
                        let existing = bincode::deserialize::<PropertyId>(existing_id_bytes)
                            .map(|id| format!("{id:?}"))
                            .unwrap_or_else(|_| "an unreadable identity".to_string());
                        return Err(RetrievalError::Other(format!(
                            "sled: system property {key:?} and {existing} both map to column {candidate:?} in collection '{collection}'; distinct properties may not share a column"
                        )));
                    }
                }
                candidate
            }
            // Registered property: seed from the resolver, dedupe by id.
            PropertyId::EntityId(ulid) => {
                let id = EntityId::from_ulid(*ulid);
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
                match resolver.and_then(|r| r.name_for(&id)).map(|name| naming::sanitize(&name)) {
                    Some(seed) => naming::dedupe(&seed, &id, is_taken).map_err(|e| RetrievalError::Other(e.to_string()))?,
                    None => {
                        warn!(
                            "sled: catalog cannot name property {} in '{collection}'; assigning fallback column (descriptor race?)",
                            id.to_base64()
                        );
                        naming::fallback("p", &id, is_taken).map_err(|e| RetrievalError::Other(e.to_string()))?
                    }
                }
            }
            // The `id` pseudo-property is the primary key, never a stored value.
            PropertyId::Id => {
                return Err(RetrievalError::Other("the id pseudo-property is never materialized as a stored value".to_string()));
            }
        };
        self.0.property_columns_tree.insert(&map_key, column.as_bytes()).map_err(sled_error)?;
        Ok(column)
    }

    /// The column assigned to a property identity in `collection`, if any
    /// (read-only map lookup: never assigns). The read path uses this to
    /// translate resolved property identities into column space. `Ok(None)` is a
    /// true absence -- this engine never materialized the property (there is NO
    /// fallback to a name; the #374 fix) -- and is distinct from `Err`, which is
    /// a real storage or decode failure and must propagate, not read as absent.
    /// The `id` pseudo-property is pinned to the reserved `"id"` primary-key
    /// column, so it is a uniform hit and never wrongly absent.
    pub fn column_of(&self, collection: &str, key: &PropertyId) -> Result<Option<String>, RetrievalError> {
        if key == &PropertyId::Id {
            return Ok(Some("id".to_string()));
        }
        let Some(ivec) = self.0.property_columns_tree.get(column_map_key(collection, key)).map_err(sled_error)? else {
            return Ok(None);
        };
        let column = String::from_utf8(ivec.to_vec()).map_err(|e| RetrievalError::Other(format!("corrupt column map entry: {e}")))?;
        Ok(Some(column))
    }

    /// This collection's `column -> PropertyId` map, built from the durable
    /// id-to-column map. The index layer holds physical columns
    /// (`IndexKeyPart<String>`) but the numeric-slot map is keyed by identity, so
    /// building an index key translates each keypart column back to its identity
    /// through this before looking up the slot.
    pub fn column_to_property_id_map(&self, collection: &str) -> Result<HashMap<String, PropertyId>, sled::Error> {
        let mut prefix = collection.as_bytes().to_vec();
        prefix.push(0);
        let mut map = HashMap::new();
        for entry in self.0.property_columns_tree.scan_prefix(&prefix) {
            let (key, value) = entry?;
            let id_bytes = &key[prefix.len()..];
            let Ok(property_id) = bincode::deserialize::<PropertyId>(id_bytes) else { continue };
            if let Ok(column) = String::from_utf8(value.to_vec()) {
                map.insert(column, property_id);
            }
        }
        Ok(map)
    }

    /// Gets or creates the numeric slot for a property identity.
    /// Uses compare_and_swap for atomic insert-if-absent to avoid race conditions.
    pub fn get_property_id(&self, key: &PropertyId) -> Result<u32, RetrievalError> {
        let key_bytes = property_id_bytes(key);
        // Fast path: check if already exists
        if let Some(ivec) = self.0.property_config_tree.get(&key_bytes).map_err(sled_error)? {
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&ivec);
            return Ok(u32::from_be_bytes(arr));
        }

        // Slow path: try to insert atomically
        loop {
            let new_id = self.0.next_property_id.fetch_add(1, Ordering::Relaxed);
            let new_id_bytes = new_id.to_be_bytes();

            // Atomically insert only if key doesn't exist (expected = None)
            match self.0.property_config_tree.compare_and_swap(&key_bytes, None::<&[u8]>, Some(&new_id_bytes[..])) {
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

    /// The property identity for a numeric slot, if any (reverse of
    /// [`Self::get_property_id`]). The scan streams decode the slot-keyed
    /// materialization back into identity space so the in-memory post-filter and
    /// sort can address values by resolved identity.
    pub fn property_id_for_slot(&self, slot: u32) -> Option<PropertyId> {
        let slot_bytes = slot.to_be_bytes();
        for (key_bytes, value_bytes) in self.0.property_config_tree.iter().flatten() {
            if value_bytes.as_ref() == slot_bytes {
                if let Ok(property_id) = bincode::deserialize::<PropertyId>(&key_bytes) {
                    return Some(property_id);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::PropertyId;
    use ulid::Ulid;

    fn manager() -> PropertyManager {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let config = db.open_tree("property_config").unwrap();
        let columns = db.open_tree("property_columns").unwrap();
        PropertyManager::open(config, columns).unwrap()
    }

    fn eid(b: u8) -> PropertyId {
        let mut x = [0u8; 16];
        x[0] = b;
        PropertyId::EntityId(Ulid::from_bytes(x))
    }

    /// Columns and numeric slots are assigned per durable PropertyId and recovered
    /// by the byte-identical key -- registered (EntityId) and system (System{name})
    /// arms alike -- including the two reverse hops the index/scan layers rely on.
    #[test]
    fn property_id_column_and_slot_roundtrip() {
        let pm = manager();
        let coll = "album";
        let sys = PropertyId::System { name: "title".to_string() };
        let reg = eid(0x11);

        // column_for_key -> column_of. No resolver: a system field sanitizes its
        // own name; a registered id with no catalog name gets a synthetic column.
        let sys_col = pm.column_for_key(coll, &sys, None).unwrap();
        assert_eq!(sys_col, "title");
        let reg_col = pm.column_for_key(coll, &reg, None).unwrap();
        assert_ne!(reg_col, sys_col, "distinct properties get distinct columns");
        assert_eq!(pm.column_of(coll, &sys).unwrap(), Some(sys_col.clone()));
        assert_eq!(pm.column_of(coll, &reg).unwrap(), Some(reg_col.clone()));

        // The id pseudo-property is pinned to "id", never assigned.
        assert_eq!(pm.column_of(coll, &PropertyId::Id).unwrap(), Some("id".to_string()));

        // Reverse: column -> PropertyId (index-build hop).
        let rev = pm.column_to_property_id_map(coll).unwrap();
        assert_eq!(rev.get(&sys_col), Some(&sys));
        assert_eq!(rev.get(&reg_col), Some(&reg));

        // Numeric slot assignment + reverse slot -> PropertyId (scan hop).
        let sys_slot = pm.get_property_id(&sys).unwrap();
        let reg_slot = pm.get_property_id(&reg).unwrap();
        assert_ne!(sys_slot, reg_slot, "distinct properties get distinct slots");
        assert_eq!(pm.get_property_id(&sys).unwrap(), sys_slot, "slot is stable");
        assert_eq!(pm.property_id_for_slot(sys_slot), Some(sys));
        assert_eq!(pm.property_id_for_slot(reg_slot), Some(reg));
    }

    /// Two distinct system identities whose names sanitize to one column must not
    /// silently share it: the second assignment hard-errors and the message names
    /// the column. Re-presenting the same identity is idempotent, and a name that
    /// sanitizes onto the reserved primary-key column also hard-errors.
    #[test]
    fn system_name_column_collision_hard_errors() {
        let pm = manager();
        let coll = "album";
        // "my-field" and "my.field" both sanitize to "my_field".
        let a = PropertyId::System { name: "my-field".to_string() };
        let b = PropertyId::System { name: "my.field".to_string() };
        assert_eq!(pm.column_for_key(coll, &a, None).unwrap(), "my_field");

        let err = pm.column_for_key(coll, &b, None).unwrap_err();
        assert!(err.to_string().contains("my_field"), "message must name the column: {err}");

        // The same identity re-presenting is idempotent, not a collision.
        assert_eq!(pm.column_for_key(coll, &a, None).unwrap(), "my_field");

        // A name that sanitizes onto the reserved "id" column hard-errors.
        let reserved = PropertyId::System { name: "id".to_string() };
        assert!(pm.column_for_key(coll, &reserved, None).is_err());
    }
}
