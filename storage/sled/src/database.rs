use ankurah_core::{error::RetrievalError, storage::StoreFormatError};
use ankurah_proto::{EntityId, ModelId, SystemModel};
use sled::Db;
use std::collections::BTreeSet;

use crate::index::IndexManager;
use crate::property::PropertyManager;

/// The sled tree holding engine-level metadata (not a user collection, not one
/// of the durable maps). Excluded from the "existing data" check below.
pub(crate) const META_TREE: &str = "_ankurah_meta";
/// The format of the stores this engine writes, recorded in each store and checked on every open.
/// Bump it when this engine's stored layout, or the encoding of anything it stores, changes
/// incompatibly.
pub const STORE_FORMAT_VERSION: u32 = 1;
/// The `_ankurah_meta` key under which a store's format version is recorded.
const STORE_FORMAT_VERSION_KEY: &str = "store_format_version";
/// sled's built-in default tree, present on every db including a fresh one, so
/// it never counts as pre-existing ankurah data.
const SLED_DEFAULT_TREE: &[u8] = b"__sled__default";
const ENTITY_MODEL_TREE_PREFIX: &str = "modelid-";
const SYSTEM_MODEL_TREE_PREFIX: &str = "system-";

/// Sled tree names are arbitrary bytes, so a model's physical materialization
/// address can be its durable identity directly. Unlike SQL identifiers, this
/// requires neither a human label nor a durable assigned-name registry.
pub(crate) fn model_tree_name(model: &ModelId) -> String {
    match model {
        ModelId::EntityId(id) => format!("{ENTITY_MODEL_TREE_PREFIX}{}", id.to_base64()),
        ModelId::System(system) => format!("{SYSTEM_MODEL_TREE_PREFIX}{system}"),
    }
}

pub(crate) fn model_from_tree_name(name: &[u8]) -> Option<ModelId> {
    let name = std::str::from_utf8(name).ok()?;
    if let Some(encoded) = name.strip_prefix(ENTITY_MODEL_TREE_PREFIX) {
        return EntityId::from_base64(encoded).ok().map(ModelId::EntityId);
    }
    let system = match name.strip_prefix(SYSTEM_MODEL_TREE_PREFIX)? {
        "model" => SystemModel::Model,
        "property" => SystemModel::Property,
        "model-property" => SystemModel::ModelProperty,
        _ => return None,
    };
    Some(ModelId::System(system))
}

pub struct Database {
    pub db: Db,
    pub(crate) entities_tree: sled::Tree,
    pub(crate) events_tree: sled::Tree,
    /// Engine-private durable entity-to-model association set.
    pub(crate) entity_models_tree: sled::Tree,
    pub property_manager: PropertyManager,
    pub index_manager: IndexManager,
}

impl Database {
    pub fn open(db: Db) -> anyhow::Result<Self> {
        // Gate on the store format version BEFORE opening any data tree, so
        // the pre-existing tree set still reflects what a prior session
        // persisted (opening a data tree here would create it and defeat the
        // "existing data" check).
        check_store_format(&db)?;

        // Open trees. Engine-owned durable registries carry the `_ankurah_`
        // prefix; sled separately namespaces materialized model trees under
        // their identity-derived `modelid-` / `system-` prefixes.
        let entities_tree = db.open_tree("entities")?; // the actual entities are stored here
        let events_tree = db.open_tree("events")?; // the events are stored here
        let entity_models_tree = db.open_tree("_ankurah_sled_entity_models")?;
        // Durable bijection: PropertyId <-> compact u32 materialization slot.
        let property_slots_tree = db.open_tree("_ankurah_sled_property_map")?;
        let index_config_tree = db.open_tree("index_config")?; // the index config is stored here

        let property_manager = PropertyManager::open(property_slots_tree)?;
        let index_manager = IndexManager::open(index_config_tree, &db)?;

        Ok(Self { db, entities_tree, events_tree, entity_models_tree, property_manager, index_manager })
    }

    /// Read an entity's memberships without loading its state buffers.
    pub(crate) fn memberships(&self, id: EntityId) -> Result<BTreeSet<ModelId>, RetrievalError> {
        let bytes = self
            .entity_models_tree
            .get(id.to_bytes())
            .map_err(crate::error::sled_error)?
            .ok_or_else(|| RetrievalError::storage(format!("missing membership record for entity {id}")))?;
        Ok(bincode::deserialize(&bytes)?)
    }

    /// Open an existing materialization without creating a tree for an unknown model.
    pub(crate) fn materialization(&self, model: &ModelId) -> Result<Option<sled::Tree>, RetrievalError> {
        let name = model_tree_name(model);
        if !self.db.tree_names().iter().any(|existing| existing.as_ref() == name.as_bytes()) {
            return Ok(None);
        }
        self.db.open_tree(name).map(Some).map_err(crate::error::sled_error)
    }

    /// Convenience method for tests - delegates to underlying sled db
    pub fn tree_names(&self) -> Vec<sled::IVec> { self.db.tree_names() }

    /// Convenience method for tests - delegates to underlying sled db
    pub fn open_tree<V: AsRef<[u8]>>(&self, name: V) -> Result<sled::Tree, sled::Error> { self.db.open_tree(name) }
}

/// Record the store format version on first initialization and refuse to
/// open a store of another format.
///
/// - no record and no existing ankurah data: write the record and proceed;
/// - matching record: proceed;
/// - different or unreadable record: refuse;
/// - existing ankurah data but no record: refuse as a store predating format records.
///
/// "Existing ankurah data" is any tree other than `_ankurah_meta` and sled's
/// default tree, or any key already in the default tree.
fn check_store_format(db: &Db) -> anyhow::Result<()> {
    let meta = db.open_tree(META_TREE)?;
    let expected = STORE_FORMAT_VERSION;

    match meta.get(STORE_FORMAT_VERSION_KEY)? {
        Some(bytes) => {
            let found = <[u8; 4]>::try_from(bytes.as_ref())
                .map(u32::from_be_bytes)
                .map_err(|_| StoreFormatError::UnreadableRecord { record: format!("{:?}", bytes.as_ref()), expected })?;
            if found != expected {
                return Err(StoreFormatError::Mismatch { found, expected }.into());
            }
        }
        None => {
            if store_has_data(db) {
                return Err(StoreFormatError::Unversioned { expected }.into());
            }
            meta.insert(STORE_FORMAT_VERSION_KEY, &expected.to_be_bytes())?;
        }
    }
    Ok(())
}

/// Whether the db already holds ankurah data: any tree beyond
/// `_ankurah_meta` and sled's default tree, or any key in the default tree.
fn store_has_data(db: &Db) -> bool {
    let has_named_tree =
        db.tree_names().into_iter().any(|name| name.as_ref() != SLED_DEFAULT_TREE && name.as_ref() != META_TREE.as_bytes());
    has_named_tree || !db.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_db() -> Db { sled::Config::new().temporary(true).flush_every_ms(None).open().unwrap() }

    /// The refusal `db` meets on open, as the typed store format error.
    fn refusal(db: Db) -> StoreFormatError {
        let error = Database::open(db).err().expect("the store is refused");
        error.downcast_ref::<StoreFormatError>().cloned().unwrap_or_else(|| panic!("not a store format refusal: {error}"))
    }

    #[test]
    fn a_fresh_store_records_its_format_version_and_reopens() {
        let db = fresh_db();
        // First open records the current version.
        let database = Database::open(db.clone()).unwrap();
        let recorded = db.open_tree(META_TREE).unwrap().get(STORE_FORMAT_VERSION_KEY).unwrap().expect("version recorded on init");
        assert_eq!(recorded.as_ref(), STORE_FORMAT_VERSION.to_be_bytes());
        drop(database);
        // Reopen: record present and equal, so it opens again.
        Database::open(db).unwrap();
    }

    #[test]
    fn a_store_with_data_but_no_format_version_is_refused() {
        let db = fresh_db();
        // A data tree with a key, but no meta record: a store predating format records.
        db.open_tree("entities").unwrap().insert(b"k", b"v").unwrap();
        assert_eq!(refusal(db), StoreFormatError::Unversioned { expected: STORE_FORMAT_VERSION });
    }

    #[test]
    fn a_store_of_another_format_is_refused_naming_both_versions() {
        let db = fresh_db();
        db.open_tree(META_TREE).unwrap().insert(STORE_FORMAT_VERSION_KEY, &(STORE_FORMAT_VERSION + 1).to_be_bytes()).unwrap();
        assert_eq!(refusal(db), StoreFormatError::Mismatch { found: STORE_FORMAT_VERSION + 1, expected: STORE_FORMAT_VERSION });

        let db = fresh_db();
        db.open_tree(META_TREE).unwrap().insert(STORE_FORMAT_VERSION_KEY, &[1, 2]).unwrap();
        assert_eq!(refusal(db), StoreFormatError::UnreadableRecord { record: "[1, 2]".into(), expected: STORE_FORMAT_VERSION });
    }
}
