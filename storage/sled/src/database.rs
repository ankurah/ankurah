use sled::Db;

use crate::index::IndexManager;
use crate::property::PropertyManager;

pub struct Database {
    pub db: Db,
    pub(crate) entities_tree: sled::Tree,
    pub(crate) events_tree: sled::Tree,
    pub property_manager: PropertyManager,
    pub index_manager: IndexManager,
}

impl Database {
    pub fn open(db: Db) -> anyhow::Result<Self> {
        // Open trees. The two engine-owned durable maps carry the `_ankurah_`
        // reserved prefix (matching sqlite's `_ankurah_sqlite_column_map`); sled
        // already namespaces every user collection under `collection_`, so the
        // prefix is belt-and-suspenders against a user-collection name clash.
        let entities_tree = db.open_tree("entities")?; // the actual entities are stored here
        let events_tree = db.open_tree("events")?; // the events are stored here
                                                   // numeric-slot map: serialized PropertyId -> u32 slot
        let property_config_tree = db.open_tree("_ankurah_sled_property_config")?;
        // durable identity map: {collection}\0{serialized PropertyId} -> column name
        let property_columns_tree = db.open_tree("_ankurah_sled_column_map")?;
        let index_config_tree = db.open_tree("index_config")?; // the index config is stored here

        let property_manager = PropertyManager::open(property_config_tree, property_columns_tree)?;
        let index_manager = IndexManager::open(index_config_tree, &db, property_manager.clone())?;

        Ok(Self { db, entities_tree, events_tree, property_manager, index_manager })
    }

    /// Convenience method for tests - delegates to underlying sled db
    pub fn tree_names(&self) -> Vec<sled::IVec> { self.db.tree_names() }

    /// Convenience method for tests - delegates to underlying sled db
    pub fn open_tree<V: AsRef<[u8]>>(&self, name: V) -> Result<sled::Tree, sled::Error> { self.db.open_tree(name) }
}
