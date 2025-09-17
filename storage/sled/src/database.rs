use sled::Db;

use ankurah_core::error::RetrievalError;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::sled_error;
use crate::index::{Index, IndexManager, IndexRecord};
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
        // Open trees
        let entities_tree = db.open_tree("entities")?; // the actual entities are stored here
        let events_tree = db.open_tree("events")?; // the events are stored here
        let property_config_tree = db.open_tree("property_config")?; // the property config is stored here
        let index_config_tree = db.open_tree("index_config")?; // the index config is stored here

        let property_manager = PropertyManager::open(property_config_tree)?;
        let index_manager = IndexManager::open(index_config_tree, &db, property_manager.clone())?;

        Ok(Self { db, entities_tree, events_tree, property_manager, index_manager })
    }

    /// Convenience method for tests - delegates to underlying sled db
    pub fn tree_names(&self) -> Vec<sled::IVec> { self.db.tree_names() }

    /// Convenience method for tests - delegates to underlying sled db
    pub fn open_tree<V: AsRef<[u8]>>(&self, name: V) -> Result<sled::Tree, sled::Error> { self.db.open_tree(name) }
}
