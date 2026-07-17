use ankurah_proto::PROTOCOL_VERSION;
use sled::Db;

use crate::index::IndexManager;
use crate::property::PropertyManager;

/// The sled tree holding engine-level metadata (not a user collection, not one
/// of the durable maps). Excluded from the "existing data" check below.
const META_TREE: &str = "meta";
/// The `meta` key under which the storage protocol version is stamped.
const PROTOCOL_VERSION_KEY: &str = "protocol_version";
/// sled's built-in default tree, present on every db including a fresh one, so
/// it never counts as pre-existing ankurah data.
const SLED_DEFAULT_TREE: &[u8] = b"__sled__default";

pub struct Database {
    pub db: Db,
    pub(crate) entities_tree: sled::Tree,
    pub(crate) events_tree: sled::Tree,
    pub property_manager: PropertyManager,
    pub index_manager: IndexManager,
}

impl Database {
    pub fn open(db: Db) -> anyhow::Result<Self> {
        // Gate on the storage protocol version BEFORE opening any data tree, so
        // the pre-existing tree set still reflects what a prior session
        // persisted (opening a data tree here would create it and defeat the
        // "existing data" check).
        check_protocol_version(&db)?;

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

/// Stamp the storage protocol version on first initialization and refuse to
/// open a store written by a different version.
///
/// This branch renamed sled's trees, so an older store would otherwise reopen
/// as silently empty; the stamp converts that into a loud refusal. Semantics:
/// - no stamp and no existing ankurah data: write the stamp and proceed;
/// - stamp present and equal to this build's version: proceed;
/// - stamp present and different: refuse, naming both versions;
/// - existing ankurah data but no stamp: refuse as a store predating the stamp.
///
/// "Existing ankurah data" is any tree other than `meta` and sled's default
/// tree, or any key already in the default tree.
fn check_protocol_version(db: &Db) -> anyhow::Result<()> {
    let meta = db.open_tree(META_TREE)?;
    let expected = PROTOCOL_VERSION;

    match meta.get(PROTOCOL_VERSION_KEY)? {
        Some(bytes) => {
            let found = <[u8; 4]>::try_from(bytes.as_ref()).map(u32::from_be_bytes).map_err(|_| {
                anyhow::anyhow!(
                    "sled store protocol version stamp is unreadable ({} bytes); expected version {expected}; reset the development database",
                    bytes.len()
                )
            })?;
            if found != expected {
                anyhow::bail!(
                    "sled store was written with protocol version {found} but this build expects version {expected}; reset the development database"
                );
            }
        }
        None => {
            if store_has_data(db) {
                anyhow::bail!(
                    "sled store predates protocol version stamping (existing data present, no version stamp); this build expects version {expected}; reset the development database"
                );
            }
            meta.insert(PROTOCOL_VERSION_KEY, &expected.to_be_bytes())?;
        }
    }
    Ok(())
}

/// Whether the db already holds ankurah data: any tree beyond `meta` and sled's
/// default tree, or any key in the default tree.
fn store_has_data(db: &Db) -> bool {
    let has_named_tree =
        db.tree_names().into_iter().any(|name| name.as_ref() != SLED_DEFAULT_TREE && name.as_ref() != META_TREE.as_bytes());
    has_named_tree || !db.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_db() -> Db { sled::Config::new().temporary(true).flush_every_ms(None).open().unwrap() }

    #[test]
    fn fresh_store_stamps_protocol_version_and_reopens() {
        let db = fresh_db();
        // First open stamps the current version.
        let database = Database::open(db.clone()).unwrap();
        let stamp = db.open_tree(META_TREE).unwrap().get(PROTOCOL_VERSION_KEY).unwrap().expect("version stamped on init");
        assert_eq!(stamp.as_ref(), PROTOCOL_VERSION.to_be_bytes());
        drop(database);
        // Reopen: stamp present and equal, so it opens again.
        Database::open(db).unwrap();
    }

    #[test]
    fn store_with_data_but_no_stamp_refuses() {
        let db = fresh_db();
        // A data tree with a key, but no meta stamp: a store predating stamping.
        db.open_tree("entities").unwrap().insert(b"k", b"v").unwrap();
        let err = Database::open(db).err().unwrap().to_string();
        assert!(err.contains("reset the development database"), "should refuse loudly: {err}");
    }

    #[test]
    fn store_with_wrong_stamp_refuses() {
        let db = fresh_db();
        db.open_tree(META_TREE).unwrap().insert(PROTOCOL_VERSION_KEY, &(PROTOCOL_VERSION + 1).to_be_bytes()).unwrap();
        let err = Database::open(db).err().unwrap().to_string();
        assert!(err.contains(&PROTOCOL_VERSION.to_string()), "should name the expected version: {err}");
        assert!(err.contains(&(PROTOCOL_VERSION + 1).to_string()), "should name the found version: {err}");
    }
}
