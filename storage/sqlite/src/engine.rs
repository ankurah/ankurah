//! SQLite storage engine implementation

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use ankql::ast::PropertyId;
use ankurah_core::entity::TemporaryEntity;
use ankurah_core::error::{MutationError, RetrievalError};
use ankurah_core::property::backend::backend_from_string;
use ankurah_core::schema::CatalogResolver;
use ankurah_core::selection::filter::evaluate_predicate;
use ankurah_core::storage::{naming, StorageCollection, StorageEngine};
use ankurah_proto::{
    AttestationSet, Attested, Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State, StateBuffers,
    PROTOCOL_VERSION,
};
use async_trait::async_trait;
use rusqlite::{params_from_iter, Connection};
use tracing::{debug, warn};

use crate::connection::{PooledConnection, SqliteConnectionManager};
use crate::error::SqliteError;
use crate::sql_builder::{split_predicate_for_sqlite, SqlBuilder};
use crate::value::SqliteValue;

/// Default connection pool size
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// Engine-level key/value metadata for the store itself (currently just the
/// 'protocol_version' record). Not a collection: it survives
/// `delete_all_collections`, because wiping the record would make the store
/// read as unversioned and refuse its own reopen.
const META_TABLE: &str = "ankurah_meta";

/// SQLite storage engine
pub struct SqliteStorageEngine {
    pool: bb8::Pool<SqliteConnectionManager>,
    /// The catalog resolver, injected post-construction by `Node` (see
    /// `StorageEngine::set_catalog_resolver`). Shared with every bucket:
    /// the name SOURCE for the engine-owned durable id-to-column map.
    resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
}

impl SqliteStorageEngine {
    /// Create a new storage engine with an existing pool.
    ///
    /// Records or checks the store's protocol version (see
    /// [`Self::check_protocol_version`]) so every construction path verifies
    /// the store it is about to serve.
    pub async fn new(pool: bb8::Pool<SqliteConnectionManager>) -> anyhow::Result<Self> {
        let engine = Self { pool, resolver: Arc::new(std::sync::RwLock::new(None)) };
        engine.check_protocol_version().await?;
        Ok(engine)
    }

    /// Open a file-based SQLite database
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let manager = SqliteConnectionManager::file(path.as_ref());
        let pool = bb8::Pool::builder().max_size(DEFAULT_POOL_SIZE).build(manager).await?;
        Self::new(pool).await
    }

    /// Open an in-memory SQLite database (for testing)
    pub async fn open_in_memory() -> anyhow::Result<Self> {
        let manager = SqliteConnectionManager::memory();
        // For in-memory, we use a single connection to keep the database alive
        let pool = bb8::Pool::builder().max_size(1).build(manager).await?;
        Self::new(pool).await
    }

    /// Check if a collection name is valid
    pub fn sane_name(collection: &str) -> bool {
        for char in collection.chars() {
            match char {
                c if c.is_alphanumeric() => {}
                '_' | '.' | ':' => {}
                _ => return false,
            }
        }
        true
    }

    /// Get a reference to the connection pool (for testing/diagnostics)
    pub fn pool(&self) -> &bb8::Pool<SqliteConnectionManager> { &self.pool }

    /// Record or check the store's protocol version
    /// ([`ankurah_proto::PROTOCOL_VERSION`]), run by every constructor:
    ///
    /// - fresh store (no record, no ankurah tables): write the record, proceed
    /// - record present and equal: proceed
    /// - record present and different: refuse, naming found and expected
    /// - ankurah tables present but no record: refuse as an unversioned store
    ///
    /// Same spirit as the peering handshake's `protocol_compatible` refusal:
    /// serving a store persisted by a different protocol version would
    /// silently misread its shapes, so refuse loudly and advise a
    /// development-database reset.
    async fn check_protocol_version(&self) -> Result<(), SqliteError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;
        conn.with_connection(|c| {
            let mut stmt = c.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
            let tables: Vec<String> = stmt.query_map([], |row| row.get(0))?.filter_map(|r| r.ok()).collect();
            let has_ankurah_tables = tables.iter().any(|t| t != META_TABLE);

            let recorded: Option<String> = if tables.iter().any(|t| t == META_TABLE) {
                match c.query_row(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#), [], |row| row.get(0))
                {
                    Ok(value) => Some(value),
                    Err(rusqlite::Error::QueryReturnedNoRows) => None,
                    Err(e) => return Err(SqliteError::Rusqlite(e)),
                }
            } else {
                None
            };

            let expected = PROTOCOL_VERSION.to_string();
            match recorded {
                Some(found) if found == expected => Ok(()),
                Some(found) => Err(SqliteError::ProtocolVersionMismatch { found, expected: PROTOCOL_VERSION }),
                None if has_ankurah_tables => Err(SqliteError::UnversionedStore { expected: PROTOCOL_VERSION }),
                None => {
                    // Fresh store: claim the record.
                    c.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "{META_TABLE}" ("key" TEXT PRIMARY KEY, "value" TEXT)"#), [])?;
                    c.execute(
                        &format!(r#"INSERT OR IGNORE INTO "{META_TABLE}" ("key", "value") VALUES ('protocol_version', ?)"#),
                        rusqlite::params![expected],
                    )?;
                    // Re-read: if another process recorded between our scan and
                    // our insert, the store must still match this binary.
                    let reread: String =
                        c.query_row(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#), [], |row| {
                            row.get(0)
                        })?;
                    if reread == expected {
                        Ok(())
                    } else {
                        Err(SqliteError::ProtocolVersionMismatch { found: reread, expected: PROTOCOL_VERSION })
                    }
                }
            }
        })
        .await
    }
}

#[async_trait]
impl StorageEngine for SqliteStorageEngine {
    type Value = SqliteValue;

    async fn collection(&self, collection_id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        if !Self::sane_name(collection_id.as_str()) {
            return Err(RetrievalError::InvalidBucketName);
        }
        // The engine's own physical tables are not collections. The reserved
        // "_ankurah_" prefix is enforced above this layer for user models, but
        // "ankurah_meta" carries no underscore and would otherwise pass
        // sane_name and collide with the protocol-version record table.
        if collection_id.as_str() == META_TABLE || collection_id.as_str() == "_ankurah_sqlite_column_map" {
            return Err(RetrievalError::InvalidBucketName);
        }

        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        let bucket = SqliteBucket::new(self.pool.clone(), collection_id.clone(), self.resolver.clone());

        // Create tables if they don't exist
        let collection_id_clone = collection_id.clone();
        // Pin the `id` pseudo-property to the reserved "id" primary-key column
        // for this collection, so a read of `id` is a uniform durable-map hit
        // (no name-as-column special case, never a read miss). "id" is a
        // BASE_COLUMN, so no assigned property can ever collide with it.
        let id_pin_key = property_key_text(&PropertyId::Id);
        conn.with_connection(move |c| {
            create_state_table(c, &collection_id_clone)?;
            create_event_table(c, &collection_id_clone)?;
            create_column_map_table(c)?;
            c.execute(
                r#"INSERT OR IGNORE INTO "_ankurah_sqlite_column_map" ("collection", "property_key", "column_name") VALUES (?, ?, 'id')"#,
                rusqlite::params![collection_id_clone.as_str(), id_pin_key],
            )?;
            Ok(())
        })
        .await?;

        // Rebuild column cache
        bucket.rebuild_columns_cache(&conn).await?;
        // Load this collection's slice of the engine-owned durable id-to-column map.
        bucket.load_column_map(&conn).await?;

        Ok(Arc::new(bucket))
    }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) {
        *self.resolver.write().expect("RwLock poisoned") = Some(resolver);
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;

        conn.with_connection(|c| {
            // Get all table names. The engine-level meta table is not a
            // collection and keeps the protocol version record across a wipe.
            let mut stmt = c.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
            let tables: Vec<String> =
                stmt.query_map([], |row| row.get(0))?.filter_map(|r| r.ok()).filter(|name: &String| name.as_str() != META_TABLE).collect();

            if tables.is_empty() {
                return Ok(false);
            }

            for table in tables {
                c.execute(&format!(r#"DROP TABLE IF EXISTS "{}""#, table), [])?;
            }

            Ok(true)
        })
        .await
        .map_err(|e| MutationError::General(Box::new(e)))
    }

    /// Non-creating collection discovery. The trait default returns nothing,
    /// which would make a durable node warm an empty catalog on restart. A
    /// collection's state table is named exactly its id
    /// (`create_state_table`), paired with an `{id}_event` companion; the
    /// engine-wide `_ankurah_sqlite_column_map`, the engine-level `ankurah_meta`
    /// version table, and sqlite's own tables are the only others. So a table is
    /// a collection iff its `{name}_event`
    /// companion also exists -- which also disambiguates a user collection whose
    /// id ends in `_event` from some other collection's event table. Unlike
    /// `collection`, this creates nothing.
    async fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;
        let collections = conn
            .with_connection(|c| {
                let mut stmt = c.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
                let names: Vec<String> = stmt.query_map([], |row| row.get(0))?.filter_map(|r| r.ok()).collect();
                let table_set: std::collections::HashSet<String> = names.iter().cloned().collect();
                let collections: Vec<CollectionId> = names
                    .into_iter()
                    .filter(|name| name.as_str() != "_ankurah_sqlite_column_map" && name.as_str() != META_TABLE)
                    .filter(|name| table_set.contains(&format!("{name}_event")))
                    .map(CollectionId::from)
                    .collect();
                Ok::<_, SqliteError>(collections)
            })
            .await?;
        Ok(collections)
    }
}

fn create_state_table(conn: &Connection, collection_id: &CollectionId) -> Result<(), SqliteError> {
    let table_name = collection_id.as_str();
    let query = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}"(
            "id" TEXT PRIMARY KEY,
            "state_buffer" BLOB NOT NULL,
            "head" TEXT NOT NULL,
            "attestations" BLOB
        )"#,
        table_name
    );
    debug!("Creating state table: {}", query);
    conn.execute(&query, [])?;
    Ok(())
}

fn create_event_table(conn: &Connection, collection_id: &CollectionId) -> Result<(), SqliteError> {
    let table_name = format!("{}_event", collection_id.as_str());
    let query = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}"(
            "id" TEXT PRIMARY KEY,
            "entity_id" TEXT,
            "operations" BLOB,
            "parent" TEXT,
            "attestations" BLOB
        )"#,
        table_name
    );
    debug!("Creating event table: {}", query);
    conn.execute(&query, [])?;

    // Create index on entity_id for efficient dump_entity_events queries
    let index_query = format!(r#"CREATE INDEX IF NOT EXISTS "{}_entity_id_idx" ON "{}"("entity_id")"#, table_name, table_name);
    conn.execute(&index_query, [])?;

    Ok(())
}

/// The durable, serialized address of a property: the JSON form of its
/// [`PropertyId`], stored as the `property_key` text in `_ankurah_sqlite_column_map`.
/// The write side (the `PropertyId` a backend's `property_values()` yields) and
/// the read side (a `PropertyId` straight off the resolved AST) both go through
/// this, so a column assigned on write is found by the byte-identical key on read.
fn property_key_text(id: &PropertyId) -> String { serde_json::to_string(id).expect("PropertyId always serializes to JSON") }

/// Create the engine-wide durable property-to-column map table. One table for
/// the whole database; rows are scoped by collection (dedup scope is
/// per-collection, the ratified naming rule). `property_key` is a serialized
/// [`PropertyId`] (JSON TEXT, see [`property_key_text`]) -- the same durable
/// address the read side resolves against, so registered AND system properties
/// alike are addressed by identity, never by a raw name. `_ankurah_sqlite_` is
/// the reserved prefix that shields this internal table from user-collection
/// name collisions.
///
/// `CREATE TABLE IF NOT EXISTS` is idempotent, so this needs no DDL lock, exactly
/// like the state/event table creators above.
fn create_column_map_table(conn: &Connection) -> Result<(), SqliteError> {
    let query = r#"CREATE TABLE IF NOT EXISTS "_ankurah_sqlite_column_map"(
            "collection" TEXT NOT NULL,
            "property_key" TEXT NOT NULL,
            "column_name" TEXT NOT NULL,
            PRIMARY KEY ("collection", "property_key"),
            UNIQUE ("collection", "column_name")
        )"#;
    debug!("Creating property column map table: {}", query);
    conn.execute(query, [])?;
    Ok(())
}

/// Column metadata
#[derive(Clone, Debug)]
pub struct SqliteColumn {
    pub name: String,
    #[allow(dead_code)]
    pub data_type: String,
}

/// SQLite storage bucket (collection)
pub struct SqliteBucket {
    pool: bb8::Pool<SqliteConnectionManager>,
    collection_id: CollectionId,
    /// Cached state table name (avoids repeated allocations)
    state_table_name: String,
    /// Cached event table name (avoids repeated allocations)
    event_table_name: String,
    columns: Arc<std::sync::RwLock<Vec<SqliteColumn>>>,
    ddl_lock: Arc<tokio::sync::Mutex<()>>,
    /// The injected catalog resolver (shared with the engine): the NAME SOURCE
    /// for [`Self::column_for_key`]. Weak so storage never keeps the node alive.
    resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    /// This collection's slice of the engine-owned durable property-to-column
    /// map (the `_ankurah_sqlite_column_map` table), cached and keyed by durable
    /// [`PropertyId`]. The map -- not the display name -- is what addresses a
    /// property's column once assigned: renames never move columns, collisions
    /// were deduped at assignment. Always carries the `PropertyId::Id -> "id"`
    /// pin so a read of the primary key is a uniform map hit.
    property_columns: Arc<std::sync::RwLock<BTreeMap<PropertyId, String>>>,
}

/// Fixed columns of every state table: reserved, never assignable to a property.
const BASE_COLUMNS: &[&str] = &["id", "state_buffer", "head", "attestations"];

impl SqliteBucket {
    /// Create a new bucket with cached table names
    fn new(
        pool: bb8::Pool<SqliteConnectionManager>,
        collection_id: CollectionId,
        resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    ) -> Self {
        let state_table_name = collection_id.as_str().to_string();
        let event_table_name = format!("{}_event", collection_id.as_str());
        Self {
            pool,
            collection_id,
            state_table_name,
            event_table_name,
            columns: Arc::new(std::sync::RwLock::new(Vec::new())),
            ddl_lock: Arc::new(tokio::sync::Mutex::new(())),
            resolver,
            property_columns: Arc::new(std::sync::RwLock::new(BTreeMap::new())),
        }
    }

    #[inline]
    fn state_table(&self) -> &str { &self.state_table_name }

    #[inline]
    fn event_table(&self) -> &str { &self.event_table_name }

    /// Returns all column names currently in the schema cache
    pub fn existing_columns(&self) -> Vec<String> {
        let columns = self.columns.read().expect("RwLock poisoned");
        columns.iter().map(|c| c.name.clone()).collect()
    }

    /// Check if a column exists in the schema cache
    pub fn has_column(&self, name: &str) -> bool {
        let columns = self.columns.read().expect("RwLock poisoned");
        columns.iter().any(|c| c.name == name)
    }

    /// Load this collection's property-to-column assignments into the cache. The
    /// `property_key` column is a serialized [`PropertyId`] (JSON TEXT, see
    /// [`property_key_text`]); a row we cannot parse refuses the collection:
    /// a hidden assignment would let that property's next cache miss claim a
    /// fresh column and silently split its data, so the map loads whole or
    /// not at all. The `PropertyId::Id -> "id"` pin is seeded last so no row
    /// can remap the primary key.
    async fn load_column_map(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
        let collection = self.collection_id.as_str().to_string();
        let rows: Vec<(String, String)> = conn
            .with_connection(move |c| {
                let mut stmt =
                    c.prepare(r#"SELECT "property_key", "column_name" FROM "_ankurah_sqlite_column_map" WHERE "collection" = ?"#)?;
                let rows = stmt
                    .query_map([&collection], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;

        let mut map = BTreeMap::new();
        for (property_key, column_name) in rows {
            match serde_json::from_str::<PropertyId>(&property_key) {
                Ok(id) => {
                    map.insert(id, column_name);
                }
                Err(e) => {
                    return Err(SqliteError::CorruptRecord(format!(
                        "property_key {:?} in the column map for collection {}: {}",
                        property_key, self.collection_id, e
                    )));
                }
            }
        }
        // The `id` pseudo-property is always the reserved primary-key column.
        map.insert(PropertyId::Id, "id".to_string());
        *self.property_columns.write().expect("RwLock poisoned") = map;
        Ok(())
    }

    /// The materialized column for a property key, addressed by durable
    /// [`PropertyId`]. Both arms route through the map (there is no name
    /// short-circuit), so a system property is addressed by identity on reads
    /// exactly like a registered one.
    ///
    /// A cache miss assigns a column NOW, then records it under the serialized
    /// `PropertyId`:
    /// - a **system** property (`Name` key) is a globally-unique catalog field
    ///   name in a homogeneous catalog collection, so its sanitized name is the
    ///   column directly -- the same invariant that made the old name-as-column
    ///   short-circuit correct, now recorded as a durable map row so reads
    ///   resolve it by identity. It has no entity id to suffix-dedupe with, so a
    ///   candidate that lands on a reserved base column or on a column assigned
    ///   to a different identity is a hard error, never a silent takeover
    ///   (ingest is catalog-free: a state buffer from an untrusted peer can
    ///   carry any name);
    /// - a **registered** property (`Id` key) seeds from the catalog resolver's
    ///   display name (sanitized), deduped against the base columns and this
    ///   collection's other assignments (`{name}_{trailing id chars}`, the
    ///   ratified collision rule), or -- when the resolver cannot name the id
    ///   (the intra-node descriptor race; should effectively never fire) -- a
    ///   synthetic `p_{trailing id chars}` name, logged loudly.
    ///
    /// The assignment is claimed with `INSERT OR IGNORE` and the winner read
    /// back, so concurrent writers converge on one column. This is DML on the
    /// map table (not DDL on the state table), and the
    /// `UNIQUE ("collection", "column_name")` constraint plus the read-back +
    /// bounded retry below handle concurrent assignment, so it takes no DDL
    /// lock -- mirroring postgres, which relies on its unique constraint rather
    /// than the advisory DDL lock here. The re-dedupe retry applies only to
    /// registered properties: a system property has exactly one candidate name,
    /// so its collision is refused on first sight rather than retried.
    async fn column_for_key(&self, conn: &PooledConnection, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(column) = self.property_columns.read().expect("RwLock poisoned").get(property_id) {
            return Ok(column.clone());
        }

        let property_key = property_key_text(property_id);

        // Assignment path. Retry on a column-name uniqueness race: reload the
        // map (fresh taken-set) and re-dedupe.
        for _attempt in 0..3 {
            let column = match property_id {
                // System property: its name is unique and is the column. There
                // is no entity id to suffix-dedupe with, so a collision with a
                // reserved base column or with a column assigned to a different
                // identity is a hard error on first sight (silently absorbing
                // the column is how aliasing arises, and the name may come from
                // an untrusted peer's state buffer).
                PropertyId::System { name } => {
                    let candidate = naming::sanitize(name);
                    if BASE_COLUMNS.contains(&candidate.as_str()) {
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!(
                                "system property {} maps to column {:?}, which is a reserved base column",
                                property_key,
                                candidate
                            )
                            .into(),
                        ));
                    }
                    let owner = self
                        .property_columns
                        .read()
                        .expect("RwLock poisoned")
                        .iter()
                        .find(|(other, col)| other != &property_id && col.as_str() == candidate)
                        .map(|(other, _)| property_key_text(other));
                    if let Some(owner) = owner {
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!(
                                "system property {} maps to column {:?}, which is already assigned to property {}",
                                property_key,
                                candidate,
                                owner
                            )
                            .into(),
                        ));
                    }
                    candidate
                }
                // Registered property: seed from the resolver, dedupe by id.
                PropertyId::EntityId(ulid) => {
                    let id = EntityId::from_ulid(*ulid);
                    let seeded = {
                        let resolver = self.resolver.read().expect("RwLock poisoned").as_ref().and_then(|weak| weak.upgrade());
                        resolver.and_then(|r| r.name_for(&id)).map(|name| naming::sanitize(&name))
                    };
                    let assigned = self.property_columns.read().expect("RwLock poisoned");
                    let is_taken = |candidate: &str| {
                        BASE_COLUMNS.contains(&candidate) || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                    };
                    match &seeded {
                        Some(seed) => naming::dedupe(seed, &id, is_taken).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?,
                        None => {
                            warn!(
                                "SqliteBucket({}): catalog cannot name property {}; assigning fallback column (descriptor race?)",
                                self.collection_id,
                                id.to_base64()
                            );
                            naming::fallback("p", &id, is_taken).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?
                        }
                    }
                }
                // The `id` pseudo-property is the primary key, never a stored
                // property value, so it never reaches column assignment (it is
                // pinned to the "id" column at table creation).
                PropertyId::Id => {
                    return Err(MutationError::UpdateFailed(
                        anyhow::anyhow!("the id pseudo-property is never materialized as a stored value").into(),
                    ))
                }
            };

            // Claim the column with INSERT OR IGNORE, then read back the winner
            // for our property key. SQLite's INSERT OR IGNORE swallows BOTH the
            // (collection, property_key) primary-key conflict and the
            // (collection, column_name) uniqueness conflict, so -- unlike
            // postgres, which targets its ON CONFLICT only at the primary key
            // and catches the uniqueness violation as an error -- we distinguish
            // them by the read-back: a row for our key means we (or a concurrent
            // writer on the SAME property) won, converge on it; NO row means the
            // name we chose was already claimed by a DIFFERENT property, so
            // reload the taken-set and re-dedupe.
            let collection = self.collection_id.as_str().to_string();
            let property_key = property_key.clone();
            let candidate = column.clone();
            let winner: Option<String> = conn
                .with_connection(move |c| {
                    c.execute(
                        r#"INSERT OR IGNORE INTO "_ankurah_sqlite_column_map" ("collection", "property_key", "column_name") VALUES (?, ?, ?)"#,
                        rusqlite::params![collection, property_key, candidate],
                    )?;
                    match c.query_row(
                        r#"SELECT "column_name" FROM "_ankurah_sqlite_column_map" WHERE "collection" = ? AND "property_key" = ?"#,
                        rusqlite::params![collection, property_key],
                        |row| row.get::<_, String>(0),
                    ) {
                        Ok(name) => Ok(Some(name)),
                        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                        Err(e) => Err(SqliteError::Rusqlite(e)),
                    }
                })
                .await?;

            match winner {
                Some(winner) => {
                    self.property_columns.write().expect("RwLock poisoned").insert(property_id.clone(), winner.clone());
                    return Ok(winner);
                }
                None => {
                    // Our candidate name is durably claimed under a different
                    // property key. Refresh the taken-set either way.
                    self.load_column_map(conn).await?;
                    if matches!(property_id, PropertyId::System { .. }) {
                        // A system property has exactly one candidate name, so
                        // retrying would recompute the same name; refuse now,
                        // naming the identity that owns the column.
                        let owner = self
                            .property_columns
                            .read()
                            .expect("RwLock poisoned")
                            .iter()
                            .find(|(other, col)| other != &property_id && col.as_str() == column)
                            .map(|(other, _)| format!("property {}", property_key_text(other)))
                            .unwrap_or_else(|| "another property".to_string());
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!(
                                "system property {} maps to column {:?}, which is already assigned to {}",
                                property_key_text(property_id),
                                column,
                                owner
                            )
                            .into(),
                        ));
                    }
                    continue;
                }
            }
        }
        Err(MutationError::UpdateFailed(
            anyhow::anyhow!("could not assign a column for property {} after repeated collisions", property_key).into(),
        ))
    }

    /// The model id written on envelopes this bucket reconstructs (#330):
    /// well-knowns, then the injected catalog resolver.
    fn model_id(&self) -> Result<ankurah_proto::EntityId, RetrievalError> {
        let resolver = self.resolver.read().expect("RwLock poisoned").as_ref().and_then(|weak| weak.upgrade());
        ankurah_core::storage::bucket_model_id(&self.collection_id, resolver.as_deref())
    }

    async fn rebuild_columns_cache(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
        let table_name = self.state_table().to_owned();
        let new_columns = conn
            .with_connection(move |c| {
                let mut stmt = c.prepare(&format!("PRAGMA table_info(\"{}\")", table_name))?;
                let columns: Vec<SqliteColumn> = stmt
                    .query_map([], |row| Ok(SqliteColumn { name: row.get(1)?, data_type: row.get(2)? }))?
                    .filter_map(|r| r.ok())
                    .collect();
                Ok(columns)
            })
            .await?;

        let mut columns = self.columns.write().expect("RwLock poisoned");
        *columns = new_columns;
        Ok(())
    }

    async fn add_missing_columns(&self, conn: &PooledConnection, missing: Vec<(String, &'static str)>) -> Result<(), SqliteError> {
        if missing.is_empty() {
            return Ok(());
        }

        // Acquire DDL lock
        let _lock = self.ddl_lock.lock().await;

        // Re-check columns after acquiring lock
        self.rebuild_columns_cache(conn).await?;

        let table_name = self.state_table();
        for (column, datatype) in missing {
            if SqliteStorageEngine::sane_name(&column) && !self.has_column(&column) {
                let alter_query = format!(r#"ALTER TABLE "{}" ADD COLUMN "{}" {}"#, table_name, column, datatype);
                debug!("Adding column: {}", alter_query);

                let query = alter_query.clone();
                conn.with_connection(move |c| {
                    c.execute(&query, [])?;
                    Ok(())
                })
                .await?;
            }
        }

        self.rebuild_columns_cache(conn).await?;
        Ok(())
    }
}

#[async_trait]
impl StorageCollection for SqliteBucket {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;

        // Ensure head is not empty for new records
        if state.payload.state.head.is_empty() {
            warn!("Warning: Empty head detected for entity {}", state.payload.entity_id);
        }

        let state_buffers = bincode::serialize(&state.payload.state.state_buffers)?;
        let head_json = serde_json::to_string(&state.payload.state.head).map_err(|e| MutationError::General(Box::new(e)))?;
        let attestations_blob = bincode::serialize(&state.attestations)?;
        let id = state.payload.entity_id.to_base64();
        let id_clone = id.clone(); // Clone for use in closure

        // Collect materialized columns (with JSONB flag for proper SQL generation)
        let mut materialized: Vec<(String, Option<SqliteValue>, bool)> = Vec::new(); // (name, value, is_jsonb)
        let mut seen_properties = std::collections::HashSet::new();

        for (name, state_buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            for (key, value) in backend.property_values() {
                // Every property -- registered or system -- addresses its
                // column through the engine-owned durable map, keyed by durable
                // PropertyId and assigned on first sight (see column_for_key).
                let column = self.column_for_key(&conn, &key).await?;
                if !seen_properties.insert(column.clone()) {
                    // Same column from another backend of this entity: first
                    // occurrence wins (cross-backend same-name is pre-existing
                    // pathology; same-collection id collisions were deduped at
                    // column assignment and cannot land here).
                    continue;
                }

                let sqlite_value: Option<SqliteValue> = value.map(|v| v.into());
                let is_jsonb = sqlite_value.as_ref().is_some_and(|v| v.is_jsonb());

                if !self.has_column(&column) {
                    if let Some(ref sv) = sqlite_value {
                        self.add_missing_columns(&conn, vec![(column.clone(), sv.sqlite_type())]).await?;
                    } else {
                        continue;
                    }
                }

                materialized.push((column, sqlite_value, is_jsonb));
            }
        }

        // Build the UPSERT query (BASE_COLUMNS is the module-level const,
        // shared with column_for_key's taken-set).
        let table_name = self.state_table();
        let num_columns = BASE_COLUMNS.len() + materialized.len();
        let mut columns: Vec<&str> = Vec::with_capacity(num_columns);
        columns.extend_from_slice(BASE_COLUMNS);

        let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(num_columns);
        values.push(rusqlite::types::Value::Text(id));
        values.push(rusqlite::types::Value::Blob(state_buffers));
        values.push(rusqlite::types::Value::Text(head_json));
        values.push(rusqlite::types::Value::Blob(attestations_blob));

        // Track which placeholders need jsonb() wrapper (base columns don't)
        let mut placeholder_is_jsonb: Vec<bool> = Vec::with_capacity(num_columns);
        placeholder_is_jsonb.resize(BASE_COLUMNS.len(), false);

        for (name, value, is_jsonb) in &materialized {
            columns.push(name.as_str());
            values.push(match value {
                Some(v) => v.to_sql(),
                None => rusqlite::types::Value::Null,
            });
            placeholder_is_jsonb.push(*is_jsonb);
        }

        let columns_str = columns.iter().map(|c| format!(r#""{}""#, c)).collect::<Vec<_>>().join(", ");
        // Use jsonb(?) for JSONB columns to convert JSON text to JSONB binary format
        let placeholders =
            placeholder_is_jsonb.iter().map(|is_jsonb| if *is_jsonb { "jsonb(?)" } else { "?" }).collect::<Vec<_>>().join(", ");
        let update_str = columns.iter().skip(1).map(|c| format!(r#""{}" = excluded."{}""#, c, c)).collect::<Vec<_>>().join(", ");

        // Use SQLite's RETURNING clause (3.35.0+) to get the old head for comparison
        // If RETURNING is not available, we'll fall back to a separate query
        let query = format!(
            r#"INSERT INTO "{}"({}) VALUES({})
               ON CONFLICT("id") DO UPDATE SET {}"#,
            table_name, columns_str, placeholders, update_str
        );

        debug!("set_state query: {}", query);

        let new_head = state.payload.state.head.clone();
        let table_name_clone = table_name.to_string();
        let query_clone = query.clone();
        let values_clone = values.clone();
        let changed = conn
            .with_connection(move |c| {
                // First, get the old head if the entity exists
                let old_head_json: Option<String> =
                    match c
                        .query_row(&format!(r#"SELECT "head" FROM "{}" WHERE "id" = ?"#, table_name_clone), [&id_clone], |row| row.get(0))
                    {
                        Ok(json) => Some(json),
                        Err(rusqlite::Error::QueryReturnedNoRows) => None,
                        Err(e) => return Err(SqliteError::Rusqlite(e)),
                    };

                // Execute the UPSERT
                c.execute(&query_clone, params_from_iter(values_clone.iter())).map_err(|e| SqliteError::Rusqlite(e))?;

                // Determine if state changed
                let changed = match old_head_json {
                    Some(json) => {
                        // Entity existed - compare heads
                        let old_head: Clock = serde_json::from_str(&json).map_err(|e| SqliteError::Json(e))?;
                        old_head != new_head
                    }
                    None => {
                        // New entity
                        true
                    }
                };

                Ok(changed)
            })
            .await?;

        debug!("set_state: Changed: {}", changed);
        Ok(changed)
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        let table_name = self.state_table().to_owned();
        let id_str = id.to_base64();
        let collection_id = self.collection_id.clone();

        let (state_buffer, head_json, attestations_blob) = conn
            .with_connection(move |c| {
                let query = format!(r#"SELECT "id", "state_buffer", "head", "attestations" FROM "{}" WHERE "id" = ?"#, table_name);

                let result = c.query_row(&query, [&id_str], |row| {
                    let _row_id: String = row.get(0)?;
                    let state_buffer: Vec<u8> = row.get(1)?;
                    let head_json: String = row.get(2)?;
                    let attestations_blob: Vec<u8> = row.get(3)?;
                    Ok((state_buffer, head_json, attestations_blob))
                });

                match result {
                    Ok(raw) => Ok(raw),
                    Err(rusqlite::Error::QueryReturnedNoRows) => {
                        // Table might not exist - create it and return EntityNotFound
                        // This matches Postgres behavior
                        let _ = create_state_table(c, &collection_id);
                        Err(SqliteError::Rusqlite(rusqlite::Error::QueryReturnedNoRows))
                    }
                    Err(e) => Err(SqliteError::Rusqlite(e)),
                }
            })
            .await
            .map_err(|e| match e {
                SqliteError::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => RetrievalError::EntityNotFound(id),
                _ => RetrievalError::StorageError(Box::new(e)),
            })?;

        let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&state_buffer).map_err(RetrievalError::storage)?;
        let head: Clock = serde_json::from_str(&head_json).map_err(RetrievalError::storage)?;
        let attestations: AttestationSet = bincode::deserialize(&attestations_blob).map_err(RetrievalError::storage)?;

        // The row exists, so the envelope needs its model id now; an absent
        // entity must surface EntityNotFound above, never a model-id error
        // (get_retrieve_or_create relies on that fallthrough on a cold
        // catalog).
        Ok(Attested {
            payload: EntityState {
                entity_id: id,
                model: self.model_id()?,
                state: State { state_buffers: StateBuffers(state_buffers), head },
            },
            attestations,
        })
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        debug!("SqliteBucket({}).fetch_states: {:?}", self.collection_id, selection);
        // This engine can only address a property by identity. Refuse a
        // selection that still carries a name.
        selection.check()?;

        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        // Resolve, through this engine's own durable map, the column for every
        // property the selection references (assigned on write, sticky under
        // rename). There is NO name fallback: a property with no assigned column
        // -- or whose column was never materialized -- is ABSENT (evaluates
        // NULL, folded below), never re-derived from a raw name. The SQL builder
        // then translates each surviving identity to its column at emit time,
        // and the in-memory post-filter keeps reading by identity, so a rename
        // is harmless end to end.
        let assigned = self.property_columns.read().expect("RwLock poisoned").clone();
        let referenced = selection.referenced_properties();

        // If the snapshot lacks a referenced property, reload the durable map
        // once before folding it to absent: another handle to this collection
        // (each collection() call builds a fresh bucket) may have assigned the
        // column after this snapshot was loaded. A shared database file
        // accessed by multiple processes is not the supported sync path (nodes
        // sync via the protocol); this reload is a consistency courtesy, not a
        // multi-process contract.
        let assigned = if referenced.iter().any(|p| !assigned.contains_key(p)) {
            self.load_column_map(&conn).await?;
            self.property_columns.read().expect("RwLock poisoned").clone()
        } else {
            assigned
        };

        // Refresh the schema cache if we reference an assigned column we have not
        // yet seen materialized (set_state adds columns on demand).
        let cached = self.existing_columns();
        let need_refresh = referenced.iter().any(|p| assigned.get(p).map_or(false, |c| !cached.contains(c)));
        if need_refresh {
            debug!("SqliteBucket({}).fetch_states: unseen assigned column referenced, refreshing schema cache", self.collection_id);
            self.rebuild_columns_cache(&conn).await?;
        }
        let existing = self.existing_columns();

        // A property is absent when it has no assigned column, or its assigned
        // column is not (yet) a physical column in the state table. Matched by
        // durable identity, so a rename never mis-targets which one is absent.
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|p| assigned.get(p).map_or(true, |c| !existing.contains(c))).collect();
        if !absent.is_empty() {
            debug!("SqliteBucket({}).fetch_states: absent properties {:?}, treating as NULL", self.collection_id, absent);
        }
        let effective_selection = if absent.is_empty() { selection.clone() } else { selection.assume_null(&absent) };

        // Split predicate for pushdown
        let split = split_predicate_for_sqlite(&effective_selection.predicate);
        let needs_post_filter = split.needs_post_filter();
        let remaining_predicate = split.remaining_predicate.clone();

        // Build SQL. The builder translates each resolved identity to its
        // assigned column via `assigned`; every identity that survives the split
        // has one (absent properties were folded to NULL above).
        let sql_selection = ankql::ast::Selection {
            predicate: split.sql_predicate,
            order_by: effective_selection.order_by.clone(),
            limit: if needs_post_filter { None } else { effective_selection.limit },
        };

        let mut builder = SqlBuilder::with_fields(vec!["id", "state_buffer", "head", "attestations"]);
        builder.table_name(self.state_table());
        builder.column_map(assigned);
        builder.selection(&sql_selection).map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;

        let (sql, params) = builder.build().map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;
        debug!("fetch_states SQL: {} with {} params", sql, params.len());

        let raw_rows = conn
            .with_connection(move |c| {
                let mut stmt = c.prepare(&sql)?;
                let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
                    let id_str: String = row.get(0)?;
                    let state_buffer: Vec<u8> = row.get(1)?;
                    let head_json: String = row.get(2)?;
                    let attestations_blob: Vec<u8> = row.get(3)?;
                    Ok((id_str, state_buffer, head_json, attestations_blob))
                })?;
                Ok(rows.collect::<Result<Vec<_>, _>>()?)
            })
            .await?;

        // A scan that matched nothing never needs a model id: a cold catalog
        // must not fail an empty fetch (e.g. the ephemeral known_matches
        // pre-fetch against a collection this node has never stored).
        let mut results = Vec::with_capacity(raw_rows.len());
        if !raw_rows.is_empty() {
            let model = self.model_id()?;
            for (id_str, state_buffer, head_json, attestations_blob) in raw_rows {
                let id = EntityId::from_base64(&id_str).map_err(|e| RetrievalError::storage(std::io::Error::other(e)))?;
                let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&state_buffer).map_err(RetrievalError::storage)?;
                let head: Clock = serde_json::from_str(&head_json).map_err(RetrievalError::storage)?;
                let attestations: AttestationSet = bincode::deserialize(&attestations_blob).map_err(RetrievalError::storage)?;

                results.push(Attested {
                    payload: EntityState { entity_id: id, model, state: State { state_buffers: StateBuffers(state_buffers), head } },
                    attestations,
                });
            }
        }

        // Post-filter if needed
        if needs_post_filter {
            debug!("Post-filtering {} results", results.len());
            results = post_filter_states(&results, &remaining_predicate, &self.collection_id);

            if let Some(limit) = effective_selection.limit {
                results.truncate(limit as usize);
            }
        }

        Ok(results)
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError> {
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;

        let operations = bincode::serialize(&entity_event.payload.operations)?;
        let attestations = bincode::serialize(&entity_event.attestations)?;
        let parent_json = serde_json::to_string(&entity_event.payload.parent).map_err(|e| MutationError::General(Box::new(e)))?;

        let table_name = self.event_table();
        let event_id = entity_event.payload.id().to_base64();
        let entity_id = entity_event.payload.entity_id.to_base64();

        let query = format!(
            r#"INSERT INTO "{}"("id", "entity_id", "operations", "parent", "attestations") VALUES(?, ?, ?, ?, ?)
               ON CONFLICT ("id") DO NOTHING"#,
            table_name
        );

        conn.with_connection(move |c| {
            let affected = c.execute(&query, rusqlite::params![event_id, entity_id, operations, parent_json, attestations])?;
            Ok(affected > 0)
        })
        .await
        .map_err(|e| MutationError::General(Box::new(e)))
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        let table_name = self.event_table().to_owned();
        let id_strings: Vec<String> = event_ids.iter().map(|id| id.to_base64()).collect();
        let num_ids = id_strings.len();

        let raw_rows = conn
            .with_connection(move |c| {
                let placeholders = (0..num_ids).map(|_| "?").collect::<Vec<_>>().join(", ");
                let query = format!(
                    r#"SELECT "id", "entity_id", "operations", "parent", "attestations" FROM "{}" WHERE "id" IN ({})"#,
                    table_name, placeholders
                );

                let mut stmt = c.prepare(&query)?;
                let params: Vec<&dyn rusqlite::ToSql> = id_strings.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
                let rows = stmt.query_map(params.as_slice(), |row| {
                    let _event_id: String = row.get(0)?;
                    let entity_id_str: String = row.get(1)?;
                    let operations: Vec<u8> = row.get(2)?;
                    let parent_json: String = row.get(3)?;
                    let attestations_blob: Vec<u8> = row.get(4)?;
                    Ok((entity_id_str, operations, parent_json, attestations_blob))
                })?;
                Ok(rows.collect::<Result<Vec<_>, _>>()?)
            })
            .await
            .map_err(|e: SqliteError| RetrievalError::StorageError(Box::new(e)))?;

        // Zero found events never need a model id: event-getter probes for
        // absent parents must surface "not found" semantics, not a
        // model-resolution error, on a cold catalog.
        let mut events = Vec::with_capacity(raw_rows.len());
        if !raw_rows.is_empty() {
            let model = self.model_id()?;
            for (entity_id_str, operations_blob, parent_json, attestations_blob) in raw_rows {
                let entity_id = EntityId::from_base64(&entity_id_str).map_err(|e| RetrievalError::storage(std::io::Error::other(e)))?;
                let operations: OperationSet = bincode::deserialize(&operations_blob).map_err(RetrievalError::storage)?;
                let parent: Clock = serde_json::from_str(&parent_json).map_err(RetrievalError::storage)?;
                let attestations: AttestationSet = bincode::deserialize(&attestations_blob).map_err(RetrievalError::storage)?;

                events.push(Attested { payload: Event { model, entity_id, operations, parent }, attestations });
            }
        }
        Ok(events)
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        let table_name = self.event_table().to_owned();
        let entity_id_str = entity_id.to_base64();

        let raw_rows = conn
            .with_connection(move |c| {
                let query = format!(r#"SELECT "id", "operations", "parent", "attestations" FROM "{}" WHERE "entity_id" = ?"#, table_name);

                let mut stmt = c.prepare(&query)?;
                let rows = stmt.query_map([&entity_id_str], |row| {
                    let _event_id: String = row.get(0)?;
                    let operations: Vec<u8> = row.get(1)?;
                    let parent_json: String = row.get(2)?;
                    let attestations_blob: Vec<u8> = row.get(3)?;
                    Ok((operations, parent_json, attestations_blob))
                })?;
                Ok(rows.collect::<Result<Vec<_>, _>>()?)
            })
            .await
            .map_err(|e: SqliteError| RetrievalError::StorageError(Box::new(e)))?;

        // Zero rows never need a model id (cold catalog; see get_events).
        let mut events = Vec::with_capacity(raw_rows.len());
        if !raw_rows.is_empty() {
            let model = self.model_id()?;
            for (operations_blob, parent_json, attestations_blob) in raw_rows {
                let operations: OperationSet = bincode::deserialize(&operations_blob).map_err(RetrievalError::storage)?;
                let parent: Clock = serde_json::from_str(&parent_json).map_err(RetrievalError::storage)?;
                let attestations: AttestationSet = bincode::deserialize(&attestations_blob).map_err(RetrievalError::storage)?;

                events.push(Attested { payload: Event { model, entity_id, operations, parent }, attestations });
            }
        }
        Ok(events)
    }
}

/// Post-filter EntityStates using a predicate that couldn't be pushed to SQL.
fn post_filter_states(
    states: &[Attested<EntityState>],
    predicate: &ankql::ast::Predicate,
    collection_id: &CollectionId,
) -> Vec<Attested<EntityState>> {
    states
        .iter()
        .filter(|attested| match TemporaryEntity::new(attested.payload.entity_id, collection_id.clone(), &attested.payload.state) {
            Ok(temp_entity) => match evaluate_predicate(&temp_entity, predicate) {
                Ok(result) => result,
                Err(e) => {
                    warn!("Post-filter evaluation error for entity {}: {}", attested.payload.entity_id, e);
                    false
                }
            },
            Err(e) => {
                warn!("Failed to create TemporaryEntity for post-filtering {}: {}", attested.payload.entity_id, e);
                false
            }
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_open_in_memory() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let collection = engine.collection(&"test_collection".into()).await.unwrap();
        let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert!(collection.fetch_states(&all).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_collections_discovery() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        // Non-creating: nothing exists yet.
        assert!(engine.list_collections().await.unwrap().is_empty());

        // Opening a collection creates its state + `{id}_event` tables (and the
        // engine-wide column-map table on first touch).
        engine.collection(&"users".into()).await.unwrap();
        engine.collection(&"_ankurah_model".into()).await.unwrap();
        // A user collection whose id ends in `_event` must still be discovered
        // (its `{id}_event` companion exists) and not be mistaken for another
        // collection's event table.
        engine.collection(&"click_event".into()).await.unwrap();

        let mut found: Vec<String> = engine.list_collections().await.unwrap().into_iter().map(|c| c.as_str().to_string()).collect();
        found.sort();
        assert_eq!(found, vec!["_ankurah_model".to_string(), "click_event".to_string(), "users".to_string()]);
        // The internal column-map table and event companions are never listed.
        for internal in ["_ankurah_sqlite_column_map", "users_event", "_ankurah_model_event", "click_event_event"] {
            assert!(!found.iter().any(|c| c == internal), "{internal} must not be listed as a collection");
        }
    }

    #[tokio::test]
    async fn test_sane_name() {
        assert!(SqliteStorageEngine::sane_name("test_collection"));
        assert!(SqliteStorageEngine::sane_name("test.collection"));
        assert!(SqliteStorageEngine::sane_name("test:collection"));
        assert!(!SqliteStorageEngine::sane_name("test;collection"));
        assert!(!SqliteStorageEngine::sane_name("test'collection"));
    }

    /// Test that SQLite JSONB functions are available and work correctly.
    ///
    /// This test verifies:
    /// 1. The `jsonb()` function exists and can convert JSON text to JSONB
    /// 2. The `->` operator works for JSON path traversal
    /// 3. Type-aware comparisons work (numeric vs string)
    /// 4. JSONB storage and retrieval works correctly
    #[tokio::test]
    async fn test_jsonb_function_availability() -> Result<(), SqliteError> {
        let engine = SqliteStorageEngine::open_in_memory().await.map_err(|e| SqliteError::DDL(e.to_string()))?;
        let conn = engine.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        // Test 1: Verify jsonb() function exists and works
        // jsonb() returns a BLOB (JSONB binary format), so we query it as BLOB
        let result = conn
            .with_connection(|c| {
                let value: Vec<u8> = c.query_row("SELECT jsonb('{\"key\": \"value\"}')", [], |row| row.get(0))?;
                Ok(value)
            })
            .await?;
        // jsonb() returns JSONB BLOB format - verify it's not empty
        assert!(!result.is_empty(), "jsonb() function should return a non-empty BLOB");

        // Test 2: Verify -> operator works for path traversal
        // json_extract returns the SQL value (unquoted string for JSON strings)
        let result = conn
            .with_connection(|c| {
                let value: String =
                    c.query_row(r#"SELECT json_extract(jsonb('{"territory": "US", "count": 10}'), '$.territory')"#, [], |row| row.get(0))?;
                Ok(value)
            })
            .await?;
        // json_extract returns the unquoted SQL value, not the JSON string representation
        assert_eq!(result, "US", "JSON path extraction should return the SQL value");

        // Test 3: Verify numeric comparison is numeric (not lexicographic)
        // In SQLite, json_extract with numeric comparison should work correctly
        let result = conn
            .with_connection(|c| {
                let value: bool = c.query_row(
                    r#"SELECT json_extract(jsonb('{"count": 9}'), '$.count') > json_extract(jsonb('{"count": 10}'), '$.count')"#,
                    [],
                    |row| row.get(0),
                )?;
                Ok(value)
            })
            .await?;
        assert!(!result, "Numeric comparison: 9 > 10 should be false");

        Ok(())
    }

    /// Test JSON path queries with the -> operator (SQLite JSONB syntax).
    ///
    /// This test verifies that:
    /// 1. JSON properties can be queried using path syntax (e.g., `data.status = 'active'`)
    /// 2. The SQL builder generates correct SQLite JSONB syntax
    /// 3. Queries return correct results
    #[tokio::test]
    async fn test_json_path_query() -> anyhow::Result<()> {
        use crate::sql_builder::SqlBuilder;
        use ankql::parser::parse_selection;

        // Test that the SQL builder generates correct JSONB syntax
        let selection = parse_selection(r#"data.status = 'active'"#).expect("Failed to parse query");
        let mut builder = SqlBuilder::with_fields(vec!["id", "state_buffer"]);
        builder.table_name("test_table");
        builder.selection(&selection).map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;

        let (sql, _params) = builder.build().map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;

        // Verify the SQL uses json_extract() for reliable JSON path comparisons
        assert!(sql.contains("json_extract"), "SQL should use json_extract() for JSON path: {}", sql);
        assert!(sql.contains(r#"json_extract("data", '$.status')"#), "SQL should extract from data column with $.status path: {}", sql);

        Ok(())
    }

    /// Test the full cycle: store JSONB via parameter, query via json_extract with parameter.
    /// This mimics exactly what the real code does.
    #[tokio::test]
    async fn test_jsonb_storage_and_parameterized_query() -> Result<(), SqliteError> {
        let engine = SqliteStorageEngine::open_in_memory().await.map_err(|e| SqliteError::DDL(e.to_string()))?;
        let conn = engine.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        conn.with_connection(|c| {
            // Create table with BLOB column for JSONB
            c.execute(r#"CREATE TABLE test_jsonb (id TEXT PRIMARY KEY, data BLOB)"#, [])?;

            // Insert using jsonb(?) - this is what the real code does
            let json_text = r#"{"territory": "US", "count": 10}"#;
            c.execute(r#"INSERT INTO test_jsonb (id, data) VALUES (?, jsonb(?))"#, rusqlite::params!["1", json_text])?;

            // Verify data is stored
            let count: i32 = c.query_row("SELECT COUNT(*) FROM test_jsonb", [], |row| row.get(0))?;
            assert_eq!(count, 1, "Should have 1 row");

            // Check what's in the data column
            let data_type: String = c.query_row("SELECT typeof(data) FROM test_jsonb WHERE id = '1'", [], |row| row.get(0))?;
            eprintln!("Data column type: {}", data_type);

            // Check what json_extract returns
            let extracted: String =
                c.query_row(r#"SELECT json_extract(data, '$.territory') FROM test_jsonb WHERE id = '1'"#, [], |row| row.get(0))?;
            eprintln!("Extracted territory: '{}'", extracted);

            // Now try the parameterized query - THIS IS WHAT THE REAL CODE DOES
            let query_param = "US";
            let result: Result<String, _> = c.query_row(
                r#"SELECT id FROM test_jsonb WHERE json_extract(data, '$.territory') = ?"#,
                rusqlite::params![query_param],
                |row| row.get(0),
            );
            eprintln!("Query result: {:?}", result);

            match result {
                Ok(id) => assert_eq!(id, "1", "Should find the row with territory = US"),
                Err(e) => panic!("Query failed: {:?}", e),
            }

            Ok(())
        })
        .await
    }
}
