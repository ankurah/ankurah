use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
    time::Duration,
};

use ankurah_core::{
    error::{MutationError, RetrievalError, StateError},
    property::backend::backend_from_string,
    schema::CatalogResolver,
    storage::{naming, StorageCollection, StorageEngine},
};
use ankurah_proto::PropertyId;
use ankurah_proto::{Attestation, AttestationSet, Attested, EntityState, EventId, OperationSet, State, StateBuffers, PROTOCOL_VERSION};

use futures_util::{pin_mut, TryStreamExt};

pub mod sql_builder;
pub mod value;

use value::PGValue;

use ankurah_proto::{Clock, CollectionId, EntityId, Event};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use tokio_postgres::{error::SqlState, types::ToSql};
use tracing::{debug, error, info, warn};

/// Default connection pool size for `Postgres::open()`.
/// Production applications should configure their own pool via `Postgres::new()`.
pub const DEFAULT_POOL_SIZE: u32 = 15;

/// Default connection timeout in seconds
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 30;

/// Engine-level key/value metadata for the store itself (currently just the
/// 'protocol_version' record). Not a collection: it survives
/// `delete_all_collections`, because wiping the record would make the store
/// read as unversioned and refuse its own reopen.
const META_TABLE: &str = "ankurah_meta";

pub struct Postgres {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    /// The catalog resolver, injected post-construction by `Node` (see
    /// `StorageEngine::set_catalog_resolver`). Shared with every bucket:
    /// the name SOURCE for the engine-owned durable id-to-column map.
    resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
}

impl Postgres {
    /// Create a new storage engine with an existing pool.
    ///
    /// Records or checks the store's protocol version (see
    /// [`Self::check_protocol_version`]) so every construction path verifies
    /// the store it is about to serve.
    pub async fn new(pool: bb8::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<Self> {
        let engine = Self { pool, resolver: Arc::new(RwLock::new(None)) };
        engine.check_protocol_version().await?;
        Ok(engine)
    }

    pub async fn open(uri: &str) -> anyhow::Result<Self> {
        let manager = PostgresConnectionManager::new_from_stringlike(uri, NoTls)?;
        let pool = bb8::Pool::builder()
            .max_size(DEFAULT_POOL_SIZE)
            .connection_timeout(Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_SECS))
            .build(manager)
            .await?;
        Self::new(pool).await
    }

    // TODO: newtype this to `BucketName(&str)` with a constructor that
    // only accepts a subset of characters.
    pub fn sane_name(collection: &str) -> bool {
        for char in collection.chars() {
            match char {
                char if char.is_alphanumeric() => {}
                char if char.is_numeric() => {}
                '_' | '.' | ':' => {}
                _ => return false,
            }
        }

        true
    }

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
    async fn check_protocol_version(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let rows = client.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", &[]).await?;
        let tables: Vec<String> = rows.iter().map(|row| row.get("table_name")).collect();
        let has_ankurah_tables = tables.iter().any(|t| t != META_TABLE);

        let recorded: Option<String> = if tables.iter().any(|t| t == META_TABLE) {
            client
                .query_opt(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#), &[])
                .await?
                .map(|row| row.get(0))
        } else {
            None
        };

        let expected = PROTOCOL_VERSION.to_string();
        match recorded {
            Some(found) if found == expected => Ok(()),
            Some(found) => anyhow::bail!(
                "incompatible store protocol version: found {found}, required {PROTOCOL_VERSION}; reset your development database (or migrate the store) before opening it with this binary"
            ),
            None if has_ankurah_tables => anyhow::bail!(
                "store has existing ankurah tables but no recorded protocol version (pre-{PROTOCOL_VERSION} store); reset your development database (or migrate the store) before opening it with this binary"
            ),
            None => {
                // Fresh store: claim the record, serializing concurrent first
                // opens the same way collection DDL is serialized.
                let lock_key = acquire_ddl_lock(&client, META_TABLE).await?;
                let result = async {
                    client
                        .execute(&format!(r#"CREATE TABLE IF NOT EXISTS "{META_TABLE}" ("key" TEXT PRIMARY KEY, "value" TEXT)"#), &[])
                        .await?;
                    client
                        .execute(
                            &format!(
                                r#"INSERT INTO "{META_TABLE}" ("key", "value") VALUES ('protocol_version', $1) ON CONFLICT ("key") DO NOTHING"#
                            ),
                            &[&expected],
                        )
                        .await?;
                    // Re-read: if another process recorded between our scan and
                    // our insert, the store must still match this binary.
                    let reread: String = client
                        .query_one(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#), &[])
                        .await?
                        .get(0);
                    if reread == expected {
                        Ok(())
                    } else {
                        anyhow::bail!(
                            "incompatible store protocol version: found {reread}, required {PROTOCOL_VERSION}; reset your development database (or migrate the store) before opening it with this binary"
                        )
                    }
                }
                .await;
                release_ddl_lock(&client, lock_key).await?;
                result
            }
        }
    }
}

/// Compute advisory lock key from a string identifier
fn advisory_lock_key(identifier: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    identifier.hash(&mut hasher);
    hasher.finish() as i64
}

/// Acquire a PostgreSQL advisory lock for DDL operations on a collection
async fn acquire_ddl_lock(client: &tokio_postgres::Client, collection_id: &str) -> Result<i64, StateError> {
    let lock_key = advisory_lock_key(&format!("ankurah_ddl:{}", collection_id));
    debug!("Acquiring advisory lock {} for collection {}", lock_key, collection_id);
    client.execute("SELECT pg_advisory_lock($1)", &[&lock_key]).await.map_err(|err| {
        error!("Failed to acquire advisory lock for {}: {:?}", collection_id, err);
        StateError::DDLError(Box::new(err))
    })?;
    Ok(lock_key)
}

/// Release a PostgreSQL advisory lock
async fn release_ddl_lock(client: &tokio_postgres::Client, lock_key: i64) -> Result<(), StateError> {
    debug!("Releasing advisory lock {}", lock_key);
    client.execute("SELECT pg_advisory_unlock($1)", &[&lock_key]).await.map_err(|err| {
        error!("Failed to release advisory lock {}: {:?}", lock_key, err);
        StateError::DDLError(Box::new(err))
    })?;
    Ok(())
}

#[async_trait]
impl StorageEngine for Postgres {
    type Value = PGValue;

    async fn collection(&self, collection_id: &CollectionId) -> Result<std::sync::Arc<dyn StorageCollection>, RetrievalError> {
        if !Postgres::sane_name(collection_id.as_str()) {
            return Err(RetrievalError::InvalidBucketName);
        }
        // The engine's own physical tables are not collections. The reserved
        // "_ankurah_" prefix is enforced above this layer for user models, but
        // "ankurah_meta" carries no underscore and would otherwise pass
        // sane_name and collide with the protocol-version record table.
        if collection_id.as_str() == META_TABLE || collection_id.as_str() == "_ankurah_postgres_column_map" {
            return Err(RetrievalError::InvalidBucketName);
        }

        let mut client = self.pool.get().await.map_err(RetrievalError::storage)?;

        // get the current schema from the database
        let schema = client.query_one("SELECT current_database()", &[]).await.map_err(RetrievalError::storage)?;
        let schema = schema.get("current_database");

        let bucket = PostgresBucket {
            pool: self.pool.clone(),
            schema,
            collection_id: collection_id.clone(),
            columns: Arc::new(RwLock::new(Vec::new())),
            resolver: self.resolver.clone(),
            property_columns: Arc::new(RwLock::new(BTreeMap::new())),
            #[cfg(debug_assertions)]
            last_spilled_predicate: Arc::new(RwLock::new(None)),
        };

        // Acquire advisory lock to serialize DDL operations for this collection
        let lock_key = acquire_ddl_lock(&client, collection_id.as_str()).await?;

        // Pin the `id` pseudo-property to the reserved "id" primary-key column
        // for this collection, so a read of `id` is a uniform durable-map hit
        // (no name-as-column special case, never a read miss). "id" is a
        // BASE_COLUMN, so no assigned property can ever collide with it.
        let id_pin_key = property_key_text(&PropertyId::Id);

        // Create tables if they don't exist (protected by advisory lock)
        let result = async {
            bucket.create_state_table(&mut client).await?;
            bucket.create_event_table(&mut client).await?;
            // The map table is engine-wide, not per-collection: first opens of
            // two DIFFERENT collections race its CREATE TABLE under their
            // disjoint collection locks, so it takes its own fixed-key lock.
            let map_lock = acquire_ddl_lock(&client, "_ankurah_postgres_column_map").await?;
            let map_result = bucket.create_column_map_table(&client).await;
            release_ddl_lock(&client, map_lock).await?;
            map_result?;
            client
                .execute(
                    r#"INSERT INTO "_ankurah_postgres_column_map" ("collection", "property_key", "column_name") VALUES ($1, $2, 'id')
                       ON CONFLICT ("collection", "property_key") DO NOTHING"#,
                    &[&bucket.collection_id.as_str(), &id_pin_key],
                )
                .await
                .map_err(|err| StateError::DDLError(Box::new(err)))?;
            bucket.rebuild_columns_cache(&mut client).await?;
            bucket.load_column_map(&client).await?;
            Ok::<_, StateError>(())
        }
        .await;

        // Always release the lock, even if DDL failed
        release_ddl_lock(&client, lock_key).await?;

        result?;
        Ok(Arc::new(bucket))
    }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { *self.resolver.write().unwrap() = Some(resolver); }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let mut client = self.pool.get().await.map_err(|err| MutationError::General(Box::new(err)))?;

        // Get all tables in the public schema
        let query = r#"
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        "#;

        let rows = client.query(query, &[]).await.map_err(|err| MutationError::General(Box::new(err)))?;
        // The engine-level meta table is not a collection and keeps the
        // protocol version record across a wipe.
        let tables: Vec<String> =
            rows.iter().map(|row| row.get("table_name")).filter(|name: &String| name.as_str() != META_TABLE).collect();
        if tables.is_empty() {
            return Ok(false);
        }

        // Start a transaction to drop all tables atomically
        let transaction = client.transaction().await.map_err(|err| MutationError::General(Box::new(err)))?;

        // Drop each table
        for table_name in tables {
            let drop_query = format!(r#"DROP TABLE IF EXISTS "{}""#, table_name);
            transaction.execute(&drop_query, &[]).await.map_err(|err| MutationError::General(Box::new(err)))?;
        }

        // Commit the transaction
        transaction.commit().await.map_err(|err| MutationError::General(Box::new(err)))?;

        Ok(true)
    }

    /// Non-creating collection discovery. The
    /// trait default returns nothing, which would make a durable node warm an
    /// empty catalog on restart. A collection's state table is named exactly
    /// its id, paired with an `{id}_event` companion; the engine-wide
    /// `_ankurah_postgres_column_map` map and the engine-level `ankurah_meta`
    /// version table are the only others. So a table is a collection iff its
    /// `{name}_event` companion also exists. Creates nothing.
    async fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> {
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        let rows = client
            .query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", &[])
            .await
            .map_err(RetrievalError::storage)?;
        let names: Vec<String> = rows.iter().map(|row| row.get("table_name")).collect();
        let table_set: std::collections::HashSet<String> = names.iter().cloned().collect();
        let collections = names
            .into_iter()
            .filter(|name| name.as_str() != "_ankurah_postgres_column_map" && name.as_str() != META_TABLE)
            .filter(|name| table_set.contains(&format!("{name}_event")))
            .map(CollectionId::from)
            .collect();
        Ok(collections)
    }
}

#[derive(Clone, Debug)]
pub struct PostgresColumn {
    pub name: String,
    pub is_nullable: bool,
    pub data_type: String,
}

pub struct PostgresBucket {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    collection_id: CollectionId,
    schema: String,
    columns: Arc<RwLock<Vec<PostgresColumn>>>,
    /// The injected catalog resolver (shared with the engine): the NAME SOURCE
    /// for [`Self::column_for_key`]. Weak so storage never keeps the node alive.
    resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    /// This collection's slice of the engine-owned durable property-to-column
    /// map (the `_ankurah_postgres_column_map` table), cached and keyed by
    /// durable [`PropertyId`]. The map -- not the display name -- is what
    /// addresses a property's column once assigned: renames never move columns,
    /// collisions were deduped at assignment. Always carries the
    /// `PropertyId::Id -> "id"` pin so a read of the primary key is a uniform
    /// map hit.
    property_columns: Arc<RwLock<BTreeMap<PropertyId, String>>>,
    /// Tracks the last predicate that spilled to post-filtering (debug builds only)
    #[cfg(debug_assertions)]
    last_spilled_predicate: Arc<RwLock<Option<ankql::ast::Predicate>>>,
}

/// Fixed columns of every state table: reserved, never assignable to a property.
const BASE_COLUMNS: [&str; 4] = ["id", "state_buffer", "head", "attestations"];

/// The durable, serialized address of a property: the JSON form of its
/// [`PropertyId`], stored as the `property_key` text in
/// `_ankurah_postgres_column_map`. The write side (the `PropertyId` a backend's
/// `property_values()` yields) and the read side (a `PropertyId` straight off
/// the resolved AST) both go through this, so a column assigned on write is
/// found by the byte-identical key on read.
fn property_key_text(id: &PropertyId) -> String { serde_json::to_string(id).expect("PropertyId always serializes to JSON") }

impl PostgresBucket {
    fn state_table(&self) -> String { self.collection_id.as_str().to_string() }

    pub fn event_table(&self) -> String { format!("{}_event", self.collection_id.as_str()) }

    /// The model id written on envelopes this bucket reconstructs (#330):
    /// well-knowns, then the injected catalog resolver.
    fn model_id(&self) -> Result<ankurah_proto::ModelId, RetrievalError> {
        let resolver = self.resolver.read().unwrap().as_ref().and_then(|weak| weak.upgrade());
        ankurah_core::storage::bucket_model_id(&self.collection_id, resolver.as_deref())
    }

    /// Create the engine-wide durable property-to-column map table. One table
    /// for the whole database; rows are scoped by collection (dedup scope is
    /// per-collection, the ratified naming rule). `property_key` is a serialized
    /// [`PropertyId`] (JSON text, see [`property_key_text`]) -- the same durable
    /// address the read side resolves against, so registered AND system
    /// properties alike are addressed by identity, never by a raw name.
    /// `_ankurah_postgres_` is the reserved prefix that shields this internal
    /// table from user-collection name collisions.
    async fn create_column_map_table(&self, client: &tokio_postgres::Client) -> Result<(), StateError> {
        let query = r#"CREATE TABLE IF NOT EXISTS "_ankurah_postgres_column_map" (
            "collection" text NOT NULL,
            "property_key" text NOT NULL,
            "column_name" text NOT NULL,
            PRIMARY KEY ("collection", "property_key"),
            UNIQUE ("collection", "column_name")
        )"#;
        client.execute(query, &[]).await.map_err(|err| StateError::DDLError(Box::new(err)))?;
        Ok(())
    }

    /// Load this collection's property-to-column assignments into the cache. The
    /// `property_key` column is a serialized [`PropertyId`] (JSON text, see
    /// [`property_key_text`]); a row we cannot parse refuses the collection:
    /// a hidden assignment would let that property's next cache miss claim a
    /// fresh column and silently split its data, so the map loads whole or
    /// not at all. The `PropertyId::Id -> "id"` pin is seeded last so no row
    /// can remap the primary key.
    async fn load_column_map(&self, client: &tokio_postgres::Client) -> Result<(), StateError> {
        let rows = client
            .query(
                r#"SELECT "property_key", "column_name" FROM "_ankurah_postgres_column_map" WHERE "collection" = $1"#,
                &[&self.collection_id.as_str()],
            )
            .await
            .map_err(|err| StateError::DDLError(Box::new(err)))?;
        let mut map = BTreeMap::new();
        for row in rows {
            let property_key: String = row.get("property_key");
            let column_name: String = row.get("column_name");
            match serde_json::from_str::<PropertyId>(&property_key) {
                Ok(id) => {
                    map.insert(id, column_name);
                }
                Err(e) => {
                    return Err(StateError::SerializationError(
                        format!("corrupt property_key {:?} in the column map for collection {}: {}", property_key, self.collection_id, e)
                            .into(),
                    ));
                }
            }
        }
        // The `id` pseudo-property is always the reserved primary-key column.
        map.insert(PropertyId::Id, "id".to_string());
        *self.property_columns.write().unwrap() = map;
        Ok(())
    }

    /// The materialized column for a property key, addressed by durable
    /// [`PropertyId`]. Both value-bearing arms route through the map (there is
    /// no name short-circuit), so a system property is addressed by identity on
    /// reads exactly like a registered one.
    ///
    /// A cache miss assigns a column NOW, then records it under the serialized
    /// `PropertyId`:
    /// - a **system** property (`System { name }`) is a globally-unique catalog
    ///   field name in a homogeneous catalog collection, so its sanitized name
    ///   is the column directly -- the same invariant that made the old
    ///   name-as-column short-circuit correct, now recorded as a durable map row
    ///   so reads resolve it by identity. It has no entity id to suffix-dedupe
    ///   with, so a candidate that lands on a reserved base column or on a
    ///   column assigned to a different identity is a hard error, never a silent
    ///   takeover (ingest is catalog-free: a state buffer from an untrusted peer
    ///   can carry any name);
    /// - a **registered** property (`EntityId`) seeds from the catalog
    ///   resolver's display name (sanitized), deduped against the base columns
    ///   and this collection's other assignments (`{name}_{trailing id chars}`,
    ///   the ratified collision rule), or -- when the resolver cannot name the
    ///   id (the intra-node descriptor race; should effectively never fire) -- a
    ///   synthetic `p_{trailing id chars}` name, logged loudly.
    ///
    /// The assignment is CAS'd into the map table (`ON CONFLICT DO NOTHING` on
    /// the primary key, then read back) so concurrent writers on the SAME
    /// property converge on one winner, while the `UNIQUE ("collection",
    /// "column_name")` constraint surfaces a DIFFERENT property racing for the
    /// same NAME as a unique violation, which reloads the taken-set and
    /// re-dedupes (registered properties only: a system property has exactly
    /// one candidate name, so its unique violation is refused on first sight).
    async fn column_for_key(&self, client: &tokio_postgres::Client, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(column) = self.property_columns.read().unwrap().get(property_id) {
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
                        .unwrap()
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
                        let resolver = self.resolver.read().unwrap().as_ref().and_then(|weak| weak.upgrade());
                        resolver.and_then(|r| r.name_for(&id)).map(|name| naming::sanitize(&name))
                    };
                    let assigned = self.property_columns.read().unwrap();
                    let is_taken = |candidate: &str| {
                        BASE_COLUMNS.contains(&candidate) || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                    };
                    match &seeded {
                        Some(seed) => naming::dedupe(seed, &id, is_taken).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?,
                        None => {
                            warn!(
                                "PostgresBucket({}): catalog cannot name property {}; assigning fallback column (descriptor race?)",
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

            let inserted = client
                .execute(
                    r#"INSERT INTO "_ankurah_postgres_column_map" ("collection", "property_key", "column_name") VALUES ($1, $2, $3)
                       ON CONFLICT ("collection", "property_key") DO NOTHING"#,
                    &[&self.collection_id.as_str(), &property_key, &column],
                )
                .await;
            match inserted {
                Ok(_) => {
                    // Read back the winner: covers both "we inserted" and "a
                    // concurrent writer beat us on the same property key".
                    let row = client
                        .query_one(
                            r#"SELECT "column_name" FROM "_ankurah_postgres_column_map" WHERE "collection" = $1 AND "property_key" = $2"#,
                            &[&self.collection_id.as_str(), &property_key],
                        )
                        .await
                        .map_err(|err| MutationError::UpdateFailed(Box::new(err)))?;
                    let winner: String = row.get(0);
                    self.property_columns.write().unwrap().insert(property_id.clone(), winner.clone());
                    return Ok(winner);
                }
                Err(err) if error_kind(&err) == ErrorKind::UniqueViolation => {
                    // A different property owns this column name durably.
                    // Refresh the taken-set either way.
                    self.load_column_map(client).await.map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
                    if matches!(property_id, PropertyId::System { .. }) {
                        // A system property has exactly one candidate name, so
                        // retrying would recompute the same name; refuse now,
                        // naming the identity that owns the column.
                        let owner = self
                            .property_columns
                            .read()
                            .unwrap()
                            .iter()
                            .find(|(other, col)| other != &property_id && col.as_str() == column)
                            .map(|(other, _)| format!("property {}", property_key_text(other)))
                            .unwrap_or_else(|| "another property".to_string());
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!(
                                "system property {} maps to column {:?}, which is already assigned to {}",
                                property_key,
                                column,
                                owner
                            )
                            .into(),
                        ));
                    }
                    // Registered property: re-dedupe against the refreshed set.
                    continue;
                }
                Err(err) => return Err(MutationError::UpdateFailed(Box::new(err))),
            }
        }
        Err(MutationError::UpdateFailed(
            anyhow::anyhow!("could not assign a column for property {} after repeated collisions", property_key).into(),
        ))
    }

    /// Returns the last predicate that spilled to post-filtering (debug builds only).
    ///
    /// Use this in tests to verify queries are fully pushed down to PostgreSQL:
    /// ```rust,ignore
    /// let spilled = bucket.last_spilled_predicate();
    /// assert!(spilled.is_none(), "Expected full pushdown, but got spill: {:?}", spilled);
    /// ```
    #[cfg(debug_assertions)]
    pub fn last_spilled_predicate(&self) -> Option<ankql::ast::Predicate> { self.last_spilled_predicate.read().unwrap().clone() }

    /// Rebuild the cache of columns in the table.
    pub async fn rebuild_columns_cache(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        debug!("PostgresBucket({}).rebuild_columns_cache", self.collection_id);
        let column_query =
            r#"SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_catalog = $1 AND table_name = $2;"#
                .to_string();
        let mut new_columns = Vec::new();
        debug!("Querying existing columns: {:?}, [{:?}, {:?}]", column_query, &self.schema, &self.collection_id.as_str());
        let rows = client
            .query(&column_query, &[&self.schema, &self.collection_id.as_str()])
            .await
            .map_err(|err| StateError::DDLError(Box::new(err)))?;
        for row in rows {
            let is_nullable: String = row.get("is_nullable");
            new_columns.push(PostgresColumn {
                name: row.get("column_name"),
                is_nullable: is_nullable.eq("YES"),
                data_type: row.get("data_type"),
            })
        }

        let mut columns = self.columns.write().unwrap();
        *columns = new_columns;
        drop(columns);

        Ok(())
    }

    pub fn existing_columns(&self) -> Vec<String> {
        let columns = self.columns.read().unwrap();
        columns.iter().map(|column| column.name.clone()).collect()
    }

    pub fn column(&self, column_name: &String) -> Option<PostgresColumn> {
        let columns = self.columns.read().unwrap();
        columns.iter().find(|column| column.name == *column_name).cloned()
    }

    pub fn has_column(&self, column_name: &String) -> bool { self.column(column_name).is_some() }

    pub async fn create_event_table(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        let create_query = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"(
                "id" character(43) PRIMARY KEY,
                "entity_id" character(22),
                "operations" bytea,
                "parent" character(43)[],
                "attestations" bytea
            )"#,
            self.event_table()
        );

        debug!("{create_query}");
        client.execute(&create_query, &[]).await.map_err(|e| StateError::DDLError(Box::new(e)))?;
        Ok(())
    }

    pub async fn create_state_table(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        let create_query = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"(
                "id" character(22) PRIMARY KEY,
                "state_buffer" BYTEA,
                "head" character(43)[],
                "attestations" BYTEA[]
            )"#,
            self.state_table()
        );

        debug!("{create_query}");
        match client.execute(&create_query, &[]).await {
            Ok(_) => Ok(()),
            Err(err) => {
                // Log full error details for debugging
                if let Some(db_err) = err.as_db_error() {
                    error!("PostgresBucket({}).create_state_table error: {} (code: {:?})", self.collection_id, db_err, db_err.code());
                } else {
                    error!("PostgresBucket({}).create_state_table error: {:?}", self.collection_id, err);
                }
                Err(StateError::DDLError(Box::new(err)))
            }
        }
    }

    pub async fn add_missing_columns(
        &self,
        client: &mut tokio_postgres::Client,
        missing: Vec<(String, &'static str)>, // column name, datatype
    ) -> Result<(), StateError> {
        if missing.is_empty() {
            return Ok(());
        }

        // Acquire advisory lock to serialize DDL operations for this collection
        let lock_key = acquire_ddl_lock(client, self.collection_id.as_str()).await?;

        let result = async {
            // Re-check columns after acquiring lock (another session may have added them)
            self.rebuild_columns_cache(client).await?;

            for (column, datatype) in missing {
                if Postgres::sane_name(&column) && !self.has_column(&column) {
                    let alter_query = format!(r#"ALTER TABLE "{}" ADD COLUMN "{}" {}"#, self.state_table(), column, datatype);
                    info!("PostgresBucket({}).add_missing_columns: {}", self.collection_id, alter_query);
                    match client.execute(&alter_query, &[]).await {
                        Ok(_) => {}
                        Err(err) => {
                            // Log full error details for debugging
                            if let Some(db_err) = err.as_db_error() {
                                warn!(
                                    "Error adding column {} to table {}: {} (code: {:?})",
                                    column,
                                    self.state_table(),
                                    db_err,
                                    db_err.code()
                                );
                            } else {
                                warn!("Error adding column {} to table {}: {:?}", column, self.state_table(), err);
                            }
                            self.rebuild_columns_cache(client).await?;
                            return Err(StateError::DDLError(Box::new(err)));
                        }
                    }
                }
            }

            self.rebuild_columns_cache(client).await?;
            Ok(())
        }
        .await;

        // Always release the lock
        release_ddl_lock(client, lock_key).await?;

        result
    }
}

#[async_trait]
impl StorageCollection for PostgresBucket {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let state_buffers = bincode::serialize(&state.payload.state.state_buffers)?;
        let attestations: Vec<Vec<u8>> = state.attestations.iter().map(bincode::serialize).collect::<Result<Vec<_>, _>>()?;
        let id = state.payload.entity_id;

        // Ensure head is not empty for new records
        if state.payload.state.head.is_empty() {
            warn!("Warning: Empty head detected for entity {}", id);
        }

        let mut client = self.pool.get().await.map_err(|err| MutationError::General(err.into()))?;

        let mut columns: Vec<String> = vec!["id".to_owned(), "state_buffer".to_owned(), "head".to_owned(), "attestations".to_owned()];
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        params.push(&id);
        params.push(&state_buffers);
        params.push(&state.payload.state.head);
        params.push(&attestations);

        let mut materialized: Vec<(String, Option<PGValue>)> = Vec::new();
        let mut seen_properties = std::collections::HashSet::new();

        // Process property values directly from state buffers
        for (name, state_buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            for (property_id, value) in backend.property_values() {
                // Every property -- registered or system -- addresses its
                // column through the engine-owned durable map, keyed by durable
                // PropertyId and assigned on first sight (see column_for_key).
                let column = self.column_for_key(&client, &property_id).await?;
                if !seen_properties.insert(column.clone()) {
                    // Same column from another backend of this entity: first
                    // occurrence wins (cross-backend same-name is pre-existing
                    // pathology; same-collection id collisions were deduped at
                    // column assignment and cannot land here).
                    continue;
                }

                let pg_value: Option<PGValue> = value.map(|value| value.into());
                if !self.has_column(&column) {
                    // We don't have the column yet and we know the type.
                    if let Some(ref pg_value) = pg_value {
                        self.add_missing_columns(&mut client, vec![(column.clone(), pg_value.postgres_type())]).await?;
                    } else {
                        // The column doesn't exist yet and we don't have a value.
                        // This means the entire column is already null/none so we
                        // don't need to set anything.
                        continue;
                    }
                }

                materialized.push((column.clone(), pg_value));
            }
        }

        for (name, parameter) in &materialized {
            columns.push(name.clone());

            match &parameter {
                Some(value) => match value {
                    PGValue::CharacterVarying(string) => params.push(string),
                    PGValue::SmallInt(number) => params.push(number),
                    PGValue::Integer(number) => params.push(number),
                    PGValue::BigInt(number) => params.push(number),
                    PGValue::DoublePrecision(float) => params.push(float),
                    PGValue::Bytea(bytes) => params.push(bytes),
                    PGValue::Boolean(bool) => params.push(bool),
                    PGValue::Jsonb(json_val) => params.push(json_val),
                },
                None => params.push(&UntypedNull),
            }
        }
        let columns_str = columns.iter().map(|name| format!("\"{}\"", name)).collect::<Vec<String>>().join(", ");
        let values_str = params.iter().enumerate().map(|(index, _)| format!("${}", index + 1)).collect::<Vec<String>>().join(", ");
        let columns_update_str = columns
            .iter()
            .enumerate()
            .skip(1) // Skip "id"
            .map(|(index, name)| format!("\"{}\" = ${}", name, index + 1))
            .collect::<Vec<String>>()
            .join(", ");

        // be careful with sql injection via bucket name
        let query = format!(
            r#"WITH old_state AS (
                SELECT "head" FROM "{0}" WHERE "id" = $1
            )
            INSERT INTO "{0}"({1}) VALUES({2})
            ON CONFLICT("id") DO UPDATE SET {3}
            RETURNING (SELECT "head" FROM old_state) as old_head"#,
            self.state_table(),
            columns_str,
            values_str,
            columns_update_str
        );

        debug!("PostgresBucket({}).set_state: {}", self.collection_id, query);
        let mut created_table = false;
        let row = loop {
            match client.query_one(&query, params.as_slice()).await {
                Ok(row) => break row,
                Err(err) => {
                    let kind = error_kind(&err);
                    if let ErrorKind::UndefinedTable { table } = kind {
                        if table == self.state_table() && !created_table {
                            self.create_state_table(&mut client).await?;
                            created_table = true;
                            continue; // retry exactly once
                        }
                    }
                    return Err(StateError::DDLError(Box::new(err)).into());
                }
            }
        };

        // If this is a new entity (no old_head), or if the heads are different, return true
        let old_head: Option<Clock> = row.get("old_head");
        let changed = match old_head {
            None => true, // New entity
            Some(old_head) => old_head != state.payload.state.head,
        };

        debug!("PostgresBucket({}).set_state: Changed: {}", self.collection_id, changed);
        Ok(changed)
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        // be careful with sql injection via bucket name
        let query = format!(r#"SELECT "id", "state_buffer", "head", "attestations" FROM "{}" WHERE "id" = $1"#, self.state_table());

        let mut client = match self.pool.get().await {
            Ok(client) => client,
            Err(err) => {
                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        debug!("PostgresBucket({}).get_state: {}", self.collection_id, query);
        let rows = match client.query(&query, &[&id]).await {
            Ok(rows) => rows,
            Err(err) => {
                let kind = error_kind(&err);
                if let ErrorKind::UndefinedTable { table } = kind {
                    if table == self.state_table() {
                        self.create_state_table(&mut client).await.map_err(|e| RetrievalError::StorageError(e.into()))?;
                        return Err(RetrievalError::EntityNotFound(id));
                    }
                }
                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        let row = match rows.into_iter().next() {
            Some(row) => row,
            None => return Err(RetrievalError::EntityNotFound(id)),
        };

        debug!("PostgresBucket({}).get_state: Row: {:?}", self.collection_id, row);
        let row_id: EntityId = row.try_get("id").map_err(RetrievalError::storage)?;
        assert_eq!(row_id, id);

        let serialized_buffers: Vec<u8> = row.try_get("state_buffer").map_err(RetrievalError::storage)?;
        let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&serialized_buffers).map_err(RetrievalError::storage)?;
        let head: Clock = row.try_get("head").map_err(RetrievalError::storage)?;
        let attestation_bytes: Vec<Vec<u8>> = row.try_get("attestations").map_err(RetrievalError::storage)?;
        let attestations = attestation_bytes
            .into_iter()
            .map(|bytes| bincode::deserialize(&bytes))
            .collect::<Result<Vec<Attestation>, _>>()
            .map_err(RetrievalError::storage)?;

        Ok(Attested {
            payload: EntityState {
                entity_id: id,
                model: self.model_id()?,
                state: State { state_buffers: StateBuffers(state_buffers), head },
            },
            attestations: AttestationSet(attestations),
        })
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        debug!("fetch_states: {:?}", selection);
        // This engine can only address a property by identity. Refuse a
        // selection that still carries a name.
        selection.check()?;
        let mut client = self.pool.get().await.map_err(|err| RetrievalError::StorageError(Box::new(err)))?;

        // Resolve, through this engine's own durable map, the column for every
        // property the selection references (assigned on write, sticky under
        // rename). There is NO name fallback: a property with no assigned column
        // -- or whose column was never materialized -- is ABSENT (evaluates
        // NULL, folded below), never re-derived from a raw name. The SQL builder
        // then translates each surviving identity to its column at emit time,
        // and the in-memory post-filter keeps reading by identity, so a rename
        // is harmless end to end.
        let assigned = self.property_columns.read().unwrap().clone();
        let referenced = selection.referenced_properties();

        // If the snapshot lacks a referenced property, reload the durable map
        // once before folding it to absent: another handle to this collection
        // (each collection() call builds a fresh bucket) may have assigned the
        // column after this snapshot was loaded. A shared database accessed by
        // multiple processes is not the supported sync path (nodes sync via
        // the protocol); this reload is a consistency courtesy, not a
        // multi-process contract.
        let assigned = if referenced.iter().any(|p| !assigned.contains_key(p)) {
            self.load_column_map(&client).await.map_err(|e| RetrievalError::StorageError(e.into()))?;
            self.property_columns.read().unwrap().clone()
        } else {
            assigned
        };

        // Refresh the schema cache if we reference an assigned column we have not
        // yet seen materialized (set_state adds columns on demand).
        let cached = self.existing_columns();
        let need_refresh = referenced.iter().any(|p| assigned.get(p).map_or(false, |c| !cached.contains(c)));
        if need_refresh {
            debug!("PostgresBucket({}).fetch_states: unseen assigned column referenced, refreshing schema cache", self.collection_id);
            self.rebuild_columns_cache(&mut client).await.map_err(|e| RetrievalError::StorageError(e.into()))?;
        }
        let existing = self.existing_columns();

        // A property is absent when it has no assigned column, or its assigned
        // column is not (yet) a physical column in the state table. Matched by
        // durable identity, so a rename never mis-targets which one is absent.
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|p| assigned.get(p).map_or(true, |c| !existing.contains(c))).collect();
        if !absent.is_empty() {
            debug!("PostgresBucket({}).fetch_states: absent properties {:?}, treating as NULL", self.collection_id, absent);
        }
        let effective_selection = if absent.is_empty() { selection.clone() } else { selection.assume_null(&absent) };

        // Split predicate into parts we can pushdown to PostgreSQL vs post-filter in Rust
        let split = sql_builder::split_predicate_for_postgres(&effective_selection.predicate);
        let needs_post_filter = split.needs_post_filter();
        let remaining_predicate = split.remaining_predicate; // Cache before moving sql_predicate
        debug!(
            "PostgresBucket({}).fetch_states: SQL predicate: {:?}, remaining: {:?}, needs_post_filter: {}",
            self.collection_id, split.sql_predicate, remaining_predicate, needs_post_filter
        );

        // Track spilled predicate for test assertions (debug builds only)
        #[cfg(debug_assertions)]
        {
            let mut spilled = self.last_spilled_predicate.write().unwrap();
            *spilled = if needs_post_filter { Some(remaining_predicate.clone()) } else { None };
        }

        // Track spilled predicate for test assertions (debug builds only)
        #[cfg(debug_assertions)]
        {
            let spilled = if needs_post_filter { Some(remaining_predicate.clone()) } else { None };
            *self.last_spilled_predicate.write().unwrap() = spilled;
        }

        // Build SQL with only the pushdown-capable predicate
        let sql_selection = ankql::ast::Selection {
            predicate: split.sql_predicate,
            order_by: effective_selection.order_by.clone(),
            limit: if needs_post_filter {
                None // Can't limit in SQL if we need to post-filter (would drop valid results)
            } else {
                effective_selection.limit
            },
        };

        let mut results = Vec::new();
        let mut builder = SqlBuilder::with_fields(vec!["id", "state_buffer", "head", "attestations"]);
        builder.table_name(self.state_table());
        // The builder translates each resolved identity to its assigned column
        // via `assigned`; every identity that survives the split has one (absent
        // properties were folded to NULL above).
        builder.column_map(assigned);
        builder.selection(&sql_selection)?;

        let (sql, args) = builder.build()?;
        debug!("PostgresBucket({}).fetch_states: SQL: {} with args: {:?}", self.collection_id, sql, args);

        let stream = match client.query_raw(&sql, args).await {
            Ok(stream) => stream,
            Err(err) => {
                let kind = error_kind(&err);
                if let ErrorKind::UndefinedTable { table } = kind {
                    if table == self.state_table() {
                        // Table doesn't exist yet, return empty results
                        return Ok(Vec::new());
                    }
                }
                return Err(RetrievalError::StorageError(err.into()));
            }
        };
        pin_mut!(stream);

        while let Some(row) = stream.try_next().await.map_err(RetrievalError::storage)? {
            let id: EntityId = row.try_get(0).map_err(RetrievalError::storage)?;
            let state_buffer: Vec<u8> = row.try_get(1).map_err(RetrievalError::storage)?;
            let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&state_buffer).map_err(RetrievalError::storage)?;
            let head: Clock = row.try_get("head").map_err(RetrievalError::storage)?;
            let attestation_bytes: Vec<Vec<u8>> = row.try_get("attestations").map_err(RetrievalError::storage)?;
            let attestations = attestation_bytes
                .into_iter()
                .map(|bytes| bincode::deserialize(&bytes))
                .collect::<Result<Vec<Attestation>, _>>()
                .map_err(RetrievalError::storage)?;

            results.push(Attested {
                payload: EntityState {
                    entity_id: id,
                    model: self.model_id()?,
                    state: State { state_buffers: StateBuffers(state_buffers), head },
                },
                attestations: AttestationSet(attestations),
            });
        }

        // Post-filter results if we have remaining predicate that couldn't be pushed down
        let results = if needs_post_filter {
            debug!(
                "PostgresBucket({}).fetch_states: Post-filtering {} results with remaining predicate",
                self.collection_id,
                results.len()
            );
            let filtered = post_filter_states(&results, &remaining_predicate, &self.collection_id);

            // Apply limit after post-filter if needed
            if let Some(limit) = effective_selection.limit {
                filtered.into_iter().take(limit as usize).collect()
            } else {
                filtered
            }
        } else {
            results
        };

        Ok(results)
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError> {
        let operations = bincode::serialize(&entity_event.payload.operations)?;
        let attestations = bincode::serialize(&entity_event.attestations)?;

        let query = format!(
            r#"INSERT INTO "{0}"("id", "entity_id", "operations", "parent", "attestations") VALUES($1, $2, $3, $4, $5)
               ON CONFLICT ("id") DO NOTHING"#,
            self.event_table(),
        );

        let mut client = self.pool.get().await.map_err(|err| MutationError::General(err.into()))?;
        debug!("PostgresBucket({}).add_event: {}", self.collection_id, query);
        let mut created_table = false;
        let affected = loop {
            match client
                .execute(
                    &query,
                    &[
                        &entity_event.payload.id(),
                        &entity_event.payload.entity_id,
                        &operations,
                        &entity_event.payload.parent,
                        &attestations,
                    ],
                )
                .await
            {
                Ok(affected) => break affected,
                Err(err) => {
                    let kind = error_kind(&err);
                    if let ErrorKind::UndefinedTable { table } = kind {
                        if table == self.event_table() && !created_table {
                            self.create_event_table(&mut client).await?;
                            created_table = true;
                            continue; // retry exactly once
                        }
                    }
                    error!("PostgresBucket({}).add_event: Error: {:?}", self.collection_id, err);
                    return Err(StateError::DMLError(Box::new(err)).into());
                }
            }
        };

        Ok(affected > 0)
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        let query = format!(
            r#"SELECT "id", "entity_id", "operations", "parent", "attestations" FROM "{0}" WHERE "id" = ANY($1)"#,
            self.event_table(),
        );

        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        let rows = match client.query(&query, &[&event_ids]).await {
            Ok(rows) => rows,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } if table == self.event_table() => return Ok(Vec::new()),
                    _ => return Err(RetrievalError::storage(err)),
                }
            }
        };

        let mut events = Vec::new();
        for row in rows {
            let entity_id: EntityId = row.try_get("entity_id").map_err(RetrievalError::storage)?;
            let operations: OperationSet = row.try_get("operations").map_err(RetrievalError::storage)?;
            let parent: Clock = row.try_get("parent").map_err(RetrievalError::storage)?;
            let attestations_binary: Vec<u8> = row.try_get("attestations").map_err(RetrievalError::storage)?;
            let attestations: Vec<Attestation> = bincode::deserialize(&attestations_binary).map_err(RetrievalError::storage)?;

            let event = Attested {
                payload: Event { model: self.model_id()?, entity_id, operations, parent },
                attestations: AttestationSet(attestations),
            };
            events.push(event);
        }
        Ok(events)
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, ankurah_core::error::RetrievalError> {
        let query =
            format!(r#"SELECT "id", "operations", "parent", "attestations" FROM "{0}" WHERE "entity_id" = $1"#, self.event_table(),);

        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        debug!("PostgresBucket({}).get_events: {}", self.collection_id, query);
        let rows = match client.query(&query, &[&entity_id]).await {
            Ok(rows) => rows,
            Err(err) => {
                let kind = error_kind(&err);
                if let ErrorKind::UndefinedTable { table } = kind {
                    if table == self.event_table() {
                        return Ok(Vec::new());
                    }
                }

                return Err(RetrievalError::storage(err));
            }
        };

        let mut events = Vec::new();
        for row in rows {
            // let event_id: EventId = row.try_get("id").map_err(|err| RetrievalError::storage(err))?;
            let operations_binary: Vec<u8> = row.try_get("operations").map_err(RetrievalError::storage)?;
            let operations = bincode::deserialize(&operations_binary).map_err(RetrievalError::storage)?;
            let parent: Clock = row.try_get("parent").map_err(RetrievalError::storage)?;
            let attestations_binary: Vec<u8> = row.try_get("attestations").map_err(RetrievalError::storage)?;
            let attestations: Vec<Attestation> = bincode::deserialize(&attestations_binary).map_err(RetrievalError::storage)?;

            events.push(Attested {
                payload: Event { model: self.model_id()?, entity_id, operations, parent },
                attestations: AttestationSet(attestations),
            });
        }

        Ok(events)
    }
}

// Some hacky shit because rust-postgres doesn't let us ask for the error kind
// TODO: remove this when https://github.com/sfackler/rust-postgres/pull/1185
//       gets merged
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorKind {
    RowCount,
    UndefinedTable { table: String },
    UndefinedColumn { table: Option<String>, column: String },
    UniqueViolation,
    Unknown,
    PostgresError(String),
}

pub fn error_kind(err: &tokio_postgres::Error) -> ErrorKind {
    let string = err.as_db_error().map(|e| e.message()).unwrap_or_default().trim().to_owned();
    let _db_error = err.as_db_error();
    let sql_code = err.code().cloned();

    // Check the error's Display string for RowCount errors (client-side, not db error)
    let err_string = err.to_string();
    if err_string.contains("query returned an unexpected number of rows") || string == "query returned an unexpected number of rows" {
        return ErrorKind::RowCount;
    }

    // Useful for adding new errors
    // error!("postgres error: {:?}", err);
    // error!("db_err: {:?}", err.as_db_error());
    // error!("sql_code: {:?}", err.code());
    // error!("err: {:?}", err);
    // error!("err: {:?}", err.to_string());
    debug!("postgres error: {:?}", err);

    let quote_indices = |s: &str| {
        let mut quotes = Vec::new();
        for (index, char) in s.char_indices() {
            if char == '"' {
                quotes.push(index)
            }
        }
        quotes
    };

    match sql_code {
        Some(SqlState::UNDEFINED_TABLE) => {
            // relation "album" does not exist
            let quotes = quote_indices(&string);
            if quotes.len() >= 2 {
                let table = &string[quotes[0] + 1..quotes[1]];
                ErrorKind::UndefinedTable { table: table.to_owned() }
            } else {
                ErrorKind::PostgresError(string.clone())
            }
        }
        Some(SqlState::UNDEFINED_COLUMN) => {
            // Handle both formats:
            // "column "name" of relation "album" does not exist"
            // "column "status" does not exist"
            let quotes = quote_indices(&string);
            if quotes.len() >= 2 {
                let column = string[quotes[0] + 1..quotes[1]].to_owned();

                let table = if quotes.len() >= 4 {
                    // Full format with table name
                    Some(string[quotes[2] + 1..quotes[3]].to_owned())
                } else {
                    // Short format without table name
                    None
                };

                ErrorKind::UndefinedColumn { table, column }
            } else {
                ErrorKind::PostgresError(string.clone())
            }
        }
        Some(SqlState::UNIQUE_VIOLATION) => ErrorKind::UniqueViolation,
        _ => ErrorKind::Unknown,
    }
}

#[allow(unused)]
pub struct MissingMaterialized {
    pub name: String,
}

use bytes::BytesMut;
use tokio_postgres::types::{to_sql_checked, IsNull, Type};

use crate::sql_builder::SqlBuilder;

/// Post-filter EntityStates using a predicate that couldn't be pushed to SQL.
///
/// This is the escape hatch for predicates that PostgreSQL can't handle natively,
/// such as complex JSON traversals or future features like Ref traversal.
fn post_filter_states(
    states: &[Attested<EntityState>],
    predicate: &ankql::ast::Predicate,
    collection_id: &CollectionId,
) -> Vec<Attested<EntityState>> {
    use ankurah_core::entity::TemporaryEntity;
    use ankurah_core::selection::filter::evaluate_predicate;

    states
        .iter()
        .filter(|attested| {
            // Create a TemporaryEntity for filtering (implements Filterable)
            match TemporaryEntity::new(attested.payload.entity_id, collection_id.clone(), &attested.payload.state) {
                Ok(temp_entity) => {
                    // Evaluate the predicate
                    match evaluate_predicate(&temp_entity, predicate) {
                        Ok(true) => true,
                        Ok(false) => false,
                        Err(e) => {
                            warn!("Post-filter evaluation error for entity {}: {}", attested.payload.entity_id, e);
                            false // Exclude entities that fail evaluation
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create TemporaryEntity for post-filtering {}: {}", attested.payload.entity_id, e);
                    false // Exclude entities we can't evaluate
                }
            }
        })
        .cloned()
        .collect()
}

#[derive(Debug)]
struct UntypedNull;

impl ToSql for UntypedNull {
    fn to_sql(&self, _ty: &Type, _out: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> { Ok(IsNull::Yes) }

    fn accepts(_ty: &Type) -> bool {
        true // Accept all types
    }

    to_sql_checked!();
}
