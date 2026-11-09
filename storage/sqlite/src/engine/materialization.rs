use ankurah_core::{
    error::{MutationError, RetrievalError},
    property::backend::backend_from_string,
    schema::CatalogResolver,
};
use ankurah_proto::{Attested, EntityState, ModelId, PropertyId};
use ankurah_storage_common::naming;
use rusqlite::{params_from_iter, Connection};
use std::{collections::BTreeMap, sync::Arc};
use tracing::debug;

use super::{SqliteStorageEngine, COLUMN_MAP_TABLE};
use crate::{
    connection::{PooledConnection, SqliteConnectionManager},
    error::SqliteError,
    value::SqliteValue,
};

fn create_materialization_table(conn: &Connection, table_name: &str) -> Result<(), SqliteError> {
    let query = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}"(
            "id" TEXT PRIMARY KEY
        )"#,
        table_name
    );
    debug!("Creating materialization table: {}", query);
    conn.execute(&query, [])?;
    Ok(())
}

/// The durable, serialized address of a property: the JSON form of its
/// [`PropertyId`], stored as the `property_key` text in `_ankurah_sqlite_column_map`.
/// The write side (the `PropertyId` a backend's `property_values()` yields) and
/// the read side (a `PropertyId` straight off the resolved AST) both go through
/// this, so a column assigned on write is found by the byte-identical key on read.
fn property_key_text(id: &PropertyId) -> String { serde_json::to_string(id).expect("PropertyId always serializes to JSON") }

/// Create the engine-wide durable property-to-column map table. One table for
/// the whole database; rows are scoped by model, so deduplication is per
/// materialization. `property_key` is a serialized
/// [`PropertyId`] (JSON TEXT, see [`property_key_text`]) -- the same durable
/// address the read side resolves against, so registered AND system properties
/// alike are addressed by identity, never by a raw name. `_ankurah_sqlite_` is
/// the reserved prefix that shields this internal table from materialization
/// name collisions.
///
/// `CREATE TABLE IF NOT EXISTS` is idempotent, so this needs no DDL lock,
/// exactly like the canonical-table creators above.
fn create_column_map_table(conn: &Connection) -> Result<(), SqliteError> {
    let query = r#"CREATE TABLE IF NOT EXISTS "_ankurah_sqlite_column_map"(
            "model_key" BLOB NOT NULL,
            "property_key" TEXT NOT NULL,
            "column_name" TEXT NOT NULL,
            PRIMARY KEY ("model_key", "property_key"),
            UNIQUE ("model_key", "column_name")
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

/// Private handle for one model's SQLite query materialization.
pub(super) struct Materialization {
    pool: bb8::Pool<SqliteConnectionManager>,
    model_key: Vec<u8>,
    materialization_table_name: String,
    columns: Arc<std::sync::RwLock<Vec<SqliteColumn>>>,
    ddl_lock: Arc<tokio::sync::Mutex<()>>,
    /// Optional labels for first-use column assignment; does not keep the node alive.
    resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    /// This model's slice of the engine-owned durable property-to-column
    /// map (the `_ankurah_sqlite_column_map` table), cached and keyed by durable
    /// [`PropertyId`]. The map -- not the display name -- is what addresses a
    /// property's column once assigned: renames never move columns, collisions
    /// were deduped at assignment. Always carries the `PropertyId::Id -> "id"`
    /// pin so a read of the primary key is a uniform map hit.
    property_columns: Arc<std::sync::RwLock<BTreeMap<PropertyId, String>>>,
}

/// A projection whose table and columns already exist.
#[derive(Clone)]
pub(super) struct PreparedMaterialization {
    entity_id: String,
    table: String,
    materialized: Vec<(String, rusqlite::types::Value, bool)>,
}

impl PreparedMaterialization {
    pub(super) fn write(&self, transaction: &rusqlite::Transaction<'_>) -> Result<(), SqliteError> {
        let mut columns = vec!["id".to_owned()];
        let mut values = vec![rusqlite::types::Value::Text(self.entity_id.clone())];
        let mut placeholder_is_jsonb = vec![false];
        for (name, value, is_jsonb) in &self.materialized {
            columns.push(name.clone());
            values.push(value.clone());
            placeholder_is_jsonb.push(*is_jsonb);
        }
        let columns_str = columns.iter().map(|column| format!(r#""{column}""#)).collect::<Vec<_>>().join(", ");
        let placeholders =
            placeholder_is_jsonb.iter().map(|is_jsonb| if *is_jsonb { "jsonb(?)" } else { "?" }).collect::<Vec<_>>().join(", ");
        // Replace the whole projection so properties absent from this state become NULL.
        let query = format!(r#"INSERT OR REPLACE INTO "{}"({}) VALUES({})"#, self.table, columns_str, placeholders);
        debug!("materialize_state query: {}", query);
        transaction.execute(&query, params_from_iter(values.iter()))?;
        Ok(())
    }
}

/// Fixed columns of every materialization table.
const BASE_COLUMNS: &[&str] = &["id"];

impl Materialization {
    /// Open this model's materialization and restore its persistent column assignments.
    pub(super) async fn open(engine: &SqliteStorageEngine, model: &ModelId) -> Result<Self, RetrievalError> {
        let table_name = engine.table_for_model(model).await?;
        let model_key = bincode::serialize(model).map_err(RetrievalError::storage)?;
        let conn = engine.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;
        engine.ensure_shared_tables(&conn).await?;
        let materialization = Self {
            pool: engine.pool.clone(),
            model_key: model_key.clone(),
            materialization_table_name: table_name.clone(),
            columns: Arc::new(std::sync::RwLock::new(Vec::new())),
            ddl_lock: Arc::new(tokio::sync::Mutex::new(())),
            resolver: engine.resolver.clone(),
            property_columns: Arc::new(std::sync::RwLock::new(BTreeMap::new())),
        };
        let id_pin_key = property_key_text(&PropertyId::Id);
        conn.with_connection(move |c| {
            create_materialization_table(c, &table_name)?;
            create_column_map_table(c)?;
            c.execute(
                &format!(
                    r#"INSERT OR IGNORE INTO "{COLUMN_MAP_TABLE}"
                       ("model_key", "property_key", "column_name") VALUES (?, ?, 'id')"#
                ),
                rusqlite::params![model_key, id_pin_key],
            )?;
            Ok(())
        })
        .await?;
        materialization.rebuild_columns_cache(&conn).await?;
        materialization.load_column_map(&conn).await?;
        Ok(materialization)
    }

    pub(super) fn table(&self) -> &str { &self.materialization_table_name }

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

    /// Load this model's property-to-column assignments into the cache. The
    /// `property_key` column is a serialized [`PropertyId`] (JSON TEXT, see
    /// [`property_key_text`]); a row we cannot parse refuses the materialization:
    /// a hidden assignment would let that property's next cache miss claim a
    /// fresh column and silently split its data, so the map loads whole or
    /// not at all. The `PropertyId::Id -> "id"` pin is seeded last so no row
    /// can remap the primary key.
    async fn load_column_map(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
        let model_key = self.model_key.clone();
        let rows: Vec<(String, String)> = conn
            .with_connection(move |c| {
                let mut stmt =
                    c.prepare(r#"SELECT "property_key", "column_name" FROM "_ankurah_sqlite_column_map" WHERE "model_key" = ?"#)?;
                let rows =
                    stmt.query_map([&model_key], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
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
                        "property_key {:?} in the column map for materialization {}: {}",
                        property_key, self.materialization_table_name, e
                    )));
                }
            }
        }
        // The `id` pseudo-property is always the reserved primary-key column.
        map.insert(PropertyId::Id, "id".to_string());
        *self.property_columns.write().expect("RwLock poisoned") = map;
        Ok(())
    }

    /// Return the materialized column assigned to a durable [`PropertyId`],
    /// creating and recording an assignment on a cache miss. Registered
    /// properties seed a readable column name from the catalog and append an
    /// identity suffix when needed. System properties use their fixed built-in
    /// names and reject collisions. Unique constraints and read-back make
    /// concurrent callers converge on one assignment.
    pub(super) async fn column_for_property(&self, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(column) = self.property_columns.read().expect("RwLock poisoned").get(property_id) {
            return Ok(column.clone());
        }

        let property_key = property_key_text(property_id);

        let label = match property_id {
            PropertyId::EntityId(_) => {
                let resolver = self.resolver.read().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                match resolver {
                    Some(resolver) => resolver.get_property_label(property_id).await,
                    None => None,
                }
            }
            _ => None,
        };
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
                PropertyId::System(property) => {
                    let candidate = naming::sanitize(property.as_str());
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
                    let id = *ulid;
                    let assigned = self.property_columns.read().expect("RwLock poisoned");
                    let is_taken = |candidate: &str| {
                        BASE_COLUMNS.contains(&candidate) || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                    };
                    match label.as_deref() {
                        Some(label) => naming::dedupe(&naming::sanitize(label), &id, is_taken),
                        None => naming::fallback("p", &id, is_taken),
                    }
                    .map_err(|e| MutationError::UpdateFailed(Box::new(e)))?
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
            // (model_key, property_key) primary-key conflict and the
            // (model_key, column_name) uniqueness conflict, so -- unlike
            // postgres, which targets its ON CONFLICT only at the primary key
            // and catches the uniqueness violation as an error -- we distinguish
            // them by the read-back: a row for our key means we (or a concurrent
            // writer on the SAME property) won, converge on it; NO row means the
            // name we chose was already claimed by a DIFFERENT property, so
            // reload the taken-set and re-dedupe.
            let model_key = self.model_key.clone();
            let property_key = property_key.clone();
            let candidate = column.clone();
            let conn = self.pool.get().await.map_err(|error| MutationError::General(error.into()))?;
            let winner: Option<String> = conn
                .with_connection(move |c| {
                    c.execute(
                        r#"INSERT OR IGNORE INTO "_ankurah_sqlite_column_map" ("model_key", "property_key", "column_name") VALUES (?, ?, ?)"#,
                        rusqlite::params![model_key, property_key, candidate],
                    )?;
                    match c.query_row(
                        r#"SELECT "column_name" FROM "_ankurah_sqlite_column_map" WHERE "model_key" = ? AND "property_key" = ?"#,
                        rusqlite::params![model_key, property_key],
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
                    self.load_column_map(&conn).await?;
                    if matches!(property_id, PropertyId::System(_)) {
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

    async fn rebuild_columns_cache(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
        let table_name = self.table().to_owned();
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

        // Another writer may have added these columns since we checked. Serialize the
        // refresh and ALTERs so concurrent writers do not both add the same column.
        let _lock = self.ddl_lock.lock().await;

        self.rebuild_columns_cache(conn).await?;

        let table_name = self.table();
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
    /// Prepare projection DML after assigning all physical names and columns.
    pub(super) async fn prepare_state(&self, state: &Attested<EntityState>) -> Result<PreparedMaterialization, MutationError> {
        let mut values = BTreeMap::new();
        for (name, state_buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            for (property_id, value) in backend.property_values() {
                values.entry(property_id).or_insert(value);
            }
        }

        let mut materialized = Vec::new();
        let mut missing = Vec::new();
        for (property_id, value) in values {
            let Some(value) = value else { continue };
            let column = self.column_for_property(&property_id).await?;
            let value = SqliteValue::from(value);
            if !self.has_column(&column) {
                missing.push((column.clone(), value.sqlite_type()));
            }
            let is_jsonb = value.is_jsonb();
            materialized.push((column, value.to_sql(), is_jsonb));
        }
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;
        self.add_missing_columns(&conn, missing).await?;

        Ok(PreparedMaterialization { entity_id: state.payload.entity_id.to_base64(), table: self.table().to_owned(), materialized })
    }

    /// Resolve the query's properties through this table's persistent column assignments.
    pub(super) async fn query_columns(&self, referenced: &[PropertyId]) -> Result<BTreeMap<PropertyId, String>, RetrievalError> {
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

        // If the snapshot lacks a referenced property, reload the durable map
        // once before folding it to absent: another handle to this
        // materialization may have assigned the column after this snapshot was
        // loaded. Reload the engine-owned durable map so another engine
        // instance using the same database file cannot leave this cache stale.
        let assigned = if referenced.iter().any(|p| !assigned.contains_key(p)) {
            self.load_column_map(&conn).await?;
            self.property_columns.read().expect("RwLock poisoned").clone()
        } else {
            assigned
        };

        // Refresh the schema cache if we reference an assigned column we have
        // not yet seen materialized (projection preparation adds columns on
        // demand).
        let cached = self.existing_columns();
        let need_refresh = referenced.iter().any(|p| assigned.get(p).map_or(false, |c| !cached.contains(c)));
        if need_refresh {
            debug!(
                "Materialization({}).query_columns: unseen assigned column referenced, refreshing schema cache",
                self.materialization_table_name
            );
            self.rebuild_columns_cache(&conn).await?;
        }
        let existing = self.existing_columns();

        Ok(assigned.into_iter().filter(|(property, column)| referenced.contains(property) && existing.contains(column)).collect())
    }
}
