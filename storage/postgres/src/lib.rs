use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use ankurah_core::{
    error::{MutationError, RetrievalError, StateError},
    property::Backends,
    storage::{StorageCollection, StorageEngine},
};
use ankurah_proto::{Attestation, AttestationSet, Attested, EventId, State, StateBuffers};

use futures_util::TryStreamExt;

pub mod predicate;
pub mod value;

use value::PGValue;

use ankurah_proto::{Clock, CollectionId, EntityId, Event};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use tokio_postgres::{error::SqlState, types::ToSql};
use tracing::{debug, error, info, warn};

pub struct Postgres {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
}

impl Postgres {
    pub fn new(pool: bb8::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<Self> { Ok(Self { pool: pool }) }

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
}

#[async_trait]
impl StorageEngine for Postgres {
    type Value = PGValue;

    async fn collection(&self, collection_id: &CollectionId) -> Result<std::sync::Arc<dyn StorageCollection>, RetrievalError> {
        if !Postgres::sane_name(collection_id.as_str()) {
            return Err(RetrievalError::InvalidBucketName);
        }

        let mut client = self.pool.get().await.map_err(|err| RetrievalError::storage(err))?;

        // get the current schema from the database
        let schema = client.query_one("SELECT current_database()", &[]).await.map_err(|err| RetrievalError::storage(err))?;
        let schema = schema.get("current_database");

        let bucket = PostgresBucket {
            pool: self.pool.clone(),
            schema,
            collection_id: collection_id.clone(),
            columns: Arc::new(RwLock::new(Vec::new())),
        };

        // Create tables if they don't exist
        bucket.create_state_table(&mut client).await?;
        bucket.create_event_table(&mut client).await?;
        bucket.rebuild_columns_cache(&mut client).await?;

        Ok(Arc::new(bucket))
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
}

impl PostgresBucket {
    fn state_table(&self) -> String { format!("{}", self.collection_id.as_str()) }

    pub fn event_table(&self) -> String { format!("{}_event", self.collection_id.as_str()) }

    /// Rebuild the cache of columns in the table.
    pub async fn rebuild_columns_cache(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        debug!("PostgresBucket({}).rebuild_columns_cache", self.collection_id);
        let column_query = format!(
            r#"SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_catalog = $1 AND table_name = $2;"#,
        );
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
                "head" character(43)[]
            )"#,
            self.state_table()
        );

        debug!("{create_query}");
        match client.execute(&create_query, &[]).await {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Error: {}", err);
                Err(StateError::DDLError(Box::new(err)))
            }
        }
    }

    pub async fn add_missing_columns(
        &self,
        client: &mut tokio_postgres::Client,
        missing: Vec<(String, &'static str)>, // column name, datatype
    ) -> Result<(), StateError> {
        for (column, datatype) in missing {
            if Postgres::sane_name(&column) {
                let alter_query = format!(r#"ALTER TABLE "{}" ADD COLUMN "{}" {}"#, self.state_table(), column, datatype,);
                info!("PostgresBucket({}).add_missing_columns: {}", self.collection_id, alter_query);
                match client.execute(&alter_query, &[]).await {
                    Ok(_) => {}
                    Err(err) => {
                        warn!("Error adding column: {} to table: {} - rebuilding columns cache", err, self.state_table());
                        self.rebuild_columns_cache(client).await?;
                        return Err(StateError::DDLError(Box::new(err)));
                    }
                }
            }
        }

        self.rebuild_columns_cache(client).await?;
        Ok(())
    }
}

#[async_trait]
impl StorageCollection for PostgresBucket {
    async fn set_state(&self, id: EntityId, state: &State) -> Result<bool, MutationError> {
        let state_buffers = bincode::serialize(&state.state_buffers)?;

        // Ensure head is not empty for new records
        if state.head.is_empty() {
            warn!("Warning: Empty head detected for entity {}", id);
        }

        let mut client = self.pool.get().await.map_err(|err| MutationError::General(err.into()))?;

        let backends = Backends::from_state_buffers(state)?;
        let mut columns: Vec<String> = vec!["id".to_owned(), "state_buffer".to_owned(), "head".to_owned()];
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        params.push(&id);
        params.push(&state_buffers);
        params.push(&state.head);

        let mut materialized: Vec<(String, Option<PGValue>)> = Vec::new();
        for (column, value) in backends.property_values() {
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

        for (name, parameter) in &materialized {
            columns.push(name.clone());

            match &parameter {
                Some(value) => match value {
                    PGValue::CharacterVarying(string) => params.push(string),
                    PGValue::SmallInt(number) => params.push(number),
                    PGValue::Integer(number) => params.push(number),
                    PGValue::BigInt(number) => params.push(number),
                    PGValue::Bytea(bytes) => params.push(bytes),
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
        let row = match client.query_one(&query, params.as_slice()).await {
            Ok(row) => row,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } => {
                        if table == self.state_table() {
                            self.create_state_table(&mut *client).await?;
                            return self.set_state(id, state).await; // retry
                        }
                    }
                    _ => {}
                }

                return Err(StateError::DDLError(Box::new(err)).into());
            }
        };

        // If this is a new entity (no old_head), or if the heads are different, return true
        let old_head: Option<Clock> = row.get("old_head");
        let changed = match old_head {
            None => true, // New entity
            Some(old_head) => old_head != state.head,
        };

        debug!("PostgresBucket({}).set_state: Changed: {}", self.collection_id, changed);
        Ok(changed)
    }

    async fn get_state(&self, id: EntityId) -> Result<State, RetrievalError> {
        // be careful with sql injection via bucket name
        let query = format!(r#"SELECT "id", "state_buffer", "head" FROM "{}" WHERE "id" = $1"#, self.state_table());

        let mut client = match self.pool.get().await {
            Ok(client) => client,
            Err(err) => {
                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        debug!("PostgresBucket({}).get_state: {}", self.collection_id, query);
        let row = match client.query_one(&query, &[&id]).await {
            Ok(row) => row,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::RowCount => {
                        return Err(RetrievalError::NotFound(id));
                    }
                    ErrorKind::UndefinedTable { table } => {
                        if table == self.state_table() {
                            self.create_state_table(&mut client).await.map_err(|e| RetrievalError::StorageError(e.into()))?;
                            return Err(RetrievalError::NotFound(id));
                        }
                    }
                    _ => {}
                }

                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        debug!("PostgresBucket({}).get_state: Row: {:?}", self.collection_id, row);
        let row_id: EntityId = row.try_get("id").map_err(|err| RetrievalError::storage(err))?;
        assert_eq!(row_id, id);

        let serialized_buffers: Vec<u8> = row.try_get("state_buffer").map_err(|err| RetrievalError::storage(err))?;
        let state_buffers: BTreeMap<String, Vec<u8>> =
            bincode::deserialize(&serialized_buffers).map_err(|err| RetrievalError::storage(err))?;
        let head: Clock = row.try_get("head").map_err(|err| RetrievalError::storage(err))?;

        Ok(State { state_buffers: StateBuffers(state_buffers), head })
    }

    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<(EntityId, State)>, RetrievalError> {
        debug!("fetch_states: {:?}", predicate);
        let client = self.pool.get().await.map_err(|err| RetrievalError::StorageError(Box::new(err)))?;

        let mut results = Vec::new();

        let mut ankql_sql = predicate::Sql::new();
        ankql_sql.predicate(&predicate);

        let (sql, args) = ankql_sql.collapse();

        let filtered_query = if !sql.is_empty() {
            format!(r#"SELECT "id", "state_buffer", "head" FROM "{}" WHERE {}"#, self.state_table(), sql,)
        } else {
            format!(r#"SELECT "id", "state_buffer", "head" FROM "{}""#, self.state_table())
        };

        debug!("PostgresBucket({}).fetch_states: SQL: {} with args: {:?}", self.collection_id, filtered_query, args);

        let rows = match client.query_raw(&filtered_query, args).await {
            Ok(stream) => match stream.try_collect::<Vec<_>>().await {
                Ok(rows) => rows,
                Err(err) => return Err(RetrievalError::StorageError(err.into())),
            },
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } => {
                        if table == self.state_table() {
                            // Table doesn't exist yet, return empty results
                            return Ok(Vec::new());
                        }
                    }
                    ErrorKind::UndefinedColumn { table, column } => {
                        // not an error, just didn't write the column yet
                        debug!("Undefined column: {} in table: {:?}, {}", column, table, self.state_table());
                        match table {
                            Some(table) if table == self.state_table() => {
                                // Modify the predicate treating this column as NULL and retry
                                return self.fetch_states(&predicate.assume_null(&[column])).await;
                            }
                            None => {
                                return self.fetch_states(&predicate.assume_null(&[column])).await;
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }

                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        for row in rows {
            let id: EntityId = row.try_get(0).map_err(|err| RetrievalError::storage(err))?;
            let state_buffer: Vec<u8> = row.try_get(1).map_err(|err| RetrievalError::storage(err))?;
            let state_buffers: BTreeMap<String, Vec<u8>> =
                bincode::deserialize(&state_buffer).map_err(|err| RetrievalError::storage(err))?;
            let head: Clock = row.try_get("head").map_err(|err| RetrievalError::storage(err))?;
            let entity_state = State { state_buffers: StateBuffers(state_buffers), head };
            results.push((id, entity_state));
        }

        Ok(results)
    }

    /// Postgres Event Table:
    /// {bucket_name}_event
    /// event_id uuid, // `ID`/`ULID`
    /// entity_id uuid, // `ID`/`ULID`
    /// operations bytea, // `Vec<Operation>`
    /// clock bytea, // `Clock`
    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError> {
        let operations = bincode::serialize(&entity_event.payload.operations)?;
        let attestations = bincode::serialize(&entity_event.attestations)?;

        let query = format!(
            r#"INSERT INTO "{0}"("id", "entity_id", "operations", "parent", "attestations") VALUES($1, $2, $3, $4, $5)"#,
            self.event_table(),
        );

        let mut client = self.pool.get().await.map_err(|err| MutationError::General(err.into()))?;
        debug!("PostgresBucket({}).add_event: {}", self.collection_id, query);
        let affected = match client
            .execute(
                &query,
                &[&entity_event.payload.id(), &entity_event.payload.entity_id, &operations, &entity_event.payload.parent, &attestations],
            )
            .await
        {
            Ok(affected) => affected,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } => {
                        if table == self.event_table() {
                            self.create_event_table(&mut *client).await?;
                            return self.add_event(entity_event).await; // retry
                        }
                    }
                    _ => {
                        error!("PostgresBucket({}).add_event: Error: {:?}", self.collection_id, err);
                    }
                }

                return Err(StateError::DMLError(Box::new(err)).into());
            }
        };

        Ok(affected > 0)
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let query = format!(r#"SELECT "id", "operations", "parent", "attestations" FROM "{0}" WHERE "id" = ANY($1)"#, self.event_table(),);

        let client = self.pool.get().await.map_err(|err| RetrievalError::storage(err))?;
        let row = client.query_one(&query, &[&entity_id, &event_ids]).await.map_err(|err| {
            let kind = error_kind(&err);
            match kind {
                ErrorKind::UndefinedTable { table } if table == self.event_table() => RetrievalError::EventNotFound,
                _ => RetrievalError::storage(err),
            }
        })?;

        let operations_binary: Vec<u8> = row.try_get("operations").map_err(|err| RetrievalError::storage(err))?;
        let operations = bincode::deserialize(&operations_binary).map_err(|err| RetrievalError::storage(err))?;
        let parent: Clock = row.try_get("parent").map_err(|err| RetrievalError::storage(err))?;
        let attestations_binary: Vec<u8> = row.try_get("attestations").map_err(|err| RetrievalError::storage(err))?;
        let attestations: Vec<Attestation> = bincode::deserialize(&attestations_binary).map_err(|err| RetrievalError::storage(err))?;

        let event = Attested {
            payload: Event { collection: self.collection_id.clone(), entity_id: entity_id, operations: operations, parent: parent },
            attestations: AttestationSet(attestations),
        };

        Ok(event)
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, ankurah_core::error::RetrievalError> {
        let query =
            format!(r#"SELECT "id", "operations", "parent", "attestations" FROM "{0}" WHERE "entity_id" = $1"#, self.event_table(),);

        let client = self.pool.get().await.map_err(|err| RetrievalError::storage(err))?;
        debug!("PostgresBucket({}).get_events: {}", self.collection_id, query);
        let rows = match client.query(&query, &[&entity_id]).await {
            Ok(rows) => rows,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } => {
                        if table == self.event_table() {
                            return Ok(Vec::new());
                        }
                    }
                    _ => {}
                }

                return Err(RetrievalError::storage(err));
            }
        };

        let mut events = Vec::new();
        for row in rows {
            // let event_id: EventId = row.try_get("id").map_err(|err| RetrievalError::storage(err))?;
            let operations_binary: Vec<u8> = row.try_get("operations").map_err(|err| RetrievalError::storage(err))?;
            let operations = bincode::deserialize(&operations_binary).map_err(|err| RetrievalError::storage(err))?;
            let parent: Clock = row.try_get("parent").map_err(|err| RetrievalError::storage(err))?;
            let attestations_binary: Vec<u8> = row.try_get("attestations").map_err(|err| RetrievalError::storage(err))?;
            let attestations: Vec<Attestation> = bincode::deserialize(&attestations_binary).map_err(|err| RetrievalError::storage(err))?;

            events.push(Attested {
                payload: Event { collection: self.collection_id.clone(), entity_id: entity_id, operations: operations, parent: parent },
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
    Unknown,
}

pub fn error_kind(err: &tokio_postgres::Error) -> ErrorKind {
    let string = err.to_string().trim().to_owned();
    let _db_error = err.as_db_error();
    let sql_code = err.code().cloned();

    if string == "query returned an unexpected number of rows" {
        return ErrorKind::RowCount;
    }

    // Useful for adding new errors
    // error!("postgres error: {:?}", err);
    // error!("db_err: {:?}", err.as_db_error());
    // error!("sql_code: {:?}", err.code());
    // error!("err: {:?}", err);
    // error!("err: {:?}", err.to_string());

    let quote_indices = |s: &str| {
        let mut quotes = Vec::new();
        for (index, char) in s.char_indices() {
            match char {
                '"' => quotes.push(index),
                _ => {}
            }
        }
        quotes
    };

    match sql_code {
        Some(SqlState::UNDEFINED_TABLE) => {
            // relation "album" does not exist
            let quotes = quote_indices(&string);
            let table = &string[quotes[0] + 1..quotes[1]];
            ErrorKind::UndefinedTable { table: table.to_owned() }
        }
        Some(SqlState::UNDEFINED_COLUMN) => {
            // Handle both formats:
            // "column "name" of relation "album" does not exist"
            // "column "status" does not exist"
            let quotes = quote_indices(&string);
            let column = string[quotes[0] + 1..quotes[1]].to_owned();

            let table = if quotes.len() >= 4 {
                // Full format with table name
                Some(string[quotes[2] + 1..quotes[3]].to_owned())
            } else {
                // Short format without table name, use empty string
                None
            };

            ErrorKind::UndefinedColumn { table, column }
        }
        _ => ErrorKind::Unknown,
    }
}

#[allow(unused)]
pub struct MissingMaterialized {
    pub name: String,
}

use bytes::BytesMut;
use tokio_postgres::types::{to_sql_checked, IsNull, Type};

#[derive(Debug)]
struct UntypedNull;

impl ToSql for UntypedNull {
    fn to_sql(&self, _ty: &Type, _out: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> { Ok(IsNull::Yes) }

    fn accepts(_ty: &Type) -> bool {
        true // Accept all types
    }

    to_sql_checked!();
}
