use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use ankurah_core::{
    error::RetrievalError,
    property::{
        backend::{BackendDowncasted, PropertyBackend}, Backends, PropertyName, PropertyValue
    },
    storage::{StorageCollection, StorageEngine},
};
use ankurah_proto::State;

use futures_util::TryStreamExt;

pub mod predicate;
pub mod value;

use value::PGValue;

use ankurah_proto::{Clock, CollectionId, Event, ID};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use tokio_postgres::{error::SqlState, types::ToSql};
use tracing::{error, info, warn};

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

        let bucket = PostgresBucket {
            pool: self.pool.clone(),
            collection_id: collection_id.clone(),
            columns: Arc::new(RwLock::new(Vec::new()))
        };


        // Try to create the table if it doesn't exist
        let mut client = self.pool.get().await.map_err(|err| RetrievalError::storage(err))?;
        bucket.create_event_table(&mut client).await?;
        bucket.create_state_table(&mut client).await?;
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

    columns: Arc<RwLock<Vec<PostgresColumn>>>,
}

impl PostgresBucket {
    fn state_table(&self) -> String { format!("{}", self.collection_id.as_str()) }

    pub fn event_table(&self) -> String { format!("{}_event", self.collection_id.as_str()) }

    /// Rebuild the cache of columns in the table.
    pub async fn rebuild_columns_cache(&self, client: &mut tokio_postgres::Client) -> anyhow::Result<()> {
        let schema = "ankurah";

        let column_query = format!(
            r#"SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2;"#,
        );
        let mut new_columns = Vec::new();
        info!("Querying existing columns: {:?}, [{:?}, {:?}]", column_query, &schema, &self.collection_id.as_str());
        let rows = client.query(&column_query, &[&schema, &self.collection_id.as_str()]).await?;
        for row in rows {
            let name: String = row.get("column_name");
            info!("found column: {:?}", name);
            new_columns.push(PostgresColumn {
                name: row.get("column_name"),
                is_nullable: row.get("is_nullable"),
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

    pub fn has_column(&self, column_name: &String) -> bool {
        self.column(column_name).is_some()
    }

    pub async fn create_event_table(&self, client: &mut tokio_postgres::Client) -> anyhow::Result<()> {
        let create_query = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"("id" UUID UNIQUE, "entity_id" UUID, "operations" bytea, "parent" UUID[])"#,
            self.event_table()
        );

        info!("Applying DDL: {}", create_query);
        client.execute(&create_query, &[]).await?;
        Ok(())
    }

    pub async fn create_state_table(&self, client: &mut tokio_postgres::Client) -> anyhow::Result<()> {
        let create_query =
            format!(r#"CREATE TABLE IF NOT EXISTS "{}"("id" UUID UNIQUE, "state_buffer" BYTEA, "head" UUID[])"#, self.state_table());

        info!("Applying DDL: {}", create_query);
        match client.execute(&create_query, &[]).await {
            Ok(_) => Ok(()),
            Err(err) => {
                info!("Error: {}", err);
                // if err.code() == Some(SqlState::UNIQUE_VIOLATION) {
                //     Ok(())
                // } else {
                Err(err.into())
                // }
            }
        }
    }

    pub async fn add_missing_columns(
        &self,
        client: &mut tokio_postgres::Client,
        missing: Vec<(String, &'static str)>, // column name, datatype
    ) -> anyhow::Result<()> {
        for (column, datatype) in missing {
            if Postgres::sane_name(&column) {
                let alter_query = format!(r#"ALTER TABLE "{}" ADD COLUMN "{}" {}"#, self.state_table(), column, datatype,);
                info!("Running: {}", alter_query);
                client.execute(&alter_query, &[]).await?;
            }
        }

        self.rebuild_columns_cache(client).await?;
        Ok(())
    }
}

pub struct PostgresSetState {
    
}

#[async_trait]
impl StorageCollection for PostgresBucket {
    async fn set_state(&self, id: ID, state: &State) -> anyhow::Result<bool> {
        let state_buffers = bincode::serialize(&state.state_buffers)?;
        let ulid: ulid::Ulid = id.into();
        let uuid: uuid::Uuid = ulid.into();

        let head_uuids: Vec<uuid::Uuid> = (&state.head).into();

        // Ensure head is not empty for new records
        if head_uuids.is_empty() {
            warn!("Warning: Empty head detected for entity {}", id);
        }

        let mut client = self.pool.get().await?;

        let backends = Backends::from_state_buffers(state)?;
        let mut columns: Vec<String> = vec!["id".to_owned(), "state_buffer".to_owned(), "head".to_owned()];
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        params.push(&uuid);
        params.push(&state_buffers);
        params.push(&head_uuids);

        self.rebuild_columns_cache(&mut client).await?;
        info!("existing columns: {:?}", self.existing_columns());
        let mut materialized: Vec<(String, Option<PGValue>)> = Vec::new();
        for (column, value) in backends.property_values() {
            let pg_value: Option<PGValue> = value.map(|value| value.into());
            if !self.has_column(&column) {
                info!("doesn't have column: {:?}", column);
                // We don't have the column yet and we know the type.
                if let Some(ref pg_value) = pg_value {
                    info!("adding missing column: {:?}", column);
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
                }
                None => params.push(&None::<i32>),
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

        info!("columns: {:?}", columns);

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

        info!("Querying: {}", query);
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
                    /*
                    ErrorKind::UndefinedColumn { table, column } => {
                        // TODO: We should check the definition of this and add all
                        // needed columns rather than recursively doing it.
                        if table == Some(self.state_table()) {
                            let index = materialized.iter().find(|(name, param)| **name == column).map(|(index, _)| index);
                            if let Some(index) = index {
                                info!("column '{}' not found in materialized, adding it", column);
                                self.add_missing_columns(&mut client, vec![(column, param.postgres_type())]).await?;
                                return self.set_state(id, state).await; // retry
                            } else {
                                error!("column '{}' not found in materialized", column);
                            }
                        }
                    }
                    */
                    _ => {}
                }

                return Err(err.into());
            }
        };

        // If this is a new entity (no old_head), or if the heads are different, return true
        let old_head: Option<Vec<uuid::Uuid>> = row.get("old_head");
        let changed = match old_head {
            None => true, // New entity
            Some(old_head) => {
                let old_clock: Clock = old_head.into();
                old_clock != state.head
            }
        };

        info!("Changed: {}", changed);
        Ok(changed)
    }

    async fn get_state(&self, id: ID) -> Result<State, RetrievalError> {
        let ulid: ulid::Ulid = id.into();
        let uuid: uuid::Uuid = ulid.into();

        // be careful with sql injection via bucket name
        let query = format!(r#"SELECT "id", "state_buffer", "head" FROM "{}" WHERE "id" = $1"#, self.state_table());

        let mut client = match self.pool.get().await {
            Ok(client) => client,
            Err(err) => {
                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        info!("Getting state: {}", query);
        let row = match client.query_one(&query, &[&uuid]).await {
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

        info!("Row: {:?}", row);
        let row_id: uuid::Uuid = row.get("id");
        assert_eq!(row_id, uuid);

        let serialized_buffers: Vec<u8> = row.get("state_buffer");
        let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&serialized_buffers)?;

        Ok(State { state_buffers, head: row.get::<_, Vec<uuid::Uuid>>("head").into() })
    }

    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<(ID, State)>, RetrievalError> {
        println!("Fetching states for: {:?}", predicate);
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

        info!("SQL: {} with args: {:?}", filtered_query, args);

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
                        println!("Undefined column: {} in table: {:?}", column, table);
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
            let uuid: uuid::Uuid = row.get(0);
            let state_buffer: Vec<u8> = row.get(1);
            let id = ID::from_ulid(ulid::Ulid::from(uuid));

            let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&state_buffer)?;

            let entity_state = State { state_buffers, head: row.get::<_, Vec<uuid::Uuid>>(2).into() };

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
    async fn add_event(&self, entity_event: &Event) -> anyhow::Result<bool> {
        let event_id = uuid::Uuid::from(ulid::Ulid::from(entity_event.id));
        let entity_id = uuid::Uuid::from(ulid::Ulid::from(entity_event.entity_id));
        let operations = bincode::serialize(&entity_event.operations)?;
        let parent_uuids: Vec<uuid::Uuid> = (&entity_event.parent).into();

        // Does it even matter if this conflicts?
        // One peers event should match any duplicates, so taking the first
        // event we receive from a peer should be fine.
        let query = format!(r#"INSERT INTO "{0}"("id", "entity_id", "operations", "parent") VALUES($1, $2, $3, $4)"#, self.event_table(),);

        let mut client = self.pool.get().await?;
        info!("Running: {}", query);
        let affected = match client.execute(&query, &[&event_id, &entity_id, &operations, &parent_uuids]).await {
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
                    _ => {}
                }

                return Err(err.into());
            }
        };

        Ok(affected > 0)
    }

    async fn get_events(&self, entity_id: ID) -> Result<Vec<Event>, ankurah_core::error::RetrievalError> {
        let query = format!(r#"SELECT "id", "operations", "parent" FROM "{0}" WHERE "entity_id" = $1"#, self.event_table(),);

        let entity_uuid = uuid::Uuid::from(ulid::Ulid::from(entity_id));

        let mut client = self.pool.get().await.map_err(|err| RetrievalError::storage(err))?;
        info!("Running: {}", query);
        let rows = match client.query(&query, &[&entity_uuid]).await {
            Ok(rows) => rows,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } => {
                        if table == self.event_table() {
                            self.create_event_table(&mut *client).await?;
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
            let event_id: uuid::Uuid = row.get("id");
            let event_id = ID::from_ulid(event_id.into());
            let operations_binary: Vec<u8> = row.get("operations");
            let operations = bincode::deserialize(&operations_binary)?;
            let parent: Vec<uuid::Uuid> = row.get("parent");
            let parent = parent.into_iter().map(|uuid| ID::from_ulid(uuid.into())).collect::<BTreeSet<_>>();
            let clock = Clock::new(parent);

            events.push(Event {
                id: event_id,
                collection: self.collection_id.clone(),
                entity_id: entity_id,
                operations: operations,
                parent: clock,
            })
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

            println!("quotes: {:?}", quotes);
            println!("column: {}", column);

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
