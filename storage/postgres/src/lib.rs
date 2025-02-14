use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use ankurah_core::{
    error::RetrievalError,
    property::{
        backend::{BackendDowncasted, PropertyBackend},
        Backends,
    },
    storage::{StorageCollection, StorageEngine},
};
use ankurah_proto::State;

use futures_util::TryStreamExt;

pub mod predicate;

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
    async fn collection(&self, collection_id: &CollectionId) -> Result<std::sync::Arc<dyn StorageCollection>, RetrievalError> {
        if !Postgres::sane_name(collection_id.as_str()) {
            return Err(RetrievalError::InvalidBucketName);
        }

        let bucket = PostgresBucket { pool: self.pool.clone(), collection_id: collection_id.clone() };

        // Try to create the table if it doesn't exist
        let mut client = self.pool.get().await.map_err(|err| RetrievalError::storage(err))?;
        bucket.create_event_table(&mut client).await?;
        bucket.create_state_table(&mut client).await?;

        Ok(Arc::new(bucket))
    }
}

pub struct PostgresBucket {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    collection_id: CollectionId,
}

impl PostgresBucket {
    fn state_table(&self) -> String { format!("{}", self.collection_id.as_str()) }

    pub fn event_table(&self) -> String { format!("{}_event", self.collection_id.as_str()) }

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

        Ok(())
    }
}

pub enum PostgresParams {
    String(String),
    Number(i64),
    Bytes(Vec<u8>),
}

impl PostgresParams {
    pub fn postgres_type(&self) -> &'static str {
        match self {
            PostgresParams::String(_) => "varchar",
            PostgresParams::Number(_) => "int",
            PostgresParams::Bytes(_) => "bytea",
        }
    }
}

#[async_trait]
impl StorageCollection for PostgresBucket {
    async fn set_state(&self, id: ID, state: &State) -> anyhow::Result<bool> {
        // TODO: Create/Alter table
        let state_buffers = bincode::serialize(&state.state_buffers)?;
        let ulid: ulid::Ulid = id.into();
        let uuid: uuid::Uuid = ulid.into();

        let head_uuids: Vec<uuid::Uuid> = (&state.head).into();

        // Ensure head is not empty for new records
        if head_uuids.is_empty() {
            warn!("Warning: Empty head detected for entity {}", id);
        }

        let backends = Backends::from_state_buffers(state)?;
        let mut columns: Vec<String> = vec!["id".to_owned(), "state_buffer".to_owned(), "head".to_owned()];
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        params.push(&uuid);
        params.push(&state_buffers);
        params.push(&head_uuids);

        let mut materialized_columns: Vec<String> = Vec::new();
        let mut materialized: Vec<PostgresParams> = Vec::new();

        for backend in backends.downcasted() {
            match backend {
                BackendDowncasted::Yrs(yrs) => {
                    info!("yrs found");
                    for property in yrs.properties() {
                        info!("property: {:?}", property);
                        if let Some(string) = yrs.get_string(property.clone()) {
                            materialized_columns.push(property);
                            materialized.push(PostgresParams::String(string));
                        }
                    }
                }
                BackendDowncasted::LWW(lww) => {
                    for property in lww.properties() {
                        let Some(data) = lww.get(property.clone()) else {
                            continue;
                        };
                        materialized_columns.push(property);
                        materialized.push(PostgresParams::Bytes(data));
                    }
                }
                BackendDowncasted::PN(pn) => {
                    for property in pn.properties() {
                        let data = pn.get(property.clone());
                        materialized_columns.push(property);
                        materialized.push(PostgresParams::Number(data));
                    }
                }
                _ => {}
            }
        }

        columns.extend(materialized_columns.clone());
        for parameter in &materialized {
            match &parameter {
                PostgresParams::String(string) => params.push(string),
                PostgresParams::Number(number) => params.push(number),
                PostgresParams::Bytes(bytes) => params.push(bytes),
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

        let mut client = self.pool.get().await?;
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
                    ErrorKind::UndefinedColumn { table, column } => {
                        // TODO: We should check the definition of this and add all
                        // needed columns rather than recursively doing it.
                        if table == Some(self.state_table()) {
                            let index = materialized_columns.iter().enumerate().find(|(_, name)| **name == column).map(|(index, _)| index);
                            if let Some(index) = index {
                                let param = &materialized[index];
                                info!("column '{}' not found in materialized, adding it", column);
                                self.add_missing_columns(&mut client, vec![(column, param.postgres_type())]).await?;
                                return self.set_state(id, state).await; // retry
                            } else {
                                error!("column '{}' not found in materialized", column);
                            }
                        }
                    }
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
