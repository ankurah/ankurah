use std::{collections::BTreeMap, sync::Arc};

use crate::{
    error::RetrievalError,
    property::{
        backend::{BackendDowncasted, PropertyBackend},
        Backends,
    },
    storage::{RecordState, StorageBucket, StorageEngine},
};

use futures_util::TryStreamExt;

pub mod predicate;

use ankurah_proto::{Clock, ID};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use tokio_postgres::{error::SqlState, types::ToSql};
use tracing::{error, info};

pub struct Postgres {
    // TODO: the rest of the owl
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
}

impl Postgres {
    pub fn new(pool: bb8::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<Self> {
        Ok(Self { pool: pool })
    }

    // TODO: newtype this to `BucketName(&str)` with a constructor that
    // only accepts a subset of characters.
    pub fn sane_name(bucket_name: &str) -> bool {
        for char in bucket_name.chars() {
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
    async fn bucket(&self, name: &str) -> anyhow::Result<std::sync::Arc<dyn StorageBucket>> {
        if !Postgres::sane_name(name) {
            return Err(anyhow::anyhow!(
                "bucket name must only contain valid characters"
            ));
        }

        let bucket = PostgresBucket {
            pool: self.pool.clone(),
            bucket_name: name.to_owned(),
        };

        // Try to create the table if it doesn't exist
        let mut client = self.pool.get().await?;
        bucket.create_table(&mut client).await?;

        Ok(Arc::new(bucket))
    }

    async fn fetch_states(
        &self,
        bucket_name: String,
        predicate: &ankql::ast::Predicate,
    ) -> Result<Vec<(ID, RecordState)>, RetrievalError> {
        if !Postgres::sane_name(&bucket_name) {
            return Err(RetrievalError::InvalidBucketName);
        }

        let client = self
            .pool
            .get()
            .await
            .map_err(|err| RetrievalError::StorageError(Box::new(err)))?;

        let mut results = Vec::new();

        let mut ankql_sql = predicate::Sql::new();
        ankql_sql.predicate(&predicate);

        let (sql, args) = ankql_sql.collapse();

        let filtered_query = if args.len() > 0 {
            format!(
                r#"SELECT "id", "state_buffer", "head" FROM "{}" WHERE {}"#,
                bucket_name.as_str(),
                sql,
            )
        } else {
            format!(
                r#"SELECT "id", "state_buffer", "head" FROM "{}""#,
                bucket_name.as_str()
            )
        };

        eprintln!("Running: {}", filtered_query);
        // `query_raw` fixes 2 problems here
        // - `query` only takes `&[&dyn ToSql + Sync]`... and rust can't coerce
        //   `&[&dyn ToSql + Send + Sync]` for reasons unknown to me.
        // - It gives us a `RowStream` instead of a `Vec<Row>`, which means
        //   we can return rows as they come instead of waiting for all of them
        //   in the future.
        //   (... I believe? Don't quote me on this)
        let rows = match client.query_raw(&filtered_query, args).await {
            Ok(stream) => match stream.try_collect::<Vec<_>>().await {
                Ok(rows) => rows,
                Err(err) => return Err(RetrievalError::StorageError(err.into())),
            },
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } => {
                        if table == bucket_name {
                            // Table doesn't exist yet, return empty results
                            return Ok(Vec::new());
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

            let record_state = RecordState {
                state_buffers,
                head: row.get::<_, Vec<uuid::Uuid>>(2).into(),
            };

            results.push((id, record_state));

            // Create record to evaluate predicate
            //let record_inner = RecordInner::from_record_state(id, bucket_name, &record_state)?;

            // Apply predicate filter
            /*if evaluate_predicate(&record_inner, predicate)? {
                results.push((id, record_state));
            }*/
        }

        Ok(results)
    }
}

pub struct PostgresBucket {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    bucket_name: String,
}

impl PostgresBucket {
    pub async fn create_table(&self, client: &mut tokio_postgres::Client) -> anyhow::Result<()> {
        // Create the table
        let create_query = format!(
            r#"CREATE TABLE "{}"("id" UUID UNIQUE, "state_buffer" BYTEA, "head" UUID[])"#,
            self.bucket_name
        );

        error!("Running: {}", create_query);
        client.execute(&create_query, &[]).await?;
        Ok(())
    }

    pub async fn add_missing_columns(
        &self,
        client: &mut tokio_postgres::Client,
        missing: Vec<(String, &'static str)>, // column name, datatype
    ) -> anyhow::Result<()> {
        for (column, datatype) in missing {
            if Postgres::sane_name(&column) {
                let alter_query = format!(
                    r#"ALTER TABLE "{}" ADD COLUMN "{}" {}"#,
                    self.bucket_name, column, datatype,
                );
                error!("Running: {}", alter_query);
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
impl StorageBucket for PostgresBucket {
    async fn set_record(&self, id: ID, state: &RecordState) -> anyhow::Result<bool> {
        // TODO: Create/Alter table
        let state_buffers = bincode::serialize(&state.state_buffers)?;
        let ulid: ulid::Ulid = id.into();
        let uuid: uuid::Uuid = ulid.into();

        let head_uuids: Vec<uuid::Uuid> = (&state.head).into();

        let backends = Backends::from_state_buffers(state)?;
        let mut columns: Vec<String> = vec![
            "id".to_owned(),
            "state_buffer".to_owned(),
            "head".to_owned(),
        ];
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

        let columns_str = columns
            .iter()
            .map(|name| format!("\"{}\"", name))
            .collect::<Vec<String>>()
            .join(", ");
        let values_str = params
            .iter()
            .enumerate()
            .map(|(index, _)| format!("${}", index + 1))
            .collect::<Vec<String>>()
            .join(", ");
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
            self.bucket_name, columns_str, values_str, columns_update_str
        );

        let mut client = self.pool.get().await?;
        error!("Running: {}", query);
        let row = match client.query_one(&query, params.as_slice()).await {
            Ok(row) => row,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } => {
                        if table == self.bucket_name {
                            self.create_table(&mut *client).await?;
                            return self.set_record(id, state).await; // retry
                        }
                    }
                    ErrorKind::UndefinedColumn { table, column } => {
                        // TODO: We should check the definition of this and add all
                        // needed columns rather than recursively doing it.
                        if table == self.bucket_name {
                            let index = materialized_columns
                                .iter()
                                .enumerate()
                                .find(|(_, name)| **name == column)
                                .map(|(index, _)| index);
                            if let Some(index) = index {
                                let param = &materialized[index];
                                self.add_missing_columns(
                                    &mut client,
                                    vec![(column, param.postgres_type())],
                                )
                                .await?;
                                return self.set_record(id, state).await; // retry
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

        // If this is a new record (no old_head), or if the heads are different, return true
        let old_head: Option<Vec<uuid::Uuid>> = row.get("old_head");
        let changed = match old_head {
            None => true, // New record
            Some(old_head) => {
                let old_clock: Clock = old_head.into();
                old_clock != state.head
            }
        };

        error!("Changed: {}", changed);
        Ok(changed)
    }

    async fn get_record(&self, id: ID) -> Result<RecordState, RetrievalError> {
        let ulid: ulid::Ulid = id.into();
        let uuid: uuid::Uuid = ulid.into();

        // be careful with sql injection via bucket name
        let query = format!(
            r#"SELECT "id", "state_buffer", "head" FROM "{}" WHERE "id" = $1"#,
            self.bucket_name
        );

        let mut client = match self.pool.get().await {
            Ok(client) => client,
            Err(err) => {
                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        error!("Running: {}", query);
        let row = match client.query_one(&query, &[&uuid]).await {
            Ok(row) => row,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::RowCount => {
                        return Err(RetrievalError::NotFound(id));
                    }
                    ErrorKind::UndefinedTable { table } => {
                        if self.bucket_name == table {
                            self.create_table(&mut client)
                                .await
                                .map_err(|e| RetrievalError::StorageError(e.into()))?;
                            return Err(RetrievalError::NotFound(id));
                        }
                    }
                    _ => {}
                }

                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        error!("Row: {:?}", row);
        let row_id: uuid::Uuid = row.get("id");
        assert_eq!(row_id, uuid);

        let serialized_buffers: Vec<u8> = row.get("state_buffer");
        let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&serialized_buffers)?;

        Ok(RecordState {
            state_buffers,
            head: row.get::<_, Vec<uuid::Uuid>>("head").into(),
        })
    }
}

// Some hacky shit because rust-postgres doesn't let us ask for the error kind
// TODO: remove this when https://github.com/sfackler/rust-postgres/pull/1185
//       gets merged
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorKind {
    RowCount,
    UndefinedTable { table: String },
    UndefinedColumn { table: String, column: String },
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
    error!("db_err: {:?}", err.as_db_error());
    error!("sql_code: {:?}", err.code());
    error!("err: {:?}", err);
    error!("err: {:?}", err.to_string());

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
            ErrorKind::UndefinedTable {
                table: table.to_owned(),
            }
        }
        Some(SqlState::UNDEFINED_COLUMN) => {
            // column "name" of relation "album" does not exist
            let quotes = quote_indices(&string);
            let column = &string[quotes[0] + 1..quotes[1]];
            let table = &string[quotes[2] + 1..quotes[3]];

            ErrorKind::UndefinedColumn {
                table: table.to_owned(),
                column: column.to_owned(),
            }
        }
        _ => ErrorKind::Unknown,
    }
}

#[allow(unused)]
pub struct MissingMaterialized {
    pub name: String,
}

impl From<tokio_postgres::Error> for RetrievalError {
    fn from(err: tokio_postgres::Error) -> Self {
        RetrievalError::StorageError(Box::new(err))
    }
}
