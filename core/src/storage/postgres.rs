use std::{collections::BTreeMap, sync::Arc};

use crate::{
    error::RetrievalError,
    model::ID,
    storage::{RecordState, StorageBucket, StorageEngine},
};

use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};

pub struct Postgres {
    // TODO: the rest of the owl
    pool: r2d2::Pool<PostgresConnectionManager<NoTls>>,
}

impl Postgres {
    pub fn new(pool: r2d2::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<Self> {
        Ok(Self { pool: pool })
    }

    // TODO: newtype this to `BucketName(&str)` with a constructor that
    // only accepts a subset of characters.
    pub fn sane_bucket_name(bucket_name: &str) -> bool {
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
impl StorageEngine for Postgres {
    fn bucket(&self, name: &str) -> anyhow::Result<std::sync::Arc<dyn StorageBucket>> {
        if !Postgres::sane_bucket_name(name) {
            return Err(anyhow::anyhow!(
                "bucket name must only contain valid characters"
            ));
        }

        Ok(Arc::new(PostgresBucket {
            pool: self.pool.clone(),
            bucket_name: name.to_owned(),
        }))
    }
}

pub struct PostgresBucket {
    pool: r2d2::Pool<PostgresConnectionManager<NoTls>>,
    bucket_name: String,
}

impl StorageBucket for PostgresBucket {
    fn set_record_state(&self, id: ID, state: &RecordState) -> anyhow::Result<()> {
        // TODO: Create/Alter table
        let state_buffers = bincode::serialize(&state.state_buffers)?;
        let uuid: uuid::Uuid = id.0.into();

        // be careful with sql injection via bucket name
        let query = format!(
            "INSERT INTO {} (id, state_buffer) VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET state_buffer = $2",
            self.bucket_name
        );

        let mut client = self.pool.get()?;
        eprintln!("Running: {}", query);
        let rows_affected = client.execute(&query, &[&uuid, &state_buffers])?;

        eprintln!("Rows affected: {}", rows_affected);
        Ok(())
    }

    fn get_record_state(&self, id: ID) -> Result<RecordState, RetrievalError> {
        let uuid: uuid::Uuid = id.0.into();

        // be careful with sql injection via bucket name
        let query = format!(
            "SELECT id, state_buffer FROM {} WHERE id = $1",
            self.bucket_name
        );

        let mut client = match self.pool.get() {
            Ok(client) => client,
            Err(err) => {
                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        eprintln!("Running: {}", query);
        let row = match client.query_one(&query, &[&uuid]) {
            Ok(row) => row,
            Err(err) => {
                let kind = error_kind(&err);
                if kind == ErrorKind::RowCount {
                    return Err(RetrievalError::NotFound(id));
                }

                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        eprintln!("Row: {:?}", row);
        let row_id: uuid::Uuid = row.get("id");
        assert_eq!(row_id, uuid);

        let serialized_buffers: Vec<u8> = row.get("state_buffer");
        let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&serialized_buffers)?;

        return Ok(RecordState {
            state_buffers: state_buffers,
        });
    }
}

// Some hacky shit because rust-postgres doesn't let us ask for the error kind
// TODO: remove this when https://github.com/sfackler/rust-postgres/pull/1185
//       gets merged
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorKind {
    RowCount,
    Unknown,
}

pub fn error_kind(err: &postgres::Error) -> ErrorKind {
    let string = err.to_string().trim().to_owned();

    if string == "query returned an unexpected number of rows" {
        return ErrorKind::RowCount;
    }

    ErrorKind::Unknown
}
