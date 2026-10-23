//! The native executor: the engine's SQL run on rusqlite connections.
//!
//! The bb8 pool, the `spawn_blocking` hop and the session pragmas stay in
//! `connection.rs`; this file only lets the engine reach a pooled rusqlite
//! connection through [`SqliteExecutor`], binding [`SqliteValue`] parameters
//! and reading each column by its SQLite storage class, with no conversion.

use std::path::Path;

use rusqlite::{
    params_from_iter,
    types::{ToSqlOutput, Type, ValueRef},
    Connection, ToSql,
};

pub(crate) use crate::connection::PooledConnection;
use crate::{
    error::SqliteError,
    exec::{Row, SqliteExecutor},
    value::SqliteValue,
    SqliteConnectionManager,
};

/// The engine's pool of rusqlite connections.
pub(crate) type Pool = bb8::Pool<SqliteConnectionManager>;

/// Default connection pool size
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// The pool behind `SqliteStorageEngine::open`.
pub(crate) async fn file_pool(path: &Path) -> anyhow::Result<Pool> {
    let manager = SqliteConnectionManager::file(path);
    Ok(bb8::Pool::builder().max_size(DEFAULT_POOL_SIZE).build(manager).await?)
}

/// The pool behind `SqliteStorageEngine::open_in_memory`.
pub(crate) async fn memory_pool() -> anyhow::Result<Pool> {
    let manager = SqliteConnectionManager::memory();
    // For in-memory, we use a single connection to keep the database alive
    Ok(bb8::Pool::builder().max_size(1).build(manager).await?)
}

impl PooledConnection {
    /// Run a closure against this connection through the executor boundary.
    /// Like [`PooledConnection::with_connection`], it runs on tokio's blocking
    /// pool while holding the connection's lock.
    pub async fn with_executor<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&dyn SqliteExecutor) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        self.with_connection(move |connection| f(connection)).await
    }
}

impl SqliteExecutor for Connection {
    fn execute(&self, sql: &str, params: &[SqliteValue]) -> Result<usize, SqliteError> {
        Ok(Connection::execute(self, sql, params_from_iter(params))?)
    }

    fn query(&self, sql: &str, params: &[SqliteValue]) -> Result<Vec<Row>, SqliteError> {
        let mut statement = self.prepare(sql)?;
        let width = statement.column_count();
        let mut rows = statement.query(params_from_iter(params))?;
        let mut collected = Vec::new();
        while let Some(row) = rows.next()? {
            collected.push(Row::new((0..width).map(|index| value_from_ref(index, row.get_ref(index)?)).collect::<Result<_, _>>()?));
        }
        Ok(collected)
    }
}

/// A column as its storage class says it is. TEXT that is not UTF-8 fails
/// with the error rusqlite gives a `String` read of it.
fn value_from_ref(index: usize, value: ValueRef<'_>) -> Result<SqliteValue, SqliteError> {
    Ok(match value {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(integer) => SqliteValue::Integer(integer),
        ValueRef::Real(real) => SqliteValue::Real(real),
        ValueRef::Text(bytes) => SqliteValue::Text(
            std::str::from_utf8(bytes)
                .map_err(|error| rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error)))?
                .to_owned(),
        ),
        ValueRef::Blob(bytes) => SqliteValue::Blob(bytes.to_vec()),
    })
}

/// Binds a parameter without copying it: TEXT and BLOB are borrowed, and a
/// JSONB value binds as its JSON text for the `jsonb(?)` that wraps it.
impl ToSql for SqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            SqliteValue::Text(text) => ToSqlOutput::Borrowed(ValueRef::Text(text.as_bytes())),
            SqliteValue::Integer(integer) => ToSqlOutput::Borrowed(ValueRef::Integer(*integer)),
            SqliteValue::Real(real) => ToSqlOutput::Borrowed(ValueRef::Real(*real)),
            SqliteValue::Blob(bytes) => ToSqlOutput::Borrowed(ValueRef::Blob(bytes)),
            SqliteValue::Jsonb(json) => ToSqlOutput::Owned(rusqlite::types::Value::Text(json.to_string())),
            SqliteValue::Null => ToSqlOutput::Borrowed(ValueRef::Null),
        })
    }
}
