//! The native executor: the engine's SQL run on rusqlite connections.
//!
//! The bb8 pool, the `spawn_blocking` hop and the session pragmas stay in
//! `connection.rs`; this file only lets the engine reach a pooled rusqlite
//! connection through [`SqliteExecutor`], binding [`SqliteValueRef`]
//! parameters and reading each column by its SQLite storage class, with no
//! conversion.

use std::path::Path;

use rusqlite::{
    params_from_iter,
    types::{ToSqlOutput, Type, ValueRef},
    Batch, Connection, Statement, ToSql,
};

pub(crate) use crate::connection::PooledConnection;
use crate::{
    error::SqliteError,
    exec::{Row, SqliteExecutor},
    sql_text,
    value::{SqliteValue, SqliteValueRef},
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
    fn execute(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError> {
        Ok(one_statement(self, sql)?.execute(params_from_iter(params))?)
    }

    fn query(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError> {
        let mut statement = one_statement(self, sql)?;
        let width = statement.column_count();
        let mut rows = statement.query(params_from_iter(params))?;
        let mut collected = Vec::new();
        while let Some(row) = rows.next()? {
            collected.push(Row::new((0..width).map(|index| value_from_ref(index, row.get_ref(index)?)).collect::<Result<_, _>>()?));
        }
        Ok(collected)
    }
}

/// The statement `sql` holds, prepared. SQL that holds a second statement is
/// refused before SQLite prepares either, since SQLite applies some PRAGMAs
/// while it prepares them: a defect fix, since rusqlite, prepared as main's
/// engine prepared it, runs the first and ignores the rest without a word.
/// Whitespace, semicolons and comments after the statement are no statement.
fn one_statement<'c>(connection: &'c Connection, sql: &str) -> Result<Statement<'c>, SqliteError> {
    if sql_text::holds_a_second_statement(sql) {
        return Err(rusqlite::Error::MultipleStatement.into());
    }
    let mut statements = Batch::new(connection, sql);
    // SQL that holds no statement at all is prepared as before, and SQLite
    // refuses it when it runs.
    let Some(statement) = statements.next()? else { return Ok(connection.prepare(sql)?) };
    // SQLite reads what follows the statement too, which the scan above found
    // to be only what it skips, so that it compiles nothing; should SQLite
    // find a statement there all the same, the SQL is refused.
    match statements.next() {
        Ok(None) => Ok(statement),
        Ok(Some(_)) | Err(_) => Err(rusqlite::Error::MultipleStatement.into()),
    }
}

/// A column as its storage class says it is, copied once out of SQLite's
/// buffer: TEXT is checked to be UTF-8 where it lies, and fails with the
/// error rusqlite gives a `String` read of it when it is not.
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

/// Binds a parameter without copying it: TEXT and BLOB stay borrowed, and a
/// JSONB value binds as its JSON text for the `jsonb(?)` that wraps it.
impl ToSql for SqliteValueRef<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match *self {
            SqliteValueRef::Text(text) => ToSqlOutput::Borrowed(ValueRef::Text(text.as_bytes())),
            SqliteValueRef::Integer(integer) => ToSqlOutput::Borrowed(ValueRef::Integer(integer)),
            SqliteValueRef::Real(real) => ToSqlOutput::Borrowed(ValueRef::Real(real)),
            SqliteValueRef::Blob(bytes) => ToSqlOutput::Borrowed(ValueRef::Blob(bytes)),
            SqliteValueRef::Jsonb(json) => ToSqlOutput::Owned(rusqlite::types::Value::Text(json.to_string())),
            SqliteValueRef::Null => ToSqlOutput::Borrowed(ValueRef::Null),
        })
    }
}
