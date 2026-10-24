//! The native executor, which runs the engine's SQL on rusqlite.
//!
//! It implements [`SqliteExecutor`] for rusqlite's `Connection`, binding
//! [`SqliteValueRef`] parameters and reading each column as its SQLite
//! storage class says it is, with no conversion, and lets the engine reach a
//! pooled connection through it. `connection` opens the connections with
//! the session settings, pools them for the engine, and runs each closure on
//! one through tokio's blocking pool.

pub(crate) mod connection;

use rusqlite::{
    params_from_iter,
    types::{ToSqlOutput, Type, ValueRef},
    Connection, Statement, ToSql,
};

use crate::{
    error::SqliteError,
    executor::{
        statement_scan::{self, Statements},
        Row, SqliteExecutor,
    },
    value::{SqliteValue, SqliteValueRef},
};
use connection::PooledConnection;

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
            collected.push(row_of(row, width)?);
        }
        Ok(collected)
    }

    fn query_row(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Option<Row>, SqliteError> {
        let mut statement = one_statement(self, sql)?;
        let width = statement.column_count();
        let mut rows = statement.query(params_from_iter(params))?;
        rows.next()?.map(|row| row_of(row, width)).transpose()
    }
}

/// A row read out of SQLite, each of its `width` columns as its storage class
/// says it is.
fn row_of(row: &rusqlite::Row<'_>, width: usize) -> Result<Row, SqliteError> {
    Ok(Row::new((0..width).map(|index| value_from_ref(index, row.get_ref(index)?)).collect::<Result<_, _>>()?))
}

/// The statement `sql` holds, prepared. SQL that holds a second statement is
/// refused before SQLite prepares either: SQLite applies some PRAGMAs while
/// it prepares them, and rusqlite, given SQL of several statements, runs the
/// first and ignores the rest without a word. Whitespace, semicolons and
/// comments after the statement are no statement; anything else after it
/// is, even a byte SQLite cannot read. Once the scan has found one
/// statement, SQLite prepares it from the SQL as it is. SQL that holds no
/// statement is prepared too, and SQLite refuses it when it runs, with
/// SQLITE_MISUSE.
fn one_statement<'c>(connection: &'c Connection, sql: &str) -> Result<Statement<'c>, SqliteError> {
    if statement_scan::statements(sql) == Statements::Several {
        return Err(rusqlite::Error::MultipleStatement.into());
    }
    Ok(connection.prepare(sql)?)
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

/// Binds a parameter by reference: TEXT and BLOB reach rusqlite borrowed,
/// without first being copied into an owned value, though SQLite still
/// copies them as rusqlite binds them; JSON binds as its JSON text, TEXT.
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
