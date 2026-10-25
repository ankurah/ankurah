//! The Node.js executor, which runs the engine's SQL from WASM on a
//! JavaScript SQLite: Node.js's built-in `node:sqlite`, or a better-sqlite3
//! database a host hands over.
//!
//! An Ankurah node built for wasm32 and run inside Node.js (a VS Code
//! extension host, an Electron main process, a JavaScript CLI) has no
//! IndexedDB, and its WASM cannot link SQLite as rusqlite does, but it can
//! call a SQLite driver the host provides: Node.js ships SQLite as
//! `node:sqlite`. The executor drives a `DatabaseSync`-shaped JavaScript
//! object: `exec(sql)`, and `prepare(sql)`, giving statements with
//! `run(...params)`, `get(...params)`, `all(...params)`, `columns()`,
//! `setReturnArrays`, `setReadBigInts` and `sourceSQL`. It accepts
//! better-sqlite3's `raw`, `safeIntegers` and `source` in place of the last
//! three. [`NodeJsConnection::open`] constructs node:sqlite's `DatabaseSync`
//! through `process.getBuiltinModule`, and a host adopts a database it built
//! with [`SqliteStorageEngine::from_database`], whose checks refuse one the
//! executor cannot read exactly. Every call is synchronous, which suits the
//! single-threaded WASM host: the engine's closures run in place.

mod adoption;
mod exception;
mod kept_readers;
mod parameters;
mod runtime;
mod statement;
mod typed_reader;

use std::{cell::RefCell, path::Path, rc::Rc};

use js_sys::Reflect;
use send_wrapper::SendWrapper;
use wasm_bindgen::JsValue;

use crate::{
    error::{Refusal, SqliteError},
    executor::{statement_scan, Row, SqliteExecutor},
    value::SqliteValueRef,
    SqliteStorageEngine,
};
use exception::js_error;
use kept_readers::KeptReaders;
use parameters::{refuse_a_named_parameter, ParameterCounts};
use statement::{
    database_method, integer_from_js, js_params, read_rows, refuse_all_but_one_statement, refuse_sql_past_its_statement, source,
    statement_method, unreadable, width, RowSelection,
};
use typed_reader::{plain_row, typed_reader, typed_row};

/// The engine names the connection its pool hands out `PooledConnection`;
/// here the pool holds a single database.
pub(crate) use NodeJsConnection as PooledConnection;
/// The engine's pool on Node.js.
pub(crate) type Pool = NodeJsPool;

/// The pool behind `SqliteStorageEngine::open`.
pub(crate) async fn file_pool(path: &Path) -> anyhow::Result<Pool> {
    let path = path.to_str().ok_or_else(|| anyhow::anyhow!("the database path is not valid UTF-8"))?;
    Ok(NodeJsPool::new(NodeJsConnection::open(path)?))
}

/// The pool behind `SqliteStorageEngine::open_in_memory`.
pub(crate) async fn memory_pool() -> anyhow::Result<Pool> { Ok(NodeJsPool::new(NodeJsConnection::open(":memory:")?)) }

impl SqliteStorageEngine {
    /// Adopt a database object the host constructed, for a host that runs
    /// SQLite itself: on better-sqlite3, or on a node:sqlite `DatabaseSync`
    /// with settings of its own. The engine keeps the host's session
    /// settings, its busy timeout and SQLite's defensive flag among them,
    /// where `open` sets native's. It refuses a database it cannot read
    /// exactly, as a [`Refusal::Database`]: one whose statements cannot
    /// hand rows over as arrays or INTEGERs as BigInts, or report their SQL;
    /// one whose node:sqlite skips a numbered parameter as it binds a call's
    /// values; one whose text encoding is not UTF-8; and one that reads a
    /// double-quoted name that matches no column as a string.
    ///
    /// A driver such as better-sqlite3 brings a copy of SQLite of its own, and
    /// two copies in one process cannot coordinate the file locks SQLite
    /// takes, which belong to the process: one copy closing the file can drop
    /// the locks the other holds, which SQLite's notes on corruption list as a
    /// way to corrupt it. So a host that adopts such a database on a file must
    /// not open the same file with another copy in the same process, as
    /// [`SqliteStorageEngine::open`] would with node:sqlite's.
    pub async fn from_database(database: JsValue) -> anyhow::Result<Self> {
        Self::new(NodeJsPool::new(NodeJsConnection::from_database(database)?)).await
    }

    /// The JavaScript database object this engine runs on, for a host that
    /// wants to share it or reopen the engine over it.
    pub fn database(&self) -> &JsValue { self.pool().connection().database() }
}

/// The Node.js executor's connection: one `DatabaseSync`-shaped database
/// object. Clones share the object and what the executor keeps for it: its
/// typed readers, its parameter counts and its own statements.
#[derive(Clone)]
pub struct NodeJsConnection {
    // The JavaScript handles are bound to the thread that made them; WASM has one.
    database: SendWrapper<JsValue>,
    readers: SendWrapper<Rc<RefCell<KeptReaders>>>,
    statements: SendWrapper<Rc<RefCell<OwnStatements>>>,
    /// `None` on a database that refuses a call passing too few values itself.
    parameters: Option<SendWrapper<ParameterCounts>>,
}

/// The statements the executor runs on a connection for itself, to read its
/// schema versions and its count of changes: each is prepared on its first
/// use and kept.
#[derive(Default)]
struct OwnStatements {
    /// `PRAGMA main.schema_version` and `PRAGMA temp.schema_version`, prepared once.
    version_statements: Option<[JsValue; 2]>,
    /// `SELECT changes()`, prepared once.
    changes_statement: Option<JsValue>,
}

impl NodeJsConnection {
    /// The JavaScript database object.
    pub fn database(&self) -> &JsValue { &self.database }

    /// Run SQL of one or more statements that yield no rows, such as a
    /// schema script, through the database's own `exec`: the one call that
    /// runs more than one statement, which [`SqliteExecutor`]'s calls refuse.
    pub fn exec(&self, sql: &str) -> Result<(), SqliteError> {
        database_method(&self.database, "exec")?.call1(&self.database, &sql.into()).map_err(js_error)?;
        Ok(())
    }

    /// Run a closure against this database through the executor boundary.
    /// Everything happens synchronously on the calling thread.
    pub async fn with_executor<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&dyn SqliteExecutor) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        f(self)
    }

    /// The count of rows `run()` reports it changed, which must fit a usize:
    /// SQLite's changes(), as rusqlite reports it. node:sqlite's `run()`
    /// reports changes() itself, as a BigInt, since its statements here read
    /// BigInts. better-sqlite3's reports it as a number whatever its
    /// statements read, and as 0 after a statement that changed no rows
    /// itself, where changes() is still the count of the last INSERT, UPDATE
    /// or DELETE; for that 0, changes() is read.
    fn changed_rows(&self, result: &JsValue) -> Result<usize, SqliteError> {
        let changes = Reflect::get(result, &"changes".into()).map_err(js_error)?;
        let count = match changes.as_f64() {
            Some(0.0) => return self.changes(),
            Some(count) if count.fract() == 0.0 => count as i64,
            Some(_) => return Err(unreadable(format!("{changes:?} as the count of rows `run()` changed"))),
            None => integer_from_js(&changes)?,
        };
        usize::try_from(count).map_err(|_| unreadable(format!("{count} as the count of rows `run()` changed")))
    }

    /// SQLite's changes(): the count of rows the connection's last INSERT,
    /// UPDATE or DELETE changed, read through one statement prepared once,
    /// whose INTEGER needs no typed reader.
    fn changes(&self) -> Result<usize, SqliteError> {
        let prepared = self.statements.borrow().changes_statement.clone();
        let statement = match prepared {
            Some(statement) => statement,
            None => {
                let statement = self.prepare("SELECT changes()")?;
                self.statements.borrow_mut().changes_statement = Some(statement.clone());
                statement
            }
        };
        let row =
            read_rows(&statement, &[], 1, RowSelection::First)?.pop().ok_or_else(|| unreadable("no row from `SELECT changes()`".into()))?;
        let changes = integer_from_js(&row.get(0))?;
        usize::try_from(changes).map_err(|_| unreadable(format!("{changes} as the count of rows SQLite's changes() reports")))
    }
}

/// The Node.js executor's stand-in for a pool: one database, handed out by clone.
#[derive(Clone)]
pub struct NodeJsPool {
    connection: NodeJsConnection,
}

impl NodeJsPool {
    pub fn new(connection: NodeJsConnection) -> Self { Self { connection } }

    pub async fn get(&self) -> Result<NodeJsConnection, SqliteError> { Ok(self.connection.clone()) }

    pub fn connection(&self) -> &NodeJsConnection { &self.connection }
}

impl SqliteExecutor for NodeJsConnection {
    /// Takes one step, as rusqlite's `execute` does. node:sqlite's `run`
    /// would take it and discard a row it yields, so a statement that has
    /// columns steps through `get`, which hands the row over to be refused.
    fn execute(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError> {
        let (statement_sql, named_parameter) = refuse_all_but_one_statement(sql)?;
        let statement = self.prepare(statement_sql)?;
        let source = source(&statement)?;
        refuse_sql_past_its_statement(statement_sql, &source)?;
        refuse_a_named_parameter(named_parameter)?;
        let width = width(&statement)?;
        self.refuse_too_few_values(sql, params.len())?;
        if width == 0 {
            // A statement without columns yields no row.
            let result = statement_method(&statement, "run")?.apply(&statement, &js_params(params)).map_err(js_error)?;
            return self.changed_rows(&result);
        }
        // `get` takes one step and resets the statement, returning the row
        // the step yielded, or undefined when it finished the statement.
        let row = statement_method(&statement, "get")?.apply(&statement, &js_params(params)).map_err(js_error)?;
        if !row.is_undefined() {
            return Err(Refusal::ExecuteReturnedRows.into());
        }
        // rusqlite then returns SQLite's changes(): the count of rows the
        // connection's last INSERT, UPDATE or DELETE changed.
        self.changes()
    }

    fn query(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError> { self.read(sql, params, RowSelection::All) }

    /// Reads and checks only the first row, stepping the reader once through
    /// `get()`; a typed reader's materialized WITH clause has still computed
    /// every row of the statement by then.
    fn query_row(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Option<Row>, SqliteError> {
        Ok(self.read(sql, params, RowSelection::First)?.into_iter().next())
    }
}

impl NodeJsConnection {
    /// Run the statement `sql` holds and read the rows `selection` takes,
    /// each value of the storage class SQLite reports for it: through a typed
    /// reader, the one kept from an earlier run of `sql` where there is one,
    /// or for a PRAGMA or an EXPLAIN as the database hands its rows over.
    fn read(&self, sql: &str, params: &[SqliteValueRef<'_>], selection: RowSelection) -> Result<Vec<Row>, SqliteError> {
        if let Some((reader, width)) = self.kept_reader(sql)? {
            self.refuse_too_few_values(sql, params.len())?;
            match read_rows(&reader, params, 2 * width, selection) {
                Ok(taken) => return taken.into_iter().map(typed_row).collect(),
                // The executor watches the schema versions of `main` and
                // `temp` only, so a reader of an attached database's table
                // can name fewer or more columns than its statement now
                // yields. SQLite then fails to prepare the reader again,
                // before it runs anything, with an error that names the
                // reader, and only on that error is the reader dropped and
                // its statement read afresh, as on its first run. Any other
                // failure is reported as it came, and the reader kept: the
                // statement may have run in part, calling a host function,
                // and SQLite may have rolled back the transaction it ran in,
                // so running it again could repeat that part, or run outside
                // the transaction.
                Err(error) if self.reader_went_stale(sql, &error) => self.drop_reader(sql),
                Err(error) => return Err(error),
            }
        }
        let (statement_sql, named_parameter) = refuse_all_but_one_statement(sql)?;
        let statement = self.prepare(statement_sql)?;
        let source = source(&statement)?;
        refuse_sql_past_its_statement(statement_sql, &source)?;
        refuse_a_named_parameter(named_parameter)?;
        let width = width(&statement)?;
        self.refuse_too_few_values(sql, params.len())?;
        if width == 0 {
            // A statement without columns yields no rows; it runs for its
            // effect, through `run`, since better-sqlite3 refuses `all` on a
            // statement it reports yields no rows.
            statement_method(&statement, "run")?.apply(&statement, &js_params(params)).map_err(js_error)?;
            return Ok(Vec::new());
        }
        // The typed reader nests, and the compound check plans, the source
        // past what SQLite skips before the statement: the source keeps a
        // leading semicolon, which a WITH clause cannot hold.
        let nested = statement_scan::skip_to_statement(&source);
        let typed = typed_reader(nested, width);
        match self.prepare(&typed.sql) {
            Ok(reader) => {
                // Checked once for each SQL text, since a kept reader skips it.
                self.refuse_a_compound_select(nested, params)?;
                self.keep_reader(sql, &reader, width, typed.name);
                read_rows(&reader, params, 2 * width, selection)?.into_iter().map(typed_row).collect()
            }
            // The typed reader reads each column as two, so SQLite refuses to
            // prepare the reader of a query that yields more than half of its
            // column limit, 2000 by default, though it prepared the query.
            Err(SqliteError::Sqlite(failure)) if failure.message == "too many columns in result set" => {
                Err(Refusal::TooManyColumns { width }.into())
            }
            // SQLite nests only a query (SELECT, VALUES, or WITH and a SELECT)
            // in a WITH clause. A PRAGMA or an EXPLAIN is read as the database
            // hands it over, its TEXT as a string (see `plain_row`). Any other
            // statement that yields rows, such as one with a RETURNING clause,
            // could write its changes before the executor refused a TEXT value
            // among its rows, so it is refused before it runs, with SQLite's
            // error.
            Err(SqliteError::Sqlite(failure)) => {
                if !["PRAGMA", "EXPLAIN"].iter().any(|keyword| statement_scan::first_keyword(&source).eq_ignore_ascii_case(keyword)) {
                    return Err(Refusal::NotNestable(failure).into());
                }
                read_rows(&statement, params, width, selection)?.into_iter().map(plain_row).collect()
            }
            Err(error) => Err(error),
        }
    }
}
