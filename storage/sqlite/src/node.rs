//! The node executor: Node.js's built-in `node:sqlite` module, driven from WASM.
//!
//! The WASM build of an Ankurah node has no IndexedDB inside Node.js (a VS
//! Code extension host, an Electron main process, a JavaScript CLI) and no
//! native library either, but Node.js ships SQLite as `node:sqlite`. The
//! engine reaches it through a `DatabaseSync`-shaped JavaScript object:
//! `prepare(sql)` giving statements with `run(...params)`, `get(...params)`,
//! `all(...params)`, `columns()` and `sourceSQL`, plus `exec(sql)`. Node's
//! own `DatabaseSync` has that shape (Node.js 22.16 or later). Compatible
//! drivers must also support positional rows and BigInt INTEGER results; the
//! adapter recognizes better-sqlite3's `raw` and `safeIntegers` methods for
//! these capabilities, and its `source` for `sourceSQL`.
//! Hosts can adopt an existing database with [`SqliteStorageEngine::from_database`];
//! [`NodeConnection::open`] constructs Node's directly through
//! `process.getBuiltinModule`. Every call is synchronous, which suits the
//! single-threaded WASM host: the engine's closures simply run in place.
//!
//! node:sqlite hands TEXT over only as a JavaScript string, made by decoding
//! the bytes up to the first NUL and replacing each byte that is not UTF-8
//! with U+FFFD, where rusqlite hands over the bytes themselves. So a query
//! reads its statement through a typed reader that takes TEXT as the bytes
//! of a BLOB, and decodes them as strictly as rusqlite does (see
//! `typed_reader`).

use std::{cell::RefCell, collections::HashMap, path::Path, rc::Rc};

use js_sys::{Array, BigInt, Function, Reflect, Uint8Array};
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};

use crate::{
    error::SqliteError,
    exec::{Row, SqliteExecutor},
    value::{SqliteValue, SqliteValueRef},
    SqliteStorageEngine,
};

/// The engine names the connection its pool hands out `PooledConnection`;
/// here the pool holds a single database.
pub(crate) use NodeConnection as PooledConnection;
/// The engine's pool on Node.
pub(crate) type Pool = NodePool;

/// The 5000 ms busy timeout rusqlite sets on every connection as it opens
/// it, before the connection runs any statement (node:sqlite's default is
/// 0): a connection then waits for another's lock instead of failing at
/// once, and on a file in a rollback journal even preparing `SELECT 1`
/// takes a lock.
const BUSY_TIMEOUT: &str = "PRAGMA busy_timeout=5000;";

/// connection.rs's session pragmas, which make a Node connection behave like
/// a native one.
const SESSION_PRAGMAS: &str = "PRAGMA journal_mode=WAL;
     PRAGMA synchronous=NORMAL;
     PRAGMA foreign_keys=ON;
     PRAGMA cache_size=-64000;
     PRAGMA mmap_size=268435456;
     PRAGMA temp_store=MEMORY;";

/// The pool behind `SqliteStorageEngine::open`.
pub(crate) async fn file_pool(path: &Path) -> anyhow::Result<Pool> {
    let path = path.to_str().ok_or_else(|| anyhow::anyhow!("the database path is not valid UTF-8"))?;
    Ok(NodePool::new(NodeConnection::open(path)?))
}

/// The pool behind `SqliteStorageEngine::open_in_memory`.
pub(crate) async fn memory_pool() -> anyhow::Result<Pool> { Ok(NodePool::new(NodeConnection::open(":memory:")?)) }

impl SqliteStorageEngine {
    /// Adopt a database object the host constructed: Node's `DatabaseSync`, or
    /// a compatible driver whose statements support positional arrays and BigInt
    /// INTEGER results. The host owns session settings, including busy timeout.
    pub async fn from_database(database: JsValue) -> anyhow::Result<Self> {
        Self::new(NodePool::new(NodeConnection::from_database(database)?)).await
    }

    /// The JavaScript database object this engine runs on, for a host that
    /// wants to share it or reopen the engine over it.
    pub fn database(&self) -> &JsValue { self.pool().connection().database() }
}

/// One `DatabaseSync`-shaped database. Clones share the same JavaScript
/// object and the same kept typed readers.
#[derive(Clone)]
pub struct NodeConnection {
    // The JavaScript handles are bound to the thread that made them; WASM has one.
    database: SendWrapper<JsValue>,
    readers: SendWrapper<Rc<RefCell<Readers>>>,
}

/// How many typed readers a connection keeps prepared.
const KEPT_READERS: usize = 64;

/// The typed readers a connection has prepared, by the SQL text of the
/// statement each reads, so that a query run again skips both prepares,
/// which cost a point lookup several times what reading its row does. At
/// most [`KEPT_READERS`] are kept, the least recently used leaving first.
/// SQLite prepares a kept statement again when the schema changes, but a
/// typed reader names its statement's columns, which a `SELECT *` may have
/// gained or lost; so every kept reader is dropped when the schema version
/// of main or temp changes. A reader is kept only for a statement that is not
/// a compound SELECT at its top level (see `refuse_a_compound_select`).
#[derive(Default)]
struct Readers {
    /// `PRAGMA main.schema_version` and `PRAGMA temp.schema_version`, prepared once.
    version_statements: Option<[JsValue; 2]>,
    /// The schema versions the kept readers were prepared at.
    versions: Option<[i64; 2]>,
    kept: HashMap<String, KeptReader>,
    /// Counts the uses of kept readers, to find the least recently used.
    clock: u64,
}

struct KeptReader {
    statement: JsValue,
    width: usize,
    used: u64,
}

impl NodeConnection {
    /// Adopt a database object the host constructed: Node's `DatabaseSync` or
    /// a compatible driver whose statements support positional arrays and BigInt
    /// INTEGER results. The host owns session settings, including busy timeout.
    pub fn from_database(database: JsValue) -> Result<Self, SqliteError> {
        if !database.is_object() {
            return Err(SqliteError::Node(format!(
                "a database must be an object with prepare and exec methods, such as node:sqlite's DatabaseSync; got {database:?}"
            )));
        }
        method(&database, "prepare")?;
        method(&database, "exec")?;
        let connection = Self { database: SendWrapper::new(database), readers: SendWrapper::new(Rc::default()) };
        // A database whose statements do not hand rows over as the executor
        // reads them is refused here rather than at its first query.
        let probe = connection.query("SELECT 1", &[])?;
        if !matches!(probe.as_slice(), [row] if matches!(row.values(), [SqliteValue::Integer(1)])) {
            return Err(SqliteError::Node(format!("`SELECT 1` read back as {probe:?}, not as one row holding the INTEGER 1")));
        }
        // The typed reader takes TEXT as the bytes SQLite stores, which are
        // UTF-8 only in a UTF-8 database. rusqlite has SQLite convert the TEXT
        // of a UTF-16 database to UTF-8; node:sqlite offers no way to, so a
        // UTF-16 database is refused rather than misread.
        let utf8 = connection.query("SELECT (SELECT encoding FROM pragma_encoding) = 'UTF-8'", &[])?;
        if !matches!(utf8.as_slice(), [row] if matches!(row.values(), [SqliteValue::Integer(1)])) {
            return Err(SqliteError::Node(
                "the database's text encoding is not UTF-8, and the node executor reads TEXT as UTF-8 bytes".into(),
            ));
        }
        // A double-quoted name that matches no column fails with "no such
        // column" on every connection the engine opens natively
        // (connection.rs), so a database that reads such a name as a string
        // literal is refused rather than let it read as data: the probe
        // prepares only on such a database.
        match connection.prepare(r#"SELECT "_ankurah_dqs_probe""#) {
            Ok(_) => {
                return Err(SqliteError::Node(
                    "the database reads a double-quoted name that matches no column as a string literal, where the engine \
                     refuses such a name: turn double-quoted string literals off (node:sqlite's DatabaseSync has them off \
                     unless it is constructed with `enableDoubleQuotedStringLiterals: true`)"
                        .into(),
                ));
            }
            Err(refusal) if refusal.to_string().contains("no such column") && refusal.to_string().contains("_ankurah_dqs_probe") => {}
            Err(error) => return Err(error),
        }
        Ok(connection)
    }

    /// Open a database through Node's built-in module; `":memory:"` for an
    /// in-memory one. Applies the session settings a native connection has.
    pub fn open(path: &str) -> Result<Self, SqliteError> {
        let global = js_sys::global();
        let process = Reflect::get(&global, &"process".into()).map_err(js_error)?;
        if process.is_undefined() || process.is_null() {
            return Err(SqliteError::Node("no `process` global: the node executor runs only inside Node.js".into()));
        }
        let get_builtin = method(&process, "getBuiltinModule")?;
        let module = get_builtin.call1(&process, &"node:sqlite".into()).map_err(js_error)?;
        if module.is_undefined() || module.is_null() {
            return Err(SqliteError::Node("`node:sqlite` is unavailable: it needs Node.js 22.16 or later".into()));
        }
        let constructor: Function = Reflect::get(&module, &"DatabaseSync".into())
            .map_err(js_error)?
            .dyn_into()
            .map_err(|_| SqliteError::Node("`node:sqlite` exports no DatabaseSync constructor".into()))?;
        let database = Reflect::construct(&constructor, &Array::of1(&path.into())).map_err(js_error)?;
        // The busy timeout comes before any other statement, as rusqlite sets
        // it at open, since from_database's checks already read the file.
        method(&database, "exec")?.call1(&database, &BUSY_TIMEOUT.into()).map_err(js_error)?;
        let connection = Self::from_database(database)?;
        // Only a database the executor accepts is switched to WAL.
        connection.exec(SESSION_PRAGMAS)?;
        Ok(connection)
    }

    /// The JavaScript database object.
    pub fn database(&self) -> &JsValue { &self.database }

    /// Run one or more statements that yield nothing, as `exec` does.
    pub fn exec(&self, sql: &str) -> Result<(), SqliteError> {
        method(&self.database, "exec")?.call1(&self.database, &sql.into()).map_err(js_error)?;
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

    /// The typed reader kept for `sql` and its statement's width, once every
    /// kept reader has been dropped if the schema changed since they were
    /// prepared.
    fn kept_reader(&self, sql: &str) -> Result<Option<(JsValue, usize)>, SqliteError> {
        let versions = self.schema_versions()?;
        let mut readers = self.readers.borrow_mut();
        if readers.versions != Some(versions) {
            readers.kept.clear();
            readers.versions = Some(versions);
        }
        readers.clock += 1;
        let clock = readers.clock;
        Ok(readers.kept.get_mut(sql).map(|reader| {
            reader.used = clock;
            (reader.statement.clone(), reader.width)
        }))
    }

    /// Keep a typed reader just prepared for `sql`, making room by dropping
    /// the least recently used one.
    fn keep_reader(&self, sql: &str, statement: &JsValue, width: usize) {
        let mut readers = self.readers.borrow_mut();
        if readers.kept.len() >= KEPT_READERS {
            let oldest = readers.kept.iter().min_by_key(|(_, reader)| reader.used).map(|(sql, _)| sql.clone());
            if let Some(oldest) = oldest {
                readers.kept.remove(&oldest);
            }
        }
        readers.clock += 1;
        let used = readers.clock;
        readers.kept.insert(sql.to_owned(), KeptReader { statement: statement.clone(), width, used });
    }

    /// The schema versions of main and temp, which change whenever their schemas do.
    fn schema_versions(&self) -> Result<[i64; 2], SqliteError> {
        let prepared = self.readers.borrow().version_statements.clone();
        let statements = match prepared {
            Some(statements) => statements,
            None => {
                let statements = [self.prepare("PRAGMA main.schema_version")?, self.prepare("PRAGMA temp.schema_version")?];
                self.readers.borrow_mut().version_statements = Some(statements.clone());
                statements
            }
        };
        let mut versions = [0; 2];
        for (version, statement) in versions.iter_mut().zip(&statements) {
            let row: Array = statement_method(statement, "get")?
                .call0(statement)
                .map_err(js_error)?
                .dyn_into()
                .map_err(|row| SqliteError::Node(format!("`PRAGMA schema_version` returned {row:?}, not a row")))?;
            *version = integer_from_js(&row.get(0))?;
        }
        Ok(versions)
    }

    /// Refuse a compound SELECT (UNION, UNION ALL, INTERSECT or EXCEPT) at the
    /// top level of a query, before it runs. Nested in the typed reader, a
    /// compound SELECT's rows take the affinity of its first SELECT's
    /// columns, which can change a value from a later SELECT: an INTEGER past
    /// 2^53 read through a REAL column reads as the nearest REAL. rusqlite
    /// reads the compound directly, where no affinity applies. SQLite's plan
    /// shows a compound at its top level as a COMPOUND QUERY, or as a MERGE
    /// when SQLite codes it as a merge of its SELECTs, as it does under an
    /// ORDER BY and, in SQLite 3.53, for UNION, INTERSECT and EXCEPT. A
    /// compound in a subquery or a CTE takes that affinity natively too, and
    /// is let through, unless SQLite flattens it into the query.
    fn refuse_a_compound_select(&self, source: &str, params: &[SqliteValueRef<'_>]) -> Result<(), SqliteError> {
        // Each step of the plan is id, parent, notused, detail. better-sqlite3
        // plans a statement only with its parameters bound.
        let plan = self.prepare(&format!("EXPLAIN QUERY PLAN {source}"))?;
        for step in all(&plan, params, 4)? {
            let detail = step.get(3);
            let detail = detail.as_string().ok_or_else(|| SqliteError::Node(format!("a step of a query plan reads {detail:?}")))?;
            if integer_from_js(&step.get(1))? == 0 && (detail == "COMPOUND QUERY" || detail.starts_with("MERGE (")) {
                return Err(SqliteError::Node(
                    "a compound SELECT at the top level of a query is refused on Node, before it runs: the node executor \
                     reads rows through a typed reader that nests the query, where every row of a compound SELECT takes \
                     the affinity of its first SELECT's columns, which can change a value from a later SELECT, as an \
                     INTEGER past 2^53 read through a REAL column changes to the nearest REAL; select from it as \
                     `WITH x AS MATERIALIZED (…) SELECT * FROM x`, which reads the same on every driver"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    /// Prepare a statement that hands rows over as the executor reads them.
    fn prepare(&self, sql: &str) -> Result<JsValue, SqliteError> {
        let statement = method(&self.database, "prepare")?.call1(&self.database, &sql.into()).map_err(js_error)?;
        return_arrays(&statement)?;
        // INTEGERs as BigInt, so that each storage class arrives as a
        // JavaScript type of its own and a value past 2^53 arrives exactly.
        read_big_ints(&statement)?;
        Ok(statement)
    }
}

/// The node executor's stand-in for a pool: one database, handed out by clone.
#[derive(Clone)]
pub struct NodePool {
    connection: NodeConnection,
}

impl NodePool {
    pub fn new(connection: NodeConnection) -> Self { Self { connection } }

    pub async fn get(&self) -> Result<NodeConnection, SqliteError> { Ok(self.connection.clone()) }

    pub fn connection(&self) -> &NodeConnection { &self.connection }
}

impl SqliteExecutor for NodeConnection {
    /// Runs a statement as rusqlite's `execute` does: one step, refusing the
    /// statement when that step yields a row, since a statement that returns
    /// rows belongs to `query`. node:sqlite's `run` takes the step and
    /// discards the row.
    fn execute(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError> {
        let statement = self.prepare(sql)?;
        if width(&statement)? == 0 {
            // A statement without columns yields no row.
            let result = statement_method(&statement, "run")?.apply(&statement, &js_params(params)).map_err(js_error)?;
            return changed_rows(&result);
        }
        // `get` takes one step and resets the statement, returning the row
        // the step yielded, or undefined when it finished the statement.
        let row = statement_method(&statement, "get")?.apply(&statement, &js_params(params)).map_err(js_error)?;
        if !row.is_undefined() {
            // rusqlite's words for the same refusal.
            return Err(SqliteError::Node("Execute returned results - did you mean to call query?".into()));
        }
        // rusqlite then reports the connection's count of changed rows, which
        // a statement that yields rows leaves as the last change set it.
        match self.query("SELECT changes()", &[])?.as_slice() {
            [row] => {
                let changes = row.integer(0)?;
                usize::try_from(changes).map_err(|_| SqliteError::Node(format!("`changes()` reported {changes} changed rows")))
            }
            rows => Err(SqliteError::Node(format!("`SELECT changes()` returned {} rows", rows.len()))),
        }
    }

    fn query(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError> {
        if let Some((reader, width)) = self.kept_reader(sql)? {
            return all(&reader, params, 2 * width)?.into_iter().map(typed_row).collect();
        }
        let statement = self.prepare(sql)?;
        let width = width(&statement)?;
        if width == 0 {
            // A statement without columns yields no rows; it runs for its
            // effect, through `run`, since better-sqlite3 refuses `all` on a
            // statement it reports yields no rows.
            statement_method(&statement, "run")?.apply(&statement, &js_params(params)).map_err(js_error)?;
            return Ok(Vec::new());
        }
        let source = source(&statement)?;
        match self.prepare(&typed_reader(&source, width)) {
            Ok(reader) => {
                // Checked once for each SQL text, since a kept reader skips it.
                self.refuse_a_compound_select(&source, params)?;
                self.keep_reader(sql, &reader, width);
                all(&reader, params, 2 * width)?.into_iter().map(typed_row).collect()
            }
            // SQLite nests only a query (SELECT, VALUES, or WITH and a SELECT)
            // in a WITH clause. A PRAGMA or an EXPLAIN is read as node:sqlite
            // hands it over, with its TEXT refused. Any other statement that
            // yields rows, such as one with a RETURNING clause, could write
            // before a TEXT value in its rows were refused, so it is refused
            // before it runs.
            Err(nesting) => {
                if !["PRAGMA", "EXPLAIN"].iter().any(|keyword| first_keyword(&source).eq_ignore_ascii_case(keyword)) {
                    return Err(SqliteError::Node(format!(
                        "the node executor reads rows exactly only from a query SQLite can nest in a WITH clause, which this \
                         statement is not ({nesting}); a statement such as one with a RETURNING clause could write before a \
                         value of its rows were refused, so it is not run: write with `execute` and read with a query"
                    )));
                }
                all(&statement, params, width)?.into_iter().map(|row| plain_row(row, &nesting)).collect()
            }
        }
    }
}

/// The first keyword of a statement's SQL, past the whitespace and comments
/// SQLite skips before it.
fn first_keyword(sql: &str) -> &str {
    let mut rest = sql;
    loop {
        rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
        if let Some(comment) = rest.strip_prefix("--") {
            rest = comment.split_once('\n').map_or("", |(_, after)| after);
        } else if let Some(comment) = rest.strip_prefix("/*") {
            rest = comment.split_once("*/").map_or("", |(_, after)| after);
        } else {
            break;
        }
    }
    let end = rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).unwrap_or(rest.len());
    &rest[..end]
}

/// The SQL of a typed reader for `statement`, whose rows have `width`
/// columns: the statement nested in a WITH clause, and each of its columns
/// read as two, the value's storage class from `typeof()` and the value
/// itself, TEXT cast to the BLOB of its bytes. MATERIALIZED has SQLite
/// compute each value once, so that the class and the value read are of the
/// same one; SQLite keeps a nested ORDER BY when the query around it has
/// none, and a materialized statement's rows are read in the order it
/// yielded them.
fn typed_reader(statement: &str, width: usize) -> String {
    // A statement's source keeps the semicolon that ended it, which a nested
    // statement cannot have.
    let statement = statement.trim_end().strip_suffix(';').unwrap_or(statement);
    let names = (0..width).map(|index| format!("c{index}")).collect::<Vec<_>>().join(", ");
    let columns = (0..width)
        .map(|index| format!("typeof(c{index}), CASE typeof(c{index}) WHEN 'text' THEN CAST(c{index} AS BLOB) ELSE c{index} END"))
        .collect::<Vec<_>>()
        .join(", ");
    // The statement stands on lines of its own, so that a comment ending it
    // cannot swallow the parenthesis that closes it.
    format!("WITH _ankurah_typed_rows({names}) AS MATERIALIZED (\n{statement}\n) SELECT {columns} FROM _ankurah_typed_rows")
}

/// A statement's SQL, which is one statement alone: node:sqlite's
/// `sourceSQL`, the first statement as SQLite compiled it, or
/// better-sqlite3's `source`, the SQL it prepared, which it refuses to
/// prepare when it holds more than one.
fn source(statement: &JsValue) -> Result<String, SqliteError> {
    for name in ["sourceSQL", "source"] {
        if let Some(source) = Reflect::get(statement, &name.into()).map_err(js_error)?.as_string() {
            return Ok(source);
        }
    }
    Err(SqliteError::Node(
        "the database's statements report no `sourceSQL` (better-sqlite3's `source`), which the node executor needs".into(),
    ))
}

/// A row of a typed reader, a storage class and a value for each column of
/// its statement, read as the statement's row. TEXT comes as its bytes and
/// is decoded as strictly as rusqlite decodes it.
fn typed_row(row: Array) -> Result<Row, SqliteError> {
    (0..row.length() / 2)
        .map(|column| {
            let index = column as usize;
            let class = row.get(2 * column);
            let class =
                class.as_string().ok_or_else(|| SqliteError::Node(format!("the storage class of column {index} arrived as {class:?}")))?;
            match (class.as_str(), value_from_js(index, row.get(2 * column + 1))?) {
                ("text", SqliteValue::Blob(bytes)) => String::from_utf8(bytes).map(SqliteValue::Text).map_err(|error| {
                    // rusqlite's words for the same refusal.
                    SqliteError::Node(format!("Conversion error from type Text at index: {index}, {}", error.utf8_error()))
                }),
                ("null", value @ SqliteValue::Null)
                | ("integer", value @ SqliteValue::Integer(_))
                | ("real", value @ SqliteValue::Real(_))
                | ("blob", value @ SqliteValue::Blob(_)) => Ok(value),
                (class, value) => {
                    Err(SqliteError::Node(format!("column {index} holds a value of storage class {class} that arrived as {value:?}")))
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Row::new)
}

/// A row of a PRAGMA or an EXPLAIN, which SQLite would not nest in a typed
/// reader, read as node:sqlite hands it over. Its TEXT arrives only as a
/// JavaScript string that may have lost bytes, so a TEXT value is refused,
/// naming why the statement could not be nested.
fn plain_row(row: Array, nesting: &SqliteError) -> Result<Row, SqliteError> {
    row.iter()
        .enumerate()
        .map(|(index, value)| {
            if value.is_string() {
                return Err(SqliteError::Node(format!(
                    "column {index} is TEXT, which node:sqlite hands over only as a string cut at its first NUL and with every \
                     byte that is not UTF-8 replaced; the node executor reads TEXT exactly only from a query SQLite can nest in \
                     a WITH clause, which this statement is not ({nesting}), so select the value through a query instead, such \
                     as a table-valued pragma function like pragma_table_info"
                )));
            }
            value_from_js(index, value)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Row::new)
}

/// Ask a statement for its rows as arrays, which keep every column at its
/// position in the statement: node:sqlite's `setReturnArrays`, or
/// better-sqlite3's `raw`. Their default, objects keyed by column name,
/// cannot: a name that reads as an integer moves ahead of the others, and two
/// columns of one name collapse into one. better-sqlite3 refuses `raw` on a
/// statement that yields no rows, which has none to hand over.
fn return_arrays(statement: &JsValue) -> Result<(), SqliteError> {
    if let Ok(set_return_arrays) = method(statement, "setReturnArrays") {
        set_return_arrays.call1(statement, &JsValue::TRUE).map_err(js_error)?;
    } else if let Ok(raw) = method(statement, "raw") {
        if yields_rows(statement)? {
            raw.call1(statement, &JsValue::TRUE).map_err(js_error)?;
        }
    } else {
        return Err(SqliteError::Node("positional rows require setReturnArrays (Node.js 22.16+) or raw".into()));
    }
    Ok(())
}

/// Ask a statement for its INTEGERs as BigInt: node:sqlite's
/// `setReadBigInts`, or better-sqlite3's `safeIntegers`.
fn read_big_ints(statement: &JsValue) -> Result<(), SqliteError> {
    if let Ok(set_read_big_ints) = method(statement, "setReadBigInts") {
        set_read_big_ints.call1(statement, &JsValue::TRUE).map_err(js_error)?;
    } else if let Ok(safe_integers) = method(statement, "safeIntegers") {
        safe_integers.call1(statement, &JsValue::TRUE).map_err(js_error)?;
    } else {
        return Err(SqliteError::Node("exact INTEGER results require setReadBigInts or safeIntegers".into()));
    }
    Ok(())
}

/// Whether a statement can yield rows, as better-sqlite3 reports it in
/// `reader`. node:sqlite's statements have no `reader`, and are taken to.
fn yields_rows(statement: &JsValue) -> Result<bool, SqliteError> {
    Ok(Reflect::get(statement, &"reader".into()).map_err(js_error)?.as_bool() != Some(false))
}

/// How many columns each row of a statement holds: none for a statement
/// better-sqlite3 reports yields no rows, whose `columns()` it refuses.
fn width(statement: &JsValue) -> Result<usize, SqliteError> {
    if !yields_rows(statement)? {
        return Ok(0);
    }
    let columns: Array = statement_method(statement, "columns")?
        .call0(statement)
        .map_err(js_error)?
        .dyn_into()
        .map_err(|_| SqliteError::Node("`columns()` did not return an array".into()))?;
    Ok(columns.length() as usize)
}

/// Run a statement and take every row it yields, refusing any row that is
/// not an array of `width` values before a value of it is read.
fn all(statement: &JsValue, params: &[SqliteValueRef<'_>], width: usize) -> Result<Vec<Array>, SqliteError> {
    let rows: Array = statement_method(statement, "all")?
        .apply(statement, &js_params(params))
        .map_err(js_error)?
        .dyn_into()
        .map_err(|_| SqliteError::Node("`all()` did not return an array".into()))?;
    rows.iter()
        .map(|row| {
            let row: Array =
                row.dyn_into().map_err(|row| SqliteError::Node(format!("a statement returned a row that is not an array: {row:?}")))?;
            if row.length() as usize != width {
                return Err(SqliteError::Node(format!("a statement of {width} columns returned a row of {}", row.length())));
            }
            Ok(row)
        })
        .collect()
}

/// The count of rows `run()` reports it changed, which must fit a usize: a
/// BigInt from node:sqlite, whose statements here read BigInts, or a number
/// from better-sqlite3, which counts changes as a number whatever its
/// statements read.
fn changed_rows(result: &JsValue) -> Result<usize, SqliteError> {
    let changes = Reflect::get(result, &"changes".into()).map_err(js_error)?;
    let count = match changes.as_f64() {
        Some(count) if count.fract() == 0.0 => count as i64,
        _ => integer_from_js(&changes)?,
    };
    usize::try_from(count).map_err(|_| SqliteError::Node(format!("`run()` reported {count} changed rows")))
}

fn js_params(params: &[SqliteValueRef<'_>]) -> Array { params.iter().map(js_param).collect() }

fn js_param(value: &SqliteValueRef<'_>) -> JsValue {
    match *value {
        SqliteValueRef::Text(text) => JsValue::from_str(text),
        // BigInt binds as INTEGER exactly; a JavaScript number would bind as REAL.
        SqliteValueRef::Integer(integer) => BigInt::from(integer).into(),
        SqliteValueRef::Real(real) => JsValue::from_f64(real),
        SqliteValueRef::Blob(bytes) => Uint8Array::from(bytes).into(),
        // The SQL wraps this placeholder in jsonb(?), so the text form binds.
        SqliteValueRef::Jsonb(json) => JsValue::from_str(&json.to_string()),
        SqliteValueRef::Null => JsValue::NULL,
    }
}

/// The value in column `index` of a row, as node:sqlite hands each storage
/// class but TEXT over to a statement reading BigInts: NULL as null, INTEGER
/// as BigInt, REAL as number (2.0 included), BLOB as Uint8Array. A string,
/// node:sqlite's lossy form of TEXT, is refused, and so is anything else,
/// `undefined` among it, rather than read as the nearest storage class.
fn value_from_js(index: usize, value: JsValue) -> Result<SqliteValue, SqliteError> {
    if value.is_null() {
        Ok(SqliteValue::Null)
    } else if value.is_bigint() {
        integer_from_js(&value).map(SqliteValue::Integer)
    } else if let Some(real) = value.as_f64() {
        Ok(SqliteValue::Real(real))
    } else if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
        Ok(SqliteValue::Blob(bytes.to_vec()))
    } else if value.is_string() {
        Err(SqliteError::Node(format!("column {index} holds TEXT as a string, which may have lost bytes, where its bytes were expected")))
    } else {
        Err(SqliteError::Node(format!("column {index} holds {value:?}, which node:sqlite hands over for no SQLite storage class")))
    }
}

/// An INTEGER, which node:sqlite hands over as a BigInt.
fn integer_from_js(value: &JsValue) -> Result<i64, SqliteError> {
    if !value.is_bigint() {
        return Err(SqliteError::Node(format!("expected an INTEGER as a BigInt, found {value:?}")));
    }
    i64::try_from(value.clone().unchecked_into::<BigInt>()).map_err(|_| SqliteError::Node(format!("{value:?} does not fit in 64 bits")))
}

fn method(target: &JsValue, name: &str) -> Result<Function, SqliteError> {
    Reflect::get(target, &name.into())
        .map_err(js_error)?
        .dyn_into::<Function>()
        .map_err(|_| SqliteError::Node(format!("the database object has no `{name}` method")))
}

/// A method of a statement the database prepared. node:sqlite's statements
/// have every method the executor calls from Node.js 22.16.
fn statement_method(statement: &JsValue, name: &str) -> Result<Function, SqliteError> {
    Reflect::get(statement, &name.into()).map_err(js_error)?.dyn_into::<Function>().map_err(|_| {
        SqliteError::Node(format!(
            "the database's statements have no `{name}` method, which the node executor needs; node:sqlite's statements have \
             every method it calls from Node.js 22.16"
        ))
    })
}

/// A JavaScript exception as an error. One that node:sqlite raises for SQLite
/// carries SQLite's extended result code as `errcode`, which is kept, as
/// rusqlite keeps it natively. better-sqlite3's errors carry no `errcode`,
/// only the code's name as `code`, which is deliberately not read, since
/// turning a name back into its number takes a table of SQLite's codes: on
/// better-sqlite3 an error SQLite raised has no [`SqliteError::sqlite_code`].
fn js_error(value: JsValue) -> SqliteError {
    let message = if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        String::from(error.message())
    } else if let Some(text) = value.as_string() {
        text
    } else {
        format!("{value:?}")
    };
    let code = if value.is_object() { Reflect::get(&value, &"errcode".into()).ok().and_then(|code| code.as_f64()) } else { None };
    match code {
        Some(code) if code.fract() == 0.0 && (f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&code) => {
            SqliteError::NodeSqlite { code: code as i32, message }
        }
        _ => SqliteError::Node(message),
    }
}
