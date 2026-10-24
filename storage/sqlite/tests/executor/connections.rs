//! The session settings and lock waits of the connections the engine opens.

use ankurah_storage_sqlite::{executor::TransactionBehavior, SqliteStorageEngine};
use anyhow::Result;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(target_arch = "wasm32")]
use crate::common::driver;
use crate::common::{self, engine_test, TempDb};

/// A connection the engine opens sets a busy timeout of five seconds, on
/// every driver: rusqlite sets it on each connection it opens, and the
/// Node.js executor before any other statement.
/// `a_writer_waits_for_a_lock_another_connection_holds` tests that a writer
/// then waits for another's lock.
#[engine_test]
#[cfg_attr(
    ankurah_sqlite_better_sqlite3,
    ignore = "the engine opens no better-sqlite3 database, and one it adopts keeps the busy timeout its host set"
)]
async fn connections_set_a_busy_timeout_of_five_seconds() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let timeout = storage.with_executor(|executor| executor.query_row("PRAGMA busy_timeout", &[])?.expect("one row").integer(0)).await?;
    assert_eq!(timeout, 5000);
    Ok(())
}

/// A connection the engine opens on a file takes the session settings of a
/// native one, on every driver.
#[engine_test]
#[cfg_attr(
    ankurah_sqlite_better_sqlite3,
    ignore = "the engine opens no better-sqlite3 database, and one it adopts keeps the session settings its host chose"
)]
async fn a_connection_on_a_file_takes_the_session_settings() -> Result<()> {
    let db = TempDb::new("settings");
    let storage = SqliteStorageEngine::open(db.path()).await?;
    assert_eq!(session_settings(&storage).await?, (5000, "wal".to_owned(), 1, 1, -64000, 268_435_456, 2));
    Ok(())
}

/// How long the other connection in the lock tests holds its lock: long
/// enough for a wait to show, and well inside the 5000 ms busy timeout.
const HOLD_MS: u64 = 400;

/// A writer waits for another connection's write lock on the same file and
/// takes it once released, rather than failing at once.
#[engine_test]
async fn a_writer_waits_for_a_lock_another_connection_holds() -> Result<()> {
    let db = TempDb::new("writer_waits");
    let storage = common::open(db.path()).await?;
    let holder = LockHolder::take(&db, "BEGIN IMMEDIATE", HOLD_MS).await;
    let waited = Stopwatch::start();
    storage.with_executor(|executor| executor.begin(TransactionBehavior::Immediate)?.commit()).await?;
    let waited = waited.elapsed_ms();
    holder.released().await;
    assert!(waited >= HOLD_MS as f64 / 2.0, "the writer took the lock after {waited} ms, before it was released");
    Ok(())
}

/// A writer refused the lock another connection holds, past its busy
/// timeout, fails with SQLITE_BUSY, 5, on every driver. The test cuts the
/// timeout to none, so that the writer fails at once.
#[engine_test]
async fn a_lock_held_past_the_busy_timeout_fails_with_sqlite_busy() -> Result<()> {
    let db = TempDb::new("writer_busy");
    let storage = common::open(db.path()).await?;
    let holder = LockHolder::take(&db, "BEGIN IMMEDIATE", HOLD_MS).await;
    let refused = storage
        .with_executor(|executor| {
            executor.query_row("PRAGMA busy_timeout = 0", &[])?;
            Ok(executor.begin(TransactionBehavior::Immediate).map(drop).map_err(|error| error.sqlite_code()))
        })
        .await?;
    holder.released().await;
    assert_eq!(refused, Err(Some(5)));
    Ok(())
}

/// Opening a store waits for a lock another connection holds on its file
/// rather than failing at once: the busy timeout is set before any statement
/// that needs the file.
#[engine_test]
#[cfg_attr(
    ankurah_sqlite_better_sqlite3,
    ignore = "the engine opens no better-sqlite3 database, and one it adopts keeps the busy timeout its host set"
)]
async fn opening_a_store_waits_for_a_lock_on_its_file() -> Result<()> {
    let db = TempDb::new("open_waits");
    // A new file starts in rollback-journal mode, where BEGIN EXCLUSIVE keeps
    // out readers too.
    let holder = LockHolder::take(&db, "BEGIN EXCLUSIVE", HOLD_MS).await;
    let waited = Stopwatch::start();
    let storage = SqliteStorageEngine::open(db.path()).await?;
    let waited = waited.elapsed_ms();
    holder.released().await;
    assert!(waited >= HOLD_MS as f64 / 2.0, "the store opened after {waited} ms, before the lock was released");
    assert_eq!(session_settings(&storage).await?.1, "wal");
    Ok(())
}

/// The session settings a connection of `storage` runs with, in the order
/// the executor sets them: busy timeout, journal mode, synchronous, foreign
/// keys, cache size, mmap size, temp store.
async fn session_settings(storage: &SqliteStorageEngine) -> anyhow::Result<(i64, String, i64, i64, i64, i64, i64)> {
    Ok(storage
        .with_executor(|executor| {
            let settings = executor
                .query_row(
                    "SELECT (SELECT timeout FROM pragma_busy_timeout), (SELECT journal_mode FROM pragma_journal_mode),
                            (SELECT synchronous FROM pragma_synchronous), (SELECT foreign_keys FROM pragma_foreign_keys),
                            (SELECT cache_size FROM pragma_cache_size), (SELECT temp_store FROM pragma_temp_store)",
                    &[],
                )?
                .expect("one row");
            // mmap_size has no table-valued function; the PRAGMA's INTEGER reads the same on both drivers.
            let mmap_size = executor.query_row("PRAGMA mmap_size", &[])?.expect("one row").integer(0)?;
            Ok((
                settings.integer(0)?,
                settings.text(1)?.to_owned(),
                settings.integer(2)?,
                settings.integer(3)?,
                settings.integer(4)?,
                mmap_size,
                settings.integer(5)?,
            ))
        })
        .await?)
}

/// Another connection to a database file, on a thread of its own, holding a
/// real lock on it: a std thread natively, a worker thread on Node.js. It runs
/// `lock` (a BEGIN), and commits once `hold_ms` have passed, while this
/// thread's connection waits for the lock.
struct LockHolder {
    #[cfg(not(target_arch = "wasm32"))]
    thread: std::thread::JoinHandle<()>,
    /// Settles when the worker has committed and exited.
    #[cfg(target_arch = "wasm32")]
    exit: js_sys::Promise,
}

/// The worker's side of a [`LockHolder`] on Node.js. Its connection is on the
/// driver the engine's is on, because two copies of SQLite in one process do
/// not see each other's locks: the file locks they take belong to the
/// process, so neither copy's locks keep out the other's connections.
#[cfg(target_arch = "wasm32")]
const LOCK_HOLDER_WORKER: &str = r#"
const { parentPort, workerData } = require('node:worker_threads');
const Database = workerData.betterSqlite3 ? require(workerData.betterSqlite3) : require('node:sqlite').DatabaseSync;
const database = new Database(workerData.path);
database.exec(workerData.lock);
parentPort.postMessage('locked');
setTimeout(() => { database.exec('COMMIT'); database.close(); }, workerData.holdMs);
"#;

impl LockHolder {
    /// Start the other connection, returning once it holds the lock.
    #[cfg(not(target_arch = "wasm32"))]
    async fn take(db: &TempDb, lock: &'static str, hold_ms: u64) -> Self {
        let path = db.path().to_owned();
        let (locked, on_locked) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let connection = rusqlite::Connection::open(path).expect("the other connection opens the file");
            connection.execute_batch(lock).expect("the other connection takes the lock");
            locked.send(()).expect("this thread waits for the lock");
            std::thread::sleep(Duration::from_millis(hold_ms));
            connection.execute_batch("COMMIT").expect("the other connection releases the lock");
        });
        on_locked.recv().expect("the other connection takes the lock");
        Self { thread }
    }

    /// Start the other connection, returning once it holds the lock.
    #[cfg(target_arch = "wasm32")]
    async fn take(db: &TempDb, lock: &'static str, hold_ms: u64) -> Self {
        use wasm_bindgen::JsCast;
        // Both promises are made before this thread yields, so neither event
        // can be missed; the exit's rejection, if the worker fails, is
        // reported where `released` awaits it.
        let start = js_sys::Function::new_with_args(
            "source, path, lock, holdMs, betterSqlite3",
            "const { Worker } = process.getBuiltinModule('node:worker_threads');
             const { once } = process.getBuiltinModule('node:events');
             const worker = new Worker(source, { eval: true, workerData: { path, lock, holdMs, betterSqlite3 } });
             const exit = once(worker, 'exit');
             exit.catch(() => {});
             return once(worker, 'message').then(() => ({ exit }));",
        );
        let better_sqlite3 = if driver::on_better_sqlite3() { driver::better_sqlite3_entry().into() } else { wasm_bindgen::JsValue::NULL };
        let arguments =
            js_sys::Array::of5(&LOCK_HOLDER_WORKER.into(), &db.path_str().into(), &lock.into(), &(hold_ms as f64).into(), &better_sqlite3);
        let started: js_sys::Promise = start.apply(&wasm_bindgen::JsValue::NULL, &arguments).unwrap().dyn_into().unwrap();
        let handle = wasm_bindgen_futures::JsFuture::from(started).await.expect("the other connection takes the lock");
        Self { exit: js_sys::Reflect::get(&handle, &"exit".into()).unwrap().dyn_into().unwrap() }
    }

    /// Wait until the other connection has committed and closed.
    async fn released(self) {
        #[cfg(not(target_arch = "wasm32"))]
        self.thread.join().expect("the other connection releases the lock");
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::JsFuture::from(self.exit).await.expect("the other connection releases the lock");
    }
}

/// Milliseconds elapsed since it started, on either target.
struct Stopwatch(f64);

impl Stopwatch {
    fn start() -> Self { Self(now_ms()) }

    fn elapsed_ms(&self) -> f64 { now_ms() - self.0 }
}

#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock before epoch").as_secs_f64() * 1000.0
}

#[cfg(target_arch = "wasm32")]
fn now_ms() -> f64 { js_sys::Date::now() }
