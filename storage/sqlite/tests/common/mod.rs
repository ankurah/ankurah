#[allow(unused)]
pub use ankurah::signals::{Peek, Subscribe};
#[cfg(not(target_arch = "wasm32"))]
use tracing::Level;

#[allow(unused)]
pub use ankurah::{
    changes::{ChangeKind, ChangeSet},
    error::MutationError,
    model::View,
    policy::DEFAULT_CONTEXT,
    proto,
    signals::{
        broadcast::{BroadcastListener, IntoBroadcastListener},
        subscribe::IntoSubscribeListener,
    },
    Context, EntityId, LiveQuery, Model, Node, PermissiveAgent,
};
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::Notify;

pub mod driver;
#[cfg(target_arch = "wasm32")]
#[allow(unused)]
pub use driver::host_database;
#[allow(unused)]
pub use driver::{on_better_sqlite3, open, open_in_memory};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}

// Initialize tracing for tests
#[cfg(not(target_arch = "wasm32"))]
#[ctor::ctor]
fn init_tracing() {
    // if LOG_LEVEL env var is set, use it
    if let Ok(level) = std::env::var("LOG_LEVEL") {
        tracing_subscriber::fmt().with_max_level(Level::from_str(&level).unwrap()).with_test_writer().init();
    } else {
        tracing_subscriber::fmt().with_max_level(Level::INFO).with_test_writer().init();
    }
}

/// Generic watcher that accumulates notifications and provides async waiting methods.
/// It waits on tokio timers, so it exists only natively.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct TestWatcher<T, U> {
    changes: Arc<Mutex<Vec<T>>>,
    notify: Arc<Notify>,
    transform: Arc<dyn Fn(T) -> U + Send + Sync>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> TestWatcher<T, T> {
    pub fn new() -> Self { Self { changes: Arc::new(Mutex::new(Vec::new())), notify: Arc::new(Notify::new()), transform: Arc::new(|x| x) } }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T, U> TestWatcher<T, U> {
    /// Creates a new generic watcher with identity transform (T -> T)
    pub fn transform(transform: impl Fn(T) -> U + Send + Sync + 'static) -> TestWatcher<T, U> {
        Self { changes: Arc::new(Mutex::new(Vec::new())), notify: Arc::new(Notify::new()), transform: Arc::new(transform) }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<R: View + Send + Sync + 'static> TestWatcher<ChangeSet<R>, Vec<(proto::EntityId, ChangeKind)>> {
    /// Creates a new changeset watcher that transforms ChangeSet<R> to Vec<(EntityId, ChangeKind)>
    pub fn changeset() -> TestWatcher<ChangeSet<R>, Vec<(proto::EntityId, ChangeKind)>> {
        TestWatcher {
            changes: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            transform: Arc::new(|changeset: ChangeSet<R>| changeset.changes.iter().map(|c| (c.entity().id(), c.into())).collect()),
        }
    }
    pub fn changeset_with_event_ids() -> TestWatcher<ChangeSet<R>, Vec<(proto::EntityId, ChangeKind, Vec<proto::EventId>)>> {
        TestWatcher {
            changes: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            transform: Arc::new(|changeset: ChangeSet<R>| {
                changeset.changes.iter().map(|c| (c.entity().id(), c.into(), c.events().iter().map(|e| e.payload.id()).collect())).collect()
            }),
        }
    }

    /// Drains and returns all accumulated changesets, sorting each changeset by the first element of the tuple
    /// Importantly, this does not sort the list of changesets, only the items within each changeset
    pub fn drain_sorted(&self) -> Vec<Vec<(proto::EntityId, ChangeKind)>> {
        let mut changes = self.drain();
        for change in changes.iter_mut() {
            change.sort_by_key(|c| c.0);
        }
        changes
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl<T, U> TestWatcher<T, U> {
    pub fn notify(&self, item: T) {
        self.changes.lock().unwrap().push(item);
        self.notify.notify_waiters();
    }

    /// Takes (empties and returns) all accumulated items, applying the transform
    pub fn drain(&self) -> Vec<U> { self.changes.lock().unwrap().drain(..).map(|item| (self.transform)(item)).collect() }

    /// Waits for exactly `count` items to accumulate, then drains and returns them
    pub async fn take(&self, count: usize) -> Result<Vec<U>, anyhow::Error> {
        if !self.wait_for_count(count, Some(Duration::from_secs(10))).await {
            return Err(anyhow::anyhow!("take({}) timed out waiting for items (waited 10 seconds, got {} items)", count, self.count()));
        }
        let mut changes = self.changes.lock().unwrap();
        Ok(changes.drain(0..count).map(|item| (self.transform)(item)).collect())
    }

    /// Returns the current number of accumulated changesets without draining them
    pub fn count(&self) -> usize { self.changes.lock().unwrap().len() }

    /// Waits for at least one changeset to be accumulated (with 10 second timeout)
    pub async fn wait(&self) -> bool { self.wait_for_count(1, Some(Duration::from_secs(10))).await }

    /// Waits for at least 1 item, then takes and returns exactly the first one
    #[track_caller]
    pub fn take_one(&self) -> impl std::future::Future<Output = U> + '_ {
        let caller = std::panic::Location::caller();
        async move {
            let _success = self.wait_for_count(1, Some(Duration::from_secs(10))).await;
            let mut changes = self.changes.lock().unwrap();
            if changes.is_empty() {
                panic!(
                    "take_one() timed out waiting for items (waited 10 seconds, got {} items) at {}:{}:{}",
                    changes.len(),
                    caller.file(),
                    caller.line(),
                    caller.column()
                );
            }
            let item = changes.remove(0);
            (self.transform)(item)
        }
    }

    /// Waits 100ms for any additional items, then returns the count (useful for asserting quiescence)
    pub async fn quiesce(&self) -> usize {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.count()
    }
    pub async fn quiesce_drain(&self) -> Vec<U> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.drain()
    }

    /// Wait for at least `count` items to accumulate, then drain and return all items
    pub async fn take_when(&self, count: usize) -> Vec<U> {
        self.wait_for_count(count, Some(Duration::from_secs(10))).await;
        self.drain()
    }

    /// Waits for at least `count` changesets to be accumulated, with optional timeout
    pub async fn wait_for_count(&self, count: usize, timeout: Option<Duration>) -> bool {
        // Check if we already have enough changes
        {
            let changes = self.changes.lock().unwrap();
            if changes.len() >= count {
                return true;
            }
        }

        match timeout {
            Some(duration) => tokio::time::timeout(duration, async {
                loop {
                    self.notify.notified().await;
                    let changes = self.changes.lock().unwrap();
                    if changes.len() >= count {
                        return true;
                    }
                }
            })
            .await
            .unwrap_or(false),
            None => loop {
                self.notify.notified().await;
                let changes = self.changes.lock().unwrap();
                if changes.len() >= count {
                    return true;
                }
            },
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T, U> TestWatcher<T, U>
where T: Clone
{
    pub fn peek(&self) -> Vec<U> { self.changes.lock().unwrap().iter().map(|item| (self.transform)(item.clone())).collect() }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + 'static, U> IntoBroadcastListener<T> for &TestWatcher<T, U> {
    fn into_broadcast_listener(self) -> BroadcastListener<T> {
        let changes = self.changes.clone();
        let notify = self.notify.clone();
        BroadcastListener::Payload(Arc::new(move |item: T| {
            changes.lock().unwrap().push(item);
            notify.notify_waiters();
        }))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + 'static, U> IntoSubscribeListener<T> for &TestWatcher<T, U> {
    fn into_subscribe_listener(self) -> Box<dyn Fn(T) + Send + Sync> {
        let changes = self.changes.clone();
        let notify = self.notify.clone();
        Box::new(move |item: T| {
            changes.lock().unwrap().push(item);
            notify.notify_waiters();
        })
    }
}

/// A uniquely named database file in the system temp dir, removed on drop
/// with the journal and WAL files SQLite keeps beside it.
#[allow(dead_code)]
pub struct TempDb(std::path::PathBuf);

#[allow(dead_code)]
impl TempDb {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(name: &str) -> Self {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock before epoch").as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("ankurah_sqlite_{name}_{}_{nanos}.db", std::process::id()));
        Self(path)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new(name: &str) -> Self {
        let os = node::builtin("node:os");
        let directory = node::call(&os, "tmpdir", &[]).as_string().expect("os.tmpdir() returns a path");
        let nonce = (js_sys::Math::random() * 1e9) as u64;
        Self(format!("{directory}/ankurah_sqlite_{name}_{}_{nonce}.db", js_sys::Date::now() as u64).into())
    }

    pub fn path(&self) -> &std::path::Path { &self.0 }

    /// The path as a string, for node:sqlite.
    pub fn path_str(&self) -> &str { self.0.to_str().expect("a UTF-8 path") }

    /// A second connection on the file, for edits behind the engine's back.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn raw(&self) -> Box<dyn ankurah_storage_sqlite::executor::SqliteExecutor> {
        Box::new(rusqlite::Connection::open(&self.0).unwrap())
    }

    /// A second connection on the file, for edits behind the engine's back,
    /// on the engine's driver (see [`LOCK_HOLDER`] for why).
    #[cfg(target_arch = "wasm32")]
    pub fn raw(&self) -> Box<dyn ankurah_storage_sqlite::executor::SqliteExecutor> {
        Box::new(if driver::on_better_sqlite3() {
            ankurah_storage_sqlite::NodeJsConnection::from_database(driver::host_database(self.path_str())).unwrap()
        } else {
            ankurah_storage_sqlite::NodeJsConnection::open(self.path_str()).unwrap()
        })
    }
}

impl Drop for TempDb {
    #[cfg(not(target_arch = "wasm32"))]
    fn drop(&mut self) {
        for suffix in ["", "-journal", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn drop(&mut self) {
        let fs = node::builtin("node:fs");
        let options = js_sys::Object::new();
        js_sys::Reflect::set(&options, &"force".into(), &wasm_bindgen::JsValue::TRUE).unwrap();
        for suffix in ["", "-journal", "-wal", "-shm"] {
            node::call(&fs, "rmSync", &[format!("{}{suffix}", self.0.display()).into(), options.clone().into()]);
        }
    }
}

/// Node.js's built-in modules.
#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub mod node {
    use js_sys::{Function, Reflect};
    use wasm_bindgen::{JsCast, JsValue};

    pub fn builtin(name: &str) -> JsValue {
        let process = Reflect::get(&js_sys::global(), &"process".into()).unwrap();
        call(&process, "getBuiltinModule", &[name.into()])
    }

    pub fn call(target: &JsValue, method: &str, arguments: &[JsValue]) -> JsValue {
        let function: Function = Reflect::get(target, &method.into()).unwrap().dyn_into().unwrap();
        function.apply(target, &arguments.iter().collect()).unwrap()
    }
}

/// Another connection to a database file, on a thread of its own, holding a
/// real lock on it: a std thread natively, a worker thread on Node.js. It runs
/// `lock` (a BEGIN), and commits once `hold_ms` have passed, while this
/// thread's connection waits for the lock.
#[allow(dead_code)]
pub struct LockHolder {
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
const LOCK_HOLDER: &str = r#"
const { parentPort, workerData } = require('node:worker_threads');
const Database = workerData.betterSqlite3 ? require(workerData.betterSqlite3) : require('node:sqlite').DatabaseSync;
const database = new Database(workerData.path);
database.exec(workerData.lock);
parentPort.postMessage('locked');
setTimeout(() => { database.exec('COMMIT'); database.close(); }, workerData.holdMs);
"#;

#[allow(dead_code)]
impl LockHolder {
    /// Start the other connection, returning once it holds the lock.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn take(db: &TempDb, lock: &'static str, hold_ms: u64) -> Self {
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
    pub async fn take(db: &TempDb, lock: &'static str, hold_ms: u64) -> Self {
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
            js_sys::Array::of5(&LOCK_HOLDER.into(), &db.path_str().into(), &lock.into(), &(hold_ms as f64).into(), &better_sqlite3);
        let started: js_sys::Promise = start.apply(&wasm_bindgen::JsValue::NULL, &arguments).unwrap().dyn_into().unwrap();
        let handle = wasm_bindgen_futures::JsFuture::from(started).await.expect("the other connection takes the lock");
        Self { exit: js_sys::Reflect::get(&handle, &"exit".into()).unwrap().dyn_into().unwrap() }
    }

    /// Wait until the other connection has committed and closed.
    pub async fn released(self) {
        #[cfg(not(target_arch = "wasm32"))]
        self.thread.join().expect("the other connection releases the lock");
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::JsFuture::from(self.exit).await.expect("the other connection releases the lock");
    }
}

/// Milliseconds elapsed since it started, on either target.
#[allow(dead_code)]
pub struct Stopwatch(f64);

#[allow(dead_code)]
impl Stopwatch {
    pub fn start() -> Self { Self(now_ms()) }

    pub fn elapsed_ms(&self) -> f64 { now_ms() - self.0 }
}

#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock before epoch").as_secs_f64() * 1000.0
}

#[cfg(target_arch = "wasm32")]
fn now_ms() -> f64 { js_sys::Date::now() }

/// The session settings a connection of `storage` runs with, in the order
/// connection.rs sets them after rusqlite's busy timeout: busy timeout,
/// journal mode, synchronous, foreign keys, cache size, mmap size, temp store.
#[allow(dead_code)]
pub async fn session_settings(
    storage: &ankurah_storage_sqlite::SqliteStorageEngine,
) -> anyhow::Result<(i64, String, i64, i64, i64, i64, i64)> {
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
