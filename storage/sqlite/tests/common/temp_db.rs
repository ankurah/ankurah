//! The database files the tests open, each removed when its test ends.

#[cfg(target_arch = "wasm32")]
use js_sys::{Function, Reflect};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue};

#[cfg(target_arch = "wasm32")]
use super::driver;

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
        let os = driver::builtin("node:os");
        let directory = call(&os, "tmpdir", &[]).as_string().expect("os.tmpdir() returns a path");
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
    /// on the engine's driver (see `LOCK_HOLDER_WORKER` in
    /// tests/executor/connections.rs for why).
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
        let fs = driver::builtin("node:fs");
        let options = js_sys::Object::new();
        js_sys::Reflect::set(&options, &"force".into(), &wasm_bindgen::JsValue::TRUE).unwrap();
        for suffix in ["", "-journal", "-wal", "-shm"] {
            call(&fs, "rmSync", &[format!("{}{suffix}", self.0.display()).into(), options.clone().into()]);
        }
    }
}

/// The result of calling `target`'s method `method` with `arguments`.
#[cfg(target_arch = "wasm32")]
fn call(target: &JsValue, method: &str, arguments: &[JsValue]) -> JsValue {
    let function: Function = Reflect::get(target, &method.into()).unwrap().dyn_into().unwrap();
    function.apply(target, &arguments.iter().collect()).unwrap()
}
