//! Protocol-version record semantics: a fresh store is marked with
//! `ankurah_proto::PROTOCOL_VERSION` on engine construction and checked on
//! every open; a store carrying a different (or missing) record refuses to
//! open, advising a development-database reset.

use ankurah_core::storage::StorageEngine;
use ankurah_proto::{ModelId, SystemModel, PROTOCOL_VERSION};
use ankurah_storage_sqlite::{exec::SqliteExecutor, SqliteStorageEngine};
// The same tests run natively under tokio, and on wasm32 under wasm-bindgen-test in Node.
#[cfg(not(target_arch = "wasm32"))]
use tokio::test as engine_test;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test as engine_test;

async fn touch_system_materialization(engine: &SqliteStorageEngine) -> anyhow::Result<()> {
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    engine.fetch_states(&selection.clone().and_member_of(ModelId::System(SystemModel::System))).await?;
    Ok(())
}

/// A uniquely named database file in the system temp dir, removed on drop.
struct TempDb(std::path::PathBuf);

impl TempDb {
    #[cfg(not(target_arch = "wasm32"))]
    fn new(name: &str) -> Self {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock before epoch").as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("ankurah_sqlite_version_{name}_{}_{nanos}.db", std::process::id()));
        Self(path)
    }

    #[cfg(target_arch = "wasm32")]
    fn new(name: &str) -> Self {
        let os = node::builtin("node:os");
        let directory = node::call(&os, "tmpdir", &[]).as_string().expect("os.tmpdir() returns a path");
        let nonce = (js_sys::Math::random() * 1e9) as u64;
        Self(format!("{directory}/ankurah_sqlite_version_{name}_{}_{nonce}.db", js_sys::Date::now() as u64).into())
    }

    fn path(&self) -> &std::path::Path { &self.0 }

    /// A second connection on the file, for edits behind the engine's back.
    #[cfg(not(target_arch = "wasm32"))]
    fn raw(&self) -> Box<dyn SqliteExecutor> { Box::new(rusqlite::Connection::open(&self.0).unwrap()) }

    /// A second connection on the file, for edits behind the engine's back.
    #[cfg(target_arch = "wasm32")]
    fn raw(&self) -> Box<dyn SqliteExecutor> {
        Box::new(ankurah_storage_sqlite::NodeConnection::open(self.0.to_str().expect("a UTF-8 path")).unwrap())
    }
}

impl Drop for TempDb {
    #[cfg(not(target_arch = "wasm32"))]
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }

    /// Removes the file and the WAL sidecars the session pragmas create.
    #[cfg(target_arch = "wasm32")]
    fn drop(&mut self) {
        let fs = node::builtin("node:fs");
        let options = js_sys::Object::new();
        js_sys::Reflect::set(&options, &"force".into(), &wasm_bindgen::JsValue::TRUE).unwrap();
        for suffix in ["", "-wal", "-shm"] {
            node::call(&fs, "rmSync", &[format!("{}{suffix}", self.0.display()).into(), options.clone().into()]);
        }
    }
}

/// Node's built-in modules, for temporary files.
#[cfg(target_arch = "wasm32")]
mod node {
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

#[engine_test]
async fn fresh_store_records_version_and_reopens() -> anyhow::Result<()> {
    let db = TempDb::new("fresh");

    // A fresh store records the version at engine construction.
    {
        let engine = SqliteStorageEngine::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
    }

    // The record carries the running protocol version.
    {
        let conn = db.raw();
        let record = conn.query_row(r#"SELECT "value" FROM "_ankurah_meta" WHERE "key" = 'protocol_version'"#, &[])?.expect("a record");
        assert_eq!(record.text(0)?, PROTOCOL_VERSION.to_string());
    }

    // Reopening a store with the matching record proceeds.
    let _engine = SqliteStorageEngine::open(db.path()).await?;
    Ok(())
}

#[engine_test]
async fn unrelated_tables_do_not_block_initialization_or_get_dropped() -> anyhow::Result<()> {
    let db = TempDb::new("shared");
    {
        let conn = db.raw();
        conn.execute(r#"CREATE TABLE "application_data" ("value" TEXT NOT NULL)"#, &[])?;
        conn.execute(r#"INSERT INTO "application_data" ("value") VALUES ('keep me')"#, &[])?;
    }

    let engine = SqliteStorageEngine::open(db.path()).await?;
    touch_system_materialization(&engine).await?;
    assert!(engine.delete_all().await?, "Ankurah-owned storage was deleted");
    assert!(!engine.delete_all().await?, "unrelated tables do not count as Ankurah storage");

    let conn = db.raw();
    let row = conn.query_row(r#"SELECT "value" FROM "application_data""#, &[])?.expect("the row survives");
    assert_eq!(row.text(0)?, "keep me");
    Ok(())
}

#[engine_test]
async fn mismatched_version_refuses() -> anyhow::Result<()> {
    let db = TempDb::new("mismatch");
    {
        let _engine = SqliteStorageEngine::open(db.path()).await?;
    }

    // Rewrite the stored version out of band.
    {
        let conn = db.raw();
        conn.execute(r#"UPDATE "_ankurah_meta" SET "value" = '999' WHERE "key" = 'protocol_version'"#, &[])?;
    }

    let err = match SqliteStorageEngine::open(db.path()).await {
        Ok(_) => panic!("expected the mismatched version to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("999") && err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the found and expected versions: {err}");
    Ok(())
}

#[engine_test]
async fn unversioned_store_with_data_refuses() -> anyhow::Result<()> {
    let db = TempDb::new("unversioned");
    {
        let engine = SqliteStorageEngine::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
    }

    // Remove the record while ankurah tables remain: the store now reads as an
    // unversioned store.
    {
        let conn = db.raw();
        conn.execute(r#"DROP TABLE "_ankurah_meta""#, &[])?;
    }

    let err = match SqliteStorageEngine::open(db.path()).await {
        Ok(_) => panic!("expected the unversioned store with existing data to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the expected version: {err}");
    Ok(())
}

#[engine_test]
async fn collection_wipe_preserves_the_version_record() -> anyhow::Result<()> {
    let db = TempDb::new("wipe");
    {
        let engine = SqliteStorageEngine::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
        assert!(engine.delete_all().await?, "wiping existing storage reports true");
        // Only compatibility metadata remains.
        assert!(!engine.delete_all().await?, "nothing left to wipe reports false");
    }

    // The wiped store keeps its version record and reopens.
    let _engine = SqliteStorageEngine::open(db.path()).await?;
    Ok(())
}
