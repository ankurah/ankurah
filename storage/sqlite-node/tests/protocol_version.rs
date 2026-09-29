//! Protocol-version record semantics on node:sqlite: a fresh store is marked
//! with `ankurah_proto::PROTOCOL_VERSION` on engine construction and checked
//! on every open; a store carrying a different (or missing) record refuses to
//! open. Mirrors `storage/sqlite/tests/protocol_version.rs`, with the
//! out-of-band edits made through a second `NodeConnection` on the same file.

use ankurah_core::storage::StorageEngine;
use ankurah_proto::{ModelId, SystemModel, PROTOCOL_VERSION};
use ankurah_storage_sqlite_node::{NodeConnection, SqliteNodeStorageEngine, SqliteValue};
use js_sys::{Function, Reflect};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

async fn touch_system_materialization(engine: &SqliteNodeStorageEngine) -> anyhow::Result<()> {
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    engine.fetch_states(&selection.clone().and_member_of(ModelId::System(SystemModel::System))).await?;
    Ok(())
}

fn builtin(name: &str) -> JsValue {
    let process = Reflect::get(&js_sys::global(), &"process".into()).unwrap();
    let get_builtin: Function = Reflect::get(&process, &"getBuiltinModule".into()).unwrap().dyn_into().unwrap();
    get_builtin.call1(&process, &name.into()).unwrap()
}

/// A uniquely named database file in the system temp dir, removed on drop
/// (with the WAL sidecars the session pragmas create).
struct TempDb(String);

impl TempDb {
    fn new(name: &str) -> Self {
        let os = builtin("node:os");
        let tmpdir: Function = Reflect::get(&os, &"tmpdir".into()).unwrap().dyn_into().unwrap();
        let dir = tmpdir.call0(&os).unwrap().as_string().unwrap();
        let nonce = (js_sys::Math::random() * 1e9) as u64;
        Self(format!("{dir}/ankurah_sqlite_node_version_{name}_{}_{nonce}.db", js_sys::Date::now() as u64))
    }

    fn path(&self) -> &str { &self.0 }

    /// A second connection on the file, for edits behind the engine's back.
    fn raw(&self) -> NodeConnection { NodeConnection::open(&self.0).unwrap() }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let fs = builtin("node:fs");
        let remove: Function = Reflect::get(&fs, &"rmSync".into()).unwrap().dyn_into().unwrap();
        let options = js_sys::Object::new();
        Reflect::set(&options, &"force".into(), &JsValue::TRUE).unwrap();
        for suffix in ["", "-wal", "-shm"] {
            let _ = remove.call2(&fs, &format!("{}{suffix}", self.0).into(), &options);
        }
    }
}

#[wasm_bindgen_test]
async fn fresh_store_records_version_and_reopens() -> anyhow::Result<()> {
    let db = TempDb::new("fresh");

    // A fresh store records the version at engine construction.
    {
        let engine = SqliteNodeStorageEngine::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
    }

    // The record carries the running protocol version.
    let value = db
        .raw()
        .with_executor(|executor| {
            executor.query_row(r#"SELECT "value" FROM "_ankurah_meta" WHERE "key" = 'protocol_version'"#, &[])?.expect("a record").text(0)
        })
        .await?;
    assert_eq!(value, PROTOCOL_VERSION.to_string());

    // Reopening a store with the matching record proceeds.
    let _engine = SqliteNodeStorageEngine::open(db.path()).await?;
    Ok(())
}

#[wasm_bindgen_test]
async fn unrelated_tables_do_not_block_initialization_or_get_dropped() -> anyhow::Result<()> {
    let db = TempDb::new("shared");
    db.raw()
        .with_executor(|executor| {
            executor.execute(r#"CREATE TABLE "application_data" ("value" TEXT NOT NULL)"#, &[])?;
            executor.execute(r#"INSERT INTO "application_data" ("value") VALUES (?)"#, &[SqliteValue::Text("keep me".into())])?;
            Ok(())
        })
        .await?;

    let engine = SqliteNodeStorageEngine::open(db.path()).await?;
    touch_system_materialization(&engine).await?;
    assert!(engine.delete_all().await?, "Ankurah-owned storage was deleted");
    assert!(!engine.delete_all().await?, "unrelated tables do not count as Ankurah storage");

    let value = db
        .raw()
        .with_executor(|executor| executor.query_row(r#"SELECT "value" FROM "application_data""#, &[])?.expect("the row survives").text(0))
        .await?;
    assert_eq!(value, "keep me");
    Ok(())
}

#[wasm_bindgen_test]
async fn mismatched_version_refuses() -> anyhow::Result<()> {
    let db = TempDb::new("mismatch");
    {
        let _engine = SqliteNodeStorageEngine::open(db.path()).await?;
    }

    // Rewrite the stored version out of band.
    db.raw()
        .with_executor(|executor| {
            executor.execute(r#"UPDATE "_ankurah_meta" SET "value" = '999' WHERE "key" = 'protocol_version'"#, &[])?;
            Ok(())
        })
        .await?;

    let err = match SqliteNodeStorageEngine::open(db.path()).await {
        Ok(_) => panic!("expected the mismatched version to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("999") && err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the found and expected versions: {err}");
    Ok(())
}

#[wasm_bindgen_test]
async fn unversioned_store_with_data_refuses() -> anyhow::Result<()> {
    let db = TempDb::new("unversioned");
    {
        let engine = SqliteNodeStorageEngine::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
    }

    // Remove the record while ankurah tables remain: the store now reads as an
    // unversioned store.
    db.raw()
        .with_executor(|executor| {
            executor.execute(r#"DROP TABLE "_ankurah_meta""#, &[])?;
            Ok(())
        })
        .await?;

    let err = match SqliteNodeStorageEngine::open(db.path()).await {
        Ok(_) => panic!("expected the unversioned store with existing data to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the expected version: {err}");
    Ok(())
}

#[wasm_bindgen_test]
async fn collection_wipe_preserves_the_version_record() -> anyhow::Result<()> {
    let db = TempDb::new("wipe");
    {
        let engine = SqliteNodeStorageEngine::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
        assert!(engine.delete_all().await?, "wiping existing storage reports true");
        // Only compatibility metadata remains.
        assert!(!engine.delete_all().await?, "nothing left to wipe reports false");
    }

    // The wiped store keeps its version record and reopens.
    let _engine = SqliteNodeStorageEngine::open(db.path()).await?;
    Ok(())
}
