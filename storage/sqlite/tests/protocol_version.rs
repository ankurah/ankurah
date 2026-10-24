//! Protocol-version record semantics: a fresh store is marked with
//! `ankurah_proto::PROTOCOL_VERSION` on engine construction and checked on
//! every open; a store carrying a different (or missing) record refuses to
//! open, advising a development-database reset.

mod common;

use ankurah_core::storage::StorageEngine;
use ankurah_proto::{ModelId, SystemModel, PROTOCOL_VERSION};
use ankurah_storage_sqlite::SqliteStorageEngine;
use common::TempDb;
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

#[engine_test]
async fn fresh_store_records_version_and_reopens() -> anyhow::Result<()> {
    let db = TempDb::new("fresh");

    // A fresh store records the version at engine construction.
    {
        let engine = common::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
    }

    // The record carries the running protocol version.
    {
        let conn = db.raw();
        let record = conn.query_row(r#"SELECT "value" FROM "_ankurah_meta" WHERE "key" = 'protocol_version'"#, &[])?.expect("a record");
        assert_eq!(record.text(0)?, PROTOCOL_VERSION.to_string());
    }

    // Reopening a store with the matching record proceeds.
    let _engine = common::open(db.path()).await?;
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

    let engine = common::open(db.path()).await?;
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
        let _engine = common::open(db.path()).await?;
    }

    // Rewrite the stored version out of band.
    {
        let conn = db.raw();
        conn.execute(r#"UPDATE "_ankurah_meta" SET "value" = '999' WHERE "key" = 'protocol_version'"#, &[])?;
    }

    let err = match common::open(db.path()).await {
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
        let engine = common::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
    }

    // Remove the record while ankurah tables remain: the store now reads as an
    // unversioned store.
    {
        let conn = db.raw();
        conn.execute(r#"DROP TABLE "_ankurah_meta""#, &[])?;
    }

    let err = match common::open(db.path()).await {
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
        let engine = common::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
        assert!(engine.delete_all().await?, "wiping existing storage reports true");
        // Only compatibility metadata remains.
        assert!(!engine.delete_all().await?, "nothing left to wipe reports false");
    }

    // The wiped store keeps its version record and reopens.
    let _engine = common::open(db.path()).await?;
    Ok(())
}

/// A table whose name the protocol check cannot read fails the open, rather
/// than drop out of the check's list of tables. SQLite never stores such a
/// name itself; the test plants one, the name's bytes as a BLOB, through
/// `writable_schema`.
#[engine_test]
async fn a_table_name_the_check_cannot_read_fails_the_open() -> anyhow::Result<()> {
    // better-sqlite3 opens its databases in SQLite's defensive mode, which
    // refuses `writable_schema`.
    if common::on_better_sqlite3() {
        return Ok(());
    }
    let db = TempDb::new("unreadable_name");
    {
        let conn = db.raw();
        conn.execute(r#"CREATE TABLE "application_data" ("value" TEXT)"#, &[])?;
        conn.execute("PRAGMA writable_schema = ON", &[])?;
        conn.execute("UPDATE sqlite_master SET name = CAST(name AS BLOB) WHERE name = 'application_data'", &[])?;
        conn.execute("PRAGMA writable_schema = OFF", &[])?;
    }

    let err = match common::open(db.path()).await {
        Ok(_) => panic!("expected a table name the check cannot read to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("column 0 holds BLOB where TEXT was expected"), "{err}");
    Ok(())
}
