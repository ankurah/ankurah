//! Protocol-version record semantics: a fresh store is marked with
//! `ankurah_proto::PROTOCOL_VERSION` on engine construction and checked on
//! every open; a store carrying a different (or missing) record refuses to
//! open, advising a development-database reset.

use ankurah_core::storage::StorageEngine;
use ankurah_proto::{ModelId, SystemModel, PROTOCOL_VERSION};
use ankurah_storage_sqlite::SqliteStorageEngine;

async fn touch_system_materialization(engine: &SqliteStorageEngine) -> anyhow::Result<()> {
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    engine.fetch_states(&ModelId::System(SystemModel::System), &selection).await?;
    Ok(())
}

/// A uniquely named database file in the system temp dir, removed on drop.
struct TempDb(std::path::PathBuf);

impl TempDb {
    fn new(name: &str) -> Self {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock before epoch").as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("ankurah_sqlite_version_{name}_{}_{nanos}.db", std::process::id()));
        Self(path)
    }

    fn path(&self) -> &std::path::Path { &self.0 }
}

impl Drop for TempDb {
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }
}

#[tokio::test]
async fn fresh_store_records_version_and_reopens() -> anyhow::Result<()> {
    let db = TempDb::new("fresh");

    // A fresh store records the version at engine construction.
    {
        let engine = SqliteStorageEngine::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
    }

    // The record carries the running protocol version.
    {
        let conn = rusqlite::Connection::open(db.path())?;
        let value: String =
            conn.query_row(r#"SELECT "value" FROM "_ankurah_meta" WHERE "key" = 'protocol_version'"#, [], |row| row.get(0))?;
        assert_eq!(value, PROTOCOL_VERSION.to_string());
    }

    // Reopening a store with the matching record proceeds.
    let _engine = SqliteStorageEngine::open(db.path()).await?;
    Ok(())
}

#[tokio::test]
async fn unrelated_tables_do_not_block_initialization_or_get_dropped() -> anyhow::Result<()> {
    let db = TempDb::new("shared");
    {
        let conn = rusqlite::Connection::open(db.path())?;
        conn.execute(r#"CREATE TABLE "application_data" ("value" TEXT NOT NULL)"#, [])?;
        conn.execute(r#"INSERT INTO "application_data" ("value") VALUES ('keep me')"#, [])?;
    }

    let engine = SqliteStorageEngine::open(db.path()).await?;
    touch_system_materialization(&engine).await?;
    assert!(engine.delete_all().await?, "Ankurah-owned storage was deleted");
    assert!(!engine.delete_all().await?, "unrelated tables do not count as Ankurah storage");

    let conn = rusqlite::Connection::open(db.path())?;
    let value: String = conn.query_row(r#"SELECT "value" FROM "application_data""#, [], |row| row.get(0))?;
    assert_eq!(value, "keep me");
    Ok(())
}

#[tokio::test]
async fn mismatched_version_refuses() -> anyhow::Result<()> {
    let db = TempDb::new("mismatch");
    {
        let _engine = SqliteStorageEngine::open(db.path()).await?;
    }

    // Rewrite the stored version out of band.
    {
        let conn = rusqlite::Connection::open(db.path())?;
        conn.execute(r#"UPDATE "_ankurah_meta" SET "value" = '999' WHERE "key" = 'protocol_version'"#, [])?;
    }

    let err = match SqliteStorageEngine::open(db.path()).await {
        Ok(_) => panic!("expected the mismatched version to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("999") && err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the found and expected versions: {err}");
    Ok(())
}

#[tokio::test]
async fn unversioned_store_with_data_refuses() -> anyhow::Result<()> {
    let db = TempDb::new("unversioned");
    {
        let engine = SqliteStorageEngine::open(db.path()).await?;
        touch_system_materialization(&engine).await?;
    }

    // Remove the record while ankurah tables remain: the store now reads as an
    // unversioned store.
    {
        let conn = rusqlite::Connection::open(db.path())?;
        conn.execute(r#"DROP TABLE "_ankurah_meta""#, [])?;
    }

    let err = match SqliteStorageEngine::open(db.path()).await {
        Ok(_) => panic!("expected the unversioned store with existing data to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the expected version: {err}");
    Ok(())
}

#[tokio::test]
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
