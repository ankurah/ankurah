//! Protocol-version stamp semantics: a fresh store is stamped with
//! `ankurah_proto::PROTOCOL_VERSION` on engine construction and checked on
//! every open; a store carrying a different (or missing) stamp refuses to
//! open, advising a development-database reset.

use ankurah_core::storage::StorageEngine;
use ankurah_proto::PROTOCOL_VERSION;
use ankurah_storage_sqlite::SqliteStorageEngine;

/// A uniquely named database file in the system temp dir, removed on drop.
struct TempDb(std::path::PathBuf);

impl TempDb {
    fn new(name: &str) -> Self {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock before epoch").as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("ankurah_sqlite_stamp_{name}_{}_{nanos}.db", std::process::id()));
        Self(path)
    }

    fn path(&self) -> &std::path::Path { &self.0 }
}

impl Drop for TempDb {
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }
}

#[tokio::test]
async fn fresh_store_stamps_and_reopens() -> anyhow::Result<()> {
    let db = TempDb::new("fresh");

    // A fresh store is stamped at engine construction.
    {
        let engine = SqliteStorageEngine::open(db.path()).await?;
        engine.collection(&"albums".into()).await?;
    }

    // The stamp carries the running protocol version.
    {
        let conn = rusqlite::Connection::open(db.path())?;
        let value: String =
            conn.query_row(r#"SELECT "value" FROM "ankurah_meta" WHERE "key" = 'protocol_version'"#, [], |row| row.get(0))?;
        assert_eq!(value, PROTOCOL_VERSION.to_string());
    }

    // Reopening a store with the matching stamp proceeds.
    let _engine = SqliteStorageEngine::open(db.path()).await?;
    Ok(())
}

#[tokio::test]
async fn mismatched_stamp_refuses() -> anyhow::Result<()> {
    let db = TempDb::new("mismatch");
    {
        let _engine = SqliteStorageEngine::open(db.path()).await?;
    }

    // Restamp the store with a different version out of band.
    {
        let conn = rusqlite::Connection::open(db.path())?;
        conn.execute(r#"UPDATE "ankurah_meta" SET "value" = '999' WHERE "key" = 'protocol_version'"#, [])?;
    }

    let err = match SqliteStorageEngine::open(db.path()).await {
        Ok(_) => panic!("expected the mismatched stamp to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("999") && err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the found and expected versions: {err}");
    Ok(())
}

#[tokio::test]
async fn unstamped_store_with_data_refuses() -> anyhow::Result<()> {
    let db = TempDb::new("unstamped");
    {
        let engine = SqliteStorageEngine::open(db.path()).await?;
        engine.collection(&"albums".into()).await?;
    }

    // Remove the stamp while ankurah tables remain: the store now reads as a
    // pre-stamp store.
    {
        let conn = rusqlite::Connection::open(db.path())?;
        conn.execute(r#"DROP TABLE "ankurah_meta""#, [])?;
    }

    let err = match SqliteStorageEngine::open(db.path()).await {
        Ok(_) => panic!("expected the unstamped store with existing data to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the expected version: {err}");
    Ok(())
}

#[tokio::test]
async fn collection_wipe_preserves_the_stamp() -> anyhow::Result<()> {
    let db = TempDb::new("wipe");
    {
        let engine = SqliteStorageEngine::open(db.path()).await?;
        engine.collection(&"albums".into()).await?;
        assert!(engine.delete_all_collections().await?, "wiping existing collections reports true");
        // Only the meta table remains, and it is not a collection.
        assert!(!engine.delete_all_collections().await?, "nothing left to wipe reports false");
    }

    // The wiped store keeps its stamp and reopens.
    let _engine = SqliteStorageEngine::open(db.path()).await?;
    Ok(())
}
