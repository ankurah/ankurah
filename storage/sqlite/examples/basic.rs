//! Basic SQLite storage example

use ankurah_storage_sqlite::SqliteStorageEngine;

#[allow(dead_code)]
async fn example() -> anyhow::Result<()> {
    // liaison id=storage-sqlite
    let storage = SqliteStorageEngine::open("myapp.db").await?;
    // liaison end

    let _ = storage;
    Ok(())
}

fn main() {}
