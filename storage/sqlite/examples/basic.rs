//! Basic SQLite storage example

// The example opens a native engine: on wasm32, `open` also takes the
// better-sqlite3 class the host passes in.
#[cfg(not(target_arch = "wasm32"))]
use ankurah_storage_sqlite::SqliteStorageEngine;

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
async fn example() -> anyhow::Result<()> {
    // liaison id=storage-sqlite
    let storage = SqliteStorageEngine::open("myapp.db").await?;
    // liaison end

    let _ = storage;
    Ok(())
}

fn main() {}
