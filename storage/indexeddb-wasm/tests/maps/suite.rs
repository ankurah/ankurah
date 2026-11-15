//! The storage contract's map conformance suite, run on IndexedDB.

use ankurah::core::test_helpers::map_conformance;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::wasm_bindgen_test;

/// Runs each case on an engine over a database of its own.
macro_rules! on_a_fresh_database {
    ($($case:ident),* $(,)?) => {
        $(#[wasm_bindgen_test]
        async fn $case() -> anyhow::Result<()> {
            let name = format!("maps_{}", ulid::Ulid::new());
            map_conformance::$case(&IndexedDBStorageEngine::open(&name).await?).await?;
            IndexedDBStorageEngine::cleanup(&name).await
        })*
    };
}

map_conformance::every_case!(on_a_fresh_database);
map_conformance::every_case_for_engines_that_lock_by_name!(on_a_fresh_database);
