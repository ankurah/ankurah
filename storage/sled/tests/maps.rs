//! The storage contract's map conformance suite, run on sled, whose locks
//! under different names keep apart.

use ankurah::core::test_helpers::map_conformance;
use ankurah_storage_sled::SledStorageEngine;

/// Runs each case on an engine over a database of its own.
macro_rules! on_a_fresh_database {
    ($($case:ident),* $(,)?) => {
        $(#[tokio::test]
        async fn $case() -> anyhow::Result<()> { map_conformance::$case(&SledStorageEngine::new_test()?).await })*
    };
}

map_conformance::every_case!(on_a_fresh_database);
map_conformance::every_case_for_engines_that_lock_by_name!(on_a_fresh_database);
