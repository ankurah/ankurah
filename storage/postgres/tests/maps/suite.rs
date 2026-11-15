//! The storage contract's map conformance suite, run on PostgreSQL.

use ankurah::core::test_helpers::map_conformance;
use ankurah_storage_postgres::Postgres;

/// Runs each case on an engine over a database of its own.
macro_rules! on_a_fresh_database {
    ($($case:ident),* $(,)?) => {
        $(#[tokio::test]
        async fn $case() -> anyhow::Result<()> {
            let server = crate::server::Server::start().await?;
            let engine = Postgres::new(server.pool().await?).await?;
            map_conformance::$case(&engine).await
        })*
    };
}

map_conformance::every_case!(on_a_fresh_database);

// PostgreSQL locks each name apart, so it keeps the cases of such engines too.
map_conformance::every_case_for_engines_that_lock_by_name!(on_a_fresh_database);

// A snapshot is a `REPEATABLE READ` transaction, which keeps its state.
map_conformance::every_case_for_engines_whose_snapshots_keep_their_state!(on_a_fresh_database);
