use std::sync::Arc;

use ankurah_core::test_helpers::selection_conformance;
use ankurah_storage_sqlite::SqliteStorageEngine;

#[tokio::test]
async fn numeric_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::numeric_ordering(Arc::new(SqliteStorageEngine::open_temporary().await?)).await
}

#[tokio::test]
async fn node_id_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::node_id_ordering(Arc::new(SqliteStorageEngine::open_temporary().await?)).await
}

#[tokio::test]
async fn json_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::json_ordering(Arc::new(SqliteStorageEngine::open_temporary().await?)).await
}

#[tokio::test]
async fn json_path_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::json_path_ordering(Arc::new(SqliteStorageEngine::open_temporary().await?)).await
}

#[tokio::test]
async fn mixed_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::mixed_ordering(Arc::new(SqliteStorageEngine::open_temporary().await?)).await
}

#[tokio::test]
async fn changes_and_refill_share_a_snapshot() -> anyhow::Result<()> {
    selection_conformance::selection_changes(Arc::new(SqliteStorageEngine::open_temporary().await?)).await
}
