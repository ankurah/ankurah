use std::sync::Arc;

use ankurah_core::test_helpers::selection_conformance;
use ankurah_storage_sled::SledStorageEngine;

#[tokio::test]
async fn numeric_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::numeric_ordering(Arc::new(SledStorageEngine::new_test()?)).await
}

#[tokio::test]
async fn json_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::json_ordering(Arc::new(SledStorageEngine::new_test()?)).await
}

#[tokio::test]
async fn json_path_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::json_path_ordering(Arc::new(SledStorageEngine::new_test()?)).await
}

#[tokio::test]
async fn mixed_order_matches_digest_trees() -> anyhow::Result<()> {
    selection_conformance::mixed_ordering(Arc::new(SledStorageEngine::new_test()?)).await
}
