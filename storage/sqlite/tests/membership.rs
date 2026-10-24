#[path = "../../common/tests/support/membership.rs"]
mod cases;
mod common;
use common::engine_test;

#[engine_test]
async fn joined_memberships() -> anyhow::Result<()> { cases::check(&common::open_in_memory().await?).await }
