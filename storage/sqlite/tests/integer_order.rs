#[path = "../../common/tests/support/integer_order.rs"]
mod cases;
mod common;

use common::engine_test;

#[engine_test]
async fn integer_comparisons_follow_numeric_order() -> anyhow::Result<()> {
    cases::check_comparisons(&common::open_in_memory().await?).await
}

#[engine_test]
async fn integer_ordering_follows_numeric_order() -> anyhow::Result<()> { cases::check_ordering(&common::open_in_memory().await?).await }
