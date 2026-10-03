#[path = "../../common/tests/support/integer_order.rs"]
mod cases;
mod common;

#[tokio::test]
async fn integer_comparisons_follow_numeric_order() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    cases::check_comparisons(&engine).await
}

#[tokio::test]
async fn integer_ordering_follows_numeric_order() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    cases::check_ordering(&engine).await
}
