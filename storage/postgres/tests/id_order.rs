#[path = "../../common/tests/support/id_order.rs"]
mod cases;
mod common;

#[tokio::test]
async fn id_comparisons_and_ordering_follow_byte_order() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    cases::check(&engine).await
}
