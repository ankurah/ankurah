mod common;
#[path = "../../common/tests/support/membership.rs"]
mod cases;

#[tokio::test]
async fn joined_memberships() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    cases::check(&engine).await
}
