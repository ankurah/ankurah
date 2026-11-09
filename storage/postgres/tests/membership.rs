#[path = "../../common/tests/support/membership.rs"]
mod cases;
mod common;

#[tokio::test]
async fn joined_memberships() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    cases::check(&engine).await
}

#[path = "../../common/tests/support/membership_removal.rs"]
mod removal;

#[tokio::test]
async fn removed_memberships() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    cases::check_removal(&engine).await
}

#[tokio::test]
async fn a_removed_membership_leaves_live_queries() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    removal::check_live_removal(engine).await
}
