mod common;
use common::*;

#[tokio::test]
async fn test_sled_basic_workflow() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_albums(&ctx, vec![("Walking on a Dream", "2008")]).await?;

    assert_eq!(names(&fetch(&ctx, "name = 'Walking on a Dream'").await?), vec!["Walking on a Dream"]);
    assert_eq!(years(&fetch(&ctx, "name = 'Walking on a Dream'").await?), vec!["2008"]);

    Ok(())
}
