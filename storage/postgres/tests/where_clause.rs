mod common;

use ankurah::{fetch, policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use anyhow::Result;
use common::{Album, AlbumView};
use std::sync::Arc;

#[tokio::test]
async fn pg_basic_where_clause() -> Result<()> {
    let (_container, storage_engine) = common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    {
        let trx = ctx.begin();

        trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await?;
        trx.create(&Album { name: "Death Magnetic".into(), year: "2008".into() }).await?;
        trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await?;
        trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await?;
        trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await?;

        trx.commit().await?;
    };

    let name = "Walking on a Dream";
    let albums: Vec<AlbumView> = fetch!(ctx, { name }).await?;

    assert_eq!(
        albums.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string()]
    );

    let year = "2008";
    let albums: Vec<AlbumView> = fetch!(ctx, { year }).await?;

    assert_eq!(
        albums.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string(), "Death Magnetic".to_string()]
    );

    let old_year = "1800";
    let albums: Vec<AlbumView> = fetch!(ctx, {name} AND year = {old_year}).await?;

    assert_eq!(albums.iter().map(|active_entity| active_entity.name().unwrap()).count(), 0,);

    // Test IN with array expansion (mixing variable and literal)
    let mixed_names = vec![name, "Death Magnetic"];
    let albums: Vec<AlbumView> = fetch!(ctx, name IN {mixed_names}).await?;
    assert_eq!(
        albums.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string(), "Death Magnetic".to_string()]
    );

    let years = vec!["2008", "2013"];
    let albums: Vec<AlbumView> = fetch!(ctx, year IN {years}).await?;
    assert_eq!(
        albums.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string(), "Death Magnetic".to_string(), "Ice on the Dune".to_string()]
    );

    Ok(())
}
