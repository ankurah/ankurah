mod common;

use ankurah::property::YrsString;
use ankurah::Model;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    #[active_type(YrsString)]
    pub name: String,
}

#[tokio::test]
async fn pg_repeatable_read() -> Result<()> {
    let (_container, postgres) = common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(postgres), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let id;
    {
        let trx = ctx.begin();
        let album_rw = trx.create(&Album { name: "I love cats".into() }).await?;
        assert_eq!(album_rw.name().value(), Some("I love cats".to_string()));
        id = album_rw.id();
        trx.commit().await?;
    }

    // TODO: implement Mutable.read() -> View
    let album_ro: AlbumView = ctx.get(id).await?;

    let trx2 = ctx.begin();
    let album_rw2 = album_ro.edit(&trx2)?;

    let trx3 = ctx.begin();
    let album_rw3 = album_ro.edit(&trx3)?;

    // tx2 cats -> tofu
    album_rw2.name().delete(7, 4)?;
    album_rw2.name().insert(7, "tofu")?;
    assert_eq!(album_rw2.name().value(), Some("I love tofu".to_string()));

    // tx3 love -> devour
    album_rw3.name().delete(2, 4)?;
    album_rw3.name().insert(2, "devour")?;
    assert_eq!(album_rw3.name().value(), Some("I devour cats".to_string()));

    // trx2 and 3 are uncommited, so the value should not be updated
    assert_eq!(album_ro.name().unwrap(), "I love cats");
    trx2.commit().await?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name().unwrap(), "I love tofu");

    trx3.commit().await?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name().unwrap(), "I devour tofu");

    Ok(())
}

#[tokio::test]
async fn pg_events() -> Result<()> {
    let (_container, postgres) = common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(postgres), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let id;
    {
        let trx = ctx.begin();
        let album_rw = trx.create(&Album { name: "I love cats".into() }).await?;
        assert_eq!(album_rw.name().value(), Some("I love cats".to_string()));
        id = album_rw.id();
        trx.commit().await?;
    }

    // TODO: implement Mutable.read() -> View
    let album_ro: AlbumView = ctx.get(id).await?;

    let trx2 = ctx.begin();
    let album_rw2 = album_ro.edit(&trx2)?;

    let trx3 = ctx.begin();
    let album_rw3 = album_ro.edit(&trx3)?;

    // tx2 cats -> tofu
    album_rw2.name().delete(7, 4)?;
    album_rw2.name().insert(7, "tofu")?;
    assert_eq!(album_rw2.name().value(), Some("I love tofu".to_string()));

    // tx3 love -> devour
    album_rw3.name().delete(2, 4)?;
    album_rw3.name().insert(2, "devour")?;
    assert_eq!(album_rw3.name().value(), Some("I devour cats".to_string()));

    // trx2 and 3 are uncommited, so the value should not be updated
    assert_eq!(album_ro.name().unwrap(), "I love cats");
    trx2.commit().await?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name().unwrap(), "I love tofu");

    trx3.commit().await?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name().unwrap(), "I devour tofu");

    Ok(())
}
