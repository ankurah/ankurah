mod common;

use ankurah::{
    policy::DEFAULT_CONTEXT as c,
    property::{value::LWW, YrsString},
    Model, Node, PermissiveAgent, Property,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Property, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum Visibility {
    Public,
    Unlisted,
    Private,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Video {
    #[active_type(YrsString)]
    pub title: String,
    #[active_type(YrsString)]
    pub description: Option<String>,
    #[active_type(LWW)]
    pub visibility: Visibility,
    #[active_type(LWW)]
    pub attribution: Option<String>,
}

#[tokio::test]
async fn pg_property_backends() -> Result<()> {
    let (_container, storage_engine) = common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let trx = ctx.begin();
    let cat_video = trx
        .create(&Video {
            title: "Cat video #2918".into(),
            description: Some("Test".into()),
            visibility: Visibility::Public,
            attribution: None,
        })
        .await?;

    let _cat_video2 = trx
        .create(&Video {
            title: "Cat video #9000".into(),
            description: None,
            visibility: Visibility::Unlisted,
            attribution: Some("That guy".into()),
        })
        .await?;

    let id = cat_video.id();
    cat_video.visibility().set(&Visibility::Unlisted)?;
    cat_video.title().insert(15, " (Very cute)")?;
    trx.commit().await?;

    let video = ctx.get::<VideoView>(id).await?;
    assert_eq!(video.visibility().unwrap(), Visibility::Unlisted);
    assert_eq!(video.title().unwrap(), "Cat video #2918 (Very cute)");

    Ok(())
}
