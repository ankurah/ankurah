mod common;
use ankurah::{
    policy::DEFAULT_CONTEXT as c,
    property::{value::LWW, YrsString},
    Model, Node, PermissiveAgent, Property,
};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[derive(Property, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[wasm_bindgen]
pub enum Visibility {
    Public,
    Unlisted,
    Private,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Video {
    #[active_type(YrsString)]
    pub title: String,
    #[active_type(YrsString<_>)]
    pub description: Option<String>,
    #[active_type(LWW<_>)]
    pub visibility: Visibility,
    /*#[active_type(PNCounter)]
    pub views: i32,*/
    #[active_type(LWW)]
    pub attribution: Option<String>,
}

#[tokio::test]
async fn property_backends() -> Result<()> {
    let node = NodeBuilder::new(Arc::new(SledStorageEngine::new_test().build_durable().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let trx = ctx.begin();
    let cat_video = trx
        .create(&Video {
            title: "Cat video #2918".into(),
            description: Some("Test".into()),
            visibility: Visibility::Public,
            //views: 0,
            attribution: None,
        })
        .await?;

    let id = cat_video.id();
    //cat_video.views.add(2); // FIXME: applying twice for some reason
    cat_video.visibility.set(&Visibility::Unlisted)?;
    cat_video.title.insert(15, " (Very cute)")?;
    trx.commit().await?;

    let video = ctx.get::<VideoView>(id).await?;
    //assert_eq!(video.views().unwrap(), 1);
    assert_eq!(video.visibility().unwrap(), Visibility::Unlisted);
    assert_eq!(video.title().unwrap(), "Cat video #2918 (Very cute)");

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg_common;

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_property_backends() -> Result<()> {
    use ankurah::PermissiveAgent;

    let (_container, storage_engine) = pg_common::create_postgres_container().await?;
    let node = NodeBuilder::new(Arc::new(storage_engine).build_durable(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let trx = ctx.begin();
    let cat_video = trx
        .create(&Video {
            title: "Cat video #2918".into(),
            description: Some("Test".into()),
            visibility: Visibility::Public,
            //views: 0,
            attribution: None,
        })
        .await?;

    let _cat_video2 = trx
        .create(&Video {
            title: "Cat video #9000".into(),
            description: None,
            visibility: Visibility::Unlisted,
            //views: 5120,
            attribution: Some("That guy".into()),
        })
        .await?;

    let id = cat_video.id();
    //cat_video.views.add(2); // FIXME: applying twice for some reason
    cat_video.visibility.set(&Visibility::Unlisted)?;
    cat_video.title.insert(15, " (Very cute)")?;
    trx.commit().await?;

    let video = ctx.get::<VideoView>(id).await?;
    //assert_eq!(video.views().unwrap(), 1);
    assert_eq!(video.visibility().unwrap(), Visibility::Unlisted);
    assert_eq!(video.title().unwrap(), "Cat video #2918 (Very cute)");

    Ok(())
}
