mod common;
use ankurah::{
    policy::DEFAULT_CONTEXT as c,
    property::{
        value::{PNCounter, LWW},
        YrsString,
    },
    Model, Mutable, Node, PermissiveAgent,
};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;

use common::{Album, AlbumView};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[wasm_bindgen]
pub enum Visibility {
    Public,
    Unlisted,
    Private,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Video {
    //#[active_type(YrsString)]
    pub title: String,
    #[active_type(YrsString<_>)]
    pub description: Option<String>,
    //#[active_type(LWW<_>)]
    pub visibility: Visibility,
    #[active_type(PNCounter)]
    pub views: i32,
    //#[active_type(LWW)]
    pub attribution: Option<String>,
}

#[tokio::test]
async fn property_backends() -> Result<()> {
    let client = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new()).context(c);

    let trx = client.begin();
    let cat_video = trx
        .create(&Video {
            title: "Cat video #2918".into(),
            description: Some("Test".into()),
            visibility: Visibility::Public,
            views: 0,
            attribution: None,
        })
        .await;

    let id = cat_video.id();
    //cat_video.views.add(2); // FIXME: applying twice for some reason
    cat_video.visibility.set(&Visibility::Unlisted)?;
    cat_video.title.insert(15, " (Very cute)");
    trx.commit().await?;

    let video = client.get::<VideoView>(id).await?;
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
    let client = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new()).context(c);

    let trx = client.begin();
    let cat_video = trx
        .create(&Video {
            title: "Cat video #2918".into(),
            description: Some("Test".into()),
            visibility: Visibility::Public,
            views: 0,
            attribution: None,
        })
        .await;

    let cat_video2 = trx
        .create(&Video {
            title: "Cat video #9000".into(),
            description: None,
            visibility: Visibility::Unlisted,
            views: 5120,
            attribution: Some("That guy".into()),
        })
        .await;

    let id = cat_video.id();
    //cat_video.views.add(2); // FIXME: applying twice for some reason
    cat_video.visibility.set(&Visibility::Unlisted)?;
    cat_video.title.insert(15, " (Very cute)");
    trx.commit().await?;

    let video = client.get::<VideoView>(id).await?;
    //assert_eq!(video.views().unwrap(), 1);
    assert_eq!(video.visibility().unwrap(), Visibility::Unlisted);
    assert_eq!(video.title().unwrap(), "Cat video #2918 (Very cute)");

    Ok(())
}
