mod common;
use ankurah::{
    policy::DEFAULT_CONTEXT as c,
    property::{value::LWW, PropertyError, PropertyValue, YrsString},
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

impl<'a> TryInto<PropertyValue> for &'a Visibility {
    type Error = PropertyError;
    fn try_into(self) -> Result<PropertyValue, PropertyError> {
        let tag = match self {
            Visibility::Public => "public",
            Visibility::Private => "private",
            Visibility::Unlisted => "unlisted",
        };
        Ok(PropertyValue::String(Some(tag.to_owned())))
    }
}

impl TryFrom<PropertyValue> for Visibility {
    type Error = PropertyError;
    fn try_from(value: PropertyValue) -> Result<Self, PropertyError> {
        match value {
            PropertyValue::String(variant_str) => {
                if let Some(variant_str) = variant_str {
                    match &*variant_str {
                        "public" => Ok(Visibility::Public),
                        "private" => Ok(Visibility::Private),
                        "unlisted" => Ok(Visibility::Unlisted),
                        value => Err(PropertyError::InvalidValue { value: value.to_owned(), ty: "Visibility".to_owned() }),
                    }
                } else {
                    Err(PropertyError::InvalidValue { value: "None".to_owned(), ty: "Visibility".to_owned() })
                }
            }
            other => Err(PropertyError::InvalidVariant { given: other, ty: "Visibility".to_owned() }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Video {
    //#[active_type(YrsString)]
    pub title: String,
    //#[active_type(YrsString<_>)]
    pub description: Option<String>,
    //#[active_type(LWW<_>)]
    pub visibility: Visibility,
    /*#[active_type(PNCounter)]
    pub views: i32,*/
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
            //views: 0,
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
            //views: 0,
            attribution: None,
        })
        .await;

    let cat_video2 = trx
        .create(&Video {
            title: "Cat video #9000".into(),
            description: None,
            visibility: Visibility::Unlisted,
            //views: 5120,
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

///
///
///

impl ::ankurah::model::Model for Video {
    type View = VideoView;
    type Mutable<'rec> = VideoMut<'rec>;
    fn collection() -> ankurah::derive_deps::ankurah_proto::CollectionId { "video".into() }
    fn create_entity(&self, id: ::ankurah::derive_deps::ankurah_proto::ID) -> ::ankurah::model::Entity {
        use ankurah::property::InitializeWith;
        let backends = ankurah::property::Backends::new();
        let entity = ankurah::model::Entity::create(id, Self::collection(), backends);
        ::ankurah::property::value::YrsString::<String>::initialize_with(&entity, "title".into(), &self.title);
        YrsString::<Option<String>>::initialize_with(&entity, "description".into(), &self.description);
        ::ankurah::property::value::LWW::<Visibility>::initialize_with(&entity, "visibility".into(), &self.visibility);
        ::ankurah::property::value::LWW::<Option<String>>::initialize_with(&entity, "attribution".into(), &self.attribution);
        entity
    }
}
use ankurah::derive_deps::wasm_bindgen::prelude::*;
#[wasm_bindgen]
#[derive(Clone)]
pub struct VideoView {
    entity: std::sync::Arc<::ankurah::model::Entity>,
}
impl ::ankurah::model::View for VideoView {
    type Model = Video;
    type Mutable<'rec> = VideoMut<'rec>;
    fn to_model(&self) -> Result<Self::Model, ankurah::property::PropertyError> {
        Ok(Video {
            title: self.title()?,
            description: self.description()?,
            visibility: self.visibility()?,
            attribution: self.attribution()?,
        })
    }
    fn entity(&self) -> &std::sync::Arc<::ankurah::model::Entity> { &self.entity }
    fn from_entity(entity: std::sync::Arc<::ankurah::model::Entity>) -> Self {
        use ::ankurah::model::View;
        assert_eq!(Self::collection(), entity.collection);
        VideoView { entity }
    }
}
impl VideoView {
    pub async fn edit<'rec, 'trx: 'rec>(
        &self,
        trx: &'trx ankurah::transaction::Transaction,
    ) -> Result<VideoMut<'rec>, ankurah::error::RetrievalError> {
        use ::ankurah::model::View;
        trx.edit::<Video>(self.id()).await
    }
}
use ankurah::derive_deps::wasm_bindgen::prelude::*;
#[wasm_bindgen]
impl VideoView {
    pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::ID { self.entity.id.clone() }
    pub fn title(&self) -> Result<String, ankurah::property::PropertyError> {
        use ankurah::property::{FromActiveType, FromEntity};
        let active_result = ::ankurah::property::value::YrsString::<String>::from_entity("title".into(), self.entity.as_ref());
        String::from_active(active_result)
    }
    pub fn description(&self) -> Result<Option<String>, ankurah::property::PropertyError> {
        use ankurah::property::{FromActiveType, FromEntity};
        let active_result = YrsString::<Option<String>>::from_entity("description".into(), self.entity.as_ref());
        Option::<String>::from_active(active_result)
    }
    pub fn visibility(&self) -> Result<Visibility, ankurah::property::PropertyError> {
        use ankurah::property::{FromActiveType, FromEntity};
        let active_result = ::ankurah::property::value::LWW::<Visibility>::from_entity("visibility".into(), self.entity.as_ref());
        Visibility::from_active(active_result)
    }
    pub fn attribution(&self) -> Result<Option<String>, ankurah::property::PropertyError> {
        use ankurah::property::{FromActiveType, FromEntity};
        let active_result = ::ankurah::property::value::LWW::<Option<String>>::from_entity("attribution".into(), self.entity.as_ref());
        Option::<String>::from_active(active_result)
    }
}
#[derive(Debug)]
pub struct VideoMut<'rec> {
    entity: &'rec std::sync::Arc<::ankurah::model::Entity>,
    pub title: ::ankurah::property::value::YrsString<String>,
    pub description: YrsString<Option<String>>,
    pub visibility: ::ankurah::property::value::LWW<Visibility>,
    pub attribution: ::ankurah::property::value::LWW<Option<String>>,
}
impl<'rec> ::ankurah::model::Mutable<'rec> for VideoMut<'rec> {
    type Model = Video;
    type View = VideoView;
    fn entity(&self) -> &std::sync::Arc<::ankurah::model::Entity> { &self.entity }
    fn new(entity: &'rec std::sync::Arc<::ankurah::model::Entity>) -> Self {
        use ankurah::{model::Mutable, property::FromEntity};
        assert_eq!(entity.collection(), Self::collection());
        Self {
            entity,
            title: ::ankurah::property::value::YrsString::<String>::from_entity("title".into(), entity),
            description: YrsString::<Option<String>>::from_entity("description".into(), entity),
            visibility: ::ankurah::property::value::LWW::<Visibility>::from_entity("visibility".into(), entity),
            attribution: ::ankurah::property::value::LWW::<Option<String>>::from_entity("attribution".into(), entity),
        }
    }
}
impl<'rec> VideoMut<'rec> {
    pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::ID { self.entity.id.clone() }
    pub fn title(&self) -> &::ankurah::property::value::YrsString<String> { &self.title }
    pub fn description(&self) -> &YrsString<Option<String>> { &self.description }
    pub fn visibility(&self) -> &::ankurah::property::value::LWW<Visibility> { &self.visibility }
    pub fn attribution(&self) -> &::ankurah::property::value::LWW<Option<String>> { &self.attribution }
}
impl<'a> Into<ankurah::derive_deps::ankurah_proto::ID> for &'a VideoView {
    fn into(self) -> ankurah::derive_deps::ankurah_proto::ID { ankurah::View::id(self) }
}
impl<'a, 'rec> Into<ankurah::derive_deps::ankurah_proto::ID> for &'a VideoMut<'rec> {
    fn into(self) -> ankurah::derive_deps::ankurah_proto::ID { ::ankurah::model::Mutable::id(self) }
}
