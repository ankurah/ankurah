use ankurah::Model;

pub struct Album {
    pub name: String,
}

#[derive(Debug)]
pub struct Session {
    pub date_connected: String,
    pub ip_address: String,
    pub node_id: String,
    #[cfg(not(feature = "wasm"))]
    #[model(ephemeral)]
    pub frobnicator: Frobnicator,
}

impl ::ankurah::model::Model for Session {
    type View = SessionView;
    type Mutable<'rec> = SessionMut<'rec>;
    fn collection() -> ankurah::derive_deps::ankurah_proto::CollectionId { "session".into() }
    fn create_entity(&self, id: ::ankurah::derive_deps::ankurah_proto::ID) -> ::ankurah::model::Entity {
        use ankurah::property::InitializeWith;
        let backends = ankurah_core::property::Backends::new();
        let entity = ankurah_core::model::Entity::create(id, Self::collection(), backends);
        ::ankurah::property::value::YrsString::initialize_with(&entity, "date_connected".into(), &self.date_connected);
        ::ankurah::property::value::YrsString::initialize_with(&entity, "ip_address".into(), &self.ip_address);
        ::ankurah::property::value::YrsString::initialize_with(&entity, "node_id".into(), &self.node_id);
        entity
    }
}
use ankurah::derive_deps::wasm_bindgen::prelude::*;
#[wasm_bindgen]
#[derive(Clone)]
pub struct SessionView {
    entity: std::sync::Arc<::ankurah::model::Entity>,
}
impl ::ankurah::model::View for SessionView {
    type Model = Session;
    type Mutable<'rec> = SessionMut<'rec>;
    fn to_model(&self) -> Self::Model {
        Session { date_connected: self.date_connected(), ip_address: self.ip_address(), node_id: self.node_id() }
    }
    fn entity(&self) -> &std::sync::Arc<::ankurah::model::Entity> { &self.entity }
    fn from_entity(entity: std::sync::Arc<::ankurah::model::Entity>) -> Self {
        use ::ankurah::model::View;
        assert_eq!(Self::collection(), entity.collection);
        SessionView { entity }
    }
}
impl SessionView {
    pub async fn edit<'rec, 'trx: 'rec>(
        &self,
        trx: &'trx ankurah::transaction::Transaction,
    ) -> Result<SessionMut<'rec>, ankurah::error::RetrievalError> {
        use ::ankurah::model::View;
        trx.edit::<Session>(self.id()).await
    }
}
use ankurah::derive_deps::wasm_bindgen::prelude::*;
#[wasm_bindgen]
impl SessionView {
    pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::ID { self.entity.id.clone() }
    pub fn date_connected(&self) -> ::ankurah::property::value::YrsString {
        use ankurah_core::property::{FromEntity, ProjectedValue};
        ::ankurah::property::value::YrsString::from_entity("date_connected".into(), self.entity.as_ref()).projected()
    }
    pub fn ip_address(&self) -> ::ankurah::property::value::YrsString {
        use ankurah_core::property::{FromEntity, ProjectedValue};
        ::ankurah::property::value::YrsString::from_entity("ip_address".into(), self.entity.as_ref()).projected()
    }
    pub fn node_id(&self) -> ::ankurah::property::value::YrsString {
        use ankurah_core::property::{FromEntity, ProjectedValue};
        ::ankurah::property::value::YrsString::from_entity("node_id".into(), self.entity.as_ref()).projected()
    }
}
#[derive(Debug)]
pub struct SessionMut<'rec> {
    entity: &'rec std::sync::Arc<::ankurah::model::Entity>,
    pub date_connected: ::ankurah::property::value::YrsString,
    pub ip_address: ::ankurah::property::value::YrsString,
    pub node_id: ::ankurah::property::value::YrsString,
}
impl<'rec> ::ankurah::model::Mutable<'rec> for SessionMut<'rec> {
    type Model = Session;
    type View = SessionView;
    fn entity(&self) -> &std::sync::Arc<::ankurah::model::Entity> { &self.entity }
    fn new(entity: &'rec std::sync::Arc<::ankurah::model::Entity>) -> Self {
        use ankurah_core::{model::Mutable, property::FromEntity};
        assert_eq!(entity.collection(), Self::collection());
        Self {
            entity,
            date_connected: ::ankurah::property::value::YrsString::from_entity("date_connected".into(), entity),
            ip_address: ::ankurah::property::value::YrsString::from_entity("ip_address".into(), entity),
            node_id: ::ankurah::property::value::YrsString::from_entity("node_id".into(), entity),
        }
    }
}
impl<'rec> SessionMut<'rec> {
    pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::ID { self.entity.id.clone() }
    pub fn date_connected(&self) -> &::ankurah::property::value::YrsString { &self.date_connected }
    pub fn ip_address(&self) -> &::ankurah::property::value::YrsString { &self.ip_address }
    pub fn node_id(&self) -> &::ankurah::property::value::YrsString { &self.node_id }
}
impl<'a> Into<ankurah::derive_deps::ankurah_proto::ID> for &'a SessionView {
    fn into(self) -> ankurah::derive_deps::ankurah_proto::ID { ankurah::View::id(self) }
}
impl<'a, 'rec> Into<ankurah::derive_deps::ankurah_proto::ID> for &'a SessionMut<'rec> {
    fn into(self) -> ankurah::derive_deps::ankurah_proto::ID { ::ankurah::model::Mutable::id(self) }
}

#[derive(Default, Clone, Debug)]
pub struct Frobnicator {}
