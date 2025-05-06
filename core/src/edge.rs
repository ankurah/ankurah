use ankurah_proto::EntityId;

use crate::{
    model::View,
    property::{value::LWW, Property, PropertyError, PropertyValue},
    Model,
};

/*
pub struct Edge<M: Model> {
    pub a: ID,
    pub b: ID,
    pub extra: M,
}

pub struct EdgeView<M: Model> {
    pub a: ID,
    pub b: ID,
    pub extra: M::View,
}

impl<M: Model> View for EdgeView<M> {
    type Model = Edge<M>;
    type Mutable<'trx> = EdgeMutable<'trx, M>;

    fn entity(&self) -> &std::sync::Arc<crate::model::Entity> { todo!() }

    fn from_entity(inner: std::sync::Arc<crate::model::Entity>) -> Self { todo!() }

    fn to_model(&self) -> anyhow::Result<Self::Model, crate::property::PropertyError> { todo!() }
}

pub struct EdgeMutable<'rec, M: Model> {
    pub a: ID,
    pub b: ID,
    pub extra: M::Mutable<'rec>,
}

impl<M: Model> Model for Edge<M> {
    type View = EdgeView<M>;
    type Mutable<'trx> = EdgeMutable<'trx, M>;

    fn collection() -> ankurah_proto::CollectionId { todo!() }

    fn create_entity(&self, id: ID) -> crate::model::Entity { todo!() }
}

*/
