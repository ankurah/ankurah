use super::{Model, View};
use crate::{entity::Entity, error::RetrievalError, schema::ModelStructDescriptor};

/// What a typed query returns for each matching entity: one view, or a tuple of views of that same entity,
/// one per component. A match belongs to every component, and the query may name any of their properties.
/// The wasm and UniFFI bindings query one view at a time; tuples are for Rust callers.
pub trait ComponentViews: Sized {
    /// The components every match belongs to, in order.
    fn components() -> Vec<&'static ModelStructDescriptor>;

    /// View `entity`, a member of every one of `components()`, through each of them.
    fn views_of(entity: Entity) -> Result<Self, RetrievalError>;
}

impl<V: View> ComponentViews for V {
    fn components() -> Vec<&'static ModelStructDescriptor> { vec![V::Model::descriptor()] }

    fn views_of(entity: Entity) -> Result<Self, RetrievalError> { V::from_entity(entity) }
}

impl<A: View, B: View> ComponentViews for (A, B) {
    fn components() -> Vec<&'static ModelStructDescriptor> { vec![A::Model::descriptor(), B::Model::descriptor()] }

    fn views_of(entity: Entity) -> Result<Self, RetrievalError> { Ok((entity.cast()?, entity.cast()?)) }
}

impl<A: View, B: View, C: View> ComponentViews for (A, B, C) {
    fn components() -> Vec<&'static ModelStructDescriptor> { vec![A::Model::descriptor(), B::Model::descriptor(), C::Model::descriptor()] }

    fn views_of(entity: Entity) -> Result<Self, RetrievalError> { Ok((entity.cast()?, entity.cast()?, entity.cast()?)) }
}
