use super::{Model, View};
use crate::{entity::Entity, error::RetrievalError, schema::ModelStructDescriptor};

/// What a typed query returns for each matching entity: one view, or a tuple of views of that same entity,
/// one per component. A match belongs to every component, and the query may name any of their properties.
/// A tuple has up to twelve members, and a member may itself be a tuple.
/// The wasm and UniFFI bindings query one view at a time; tuples are for Rust callers.
///
/// A bare property name is asked of these components only, and refused when two declare it as different
/// properties. `Component::name` names one of them by display name or label, else any component by label.
pub trait ComponentViews: sealed::Sealed + Sized {
    /// The components every match belongs to, in order.
    fn components() -> Vec<&'static ModelStructDescriptor>;

    /// View `entity`, a member of every one of `components()`, through each of them.
    fn views_of(entity: Entity) -> Result<Self, RetrievalError>;
}

// Seals `ComponentViews` to a view and to tuples built from views. Core relies on `components()` naming at
// least one component and on `views_of` succeeding for every entity a query matched.
mod sealed {
    pub trait Sealed {}
}

impl<V: View> sealed::Sealed for V {}

impl<V: View> ComponentViews for V {
    fn components() -> Vec<&'static ModelStructDescriptor> { vec![V::Model::descriptor()] }

    fn views_of(entity: Entity) -> Result<Self, RetrievalError> { V::from_entity(entity) }
}

/// Implements `ComponentViews` for the tuple of the given members, and for each shorter tuple down to a pair.
macro_rules! component_views_for_tuples {
    ($first:ident, $($rest:ident),+) => {
        impl<$first: ComponentViews, $($rest: ComponentViews),+> sealed::Sealed for ($first, $($rest),+) {}

        impl<$first: ComponentViews, $($rest: ComponentViews),+> ComponentViews for ($first, $($rest),+) {
            fn components() -> Vec<&'static ModelStructDescriptor> { [$first::components(), $($rest::components()),+].concat() }

            fn views_of(entity: Entity) -> Result<Self, RetrievalError> {
                Ok(($first::views_of(entity.clone())?, $($rest::views_of(entity.clone())?),+))
            }
        }
        component_views_for_tuples!($($rest),+);
    };
    ($last:ident) => {};
}
component_views_for_tuples!(A, B, C, D, E, F, G, H, I, J, K, L);
