pub mod tsify;

use std::sync::Arc;

use ankurah_proto::{CollectionId, EntityId, State};

use crate::entity::Entity;
use crate::error::StateError;

use crate::property::PropertyError;

use anyhow::Result;

#[cfg(feature = "wasm")]
use js_sys;
#[cfg(feature = "wasm")]
use wasm_bindgen;

/// A model is a struct that represents the present values for a given entity
/// Schema is defined primarily by the Model object, and the View is derived from that via macro.
pub trait Model {
    type View: View;
    type Mutable: Mutable;
    fn collection() -> CollectionId;
    // TODO - this seems to be necessary, but I don't understand why
    // Backend fields should be getting initialized on demand when the values are set
    fn initialize_new_entity(&self, entity: &Entity);
}

/// A read only view of an Entity which offers typed accessors
pub trait View {
    type Model: Model;
    type Mutable: Mutable;
    fn id(&self) -> EntityId { self.entity().id() }

    fn collection() -> CollectionId { <Self::Model as Model>::collection() }
    fn entity(&self) -> &Entity;
    fn from_entity(inner: Entity) -> Self;
    fn to_model(&self) -> Result<Self::Model, PropertyError>;
}

/// A lifetime-constrained wrapper around a Mutable for compile-time transaction safety
#[derive(Debug)]
pub struct MutableBorrow<'rec, T: Mutable> {
    mutable: T,
    _entity_ref: &'rec Entity,
}

impl<'rec, T: Mutable> MutableBorrow<'rec, T> {
    pub fn new(entity_ref: &'rec Entity) -> Self { Self { mutable: T::new(entity_ref.clone()), _entity_ref: entity_ref } }

    /// Extract the core mutable (for WASM usage)
    pub fn into_core(self) -> T { self.mutable }
}

impl<'rec, T: Mutable> std::ops::Deref for MutableBorrow<'rec, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target { &self.mutable }
}

impl<'rec, T: Mutable> std::ops::DerefMut for MutableBorrow<'rec, T> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.mutable }
}

/// A mutable Model instance for an Entity with typed accessors.
/// It is associated with a transaction, and may not outlive said transaction.
pub trait Mutable {
    type Model: Model;
    type View: View;
    fn id(&self) -> EntityId { self.entity().id() }
    fn collection() -> CollectionId { <Self::Model as Model>::collection() }

    fn entity(&self) -> &Entity;
    fn new(entity: Entity) -> Self
    where Self: Sized;

    fn state(&self) -> Result<State, StateError> { self.entity().to_state() }

    fn read(&self) -> Self::View {
        let inner = self.entity();

        let new_inner = match &inner.kind {
            // If there is an upstream, use it
            crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
            // Else we're a new Entity, and we have to rely on the commit to add this to the node
            crate::entity::EntityKind::Primary => inner.clone(),
        };

        Self::View::from_entity(new_inner)
    }
}

// Helper function for Subscribe implementations in generated Views
// don't document this
#[doc(hidden)]
pub fn js_view_subscribe<V, F>(view: &V, listener: F) -> ankurah_signals::SubscriptionGuard
where
    V: ankurah_signals::Signal + View + Clone + Send + Sync + 'static,
    F: ankurah_signals::subscribe::IntoSubscribeListener<V>,
{
    let listener = listener.into_subscribe_listener();
    let view_clone = view.clone();
    let subscription = view.listen(Arc::new(move |_| {
        // Call the listener with the current view when the broadcast fires
        listener(view_clone.clone());
    }));
    ankurah_signals::SubscriptionGuard::new(subscription)
}

// Helper function for map implementations in generated WASM ResultSet wrappers
// don't document this
#[doc(hidden)]
#[cfg(feature = "wasm")]
pub fn js_resultset_map<V>(resultset: &crate::resultset::ResultSet<V>, callback: &js_sys::Function) -> js_sys::Array
where V: View + Clone + 'static + Into<wasm_bindgen::JsValue> {
    use ankurah_signals::Get;
    let items = resultset.get();
    let result_array = js_sys::Array::new();

    for item in items {
        let js_item = item.into();
        if let Ok(mapped_value) = callback.call1(&wasm_bindgen::JsValue::NULL, &js_item) {
            result_array.push(&mapped_value);
        }
    }

    result_array
}
