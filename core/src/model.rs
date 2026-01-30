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
#[cfg(feature = "wasm")]
use wasm_bindgen::JsCast;

/// A model is a struct that represents the present values for a given entity
/// Schema is defined primarily by the Model object, and the View is derived from that via macro.
pub trait Model: Sized {
    type View: View;
    type Mutable: Mutable;

    /// WASM wrapper type for Ref<Self> - enables typed entity references in TypeScript.
    /// The RefWrapper is a monomorphized struct (e.g., RefUser for Ref<User>) that
    /// provides methods like `.get(ctx)` and `.id()` with proper TypeScript types.
    #[cfg(feature = "wasm")]
    type RefWrapper: From<crate::property::Ref<Self>> + Into<crate::property::Ref<Self>>;

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

// Helper function to convert Result<T, PropertyError> to Result<T, JsValue> with context for generated WASM accessors
#[doc(hidden)]
#[cfg(feature = "wasm")]
pub fn wasm_prop<T>(result: Result<T, PropertyError>, property: &'static str, model: &'static str) -> Result<T, wasm_bindgen::JsValue> {
    result.map_err(|err| match err {
        PropertyError::Missing => {
            wasm_bindgen::JsValue::from_str(&format!("property '{}' is missing in model '{}'", property, model))
        }
        _ => wasm_bindgen::JsValue::from_str(&err.to_string()),
    })
}

// Helper function for Subscribe implementations in generated Views
// don't document this
#[doc(hidden)]
pub fn view_subscribe<V, F>(view: &V, listener: F) -> ankurah_signals::SubscriptionGuard
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

#[doc(hidden)]
pub fn view_subscribe_no_clone<V, F>(view: &V, listener: F) -> ankurah_signals::SubscriptionGuard
where
    V: ankurah_signals::Signal + View + Send + Sync + 'static,
    F: ankurah_signals::subscribe::IntoSubscribeListener<()>,
{
    let listener = listener.into_subscribe_listener();
    let subscription = view.listen(Arc::new(move |_| {
        listener(());
    }));
    ankurah_signals::SubscriptionGuard::new(subscription)
}

// Preprocess a Ref<T> field in a JS object before serde deserialization.
// Uses duck typing: if value has an `.id` property (View/Ref types), extracts it.
// Otherwise tries to parse as base64 string.
#[doc(hidden)]
#[cfg(feature = "wasm")]
pub fn js_preprocess_ref_field(obj: &wasm_bindgen::JsValue, field_name: &str) -> Result<(), wasm_bindgen::JsValue> {
    let field_key = wasm_bindgen::JsValue::from_str(field_name);
    if let Ok(v) = js_sys::Reflect::get(obj, &field_key) {
        // Skip if already a string
        if v.as_string().is_some() {
            return Ok(());
        }

        // Duck typing: check if value has an `.id` property (View/Ref types have this)
        let id_key = wasm_bindgen::JsValue::from_str("id");
        if let Ok(id_value) = js_sys::Reflect::get(&v, &id_key) {
            // id_value should be an EntityId - get its base64 representation
            let base64_key = wasm_bindgen::JsValue::from_str("to_base64");
            if let Ok(to_base64_fn) = js_sys::Reflect::get(&id_value, &base64_key) {
                if let Some(func) = to_base64_fn.dyn_ref::<js_sys::Function>() {
                    if let Ok(result) = func.call0(&id_value) {
                        if let Some(id_str) = result.as_string() {
                            js_sys::Reflect::set(obj, &field_key, &wasm_bindgen::JsValue::from_str(&id_str))?;
                            return Ok(());
                        }
                    }
                }
            }
        }

        // If we get here and it's not a string, it's an invalid value
        if !v.is_undefined() && !v.is_null() {
            return Err(wasm_bindgen::JsValue::from_str(&format!("Field '{}' must be a View, Ref, or base64 string", field_name)));
        }
    }
    Ok(())
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

// Helper function for subscribe implementations in generated WASM LiveQuery wrappers
#[doc(hidden)]
#[cfg(feature = "wasm")]
pub fn js_livequery_subscribe<V, W, F>(
    livequery: &crate::livequery::LiveQuery<V>,
    callback: js_sys::Function,
    immediate: bool,
    wrap_changeset: F,
) -> ankurah_signals::SubscriptionGuard
where
    V: View + Clone + Send + Sync + 'static,
    W: Into<wasm_bindgen::JsValue>,
    F: Fn(crate::changes::ChangeSet<V>) -> W + Send + Sync + 'static,
{
    use ankurah_signals::{Peek, Subscribe};

    // If immediate, call the callback with current state first
    if immediate {
        let current_items = livequery.peek();
        let changes = current_items.into_iter().map(|item| crate::changes::ItemChange::Add { item, events: vec![] }).collect();
        let initial_changeset = crate::changes::ChangeSet { resultset: livequery.resultset(), changes };
        let wrapped = wrap_changeset(initial_changeset);
        let _ = callback.call1(&wasm_bindgen::JsValue::NULL, &wrapped.into());
    }

    // Set up the subscription for future changes
    let callback = ::send_wrapper::SendWrapper::new(callback);
    livequery.subscribe(move |changeset: crate::changes::ChangeSet<V>| {
        let wrapped_changeset = wrap_changeset(changeset);
        let _ = callback.call1(&wasm_bindgen::JsValue::NULL, &wrapped_changeset.into());
    })
}
