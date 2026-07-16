use std::{marker::PhantomData, sync::Arc};

use crate::{
    entity::Entity,
    property::{
        backend::{LWWBackend, PropertyBackend},
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, Property, PropertyAddress, PropertyName, Value,
    },
};
use ankql::ast::PropertyId;

use ankurah_signals::{
    signal::{Listener, ListenerGuard},
    Signal,
};

#[derive(Clone)]
pub struct LWW<T: Property> {
    pub property_name: PropertyName,
    /// This field's durable identity, resolved ONCE at construction
    /// (`from_entity`) from the compiled `PropertyAddress`, via the entity's
    /// catalog resolver (rfc.md 5.5/5.6 in specs/model-property-metadata/rfc.md):
    /// an explicit `#[property(id = "...")]` binding's literal id, else the
    /// catalog's id for `property_name`, else (no resolver stamped: a
    /// system/bare/catalog entity) `property_name` itself as a
    /// [`PropertyId::System`]. `None` only when a resolver IS stamped but does
    /// NOT know this name (an unbound field) -- `set`/`get` then surface
    /// `PropertyError::UnknownProperty`. The backend keys on this value alone
    /// and never resolves: this is the one and only resolution hop.
    pub property_id: Option<PropertyId>,
    pub backend: Arc<LWWBackend>,
    pub entity: Entity,
    phantom: PhantomData<T>,
}

impl<T: Property> std::fmt::Debug for LWW<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LWW").field("property_name", &self.property_name).finish()
    }
}

impl<T: Property> LWW<T> {
    /// The resolved key, or the unbound-field error (rfc.md 5.5): the single
    /// point where an unresolvable field surfaces to the caller. `set` and
    /// `get` (via [`Self::stored_value`]) both route through this.
    fn resolved_id(&self) -> Result<PropertyId, PropertyError> {
        self.property_id.clone().ok_or_else(|| PropertyError::UnknownProperty {
            collection: self.entity.collection().to_string(),
            name: self.property_name.clone(),
        })
    }

    pub fn set(&self, value: &T) -> Result<(), PropertyError> {
        if !self.entity.is_writable() {
            return Err(PropertyError::TransactionClosed);
        }
        let pid = self.resolved_id()?;
        let value = value.into_value()?;
        self.backend.set(pid, value);
        Ok(())
    }

    /// Project the property under the RFC 5.4 (specs/model-property-metadata/rfc.md) read rules (the #175 fix):
    ///   - present -> the value;
    ///   - absent + REQUIRED (a defaulting value type, e.g. `String`/`i64`) ->
    ///     the type default via [`Property::absent_default`];
    ///   - absent + OPTIONAL (`Option<T>`) -> `None`, because the projected
    ///     type is `Option<T>` and its `absent_default` is `None`, so the inner
    ///     default never fires;
    ///   - absent + no fabricable default (`EntityId`/`Ref<T>`, derived enums)
    ///     -> `PropertyError::Missing` (or `None` under an `Option`).
    ///
    /// A present value is stored CANONICALLY typed (rfc.md 5.6 as amended
    /// 2026-07-10); a compiled type drifted from the canonical one reads
    /// through `Value::cast_to` (canonical -> compiled), and a per-value cast
    /// failure surfaces as the fail-visible `NonCastable`, never a fabricated
    /// default. The same hop covers a legacy or ill-typed payload
    /// defensively. Type-pair admission is REGISTRATION's job (the canonical
    /// value_type ruling): reads carry no gate. An unbound field (no resolved
    /// id) surfaces `PropertyError::UnknownProperty` here too.
    pub fn get(&self) -> Result<T, PropertyError> {
        match self.stored_value()? {
            Some(value) => {
                let value = match crate::value::ValueType::from_property_str(T::VALUE_TYPE) {
                    Some(target) if crate::value::ValueType::of(&value) != target => value.cast_to(target)?,
                    _ => value,
                };
                T::from_value(Some(value))
            }
            // Absent: feed the type's required-absent default to
            // `from_value`. `None` -> `from_value(None)` keeps today's meaning
            // (Missing for a required scalar, None for an Option).
            None => T::from_value(T::absent_default()),
        }
    }

    /// The stored value: `Some` present, `None` absent. Keys by the resolved
    /// [`PropertyId`] alone -- no id-then-name fallback (the identity model
    /// has exactly one durable key per property, full stop). Errors if the
    /// field never resolved (an unbound name-addressed field).
    pub fn stored_value(&self) -> Result<Option<Value>, PropertyError> {
        let pid = self.resolved_id()?;
        Ok(crate::property::read_by_id(self.backend.as_ref(), &pid))
    }
}

impl<T: Property> FromEntity for LWW<T> {
    fn from_entity(property: PropertyAddress, entity: &Entity) -> Self {
        let backend = entity.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        // Resolve ONCE, here at construction (rfc.md 5.5): an explicit id
        // binding wins outright; otherwise ask the entity's catalog resolver.
        // `resolve_property_id` already implements the no-resolver -> System,
        // unbound -> Err(UnknownProperty) branches; `.ok()` folds the latter
        // into `None` (this accessor's unbound marker).
        let property_id = match property.explicit_id {
            Some(id) => Some(PropertyId::EntityId(id.to_ulid())),
            None => entity.resolve_property_id(&property.name).ok(),
        };
        Self { property_name: property.name, property_id, backend, entity: entity.clone(), phantom: PhantomData }
    }
}

// One generic projection covers every LWW-backed field, because LWW's
// projected type is OPEN (any `Property`: scalars, `Option<_>`, `Json`,
// `Ref<T>`, derived enums), so it cannot enumerate concrete impls the way
// `YrsString`'s closed set does. The RFC 5.4 read rules are threaded through
// `LWW::get` -> `Property::absent_default` instead: the required-vs-optional-
// vs-default decision is keyed on the projected type (the A10 spec, as
// amended by the canonical value_type ruling 2026-07-10).
impl<T: Property> FromActiveType<LWW<T>> for T {
    fn from_active(active: LWW<T>) -> Result<Self, PropertyError>
    where Self: Sized {
        active.get()
    }
}

impl<T: Property> InitializeWith<T> for LWW<T> {
    fn initialize_with(entity: &Entity, property: PropertyAddress, value: &T) -> Result<Self, crate::error::MutationError> {
        let new = Self::from_entity(property, entity);
        new.set(value)?;
        Ok(new)
    }
}

impl<T: Property> ankurah_signals::Signal for LWW<T> {
    fn listen(&self, listener: Listener) -> ListenerGuard {
        match &self.property_id {
            Some(pid) => self.backend.listen_field(pid, listener),
            // Unbound: Signal has no fallible surface, and there is no
            // resolved key to listen on, so track nothing (a no-op guard).
            // Typed reads and writes still return the resolution error.
            None => self.entity.broadcast().reference().listen(listener).into(),
        }
    }

    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId {
        match &self.property_id {
            Some(pid) => self.backend.field_broadcast_id(pid),
            None => self.entity.broadcast().id(),
        }
    }
}

impl<T: Property> ankurah_signals::Subscribe<T> for LWW<T>
where T: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> ankurah_signals::SubscriptionGuard
    where F: ankurah_signals::subscribe::IntoSubscribeListener<T> {
        let listener = listener.into_subscribe_listener();
        let lww = self.clone();
        let subscription = self.listen(Arc::new(move |_| {
            // Get current value when the broadcast fires
            if let Ok(current_value) = lww.get() {
                listener(current_value);
            }
        }));
        ankurah_signals::SubscriptionGuard::new(subscription)
    }
}

#[cfg(any(feature = "wasm", feature = "uniffi"))]
pub mod ffi {
    //! FFI wrapper types for LWW backend (WASM and UniFFI)
    use super::*;
    use crate::property::Json;
    #[cfg(feature = "wasm")]
    use ::wasm_bindgen::prelude::*;
    use ankurah_derive::impl_provided_wrapper_types;
    impl_provided_wrapper_types!("src/property/value/lww.ron");
}
#[cfg(any(feature = "wasm", feature = "uniffi"))]
pub use ffi::*;
