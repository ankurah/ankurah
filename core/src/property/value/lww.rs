use std::{marker::PhantomData, sync::Arc};

use crate::{
    entity::Entity,
    property::{
        backend::{LWWBackend, PropertyBackend},
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, Property, PropertyAddress, PropertyKey, PropertyName, Value,
    },
};

use ankurah_signals::{
    signal::{Listener, ListenerGuard},
    Signal,
};

#[derive(Clone)]
pub struct LWW<T: Property> {
    pub property_name: PropertyName,
    /// Literal identity from `#[property(id = "...")]`, when present.
    /// Ordinary fields remain resolver/name-addressed until commit.
    pub property_id: Option<ankurah_proto::EntityId>,
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
    fn addressed_key(&self) -> PropertyKey {
        self.property_id.map(PropertyKey::Id).unwrap_or_else(|| self.entity.property_key(&self.property_name))
    }

    fn staging_key(&self) -> PropertyKey {
        self.property_id.map(PropertyKey::Id).unwrap_or_else(|| PropertyKey::Name(self.property_name.clone()))
    }

    pub fn set(&self, value: &T) -> Result<(), PropertyError> {
        self.entity.ensure_system_alive()?;
        if !self.entity.is_writable() {
            return Err(PropertyError::TransactionClosed);
        }
        let value = value.into_value()?;
        // Ordinary sync accessors stage a transient Name key and commit
        // resolves it. An explicit binding already carries its authoritative
        // id, so it stages by Id even when the local and catalog names differ.
        self.backend.set(self.staging_key(), value);
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
    /// value_type ruling): reads carry no gate.
    pub fn get(&self) -> Result<T, PropertyError> {
        self.entity.ensure_system_alive()?;
        match self.stored_value() {
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

    /// The stored value: `Some` present, `None` absent. An ordinary field's
    /// transaction-local staged write overlays the generic resolved-property
    /// dispatch ([`crate::property::read_resolved`]); committed reads retain
    /// the dispatcher's authoritative Id-then-legacy-Name ordering.
    pub fn stored_value(&self) -> Option<Value> {
        if !self.entity.is_system_alive() {
            return None;
        }
        // Ordinary accessors stage under their compiled name until commit can
        // confirm and re-key the write. Give only that transaction-local
        // write precedence here so `set` followed by `get` observes the new
        // value (including a clear). A committed Name entry is legacy residue
        // and must never override a present Id entry or Id tombstone.
        if self.property_id.is_none() {
            if let Some(value) = self.backend.uncommitted_entry(&self.staging_key()) {
                return value;
            }
        }
        match self.addressed_key() {
            PropertyKey::Id(id) => crate::property::read_resolved(self.backend.as_ref(), id, &self.property_name),
            // Unregistered or system field: read the bare name.
            key @ PropertyKey::Name(_) => self.backend.get(&key),
        }
    }
}

impl<T: Property> FromEntity for LWW<T> {
    fn from_entity(property: PropertyAddress, entity: &Entity) -> Self {
        let backend = entity.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        Self { property_name: property.name, property_id: property.explicit_id, backend, entity: entity.clone(), phantom: PhantomData }
    }
}

// One generic projection covers every LWW-backed field, because LWW's
// projected type is OPEN (any `Property`: scalars, `Option<_>`, `Json`,
// `Ref<T>`, derived enums), so it cannot enumerate concrete impls the way
// `YrsString`'s closed set does. The RFC 5.4 read rules are threaded through
// `LWW::get` -> `read_resolved` -> `Property::absent_default` instead: the
// required-vs-optional-vs-default decision is keyed on the projected type
// (the A10 spec, as amended by the canonical value_type ruling 2026-07-10).
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
    fn listen(&self, listener: Listener) -> ListenerGuard { self.backend.listen_field(&self.addressed_key(), listener) }

    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId { self.backend.field_broadcast_id(&self.addressed_key()) }
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
