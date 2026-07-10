use std::{marker::PhantomData, sync::Arc};

use crate::{
    entity::Entity,
    property::{
        backend::{LWWBackend, PropertyBackend},
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, Property, PropertyName, Value,
    },
};

use ankurah_signals::{
    signal::{Listener, ListenerGuard},
    Signal,
};

#[derive(Clone)]
pub struct LWW<T: Property> {
    pub property_name: PropertyName,
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
    pub fn set(&self, value: &T) -> Result<(), PropertyError> {
        if !self.entity.is_writable() {
            return Err(PropertyError::TransactionClosed);
        }
        let value = value.into_value()?;
        // The sync accessor has only the field name: stage a transient Name
        // key. commit_local_trx resolves it to the property id before the event
        // is generated (the PropertyKey amendment, #289).
        self.backend.set(crate::property::PropertyKey::Name(self.property_name.clone()), value);
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
    /// The read is gated: data under a sibling id the catalog cannot reconcile
    /// surfaces `PropertyError::TypeSkew` instead of any of the above
    /// (the catalog-side dispatch, `crate::property::lww_read_checked`).
    ///
    /// A present value is stored CANONICALLY typed (rfc.md 5.6 as amended
    /// 2026-07-10); a compiled type drifted from the canonical one reads
    /// through `Value::cast_to` (canonical -> compiled), and a per-value cast
    /// failure surfaces as the fail-visible `CastError`, never a fabricated
    /// default. The same hop covers a legacy or ill-typed payload
    /// defensively.
    pub fn get(&self) -> Result<T, PropertyError> {
        match self.get_checked_value()? {
            Some(value) => {
                let value = match crate::value::ValueType::from_property_str(T::VALUE_TYPE) {
                    Some(target) if crate::value::ValueType::of(&value) != target => value.cast_to(target)?,
                    _ => value,
                };
                T::from_value(Some(value))
            }
            // Absent (and no skew): feed the type's required-absent default to
            // `from_value`. `None` -> `from_value(None)` keeps today's meaning
            // (Missing for a required scalar, None for an Option).
            None => T::from_value(T::absent_default()),
        }
    }

    /// The stored value under the RFC 5.4 sibling gate: `Ok(Some)` present,
    /// `Ok(None)` absent, `Err(TypeSkew)` when a retype lineage holds data
    /// here (delegates to the catalog-side dispatch,
    /// [`crate::property::lww_read_checked`]).
    pub fn get_checked_value(&self) -> Result<Option<Value>, PropertyError> {
        match self.entity.property_key(&self.property_name) {
            crate::property::PropertyKey::Id(id) => crate::property::lww_read_checked(
                &self.backend,
                id,
                &self.property_name,
                &self.entity.siblings(&self.property_name),
                // The BOUND getter arms the foreign-data gate (plan decision
                // 15): absent under our id with data sitting under ids the
                // catalog cannot name (a cross-root raw-state copy) must fail
                // visible, never read a fabricated default over it.
                |other| self.entity.catalog_knows_id(other),
            ),
            // Unregistered or system field: read the bare name, no sibling gate.
            key @ crate::property::PropertyKey::Name(_) => Ok(self.backend.get(&key)),
        }
    }
}

impl<T: Property> FromEntity for LWW<T> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        Self { property_name, backend, entity: entity.clone(), phantom: PhantomData }
    }
}

// One generic projection covers every LWW-backed field, because LWW's
// projected type is OPEN (any `Property`: scalars, `Option<_>`, `Json`,
// `Ref<T>`, derived enums), so it cannot enumerate concrete impls the way
// `YrsString`'s closed set does. The RFC 5.4 read rules are threaded through
// `LWW::get` -> `lww_read_checked` -> `Property::absent_default` instead: the
// required-vs-optional-vs-default decision is keyed on the projected type, and
// the sibling gate rides along uniformly (so `TypeSkew` propagates for every
// projection, per the A10 spec).
impl<T: Property> FromActiveType<LWW<T>> for T {
    fn from_active(active: LWW<T>) -> Result<Self, PropertyError>
    where Self: Sized {
        active.get()
    }
}

impl<T: Property> InitializeWith<T> for LWW<T> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &T) -> Self {
        let new = Self::from_entity(property_name, entity);
        new.set(value).unwrap();
        new
    }
}

impl<T: Property> ankurah_signals::Signal for LWW<T> {
    fn listen(&self, listener: Listener) -> ListenerGuard {
        self.backend.listen_field(&self.entity.property_key(&self.property_name), listener)
    }

    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId {
        self.backend.field_broadcast_id(&self.entity.property_key(&self.property_name))
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
