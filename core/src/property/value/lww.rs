use std::{marker::PhantomData, sync::Arc};

use crate::{
    entity::LocalTrxEntity,
    property::{
        backend::{LWWBackend, PropertyBackend},
        traits::{FromLocalTrxEntity, PropertyError},
        InitializeWith, Property, PropertyId, Value,
    },
};

use ankurah_signals::{
    signal::{Listener, ListenerGuard},
    Signal,
};

#[derive(Clone)]
pub struct LWWMut<T: Property> {
    pub property: PropertyId,
    pub backend: Arc<LWWBackend>,
    pub entity: LocalTrxEntity,
    phantom: PhantomData<T>,
}

impl<T: Property> std::fmt::Debug for LWWMut<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_struct("LWWMut").field("property", &self.property).finish() }
}

impl<T: Property> LWWMut<T> {
    pub fn set(&self, value: &T) -> Result<(), PropertyError> {
        self.entity.check_open()?;
        let value = value.into_value()?;
        self.backend.set(self.property.clone(), value);
        self.entity.notify_changed();
        Ok(())
    }

    pub fn get(&self) -> Result<T, PropertyError> {
        let value = self.get_value();
        T::from_value(value)
    }

    pub fn get_value(&self) -> Option<Value> { self.backend.get(&self.property) }
}

impl<T: Property> crate::property::traits::ActiveType for LWWMut<T> {
    const BACKEND: &'static str = "lww";
}

impl<T: Property> FromLocalTrxEntity for LWWMut<T> {
    fn from_local_entity(property: PropertyId, entity: &LocalTrxEntity) -> Result<Self, PropertyError> {
        let backend = entity.get_backend::<LWWBackend>()?;
        Ok(Self { property, backend, entity: entity.clone(), phantom: PhantomData })
    }
}

impl<T: Property> InitializeWith<T> for LWWMut<T> {
    fn initialize_with(entity: &LocalTrxEntity, property: PropertyId, value: &T) {
        let backend = entity.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        backend.set(property, value.into_value().unwrap());
    }
}

impl<T: Property> ankurah_signals::Signal for LWWMut<T> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.backend.listen_field(&self.property, listener) }

    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId { self.backend.field_broadcast_id(&self.property) }
}

impl<T: Property> ankurah_signals::Subscribe<T> for LWWMut<T>
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
