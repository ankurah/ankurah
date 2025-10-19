use std::{marker::PhantomData, sync::Arc};

use crate::{
    entity::Entity,
    property::{
        backend::{LWWBackend, PropertyBackend},
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, Property, PropertyName, Value,
    },
};

use ankurah_signals::Signal;

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
        if !self.entity.is_user_writable() {
            return Err(PropertyError::TransactionClosed);
        }
        let value = value.into_value()?;
        self.backend.set(self.property_name.clone(), value);
        Ok(())
    }

    pub fn get(&self) -> Result<T, PropertyError> {
        let value = self.get_value();
        T::from_value(value)
    }

    pub fn get_value(&self) -> Option<Value> { self.backend.get(&self.property_name) }
}

impl<T: Property> FromEntity for LWW<T> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        Self { property_name, backend, entity: entity.clone(), phantom: PhantomData }
    }
}

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
    fn listen(&self, listener: ankurah_signals::broadcast::Listener<()>) -> ankurah_signals::broadcast::ListenerGuard<()> {
        self.backend.listen_field(&self.property_name, listener)
    }

    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId { self.backend.field_broadcast_id(&self.property_name) }
}

impl<T: Property> ankurah_signals::Subscribe<T> for LWW<T>
where T: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> ankurah_signals::SubscriptionGuard
    where F: ankurah_signals::subscribe::IntoSubscribeListener<T> {
        let listener = listener.into_subscribe_listener();
        let lww = self.clone();
        let subscription = self.listen(ankurah_signals::broadcast::IntoListener::into_listener(move |_| {
            // Get current value when the broadcast fires
            if let Ok(current_value) = lww.get() {
                listener(current_value);
            }
        }));
        ankurah_signals::SubscriptionGuard::new(subscription)
    }
}

#[cfg(feature = "wasm")]
pub mod wasm {
    //! WASM wrapper types for LWW backend
    use ankurah_derive::impl_provided_wrapper_types;
    impl_provided_wrapper_types!("src/property/value/lww.ron");
}
