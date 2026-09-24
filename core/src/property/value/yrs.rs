use crate::internal::prelude::*;
use std::{marker::PhantomData, sync::Arc};

use crate::entity::LocalTrxEntity;
use crate::property::backend::{PropertyBackend, YrsBackend};
use crate::property::traits::{FromLocalTrxEntity, InitializeWith, PropertyError};
use crate::property::PropertyId;

use ankurah_signals::{
    signal::{Listener, ListenerGuard},
    Signal,
};

#[derive(Debug, Clone)]
pub struct YrsStringMut<Projected> {
    // ideally we'd store the yrs::TransactionMut in the Transaction as an ExtendableOp or something like that
    // and call encode_update_v2 on it when we're ready to commit
    // but its got a lifetime of 'doc and that requires some refactoring
    pub property: PropertyId,
    pub backend: Arc<YrsBackend>,
    pub entity: LocalTrxEntity,
    phantom: PhantomData<Projected>,
    // TODO: Pretty sure we need to store a clone of the Entity here so it's kept alive for the lifetime of the YrsStringMut
    // Previously this didn't matter because the YrsStringMut wasn't clonable. Followup question on this:
    // Will we need to update ListenerGuard to hold a dyn Any to achieve this?
    // I ask because the ListenerGuard/SubscriptionGuard will be the only thing directly held by the user, not the YrsStringMut/LWWMut
    // OR - will the closure be enough to hold the Entity or the YrsStringMut/LWWMut alive? Ideally we wouldn't overthink this and just
    // use TDD to determine it imperically.
}

// Starting with basic string type operations
impl<Projected> YrsStringMut<Projected> {
    pub fn new(property: PropertyId, backend: Arc<YrsBackend>, entity: LocalTrxEntity) -> Self {
        Self { property, backend, entity, phantom: PhantomData }
    }
    pub fn value(&self) -> Option<String> { self.backend.get_string(&self.property) }
    pub fn insert(&self, index: u32, value: &str) -> Result<(), MutationError> {
        self.entity.check_open()?;
        self.backend.insert(&self.property, index, value)?;
        self.entity.notify_changed();
        Ok(())
    }
    pub fn delete(&self, index: u32, length: u32) -> Result<(), MutationError> {
        self.entity.check_open()?;
        self.backend.delete(&self.property, index, length)?;
        self.entity.notify_changed();
        Ok(())
    }
    pub fn overwrite(&self, start: u32, length: u32, value: &str) -> Result<(), MutationError> {
        self.entity.check_open()?;
        self.backend.delete(&self.property, start, length)?;
        self.backend.insert(&self.property, start, value)?;
        self.entity.notify_changed();
        Ok(())
    }
    pub fn replace(&self, value: &str) -> Result<(), MutationError> {
        self.entity.check_open()?;
        self.backend.delete(&self.property, 0, self.value().unwrap_or_default().len() as u32)?;
        self.backend.insert(&self.property, 0, value)?;
        self.entity.notify_changed();
        Ok(())
    }
}

impl<Projected> crate::property::traits::ActiveType for YrsStringMut<Projected> {
    const BACKEND: &'static str = "yrs";
}

impl<Projected> FromLocalTrxEntity for YrsStringMut<Projected> {
    fn from_local_entity(property: PropertyId, entity: &LocalTrxEntity) -> Result<Self, PropertyError> {
        let backend = entity.get_backend::<YrsBackend>()?;
        Ok(Self::new(property, backend, entity.clone()))
    }
}

impl<Projected> InitializeWith<String> for YrsStringMut<Projected> {
    fn initialize_with(entity: &LocalTrxEntity, property: PropertyId, value: &String) {
        let backend = entity.get_backend::<YrsBackend>().expect("YrsBackend should exist");
        backend.insert(&property, 0, value).unwrap();
    }
}

impl<Projected> InitializeWith<Option<String>> for YrsStringMut<Projected> {
    fn initialize_with(entity: &LocalTrxEntity, property: PropertyId, value: &Option<String>) {
        // The backend is created even when there is no value: whether a
        // model's yrs document exists at all is part of what the genesis
        // preimage commits to.
        let backend = entity.get_backend::<YrsBackend>().expect("YrsBackend should exist");
        if let Some(value) = value {
            backend.insert(&property, 0, value).unwrap();
        }
    }
}

impl<Projected> ankurah_signals::Signal for YrsStringMut<Projected> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.backend.listen_field(&self.property, listener) }

    // TODO: determine if we should cache this or not.
    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId { self.backend.field_broadcast_id(&self.property) }
}

impl<Projected> ankurah_signals::Subscribe<String> for YrsStringMut<Projected>
where Projected: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> ankurah_signals::SubscriptionGuard
    where F: ankurah_signals::subscribe::IntoSubscribeListener<String> {
        let listener = listener.into_subscribe_listener();
        let yrs_string = self.clone();
        let subscription = self.listen(Arc::new(move |_| {
            // Get current value when the broadcast fires
            if let Some(current_value) = yrs_string.value() {
                listener(current_value);
            }
        }));
        ankurah_signals::SubscriptionGuard::new(subscription)
    }
}

#[cfg(any(feature = "wasm", feature = "uniffi"))]
pub mod ffi {
    //! FFI wrapper types for YrsStringMut (WASM and UniFFI)
    use super::*;
    #[cfg(feature = "wasm")]
    use ::wasm_bindgen::prelude::*;
    use ankurah_derive::impl_provided_wrapper_types;
    impl_provided_wrapper_types!("src/property/value/yrs.ron");
}
#[cfg(any(feature = "wasm", feature = "uniffi"))]
pub use ffi::*;
