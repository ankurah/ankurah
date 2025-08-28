use std::{marker::PhantomData, sync::Arc};

use crate::{
    entity::Entity,
    error::MutationError,
    property::{
        backend::{PropertyBackend, YrsBackend},
        traits::{FromActiveType, FromEntity, InitializeWith, PropertyError},
        PropertyName,
    },
};

use ankurah_signals::Signal;

#[derive(Debug, Clone)]
pub struct YrsString<Projected> {
    // ideally we'd store the yrs::TransactionMut in the Transaction as an ExtendableOp or something like that
    // and call encode_update_v2 on it when we're ready to commit
    // but its got a lifetime of 'doc and that requires some refactoring
    pub property_name: PropertyName,
    pub backend: Arc<YrsBackend>,
    phantom: PhantomData<Projected>,
    // TODO: Pretty sure we need to store a clone of the Entity here so it's kept alive for the lifetime of the YrsString
    // Previously this didn't matter because the YrsString wasn't clonable. Followup question on this:
    // Will we need to update ListenerGuard to hold a dyn Any to achieve this?
    // I ask because the ListenerGuard/SubscriptionGuard will be the only thing directly held by the user, not the YrsString/LWW
    // OR - will the closure be enough to hold the Entity or the YrsString/LWW alive? Ideally we wouldn't overthink this and just
    // use TDD to determine it imperically.
}

// Starting with basic string type operations
impl<Projected> YrsString<Projected> {
    pub fn new(property_name: PropertyName, backend: Arc<YrsBackend>) -> Self { Self { property_name, backend, phantom: PhantomData } }
    pub fn value(&self) -> Option<String> { self.backend.get_string(&self.property_name) }
    pub fn insert(&self, index: u32, value: &str) -> Result<(), MutationError> { self.backend.insert(&self.property_name, index, value) }
    pub fn delete(&self, index: u32, length: u32) -> Result<(), MutationError> { self.backend.delete(&self.property_name, index, length) }
    pub fn overwrite(&self, start: u32, length: u32, value: &str) -> Result<(), MutationError> {
        self.backend.delete(&self.property_name, start, length)?;
        self.backend.insert(&self.property_name, start, value)?;
        Ok(())
    }
    pub fn replace(&self, value: &str) -> Result<(), MutationError> {
        self.backend.delete(&self.property_name, 0, self.value().unwrap_or_default().len() as u32)?;
        self.backend.insert(&self.property_name, 0, value)?;
        Ok(())
    }
}

impl<Projected> FromEntity for YrsString<Projected> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.get_backend::<YrsBackend>().expect("YrsBackend should exist");
        Self::new(property_name, backend)
    }
}

impl<Projected, S: FromActiveType<YrsString<Projected>>> FromActiveType<YrsString<Projected>> for Option<S> {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> {
        match S::from_active(active) {
            Ok(value) => Ok(Some(value)),
            Err(PropertyError::Missing) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<Projected> FromActiveType<YrsString<Projected>> for String {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> {
        match active.value() {
            Some(value) => Ok(value),
            None => Err(PropertyError::Missing),
        }
    }
}

impl<'a, Projected> FromActiveType<YrsString<Projected>> for std::borrow::Cow<'a, str> {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> {
        match active.value() {
            Some(value) => Ok(Self::from(value)),
            None => Err(PropertyError::Missing),
        }
    }
}

impl<Projected> InitializeWith<String> for YrsString<Projected> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &String) -> Self {
        let new_string = Self::from_entity(property_name, entity);
        new_string.insert(0, value).unwrap();
        new_string
    }
}

impl<Projected> InitializeWith<Option<String>> for YrsString<Projected> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &Option<String>) -> Self {
        let new_string = Self::from_entity(property_name, entity);
        if let Some(value) = value {
            new_string.insert(0, value).unwrap();
        }
        new_string
    }
}

impl<Projected> ankurah_signals::Signal for YrsString<Projected> {
    fn listen(&self, listener: ankurah_signals::broadcast::Listener<()>) -> ankurah_signals::broadcast::ListenerGuard<()> {
        self.backend.listen_field(&self.property_name, listener)
    }

    // TODO: determine if we should cache this or not.
    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId { self.backend.field_broadcast_id(&self.property_name) }
}

impl<Projected> ankurah_signals::Subscribe<String> for YrsString<Projected>
where Projected: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> ankurah_signals::SubscriptionGuard
    where F: ankurah_signals::subscribe::IntoSubscribeListener<String> {
        let listener = listener.into_subscribe_listener();
        let yrs_string = self.clone();
        let subscription = self.listen(ankurah_signals::broadcast::IntoListener::into_listener(move |_| {
            // Get current value when the broadcast fires
            if let Some(current_value) = yrs_string.value() {
                listener(current_value);
            }
        }));
        ankurah_signals::SubscriptionGuard::new(subscription)
    }
}
