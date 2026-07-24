use std::{marker::PhantomData, sync::Arc};

use crate::{
    entity::Entity,
    error::MutationError,
    property::{
        backend::{PropertyBackend, YrsBackend},
        traits::{FromActiveType, FromEntity, InitializeWith, PropertyError},
        PropertyAddress, PropertyName,
    },
};
use ankql::ast::PropertyId;

use ankurah_signals::{
    signal::{Listener, ListenerGuard},
    Signal,
};

#[derive(Debug, Clone)]
pub struct YrsString<Projected> {
    // ideally we'd store the yrs::TransactionMut in the Transaction as an ExtendableOp or something like that
    // and call encode_update_v2 on it when we're ready to commit
    // but its got a lifetime of 'doc and that requires some refactoring
    pub property_name: PropertyName,
    /// This field's durable identity, resolved ONCE at construction -- see
    /// [`crate::property::value::LWW::property_id`] for the exact resolution
    /// rule (rfc.md 5.5/5.6 in specs/model-property-metadata/rfc.md); `None`
    /// marks a resolution failure. `resolution_error` distinguishes a specific
    /// resolver failure, such as ambiguity, from an unknown field.
    pub property_id: Option<PropertyId>,
    resolution_error: Option<String>,
    pub backend: Arc<YrsBackend>,
    pub entity: Entity,
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
    pub fn new(property_name: PropertyName, backend: Arc<YrsBackend>, entity: Entity) -> Self {
        // No compiled `PropertyAddress` here (no explicit-id case): resolve
        // straight from the bare name, same as `new_addressed` without an
        // override.
        let resolved = entity.resolve_property_id(&property_name);
        let (property_id, resolution_error) = match resolved {
            Ok(id) => (Some(id), None),
            Err(PropertyError::UnknownProperty { .. }) => (None, None),
            Err(error) => (None, Some(error.to_string())),
        };
        Self { property_name, property_id, resolution_error, backend, entity, phantom: PhantomData }
    }
    fn new_addressed(property: PropertyAddress, backend: Arc<YrsBackend>, entity: Entity) -> Self {
        let resolved = match property.explicit_id {
            Some(id) => Ok(PropertyId::EntityId(id)),
            None => entity.resolve_property_id(&property.name),
        };
        let (property_id, resolution_error) = match resolved {
            Ok(id) => (Some(id), None),
            Err(PropertyError::UnknownProperty { .. }) => (None, None),
            Err(error) => (None, Some(error.to_string())),
        };
        Self { property_name: property.name, property_id, resolution_error, backend, entity, phantom: PhantomData }
    }

    /// The resolved key, or the unbound-field error (rfc.md 5.5): the single
    /// point where an unresolvable field surfaces to the caller.
    fn resolved_id(&self) -> Result<PropertyId, PropertyError> {
        if let Some(error) = &self.resolution_error {
            return Err(PropertyError::RetrievalError(crate::error::RetrievalError::Other(error.clone())));
        }
        self.property_id
            .clone()
            .ok_or_else(|| PropertyError::UnknownProperty { model: self.entity.collection().to_string(), name: self.property_name.clone() })
    }

    /// Return the current raw text value. Resolution failures are represented
    /// as absence here for API compatibility; generated typed getters call
    /// `try_value` and surface the actual error.
    pub fn value(&self) -> Option<String> { self.try_value().ok().flatten() }

    fn try_value(&self) -> Result<Option<String>, PropertyError> {
        let key = self.resolved_id()?;
        Ok(self.backend.get_string(&key))
    }

    pub fn insert(&self, index: u32, value: &str) -> Result<(), MutationError> {
        if !self.entity.is_writable() {
            return Err(PropertyError::TransactionClosed.into());
        }
        let key = self.resolved_id()?;
        self.backend.insert(&key, index, value)
    }
    pub fn delete(&self, index: u32, length: u32) -> Result<(), MutationError> {
        if !self.entity.is_writable() {
            return Err(PropertyError::TransactionClosed.into());
        }
        let key = self.resolved_id()?;
        self.backend.delete(&key, index, length)
    }
    pub fn overwrite(&self, start: u32, length: u32, value: &str) -> Result<(), MutationError> {
        if !self.entity.is_writable() {
            return Err(PropertyError::TransactionClosed.into());
        }
        let key = self.resolved_id()?;
        self.backend.delete(&key, start, length)?;
        self.backend.insert(&key, start, value)?;
        Ok(())
    }
    pub fn replace(&self, value: &str) -> Result<(), MutationError> {
        if !self.entity.is_writable() {
            return Err(PropertyError::TransactionClosed.into());
        }
        let key = self.resolved_id()?;
        let length = self.backend.get_string(&key).unwrap_or_default().len() as u32;
        self.backend.delete(&key, 0, length)?;
        self.backend.insert(&key, 0, value)?;
        Ok(())
    }
}

impl<Projected> FromEntity for YrsString<Projected> {
    fn from_entity(property: PropertyAddress, entity: &Entity) -> Self {
        let backend = entity.get_backend::<YrsBackend>().expect("YrsBackend should exist");
        Self::new_addressed(property, backend, entity.clone())
    }
}

impl<Projected, S: FromActiveType<YrsString<Projected>>> FromActiveType<YrsString<Projected>> for Option<S> {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> {
        // Compiled-OPTIONAL: an absent root is None, never a fabricated
        // default (RFC 5.4 in specs/model-property-metadata/rfc.md rule 2). Checked here because the required
        // projections below default instead of erroring.
        if active.try_value()?.is_none() {
            return Ok(None);
        }
        match S::from_active(active) {
            Ok(value) => Ok(Some(value)),
            Err(PropertyError::Missing) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<Projected> FromActiveType<YrsString<Projected>> for String {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> {
        // Compiled-REQUIRED: an operation-based CRDT cannot distinguish an
        // empty text from an untouched one, so "no operations" is a
        // legitimate encoding OF the default: absent reads as "" instead of
        // erroring Missing (RFC 5.4 rule 3; the #175 fix).
        Ok(active.try_value()?.unwrap_or_default())
    }
}

impl<'a, Projected> FromActiveType<YrsString<Projected>> for std::borrow::Cow<'a, str> {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> { Ok(Self::from(active.try_value()?.unwrap_or_default())) }
}

impl<Projected> InitializeWith<String> for YrsString<Projected> {
    fn initialize_with(entity: &Entity, property: PropertyAddress, value: &String) -> Result<Self, MutationError> {
        let new_string = Self::from_entity(property, entity);
        new_string.insert(0, value)?;
        Ok(new_string)
    }
}

impl<Projected> InitializeWith<Option<String>> for YrsString<Projected> {
    fn initialize_with(entity: &Entity, property: PropertyAddress, value: &Option<String>) -> Result<Self, MutationError> {
        let new_string = Self::from_entity(property, entity);
        if let Some(value) = value {
            new_string.insert(0, value)?;
        }
        Ok(new_string)
    }
}

impl<Projected> ankurah_signals::Signal for YrsString<Projected> {
    fn listen(&self, listener: Listener) -> ListenerGuard {
        // Observe the same root that mutations target -- the resolved key,
        // fixed once at construction.
        match &self.property_id {
            Some(key) => self.backend.listen_field(key, listener),
            // Unbound: Signal has no fallible surface. Track the whole entity
            // rather than inventing a key for an unresolvable field; typed
            // reads and mutations still return the resolution error.
            None => self.entity.broadcast().reference().listen(listener).into(),
        }
    }

    // TODO: determine if we should cache this or not.
    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId {
        match &self.property_id {
            Some(key) => self.backend.field_broadcast_id(key),
            None => self.entity.broadcast().id(),
        }
    }
}

impl<Projected> ankurah_signals::Subscribe<String> for YrsString<Projected>
where Projected: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> ankurah_signals::SubscriptionGuard
    where F: ankurah_signals::subscribe::IntoSubscribeListener<String> {
        let listener = listener.into_subscribe_listener();
        let yrs_string = self.clone();
        let subscription = self.listen(Arc::new(move |_| {
            // Get current value when the broadcast fires
            if let Ok(Some(current_value)) = yrs_string.try_value() {
                listener(current_value);
            }
        }));
        ankurah_signals::SubscriptionGuard::new(subscription)
    }
}

#[cfg(any(feature = "wasm", feature = "uniffi"))]
pub mod ffi {
    //! FFI wrapper types for YrsString backend (WASM and UniFFI)
    use super::*;
    #[cfg(feature = "wasm")]
    use ::wasm_bindgen::prelude::*;
    use ankurah_derive::impl_provided_wrapper_types;
    impl_provided_wrapper_types!("src/property/value/yrs.ron");
}
#[cfg(any(feature = "wasm", feature = "uniffi"))]
pub use ffi::*;
