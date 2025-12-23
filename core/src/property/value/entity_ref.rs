//! Typed entity reference property.
//!
//! The `Ref<T>` type wraps an `EntityId` with compile-time knowledge of the target model type.
//! This enables type-safe entity traversal:
//!
//! ```rust,ignore
//! #[derive(Model)]
//! pub struct Album {
//!     pub name: String,
//!     pub artist: Ref<Artist>,
//! }
//!
//! // Fetch referenced entity
//! let album: AlbumView = ctx.get(album_id).await?;
//! let artist: ArtistView = album.artist().get(&ctx).await?;
//! ```

use crate::model::View;
use ankurah_proto::EntityId;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;

use crate::context::Context;
use crate::error::RetrievalError;
use crate::model::Model;
use crate::property::{Property, PropertyError};
use crate::value::Value;

/// A typed reference to another entity.
///
/// Stores an `EntityId` internally but carries compile-time type information
/// about the target model, enabling type-safe `.get()` calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ref<T> {
    id: EntityId,
    #[serde(skip)]
    _phantom: PhantomData<T>,
}

impl<T> Deref for Ref<T> {
    type Target = EntityId;
    fn deref(&self) -> &EntityId { &self.id }
}

impl<T> AsRef<EntityId> for Ref<T> {
    fn as_ref(&self) -> &EntityId { &self.id }
}

impl<T> Borrow<EntityId> for Ref<T> {
    fn borrow(&self) -> &EntityId { &self.id }
}

impl<T> Ref<T> {
    /// Create a new Ref from an EntityId.
    pub fn new(id: EntityId) -> Self { Ref { id, _phantom: PhantomData } }

    /// Create a Ref from a base64-encoded EntityId string.
    pub fn from_base64(s: &str) -> Result<Self, ankurah_proto::DecodeError> { Ok(Ref::new(EntityId::from_base64(s)?)) }

    /// Get the underlying EntityId.
    pub fn id(&self) -> EntityId { self.id.clone() }

    /// Get the underlying EntityId as a reference.
    pub fn id_ref(&self) -> &EntityId { &self.id }
}

impl<T: Model> Ref<T> {
    /// Fetch the referenced entity from the given context.
    ///
    /// # Example
    /// ```rust,ignore
    /// let album: AlbumView = ctx.get(album_id).await?;
    /// let artist: ArtistView = album.artist().get(&ctx).await?;
    /// ```
    pub async fn get(&self, ctx: &Context) -> Result<T::View, RetrievalError> { ctx.get::<T::View>(self.id.clone()).await }
}

impl<T> From<EntityId> for Ref<T> {
    fn from(id: EntityId) -> Self { Ref::new(id) }
}

impl<T> From<&EntityId> for Ref<T> {
    fn from(id: &EntityId) -> Self { Ref::new(id.clone()) }
}

impl<T> TryFrom<&str> for Ref<T> {
    type Error = ankurah_proto::DecodeError;
    fn try_from(s: &str) -> Result<Self, Self::Error> { Ref::from_base64(s) }
}

impl<T> TryFrom<String> for Ref<T> {
    type Error = ankurah_proto::DecodeError;
    fn try_from(s: String) -> Result<Self, Self::Error> { Ref::from_base64(&s) }
}

impl<T> From<Ref<T>> for EntityId {
    fn from(r: Ref<T>) -> Self { r.id }
}

impl<T> From<&Ref<T>> for EntityId {
    fn from(r: &Ref<T>) -> Self { r.id.clone() }
}

impl<T> fmt::Display for Ref<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.id.to_base64()) }
}

// Ref<T> support for predicates (queries)
impl<T> From<Ref<T>> for ::ankql::ast::Expr {
    fn from(r: Ref<T>) -> ::ankql::ast::Expr { r.id.into() }
}

impl<T> From<&Ref<T>> for ::ankql::ast::Expr {
    fn from(r: &Ref<T>) -> ::ankql::ast::Expr { (&r.id).into() }
}

// Any View can be converted to Ref<Model> by borrowing
impl<V: View> From<&V> for Ref<V::Model> {
    fn from(view: &V) -> Ref<V::Model> { Ref::new(view.id()) }
}

impl<T> Property for Ref<T> {
    fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::EntityId(self.id.clone()))) }

    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
        match value {
            Some(Value::EntityId(id)) => Ok(Ref::new(id)),
            // Backwards compatibility: accept string EntityIds (e.g., from older schema)
            Some(Value::String(s)) => {
                EntityId::from_base64(&s).map(Ref::new).map_err(|e| PropertyError::InvalidValue { value: s, ty: format!("Ref ({})", e) })
            }
            Some(other) => Err(PropertyError::InvalidVariant { given: other, ty: "Ref".to_string() }),
            None => Err(PropertyError::Missing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Dummy model for testing
    struct TestModel;

    #[test]
    fn test_ref_roundtrip() {
        let id = EntityId::new();
        let r: Ref<TestModel> = Ref::new(id.clone());

        let value = r.into_value().unwrap().unwrap();
        assert!(matches!(value, Value::EntityId(_)));

        let recovered: Ref<TestModel> = Ref::from_value(Some(value)).unwrap();
        assert_eq!(recovered.id(), id);
    }

    #[test]
    fn test_ref_from_entity_id() {
        let id = EntityId::new();
        let r: Ref<TestModel> = id.clone().into();
        assert_eq!(r.id(), id);
    }

    #[test]
    fn test_ref_into_entity_id() {
        let id = EntityId::new();
        let r: Ref<TestModel> = Ref::new(id.clone());
        let recovered: EntityId = r.into();
        assert_eq!(recovered, id);
    }

    #[test]
    fn test_ref_missing() {
        let result: Result<Ref<TestModel>, _> = Ref::from_value(None);
        assert!(matches!(result, Err(PropertyError::Missing)));
    }

    #[test]
    fn test_ref_invalid_string() {
        // Invalid base64 string should return InvalidValue (backwards compat path tries to parse)
        let result: Result<Ref<TestModel>, _> = Ref::from_value(Some(Value::String("not an id".to_string())));
        assert!(matches!(result, Err(PropertyError::InvalidValue { .. })));
    }

    #[test]
    fn test_ref_invalid_variant() {
        // Completely wrong type should return InvalidVariant
        let result: Result<Ref<TestModel>, _> = Ref::from_value(Some(Value::I64(42)));
        assert!(matches!(result, Err(PropertyError::InvalidVariant { .. })));
    }
}
