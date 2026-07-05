pub mod backend;
pub mod traits;
pub mod value;

use ankurah_proto::EntityId;

pub use traits::{FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::{Json, Ref, YrsString};

use crate::value::Value;

pub type PropertyName = String;

pub trait Property: Sized {
    fn into_value(&self) -> Result<Option<Value>, PropertyError>;
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError>;

    /// The value an ABSENT REQUIRED property of this type reads back as under
    /// RFC 5.4 rule 3 (the #175 fix): the value type's default, fed to
    /// [`from_value`] by the compiled projection when the backend holds no
    /// entry AND the sibling gate is clear (see
    /// `LWWBackend::get_checked` and `property/value/lww.rs`).
    ///
    /// `None` means "no fabricable default" (`from_value(None)` then decides:
    /// a required scalar would error `Missing`, an `Option<T>` swallows it to
    /// `None`). This is the correct behavior for `EntityId`/`Ref<T>` (rule 3's
    /// flagged exception -- an entity id has no zero value to invent) and for
    /// arbitrary derived `Property` types (enums etc. have no natural default),
    /// so it is the trait default here. The primitive value types override it
    /// with their concrete default; `Json` overrides it with JSON null.
    ///
    /// Note this is keyed on the PROJECTED type, so an `Option<i64>` field
    /// resolves to `Option::<i64>::absent_default()` (the trait default `None`,
    /// hence `None`), and the inner `i64` default never fires: optionality
    /// short-circuits ahead of the required default, exactly as intended.
    fn absent_default() -> Option<Value> { None }
}

impl<T> Property for Option<T>
where T: Property
{
    fn into_value(&self) -> Result<Option<Value>, PropertyError> {
        match self {
            Some(value) => Ok(<T as Property>::into_value(value)?),
            None => Ok(None),
        }
    }
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
        match T::from_value(value) {
            Ok(value) => Ok(Some(value)),
            Err(PropertyError::Missing) => Ok(None),
            Err(err) => Err(err),
        }
    }
    // Inherit the trait default (`None`): an absent optional reads as `None`,
    // never a fabricated inner default (RFC 5.4 rule 2).
}

/// Property types with a fabricable required-absent default (RFC 5.4 rule 3).
macro_rules! into {
    ($ty:ty => $variant:ident) => {
        impl Property for $ty {
            fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::$variant(self.clone()))) }
            fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
                match value {
                    Some(Value::$variant(value)) => Ok(value),
                    Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
                    None => Err(PropertyError::Missing),
                }
            }
            fn absent_default() -> Option<Value> { Some(Value::$variant(<$ty>::default())) }
        }
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self { Value::$variant(value) }
        }
    };
}

/// Property types with NO fabricable default: absent required reads stay
/// `Missing` (RFC 5.4 rule 3's flagged exception -- e.g. `EntityId`, which has
/// no zero value to invent). Same wire mapping as [`into!`], but leaves
/// `absent_default` at the trait default (`None`).
macro_rules! into_no_default {
    ($ty:ty => $variant:ident) => {
        impl Property for $ty {
            fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::$variant(self.clone()))) }
            fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
                match value {
                    Some(Value::$variant(value)) => Ok(value),
                    Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
                    None => Err(PropertyError::Missing),
                }
            }
        }
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self { Value::$variant(value) }
        }
    };
}

into!(String => String);
into!(i16 => I16);
into!(i32 => I32);
into!(i64 => I64);
into!(f64 => F64);
into!(bool => Bool);
into!(Vec<u8> => Binary);
// EntityId has no zero value to fabricate: absent required stays Missing.
into_no_default!(EntityId => EntityId);

impl<'a> Property for std::borrow::Cow<'a, str> {
    fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::String(self.to_string()))) }

    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
        match value {
            Some(Value::String(value)) => Ok(value.into()),
            Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
            None => Err(PropertyError::Missing),
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self { Value::String(value.to_string()) }
}
