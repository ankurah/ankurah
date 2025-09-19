pub mod backend;
pub mod traits;
pub mod value;

use std::fmt::Display;

use ankurah_proto::EntityId;

pub use traits::{FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::YrsString;

use crate::{collation::Collatable, value::Value};

pub type PropertyName = String;

pub trait Property: Sized {
    fn into_value(&self) -> Result<Option<Value>, PropertyError>;
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError>;
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
}

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
into!(EntityId => EntityId);

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
