pub mod backend;
pub mod traits;
pub mod value;

use std::fmt::Display;

pub use backend::Backends;
pub use traits::{FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::YrsString;

use serde::{Deserialize, Serialize};

pub type PropertyName = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PropertyValue {
    // Numbers
    I16(i16),
    I32(i32),
    I64(i64),

    String(String),
    Object(Vec<u8>),
    Binary(Vec<u8>),
}

impl Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyValue::I16(int) => write!(f, "{:?}", int),
            PropertyValue::I32(int) => write!(f, "{:?}", int),
            PropertyValue::I64(int) => write!(f, "{:?}", int),
            PropertyValue::String(string) => write!(f, "{:?}", string),
            PropertyValue::Object(object) => write!(f, "{:?}", object),
            PropertyValue::Binary(binary) => write!(f, "{:?}", binary),
        }
    }
}

pub trait Property: Sized {
    fn into_value(&self) -> Result<Option<PropertyValue>, PropertyError>;
    fn from_value(value: Option<PropertyValue>) -> Result<Self, PropertyError>;

    /*
    fn from_option(option: Option<PropertyValue>) -> Result<Self, PropertyError> {
        match option {
            Some(value) => Self::from_value(value),
            None => Err(PropertyError::Missing),
        }
    }
    fn from_result(result: Result<Option<PropertyValue>, PropertyError>) -> Result<Self, PropertyError> {
        Self::from_option(result?)
    }
    */
}

impl<T> Property for Option<T>
where 
    T: Property,
{
    fn into_value(&self) -> Result<Option<PropertyValue>, PropertyError> {
        match self {
            Some(value) => Ok(<T as Property>::into_value(value)?),
            None => Ok(None),
        }
    }
    fn from_value(value: Option<PropertyValue>) -> Result<Self, PropertyError> {
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
            fn into_value(&self) -> Result<Option<PropertyValue>, PropertyError> {
                Ok(Some(PropertyValue::$variant(self.clone())))
            }
            fn from_value(value: Option<PropertyValue>) -> Result<Self, PropertyError> {
                match value {
                    Some(PropertyValue::$variant(value)) => Ok(value),
                    Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
                    None => Err(PropertyError::Missing),
                }
            }
        }
    };
}

into!(String => String);
into!(i16 => I16);
into!(i32 => I32);
into!(i64 => I64);
