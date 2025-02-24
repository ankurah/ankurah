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
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),

    String(Option<String>),
    Object(Option<Vec<u8>>),
    Binary(Option<Vec<u8>>),
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

macro_rules! into {
    ($ty:ty => $variant:ident) => {
        impl<'a> TryInto<PropertyValue> for &'a $ty {
            type Error = PropertyError;
            fn try_into(self) -> Result<PropertyValue, PropertyError> {
                Ok(PropertyValue::$variant(Some(self.clone())))
            }
        }

        impl TryFrom<PropertyValue> for $ty {
            type Error = PropertyError;
            fn try_from(value: PropertyValue) -> Result<$ty, PropertyError> {
                match value {
                    PropertyValue::$variant(value) => match value {
                        Some(value) => Ok(value),
                        None => Err(PropertyError::Missing),
                    },
                    variant => Err(PropertyError::InvalidVariant {
                        given: variant,
                        ty: stringify!($ty).to_owned(),
                    })
                }

            }
        }

        impl<'a> TryInto<PropertyValue> for &'a Option<$ty> {
            type Error = PropertyError;
            fn try_into(self) -> Result<PropertyValue, PropertyError> {
                Ok(PropertyValue::$variant(self.clone()))
            }
        }

        impl TryFrom<PropertyValue> for Option<$ty> {
            type Error = PropertyError;
            fn try_from(value: PropertyValue) -> Result<Option<$ty>, PropertyError> {
                match <$ty>::try_from(value) {
                    Ok(value) => Ok(Some(value)),
                    Err(PropertyError::Missing) => Ok(None),
                    Err(err) => Err(err),
                }
            }
        }
    };
}

into!(String => String);
into!(i16 => I16);
into!(i32 => I32);
into!(i64 => I64);
