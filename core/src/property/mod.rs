pub mod backend;
pub mod traits;
pub mod value;

use std::fmt::Display;

use ankurah_proto::EntityId;

pub use traits::{FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::YrsString;

use serde::{Deserialize, Serialize};

pub type PropertyName = String;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PropertyValue {
    // Numbers
    I16(i16),
    I32(i32),
    I64(i64),

    Bool(bool),
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
            PropertyValue::Bool(bool) => write!(f, "{:?}", bool),
            PropertyValue::String(string) => write!(f, "{:?}", string),
            PropertyValue::Object(object) => write!(f, "{:?}", object),
            PropertyValue::Binary(binary) => write!(f, "{:?}", binary),
        }
    }
}

pub trait Property: Sized {
    fn into_value(&self) -> Result<Option<PropertyValue>, PropertyError>;
    fn from_value(value: Option<PropertyValue>) -> Result<Self, PropertyError>;
}

impl<T> Property for Option<T>
where T: Property
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
            fn into_value(&self) -> Result<Option<PropertyValue>, PropertyError> { Ok(Some(PropertyValue::$variant(self.clone()))) }
            fn from_value(value: Option<PropertyValue>) -> Result<Self, PropertyError> {
                match value {
                    Some(PropertyValue::$variant(value)) => Ok(value),
                    Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
                    None => Err(PropertyError::Missing),
                }
            }
        }
        impl From<$ty> for PropertyValue {
            fn from(value: $ty) -> Self { PropertyValue::$variant(value) }
        }
    };
}

into!(String => String);
into!(i16 => I16);
into!(i32 => I32);
into!(i64 => I64);
into!(bool => Bool);

impl<'a> Property for std::borrow::Cow<'a, str> {
    fn into_value(&self) -> Result<Option<PropertyValue>, PropertyError> { Ok(Some(PropertyValue::String(self.to_string()))) }

    fn from_value(value: Option<PropertyValue>) -> Result<Self, PropertyError> {
        match value {
            Some(PropertyValue::String(value)) => Ok(value.into()),
            Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
            None => Err(PropertyError::Missing),
        }
    }
}

impl Property for EntityId {
    fn into_value(&self) -> Result<Option<PropertyValue>, PropertyError> { Ok(Some(PropertyValue::String(self.to_base64()))) }
    fn from_value(value: Option<PropertyValue>) -> Result<Self, PropertyError> {
        match value {
            Some(PropertyValue::String(value)) => Ok(EntityId::from_base64(&value).unwrap()),
            Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
            None => Err(PropertyError::Missing),
        }
    }
}

impl From<&str> for PropertyValue {
    fn from(value: &str) -> Self { PropertyValue::String(value.to_string()) }
}

// WASM JsValue conversions for IndexedDB
#[cfg(feature = "wasm")]
mod wasm_conversions {
    use super::PropertyValue;
    use wasm_bindgen::{JsCast, JsValue};

    impl From<PropertyValue> for JsValue {
        fn from(value: PropertyValue) -> Self {
            match value {
                PropertyValue::String(s) => JsValue::from_str(&s),
                PropertyValue::I16(i) => JsValue::from_f64(i as f64),
                PropertyValue::I32(i) => JsValue::from_f64(i as f64),
                PropertyValue::I64(i) => JsValue::from_f64(i as f64),
                PropertyValue::Bool(b) => JsValue::from_bool(b),
                PropertyValue::Object(bytes) => js_sys::Uint8Array::from(&bytes[..]).into(),
                PropertyValue::Binary(bytes) => js_sys::Uint8Array::from(&bytes[..]).into(),
            }
        }
    }

    impl From<&PropertyValue> for JsValue {
        fn from(value: &PropertyValue) -> Self {
            match value {
                PropertyValue::String(s) => JsValue::from_str(s),
                PropertyValue::I16(i) => JsValue::from_f64(*i as f64),
                PropertyValue::I32(i) => JsValue::from_f64(*i as f64),
                PropertyValue::I64(i) => JsValue::from_f64(*i as f64),
                PropertyValue::Bool(b) => JsValue::from_bool(*b),
                PropertyValue::Object(bytes) => js_sys::Uint8Array::from(&bytes[..]).into(),
                PropertyValue::Binary(bytes) => js_sys::Uint8Array::from(&bytes[..]).into(),
            }
        }
    }

    impl TryFrom<JsValue> for PropertyValue {
        type Error = JsValue;

        fn try_from(value: JsValue) -> Result<Self, Self::Error> {
            if value.is_null() || value.is_undefined() {
                return Err(value);
            }

            // Try string first
            if let Some(s) = value.as_string() {
                return Ok(PropertyValue::String(s));
            }

            // Try boolean
            if let Some(b) = value.as_bool() {
                return Ok(PropertyValue::Bool(b));
            }

            // Try number
            if let Some(n) = value.as_f64() {
                // Try to fit in smaller integer types first
                if n.fract() == 0.0 {
                    let n_int = n as i64;
                    if n_int >= i16::MIN as i64 && n_int <= i16::MAX as i64 {
                        return Ok(PropertyValue::I16(n_int as i16));
                    } else if n_int >= i32::MIN as i64 && n_int <= i32::MAX as i64 {
                        return Ok(PropertyValue::I32(n_int as i32));
                    } else {
                        return Ok(PropertyValue::I64(n_int));
                    }
                }
            }

            // Try Uint8Array (for binary data)
            if value.is_instance_of::<js_sys::Uint8Array>() {
                let array: js_sys::Uint8Array = value.unchecked_into();
                let mut bytes = vec![0u8; array.length() as usize];
                array.copy_to(&mut bytes);
                return Ok(PropertyValue::Binary(bytes));
            }

            // If nothing matches, return error
            Err(value)
        }
    }
}
