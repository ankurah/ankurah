pub mod backend;
pub mod traits;
pub mod value;

use ankurah_proto::EntityId;

pub use traits::{ActiveType, FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::{Json, Ref, YrsString};

use crate::value::Value;

pub use ankurah_proto::PropertyId;

pub trait Property: Sized {
    /// The catalog value type this Rust type serializes to: the name of the
    /// `Value` variant [`Self::into_value`] produces ("string", "i64",
    /// "entityid", ...). Registration records it as the property's canonical
    /// type on first allocation and checks later declarations against it, so
    /// it must tell the truth about the wire representation. No default:
    /// every impl declares its own (`#[derive(Property)]` emits "string" for
    /// its JSON-string serialization).
    const VALUE_TYPE: &'static str;

    fn into_value(&self) -> Result<Option<Value>, PropertyError>;
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError>;
}

impl<T> Property for Option<T>
where T: Property
{
    const VALUE_TYPE: &'static str = T::VALUE_TYPE;

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

macro_rules! impl_property {
    ($ty:ty => $variant:ident, $value_type:literal) => {
        impl Property for $ty {
            const VALUE_TYPE: &'static str = $value_type;

            fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::$variant(self.clone()))) }
            fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
                match value {
                    Some(Value::$variant(value)) => Ok(value),
                    Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
                    None => Err(PropertyError::Missing),
                }
            }
        }
    };
}

impl_property!(String => String, "string");
impl_property!(i16 => I16, "i16");
impl_property!(i32 => I32, "i32");
impl_property!(i64 => I64, "i64");
impl_property!(f64 => F64, "f64");
impl_property!(bool => Bool, "bool");
impl_property!(EntityId => EntityId, "entityid");
impl_property!(Vec<u8> => Binary, "binary");

impl<'a> Property for std::borrow::Cow<'a, str> {
    const VALUE_TYPE: &'static str = "string";

    fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::String(self.to_string()))) }

    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
        match value {
            Some(Value::String(value)) => Ok(value.into()),
            Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
            None => Err(PropertyError::Missing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::backend::{LWWBackend, PropertyBackend, YrsBackend};
    use super::value::{YrsString, LWW};
    use super::ActiveType;

    /// The active types' declared backend names must match the names those
    /// backends register at runtime; a mismatch would compile descriptors
    /// naming a backend the registry cannot construct.
    #[test]
    fn active_type_backend_names_match_the_registry() {
        assert_eq!(<LWW<String> as ActiveType>::BACKEND, LWWBackend::property_backend_name());
        assert_eq!(<YrsString<String> as ActiveType>::BACKEND, YrsBackend::property_backend_name());
    }
}
