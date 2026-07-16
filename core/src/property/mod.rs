pub mod backend;
pub mod traits;
pub mod value;

use ankql::ast::PropertyId;
use ankurah_proto::EntityId;

pub use traits::{FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::{Json, Ref, YrsString};

use crate::value::Value;

pub type PropertyName = String;

/// Read a property from ONE backend by its durable [`PropertyId`]. A backend is
/// a dumb `PropertyId`-keyed store: the entry is authoritative, and an absent
/// (or cleared) entry is absent with NO fallback to a name-keyed slot. The name
/// was resolved to a `PropertyId` upstream, once, when the editable view was
/// built (a backend never touches the catalog), so a display-name fallback here
/// would be exactly the leak the identity model removes. Absent-handling policy
/// (required defaults, `Option` `None`, predicate NULL) belongs to the caller;
/// TYPE policy lives at REGISTRATION (the canonical value_type ruling), never in
/// the read dispatch.
pub fn read_by_id(backend: &dyn backend::PropertyBackend, property_id: &PropertyId) -> Option<Value> {
    backend.entry(property_id).flatten()
}

#[cfg(test)]
mod read_dispatch_tests {
    use super::backend::{LWWBackend, PropertyBackend};
    use super::{read_by_id, PropertyId, Value};
    use ulid::Ulid;

    fn pid(byte: u8) -> PropertyId {
        let mut bytes = [0u8; 16];
        bytes[0] = byte;
        PropertyId::EntityId(Ulid::from_bytes(bytes))
    }

    fn lww() -> LWWBackend { LWWBackend::new() }

    #[test]
    fn returns_present_id_value() {
        let backend = lww();
        backend.set(pid(0x11), Some(Value::String("alpha".into())));
        assert_eq!(read_by_id(&backend, &pid(0x11)), Some(Value::String("alpha".into())));
        // An unwritten property reads absent (the caller then applies its
        // absent policy). Data under OTHER ids is simply not this field: the
        // type question was settled at registration, and there is no read-time
        // gate (the canonical value_type ruling, 2026-07-10).
        assert_eq!(read_by_id(&backend, &pid(0x22)), None);
    }

    #[test]
    fn trait_entry_is_three_way_for_lww() {
        // The generic `entry` contract the dispatch runs on: absent /
        // present-but-cleared / value must stay distinguishable through
        // `&dyn PropertyBackend`. A cleared id is a present `None` tombstone,
        // never absent -- there is no name slot left to resurrect.
        let backend = lww();
        backend.set(pid(0x11), Some(Value::I64(7)));
        backend.set(pid(0x22), None);
        let dyn_backend: &dyn PropertyBackend = &backend;
        assert_eq!(dyn_backend.entry(&pid(0x11)), Some(Some(Value::I64(7))));
        assert_eq!(dyn_backend.entry(&pid(0x22)), Some(None), "tombstone must read present-but-cleared");
        assert_eq!(dyn_backend.entry(&pid(0x33)), None, "absent must read absent");
    }
}

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
into!(Vec<u8> => Binary);

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
