pub mod backend;
pub mod traits;
pub mod value;

use ankql::ast::PropertyId;
use ankurah_proto::EntityId;

pub use traits::{FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::{Json, Ref, YrsString};

use crate::value::Value;

/// A property display name: the pre-resolution address a derive-generated
/// accessor carries until the editable view resolves it, once, to a
/// [`PropertyId`] (system/catalog collections have no resolver, so they stay
/// [`PropertyId::System`] by this name).
pub type PropertyName = String;

/// The address a derive-generated accessor uses for one compiled field, before
/// resolution.
///
/// Most fields are name-addressed here and are resolved to a [`PropertyId`] once
/// at editable-view construction (`from_entity`). An explicit
/// `#[property(id = "...")]` binding is different: the literal id is the field's
/// identity even when its local Rust name differs from the catalog's current
/// display name, so the accessor takes it directly without a catalog lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PropertyAddress {
    pub name: PropertyName,
    pub explicit_id: Option<EntityId>,
}

impl PropertyAddress {
    pub fn new(name: impl Into<PropertyName>, explicit_id: Option<EntityId>) -> Self { Self { name: name.into(), explicit_id } }

    pub fn named(name: impl Into<PropertyName>) -> Self { Self::new(name, None) }
}

impl From<String> for PropertyAddress {
    fn from(name: String) -> Self { Self::named(name) }
}

impl From<&str> for PropertyAddress {
    fn from(name: &str) -> Self { Self::named(name) }
}

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

/// Collapse a backend's [`PropertyId`]-keyed values to display-name-keyed
/// values, for the name-keyed system and catalog collections (RFC section 4
/// bootstrap exemption), which key every field as [`PropertyId::System`].
/// Registered `EntityId` keys, which those collections never hold, are dropped.
pub(crate) fn name_keyed(
    values: std::collections::BTreeMap<PropertyId, Option<Value>>,
) -> std::collections::BTreeMap<String, Option<Value>> {
    values
        .into_iter()
        .filter_map(|(key, value)| match key {
            PropertyId::System { name } => Some((name, value)),
            PropertyId::EntityId(_) | PropertyId::Id => None,
        })
        .collect()
}

pub trait Property: Sized {
    /// The NORMATIVE catalog value_type this Rust type carries on the wire: a
    /// lowercased `core::value::ValueType` variant name ("string", "i64",
    /// "entityid", ...) that MUST equal the `Value` variant [`into_value`]
    /// actually produces (RFC 4 in specs/model-property-metadata/rfc.md mapping table).
    ///
    /// CONTRACT-CRITICAL: `#[derive(Model)]` reads this const (at compile
    /// time, via the associated const) for field types outside the built-in
    /// table, and the value is what registration declares against the
    /// property's CANONICAL value_type (rfc.md 5.6 as amended 2026-07-10):
    /// the canonical type is fixed at first allocation and never changed by
    /// registration; a binary declaring a different but mutually castable
    /// type writes and reads through `Value::cast_to`, and a non-castable
    /// declaration refuses registration loudly. Changing a canonical type is
    /// a deliberate migration (#303), never a code edit. Defaults to "string"
    /// because the JSON-in-a-string register is the catch-all serialization
    /// (`#[derive(Property)]` pins it explicitly); a hand-written impl
    /// producing any other variant must override this to match.
    const VALUE_TYPE: &'static str = "string";

    fn into_value(&self) -> Result<Option<Value>, PropertyError>;
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError>;

    /// The value an ABSENT REQUIRED property of this type reads back as under
    /// RFC 5.4 rule 3 (the #175 fix): the value type's default, fed to
    /// [`from_value`] by the compiled projection when the backend holds no
    /// entry (see [`read_by_id`](crate::property::read_by_id) and
    /// `property/value/lww.rs`).
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
    // Optionality is a membership fact, not a value-type fact (RFC 4): the
    // wire type is the inner type's.
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
    // Inherit the trait default (`None`): an absent optional reads as `None`,
    // never a fabricated inner default (RFC 5.4 rule 2).
}

/// Property types with a fabricable required-absent default (RFC 5.4 rule 3).
macro_rules! into {
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
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self { Value::$variant(value) }
        }
    };
}

into!(String => String, "string");
into!(i16 => I16, "i16");
into!(i32 => I32, "i32");
into!(i64 => I64, "i64");
into!(f64 => F64, "f64");
into!(bool => Bool, "bool");
into!(Vec<u8> => Binary, "binary");
// EntityId has no zero value to fabricate: absent required stays Missing.
into_no_default!(EntityId => EntityId, "entityid");

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

impl From<&str> for Value {
    fn from(value: &str) -> Self { Value::String(value.to_string()) }
}
