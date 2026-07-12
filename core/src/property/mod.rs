pub mod backend;
pub mod traits;
pub mod value;

use ankurah_proto::EntityId;
use serde::{Deserialize, Serialize};

pub use traits::{FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::{Json, Ref, YrsString};

use crate::value::Value;

/// A property display name (system/catalog collections address by this, and
/// derive-generated field accessors carry it before the write path resolves it
/// to an id).
pub type PropertyName = String;

/// The address a derive-generated accessor uses for one compiled field.
///
/// Most fields are name-addressed at the synchronous API boundary and are
/// resolved through the live catalog before commit. An explicit
/// `#[property(id = "...")]` binding is different: the literal id is the
/// field's identity even when its local Rust name differs from the catalog's
/// current display name. Keeping both pieces lets reads retain the legacy
/// name-residue fallback while writes, listeners, and new CRDT roots use the
/// explicit id directly.
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

/// How a property is keyed inside a property backend and on the wire.
///
/// A registered user-collection property is keyed by its property-definition
/// id -- the normal case for all user data. A system or catalog collection
/// property, or a legacy pre-epoch value not yet resolved, is keyed by its
/// display name. A key is `Id` or `Name`, never both (RFC 5.5 in
/// specs/model-property-metadata/rfc.md, and the ratified PropertyKey
/// amendment on #289). Variant order is Id-then-Name; it carries no semantics
/// beyond deterministic `BTreeMap` ordering.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PropertyKey {
    Id(EntityId),
    Name(String),
}

impl PropertyKey {
    /// A name-keyed property: a system/catalog collection field, or a transient
    /// uncommitted user write before the commit path resolves it to an id.
    pub fn name(name: impl Into<String>) -> Self { PropertyKey::Name(name.into()) }

    /// A display string for diagnostics and value dumps. An id key renders as
    /// its base64 id; a name key renders as-is. NOT a storage column name:
    /// engines name columns through their durable id-to-name maps, seeded from
    /// the catalog resolver (never from this rendering).
    pub fn display_name(&self) -> String {
        match self {
            PropertyKey::Id(id) => id.to_base64(),
            PropertyKey::Name(name) => name.clone(),
        }
    }
}

// A bare string IS a display name, so it keys as `Name`. This is safe against
// the id-keyed invariant: an uncommitted `Name` key a user-collection write
// stages this way is still resolved to its `Id` before commit serialization.
impl From<&str> for PropertyKey {
    fn from(name: &str) -> Self { PropertyKey::Name(name.to_string()) }
}
impl From<String> for PropertyKey {
    fn from(name: String) -> Self { PropertyKey::Name(name) }
}

/// Read a resolved property from ONE backend: the read dispatch (rfc.md 5.4
/// as amended 2026-07-10), on the catalog-aware side of the backend boundary.
/// Generic over [`backend::PropertyBackend::entry`] -- no caller may downcast
/// to a concrete backend for a read.
///
/// Order: a PRESENT `Id(resolved_id)` entry -- even a cleared `None` tombstone
/// -- is authoritative and never falls back (an id-keyed clear cannot
/// resurrect a stale legacy name value); then legacy pre-epoch `Name` residue;
/// then absent. Absent-handling policy (required defaults, `Option` `None`,
/// predicate NULL) belongs to the caller. TYPE policy does not live here at
/// all: the compiled/canonical type pair is admitted at REGISTRATION (the
/// canonical value_type ruling), and a per-value cast failure surfaces where
/// a value is staged or projected (`PropertyError::NonCastable`), never as a
/// read-dispatch concern.
pub fn read_resolved(backend: &dyn backend::PropertyBackend, resolved_id: EntityId, name: &str) -> Option<Value> {
    match backend.entry(&PropertyKey::Id(resolved_id)) {
        Some(value) => value,
        None => backend.entry(&PropertyKey::Name(name.to_string())).flatten(),
    }
}

#[cfg(test)]
mod read_dispatch_tests {
    use super::backend::{LWWBackend, PropertyBackend};
    use super::{read_resolved, PropertyKey, Value};
    use ankurah_proto::EntityId;

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 16];
        bytes[0] = byte;
        EntityId::from_bytes(bytes)
    }

    fn lww() -> LWWBackend { LWWBackend::new() }

    #[test]
    fn returns_present_id_value() {
        let backend = lww();
        backend.set(PropertyKey::Id(id(0x11)), Some(Value::String("alpha".into())));
        assert_eq!(read_resolved(&backend, id(0x11), "title"), Some(Value::String("alpha".into())));
        // An unwritten registered id reads absent (the caller then applies
        // its absent policy). Data under OTHER ids is simply not this field:
        // the type question was settled at registration, and there is no
        // read-time gate (the canonical value_type ruling, 2026-07-10).
        assert_eq!(read_resolved(&backend, id(0x22), "author"), None);
    }

    #[test]
    fn falls_back_to_name_residue_only_when_id_absent() {
        // Legacy Name residue with no id entry: the id is absent -> the bare
        // Name value is read (the legacy fallback).
        let backend = lww();
        backend.set(PropertyKey::name("title"), Some(Value::String("alpha".into())));
        assert_eq!(read_resolved(&backend, id(0x11), "title"), Some(Value::String("alpha".into())));
    }

    #[test]
    fn cleared_id_shadows_name_residue_no_resurrection() {
        // THE map-level-presence invariant: an id-keyed CLEAR (present None
        // tombstone) must shadow a stale legacy Name value, never resurrect
        // it. An `Option`-chained `get(Id).or_else(get(Name))` would return
        // the stale name value here; presence dispatch returns None. This is
        // exactly why `PropertyBackend::entry` is three-way.
        let backend = lww();
        backend.set(PropertyKey::name("title"), Some(Value::String("stale".into())));
        backend.set(PropertyKey::Id(id(0x11)), None);
        assert_eq!(read_resolved(&backend, id(0x11), "title"), None, "cleared id must not resurrect the stale name");
    }

    #[test]
    fn trait_entry_is_three_way_for_lww() {
        // The generic `entry` contract the dispatch runs on: absent /
        // present-but-cleared / value must stay distinguishable through
        // `&dyn PropertyBackend`.
        let backend = lww();
        backend.set(PropertyKey::Id(id(0x11)), Some(Value::I64(7)));
        backend.set(PropertyKey::Id(id(0x22)), None);
        let dyn_backend: &dyn PropertyBackend = &backend;
        assert_eq!(dyn_backend.entry(&PropertyKey::Id(id(0x11))), Some(Some(Value::I64(7))));
        assert_eq!(dyn_backend.entry(&PropertyKey::Id(id(0x22))), Some(None), "tombstone must read present-but-cleared");
        assert_eq!(dyn_backend.entry(&PropertyKey::Id(id(0x33))), None, "absent must read absent");
    }
}

/// Collapse a backend's [`PropertyKey`]-keyed values to display-name-keyed
/// values, for the name-keyed system and catalog collections (RFC section 4
/// bootstrap exemption). Id keys, which those collections never hold, are
/// dropped.
pub(crate) fn name_keyed(
    values: std::collections::BTreeMap<PropertyKey, Option<Value>>,
) -> std::collections::BTreeMap<String, Option<Value>> {
    values
        .into_iter()
        .filter_map(|(key, value)| match key {
            PropertyKey::Name(name) => Some((name, value)),
            PropertyKey::Id(_) => None,
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
    /// entry (see [`read_resolved`](crate::property::read_resolved) and
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
