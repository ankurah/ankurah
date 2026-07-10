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
// stages this way is still resolved to its `Id` at commit (decision 27).
impl From<&str> for PropertyKey {
    fn from(name: &str) -> Self { PropertyKey::Name(name.to_string()) }
}
impl From<String> for PropertyKey {
    fn from(name: String) -> Self { PropertyKey::Name(name) }
}

/// Name-to-id resolution for the property read/write paths, implemented by the
/// catalog. It lets the catalog-blind [`crate::entity::Entity`] resolve a
/// display name to the property-definition id its data is keyed under, without
/// any property backend holding schema state: the id-keyed contract is carried
/// by the [`PropertyKey`], not by a binding injected into the backend.
pub trait PropertyResolver: Send + Sync {
    /// The property-definition id for `name` in `collection`, if the catalog
    /// knows it. `None` for an unregistered name (a system or catalog
    /// collection field, or a user field not yet registered), which the caller
    /// keeps as a [`PropertyKey::Name`].
    fn resolve(&self, collection: &str, name: &str) -> Option<EntityId>;
    /// Every live property-definition id sharing display `name` across all
    /// contracts -- the RFC 5.4 sibling-gate input the checked read needs.
    fn siblings(&self, name: &str) -> Vec<EntityId>;
    /// The display name of a property-definition id, if the catalog knows it:
    /// the reverse of [`resolve`](Self::resolve). Storage engines seed their
    /// durable id-to-column-name maps from this at materialization time.
    fn name_for(&self, id: &EntityId) -> Option<String>;
    /// The model-definition id for `collection`, if the catalog knows it --
    /// the wire-envelope egress lookup (#330) for entities, which are
    /// catalog-blind but resolver-stamped. Well-known system/catalog ids are
    /// NOT the resolver's concern (callers consult
    /// `crate::schema::well_known_model_id` first). Defaults to `None` so
    /// test resolvers only implement what they exercise.
    fn model_id_for(&self, collection: &str) -> Option<EntityId> {
        let _ = collection;
        None
    }
}

/// Checked read of a resolved property over an LWW backend: the RFC 5.4 read
/// dispatch, on the catalog-aware side of the backend boundary (the backend
/// itself holds no name-to-id knowledge; it only answers per-key
/// [`backend::LWWBackend::entry`] presence).
///
/// Order: a PRESENT `Id(resolved_id)` entry -- even a cleared `None` tombstone
/// -- is authoritative and never falls back (so an id-keyed clear cannot
/// resurrect a stale legacy name value). Only a truly ABSENT id consults the
/// sibling gate (rule 4: a same-display-name retype lineage holding data fails
/// visible as `TypeSkew` rather than defaulting over it), then the legacy
/// `Name` residue (pre-epoch data not yet re-keyed onto the id), then the
/// FOREIGN-DATA gate: data present under an id `catalog_knows` cannot name at
/// all means this buffer holds another system's allocations (a cross-root
/// raw-state copy, plan decision 15: "different roots means different
/// systems"), and the read cannot prove the absent property is not among that
/// foreign data -- fail visible as `TypeSkew`, never fabricate a default over
/// it. Callers without catalog knowledge (unbound reads) or whose consumer
/// treats absent as NULL by design (resolved-predicate evaluation, plan
/// decision 14) pass `|_| true`, which disables the foreign gate.
pub fn lww_read_checked(
    lww: &backend::LWWBackend,
    resolved_id: EntityId,
    name: &str,
    sibling_ids: &[EntityId],
    catalog_knows: impl Fn(&EntityId) -> bool,
) -> Result<Option<Value>, traits::PropertyError> {
    if let Some(value) = lww.entry(&PropertyKey::Id(resolved_id)) {
        return Ok(value);
    }
    for sibling in sibling_ids {
        if *sibling == resolved_id {
            continue;
        }
        if lww.entry(&PropertyKey::Id(*sibling)).flatten().is_some() {
            return Err(traits::PropertyError::TypeSkew { name: name.to_string(), a: resolved_id.to_base64(), b: sibling.to_base64() });
        }
    }
    if let Some(value) = lww.entry(&PropertyKey::Name(name.to_string())).flatten() {
        return Ok(Some(value));
    }
    for (key, value) in backend::PropertyBackend::property_values(lww) {
        if let PropertyKey::Id(other) = key {
            if other != resolved_id && value.is_some() && !catalog_knows(&other) {
                return Err(traits::PropertyError::TypeSkew { name: name.to_string(), a: resolved_id.to_base64(), b: other.to_base64() });
            }
        }
    }
    Ok(None)
}

/// Lenient counterpart of [`lww_read_checked`]: the same Id-then-legacy-Name
/// presence dispatch, without the sibling gate. Same anti-resurrection rule: a
/// present-but-cleared id entry reads `None` and never falls back to the name.
pub fn lww_read_lenient(lww: &backend::LWWBackend, resolved_id: EntityId, name: &str) -> Option<Value> {
    match lww.entry(&PropertyKey::Id(resolved_id)) {
        Some(value) => value,
        None => lww.entry(&PropertyKey::Name(name.to_string())).flatten(),
    }
}

#[cfg(test)]
mod read_dispatch_tests {
    use super::backend::LWWBackend;
    use super::traits::PropertyError;
    use super::{lww_read_checked, lww_read_lenient, PropertyKey, Value};
    use ankurah_proto::EntityId;

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 16];
        bytes[0] = byte;
        EntityId::from_bytes(bytes)
    }

    #[test]
    fn checked_returns_present_id_value() {
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Id(id(0x11)), Some(Value::String("alpha".into())));
        assert_eq!(lww_read_checked(&backend, id(0x11), "title", &[], |_| true).unwrap(), Some(Value::String("alpha".into())));
        // An unwritten registered id with no sibling reads absent (the caller
        // then fabricates the required default). `|_| true` keeps the
        // foreign-data gate off; the 0x11 data would otherwise trip it.
        assert_eq!(lww_read_checked(&backend, id(0x22), "author", &[], |_| true).unwrap(), None);
    }

    #[test]
    fn checked_sibling_gate_fails_visible() {
        // A same-display-name sibling (retype lineage) holds data under a
        // different id; the resolved id is absent -> TypeSkew, not a default.
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Id(id(0x33)), Some(Value::I64(7)));
        let err = lww_read_checked(&backend, id(0x11), "title", &[id(0x33)], |_| true);
        assert!(matches!(err, Err(PropertyError::TypeSkew { .. })), "sibling with data must skew: {err:?}");
        // A sibling that holds NO data does not skew.
        let empty = LWWBackend::new();
        assert_eq!(lww_read_checked(&empty, id(0x11), "title", &[id(0x33)], |_| true).unwrap(), None);
    }

    #[test]
    fn checked_falls_back_to_name_residue_only_when_id_absent() {
        // Legacy Name residue with no id entry: the id is absent, no sibling ->
        // the bare Name value is read (the legacy fallback).
        let backend = LWWBackend::new();
        backend.set(PropertyKey::name("title"), Some(Value::String("alpha".into())));
        assert_eq!(lww_read_checked(&backend, id(0x11), "title", &[], |_| true).unwrap(), Some(Value::String("alpha".into())));
    }

    #[test]
    fn cleared_id_shadows_name_residue_no_resurrection() {
        // THE map-level-presence invariant: an id-keyed CLEAR (present None
        // tombstone) must shadow a stale legacy Name value, never resurrect
        // it. An `Option`-chained `get(Id).or_else(get(Name))` would return
        // the stale name value here; presence dispatch returns None.
        let backend = LWWBackend::new();
        backend.set(PropertyKey::name("title"), Some(Value::String("stale".into())));
        backend.set(PropertyKey::Id(id(0x11)), None);
        assert_eq!(
            lww_read_checked(&backend, id(0x11), "title", &[], |_| true).unwrap(),
            None,
            "cleared id must not resurrect the stale name"
        );
        assert_eq!(lww_read_lenient(&backend, id(0x11), "title"), None, "lenient read honors the same rule");
    }

    #[test]
    fn checked_foreign_data_gate_fails_visible() {
        // Plan decision 15 under the hint-less regime: the resolved id is
        // absent and the buffer holds DATA under an id the catalog cannot
        // name (another system's allocation, e.g. a cross-root raw-state
        // copy). The bound read cannot prove the absent property is not among
        // that foreign data -> TypeSkew, never a fabricated default.
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Id(id(0x99)), Some(Value::String("foreign".into())));
        let known = [id(0x11), id(0x22)];
        let err = lww_read_checked(&backend, id(0x11), "title", &[], |other| known.contains(other));
        assert!(matches!(err, Err(PropertyError::TypeSkew { .. })), "foreign data must skew: {err:?}");

        // A KNOWN other id (a different property of this entity) does not
        // trip the gate: absent reads absent.
        let mut known_all = known.to_vec();
        known_all.push(id(0x99));
        assert_eq!(lww_read_checked(&backend, id(0x11), "title", &[], |other| known_all.contains(other)).unwrap(), None);

        // A foreign TOMBSTONE (cleared, no data to lose) does not trip it.
        let cleared = LWWBackend::new();
        cleared.set(PropertyKey::Id(id(0x99)), None);
        assert_eq!(lww_read_checked(&cleared, id(0x11), "title", &[], |other| known.contains(other)).unwrap(), None);

        // Name residue still wins over the gate: a real legacy value for THIS
        // name is data, not a fabrication.
        backend.set(PropertyKey::name("title"), Some(Value::String("legacy".into())));
        assert_eq!(
            lww_read_checked(&backend, id(0x11), "title", &[], |other| known.contains(other)).unwrap(),
            Some(Value::String("legacy".into()))
        );
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
    /// IDENTITY-CRITICAL: `#[derive(Model)]` reads this const (at compile
    /// time, via the associated const) for field types outside the built-in
    /// table, and the value feeds the property-id derivation (RFC 5.1) --
    /// two nodes disagreeing on it mint different ids for the same field,
    /// and CHANGING it for a shipped type is a retype (mints a new property
    /// identity, RFC 5.8). Defaults to "string" because the JSON-in-a-string
    /// register is the catch-all serialization (`#[derive(Property)]` pins
    /// it explicitly); a hand-written impl producing any other variant must
    /// override this to match.
    const VALUE_TYPE: &'static str = "string";

    fn into_value(&self) -> Result<Option<Value>, PropertyError>;
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError>;

    /// The value an ABSENT REQUIRED property of this type reads back as under
    /// RFC 5.4 rule 3 (the #175 fix): the value type's default, fed to
    /// [`from_value`] by the compiled projection when the backend holds no
    /// entry AND the sibling gate is clear (see
    /// [`lww_read_checked`](crate::property::lww_read_checked) and `property/value/lww.rs`).
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
