//! The local compiled schema: the derive macro's static description of a
//! model and its properties (specs/model-property-metadata/rfc.md sections 4,
//! 5.8, 5.9, and section 7's
//! reconciliation of the phase-3 SchemaRegistry).
//!
//! Rust structs are ONE binding to the catalog, not the definitive schema
//! (RFC section 3): `#[derive(Model)]` emits a [`ModelSchema`] whose
//! `(backend, value_type)` pairs come from the NORMATIVE mapping table
//! (RFC section 4). A property's minting model and name locate its identity;
//! registration then checks the compiled pair against the immutable canonical
//! pair (exact backend and a mutually castable value type), refusing an
//! incompatible binding rather than minting another identity. The catalog
//! entities themselves remain the definitive schema; ids exist only there and
//! in registration responses. This type is how a compiled binary names its
//! properties and how it builds a RegisterSchema request.
//!
//! These types are entirely `&'static`: the derive macro emits a `static
//! ModelSchema` and a `Model::schema()` returning `&'static` to it, so
//! there is no per-call allocation and the schema is a `const`-shaped fact
//! of the program.

/// The compiled schema for one model: its collection binding, display name,
/// and the ordered active (non-ephemeral) fields. Emitted as a `static` by
/// `#[derive(Model)]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelSchema {
    /// The collection this model's entities live in (the model lookup key
    /// at registration; RFC 4, 5.1). Today this is the lowercased struct
    /// name (derive/src/model/description.rs).
    pub collection: &'static str,
    /// Display name, initially the struct name (mutable catalog metadata).
    pub name: &'static str,
    /// The active fields, in declaration order. Ephemeral fields are
    /// EXCLUDED (they carry no persisted state and never enter the catalog;
    /// RFC 5.2, derive description split).
    pub properties: &'static [FieldSchema],
    /// `#[model(id = "...")]`: bind this model to a KNOWN model entity by
    /// explicit id (RFC 5.9), bypassing by-collection registration. `None`
    /// for the default by-name/by-collection registration path. The value is
    /// URL-safe base64 of a 16-byte EntityId, validated at derive time.
    pub explicit_id: Option<&'static str>,
}

/// The compiled schema for one active field of a model. `(backend,
/// value_type)` are the NORMATIVE descriptor pair (RFC 4 table) checked
/// against the property's immutable canonical pair; `target_collection`
/// identifies the target of a reference-typed property; `renamed_from` is the
/// transient rename hint (RFC 5.8); `explicit_id` is a 5.9 shared-property
/// binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldSchema {
    /// The Rust field identifier (as declared).
    pub field: &'static str,
    /// The display name. Equals `field` (lowercased) today; catalog
    /// resolution binds queries against this name, and it is part of the
    /// property lookup key at registration. Engines seed a physical column
    /// name from it (via the catalog) on first materialization, but address
    /// properties by identity, never by this name.
    pub name: &'static str,
    /// `#[property(renamed_from = "...")]`: the transient rename hint (RFC
    /// 5.8). The registration executor applies "a property under this old
    /// name exists on this model -> update its name" before
    /// lookup-or-create, guarded; the attribute is removable once every
    /// target system has seen it.
    pub renamed_from: Option<&'static str>,
    /// Backend registry name, "yrs" or "lww", per the active type the
    /// backend registry resolved for this field (RFC 4).
    pub backend: &'static str,
    /// Language-agnostic value type (a lowercased `core::value::ValueType`
    /// variant, e.g. "string", "i64", "entityid"), taken from the field's
    /// ORIGINAL Rust type before active-type wrapping (RFC 4 table).
    pub value_type: &'static str,
    /// The referenced model's collection for `Ref<T>` / `Option<Ref<T>>`.
    /// Registration resolves this collection to the catalog model id stored
    /// as `target_model`; non-reference fields carry `None`.
    pub target_collection: Option<&'static str>,
    /// `true` for `Option<T>` fields. Feeds the MEMBERSHIP record's
    /// `optional`, NOT the property identity (flipping optionality must not
    /// re-key; RFC 4).
    pub optional: bool,
    /// `#[property(id = "...")]`: bind this field to a KNOWN, possibly
    /// shared, property entity by explicit id (RFC 5.9). `None` for the
    /// default by-name registration. URL-safe base64 of a 16-byte EntityId,
    /// validated at derive time.
    pub explicit_id: Option<&'static str>,
}

impl ModelSchema {
    /// The active field whose display name is `name`, if any.
    pub fn field_by_name(&self, name: &str) -> Option<&'static FieldSchema> { self.properties.iter().find(|f| f.name == name) }
}

/// Decode an explicit-id attribute value into an `EntityId`. The derive
/// macro already validated the shape at compile time (URL-safe base64 of 16
/// bytes), so a malformed value here is a bug in that validation, not user
/// error; hence the panic carries the offending string.
pub(crate) fn parse_explicit_id(s: &str) -> ankurah_proto::EntityId {
    ankurah_proto::EntityId::from_base64(s).unwrap_or_else(|e| panic!("derive macro emitted an invalid explicit id {s:?}: {e}"))
}
