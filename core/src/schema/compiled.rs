//! The local compiled schema: the derive macro's static description of a
//! model and its properties.
//!
//! Rust structs are ONE binding to the catalog, not the definitive schema
//!: `#[derive(Model)]` emits a [`ModelStructDescriptor`] whose
//! `(backend, value_type)` pairs come from the NORMATIVE mapping table
//!. A property's minting model and name locate its identity;
//! registration then checks the compiled pair against the immutable canonical
//! pair (exact backend and a mutually castable value type), refusing an
//! incompatible binding rather than minting another identity. The catalog
//! entities themselves remain the definitive schema; ids exist only there and
//! in registration responses. This type is how a compiled binary names its
//! properties and how it builds a RegisterSchema request.
//!
//! These types are entirely `&'static`: the derive macro emits a `static
//! ModelStructDescriptor` and a `Model::descriptor()` returning `&'static` to it, so
//! there is no per-call allocation and the schema is a `const`-shaped fact
//! of the program.

use super::cell::SchemaOnceCell;
use ankurah_proto::{ModelId, PropertyId, RegisterModel, RegisterProperty};

/// The compiled schema for one model: its registration hints and ordered
/// active (non-ephemeral) fields. It contains no runtime
/// [`ankurah_proto::ModelId`]; that
/// identity is returned by catalog admission. Emitted as a `static` by
/// `#[derive(Model)]`.
#[derive(Debug)]
pub struct ModelStructDescriptor {
    /// The source-level registration label: the lookup key the allocator
    /// files this model under. Not a runtime model identity and not a
    /// physical storage name. Today this is the lowercased struct name
    /// (derive/src/model/description.rs).
    pub label: &'static str,
    /// Display name, initially the struct name (mutable catalog metadata).
    pub name: &'static str,
    /// The active fields, in declaration order. Ephemeral fields are
    /// EXCLUDED (they carry no persisted state and never enter the catalog;
    /// they are struct-only conveniences).
    pub properties: &'static [StructProperty],
    /// `#[model(id = "...")]`: bind this model to a KNOWN model entity by
    /// explicit id, bypassing label-based registration. `None` for
    /// the default label-based registration path. The value is URL-safe base64
    /// of a 32-byte EntityId -- the id that model entity's genesis event
    /// derives -- validated at derive time.
    pub explicit_id: Option<&'static str>,
    /// A random identity for this compiled declaration, minted fresh each
    /// time the derive expands (so it is unique to the containing build).
    /// It rides the registration RPC as a future fallback matching key: a
    /// durable node could keep a supplemental lookup from these to catalog
    /// identities, matching a precompiled binary whose labels have drifted
    /// and which pins no explicit ids. Inoperative today -- the registration
    /// executor ignores it.
    pub build_id: [u8; 16],
    /// The durable model identity this descriptor resolved to, per schema
    /// epoch. Populated by the registration gate; read by everything past
    /// it.
    pub resolved: SchemaOnceCell<ModelId>,
}

/// The compiled schema for one active field of a model. `(backend,
/// value_type)` are the NORMATIVE descriptor pair checked
/// against the property's immutable canonical pair; `target_label`
/// identifies the target of a reference-typed property; `renamed_from` is the
/// transient rename hint; `explicit_id` is a 5.9 shared-property
/// binding.
#[derive(Debug)]
pub struct StructProperty {
    /// The Rust field identifier (as declared).
    pub field: &'static str,
    /// The display name. Equals `field` (lowercased) today; catalog
    /// resolution binds queries against this name, and it is part of the
    /// property lookup key at registration. Engines seed a physical column
    /// name from it (via the catalog) on first materialization, but address
    /// properties by identity, never by this name.
    pub name: &'static str,
    /// `#[property(renamed_from = "...")]`: the transient rename hint. The registration executor applies "a property under this old
    /// name exists on this model -> update its name" before
    /// lookup-or-create, guarded; the attribute is removable once every
    /// target system has seen it.
    pub renamed_from: Option<&'static str>,
    /// Backend registry name, "yrs" or "lww", per the active type the
    /// backend registry resolved for this field.
    pub backend: &'static str,
    /// Language-agnostic value type (a lowercased `core::value::ValueType`
    /// variant, e.g. "string", "i64", "entityid"), taken from the field's
    /// ORIGINAL Rust type before active-type wrapping.
    pub value_type: &'static str,
    /// The referenced model's source label for `Ref<T>` / `Option<Ref<T>>`.
    /// Registration resolves this label to the catalog model id stored as
    /// `target_model`; non-reference fields carry `None`. The field name is
    /// retained for source/API compatibility.
    pub target_label: Option<&'static str>,
    /// `true` for `Option<T>` fields. Feeds the MEMBERSHIP record's
    /// `optional`, NOT the property identity (flipping optionality must not
    /// re-key).
    pub optional: bool,
    /// `#[property(id = "...")]`: bind this field to a KNOWN, possibly
    /// shared, property entity by explicit id. `None` for the
    /// default by-name registration. URL-safe base64 of a 32-byte EntityId --
    /// the id that property entity's genesis event derives -- validated at
    /// derive time.
    pub explicit_id: Option<&'static str>,
    /// A random identity for this compiled field, minted fresh each time
    /// the derive expands (unique to the containing build). Rides the
    /// registration RPC beside the model's `build_id` as a future fallback
    /// matching key; inoperative today.
    pub build_id: [u8; 16],
    /// The durable property identity this field resolved to, per schema
    /// epoch. Populated by the registration gate together with the model's
    /// cell; read by everything past it.
    pub resolved: SchemaOnceCell<PropertyId>,
}

impl ModelStructDescriptor {
    /// The active field whose display name is `name`, if any.
    pub fn field_by_name(&self, name: &str) -> Option<&'static StructProperty> { self.properties.iter().find(|f| f.name == name) }

    /// The durable identity of the field at `index` (declaration order),
    /// resolved under the entity's stamped epoch. This is the read behind
    /// every generated typed accessor: past the registration gate it always
    /// hits, and a miss (an entity materialized before any system was ready,
    /// or a handle that outlived a reset) is a mechanical resolution error,
    /// never a wrong identity.
    pub fn resolved_field(
        &'static self,
        index: usize,
        entity: &crate::entity::Entity,
    ) -> Result<PropertyId, crate::property::PropertyError> {
        self.resolved_field_at(index, entity.schema_epoch())
    }

    /// The durable identity of the field at `index`, resolved under a
    /// caller-held epoch. The create path uses this before any entity
    /// exists (initial values are staged into a vessel with no identity).
    pub fn resolved_field_at(&'static self, index: usize, epoch: super::SchemaEpoch) -> Result<PropertyId, crate::property::PropertyError> {
        let field = &self.properties[index];
        field.resolved.get(epoch).ok_or(crate::property::PropertyError::Unresolved { model: self.label, field: field.field })
    }
}

/// Convert a compiled struct descriptor into the RegisterSchema request
/// entry it declares: one model with its properties nested (position is the
/// membership assertion; explicit ids ride as bindings). The request is
/// id-free except those explicit bindings -- ids are the executor's to
/// allocate or resolve, so a request is portable across systems.
impl From<&ModelStructDescriptor> for RegisterModel {
    fn from(schema: &ModelStructDescriptor) -> Self {
        RegisterModel {
            label: schema.label.to_string(),
            name: schema.name.to_string(),
            explicit_id: schema.explicit_id.map(parse_explicit_id),
            build_id: schema.build_id,
            properties: schema
                .properties
                .iter()
                .map(|field| RegisterProperty {
                    name: field.name.to_string(),
                    renamed_from: field.renamed_from.map(|s| s.to_string()),
                    backend: field.backend.to_string(),
                    value_type: field.value_type.to_string(),
                    target_label: field.target_label.map(str::to_string),
                    explicit_id: field.explicit_id.map(parse_explicit_id),
                    build_id: field.build_id,
                    optional: field.optional,
                })
                .collect(),
        }
    }
}

/// Decode an explicit-id attribute value into an `EntityId`. The derive
/// macro already validated the shape at compile time (URL-safe base64 of 16
/// bytes), so a malformed value here is a bug in that validation, not user
/// error; hence the panic carries the offending string.
pub(crate) fn parse_explicit_id(s: &str) -> ankurah_proto::EntityId {
    ankurah_proto::EntityId::from_base64(s).unwrap_or_else(|e| panic!("derive macro emitted an invalid explicit id {s:?}: {e}"))
}
