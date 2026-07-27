//! The RegisterSchema vocabulary: what a binary sends to declare its models
//! (`RegisterModel` / `RegisterProperty`) and what the allocator returns
//! (`RegisteredModel` / `RegisteredProperty`).
//!
//! A request nests properties inside their model, and the nesting IS the
//! membership assertion: each entry binds that property to that model, with
//! `optional` riding the entry. There is no separate membership item and no
//! way to express a dangling one. The response mirrors the nesting with
//! every id resolved: the requester folds one tree.

use serde::{Deserialize, Serialize};

use ankurah_core_types::{UniqueFieldId, UniqueStructId};

use crate::id::EntityId;

/// One model declaration within a RegisterSchema request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterModel {
    /// The registration lookup label the allocator files this model under
    /// (today: the lowercased struct name). Not a runtime identity and not
    /// a physical storage name.
    pub label: String,
    /// Display name seed (initially the struct name); mutable catalog
    /// metadata thereafter.
    pub name: String,
    /// Explicit binding: reference an EXISTING model entity instead of
    /// looking one up by label. Never mints; hard-fails if absent or if the
    /// bound entity's label differs.
    pub explicit_id: Option<EntityId>,
    /// The declaring struct's deterministic source identity, hashed from its
    /// names at compile time. An identity hint for future migrations: the
    /// executor accepts and ignores it today. `None` for registrations with
    /// no compile-time source (dynamic or non-Rust declarations); the derive
    /// always supplies it.
    pub unique_id: Option<UniqueStructId>,
    /// The model's properties, in declaration order. Each entry asserts a
    /// model-property membership; an entry with an explicit id references an
    /// existing (possibly shared) property and never mints.
    pub properties: Vec<RegisterProperty>,
}

/// One property entry within a [`RegisterModel`]. Language-agnostic:
/// `backend` and `value_type` follow the normative mapping table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterProperty {
    /// Current display name; the lookup key within the model's membership
    /// set.
    pub name: String,
    /// The name this property had before you renamed the field. Lets the
    /// executor find the existing property under its old name and move it to
    /// the new one, instead of minting a duplicate under the new name. Only
    /// consulted when the new name misses and the old name hits, so it is
    /// harmless to leave in place (it does nothing once applied) and safe to
    /// delete from source once every system has seen the rename.
    pub renamed_from: Option<String>,
    /// Backend registry name, e.g. "lww", "yrs".
    pub backend: String,
    /// Language-agnostic value type, e.g. "string", "i64".
    pub value_type: String,
    /// For reference-typed properties: the target model, named by its
    /// registration label. The executor resolves it against the catalog,
    /// allocating the target model entity on miss. Mutable metadata, not
    /// identity.
    pub target_label: Option<String>,
    /// Explicit binding: reference an EXISTING property entity instead of
    /// looking one up by name. Never mints; hard-fails if absent.
    pub explicit_id: Option<EntityId>,
    /// The declaring field's deterministic source identity, hashed from its
    /// names at compile time. An identity hint for future migrations: the
    /// executor accepts and ignores it today. `None` for registrations with
    /// no compile-time source; the derive always supplies it.
    pub unique_id: Option<UniqueFieldId>,
    /// Whether entities of this model may omit the property. Per membership:
    /// the same property may be required in one model and optional in
    /// another.
    pub optional: bool,
}

/// A resolved model, as returned by `SchemaRegistered`: the allocated (or
/// existing) entity id plus the definition state the catalog now holds,
/// with the model's resolved properties nested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredModel {
    /// The model's allocated or previously registered catalog entity id.
    pub id: EntityId,
    /// The registration lookup label.
    pub label: String,
    /// The model's current registered display name.
    pub name: String,
    /// The resolved properties this request asserted for the model. May be
    /// empty for models the executor allocated as reference targets.
    pub properties: Vec<RegisteredProperty>,
}

/// A resolved property within a [`RegisteredModel`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredProperty {
    /// The property's allocated or previously registered catalog entity id.
    pub id: EntityId,
    /// The membership entity binding this property to the parent model.
    pub membership_id: EntityId,
    /// The property's current registered display name.
    pub name: String,
    /// The property's canonical state-backend identifier.
    pub backend: String,
    /// The property's canonical logical value-type spelling.
    pub value_type: String,
    /// Resolved target model id for reference-typed properties.
    pub target_model: Option<EntityId>,
    /// Provenance: the model in whose scope the property was originally
    /// minted. Differs from the parent model when the entry shares an
    /// explicitly bound property minted elsewhere.
    pub minted_for: Option<EntityId>,
    /// Whether this property is optional in the parent model.
    pub optional: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The optional unique-id hints survive the serialized encoding in both
    /// states. The in-process connection the registration suite runs over
    /// passes structs in memory, so the encoded form is exercised here.
    #[test]
    fn unique_id_hints_round_trip_the_encoding() {
        for (model_hint, field_hint) in [
            (
                Some(UniqueStructId::from_names("some_app::records", "Album")),
                Some(UniqueFieldId::from_names("some_app::records", "Album", "name")),
            ),
            (None, None),
        ] {
            let model = RegisterModel {
                label: "album".into(),
                name: "Album".into(),
                explicit_id: None,
                unique_id: model_hint,
                properties: vec![RegisterProperty {
                    name: "name".into(),
                    renamed_from: None,
                    backend: "yrs".into(),
                    value_type: "string".into(),
                    target_label: None,
                    explicit_id: None,
                    unique_id: field_hint,
                    optional: false,
                }],
            };
            let decoded: RegisterModel = bincode::deserialize(&bincode::serialize(&model).unwrap()).unwrap();
            assert_eq!(decoded.unique_id, model_hint);
            assert_eq!(decoded.properties[0].unique_id, field_hint);
            assert_eq!((decoded.label, decoded.name), (model.label, model.name));
        }
    }
}
