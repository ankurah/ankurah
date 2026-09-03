//! Static model declarations and their catalog bindings.

use super::cell::SchemaOnceCell;
use ankurah_proto::{ModelId, PropertyId, RegisterModel, RegisterProperty, SystemModel};

/// One model's registration metadata and resolved identities.
#[derive(Debug)]
pub struct ModelStructDescriptor {
    /// Source-level registration label, currently the lowercased struct name.
    pub label: &'static str,
    /// Display name, initially the struct name (mutable catalog metadata).
    pub name: &'static str,
    /// Active persisted fields in declaration order.
    pub properties: &'static [StructProperty],
    /// Built-in model identity; `None` for ordinary registered models.
    pub system: Option<SystemModel>,
    /// Explicit existing model binding, encoded as a 32-byte EntityId.
    pub explicit_id: Option<&'static str>,
    /// Per-build declaration identity, reserved as a future fallback key.
    pub build_id: [u8; 16],
    /// Durable model identity resolved for each schema epoch.
    pub resolved: SchemaOnceCell<ModelId>,
}

/// One active field's registration metadata and resolved identity.
#[derive(Debug)]
pub struct StructProperty {
    /// The Rust field identifier (as declared).
    pub field: &'static str,
    /// Display and registration name, currently equal to `field`.
    pub name: &'static str,
    /// Transient hint for finding this property under its previous name.
    pub renamed_from: Option<&'static str>,
    /// Backend registry name, such as `yrs` or `lww`.
    pub backend: &'static str,
    /// Language-independent value type derived from the original Rust type.
    pub value_type: &'static str,
    /// Target model label for reference-typed fields.
    pub target_label: Option<&'static str>,
    /// Whether this model-property membership is optional.
    pub optional: bool,
    /// Explicit existing property binding, possibly shared across models.
    pub explicit_id: Option<&'static str>,
    /// Per-build correlator echoed by the registration response.
    pub build_id: [u8; 16],
    /// Durable property identity resolved for each schema epoch.
    pub resolved: SchemaOnceCell<PropertyId>,
}

impl ModelStructDescriptor {
    /// The active field whose display name is `name`, if any.
    pub fn field_by_name(&self, name: &str) -> Option<&'static StructProperty> { self.properties.iter().find(|f| f.name == name) }

    /// Resolve a field by index under the entity's stamped epoch.
    pub fn resolved_field(
        &'static self,
        index: usize,
        entity: &crate::entity::Entity,
    ) -> Result<PropertyId, crate::property::PropertyError> {
        entity
            .with_current_schema_epoch(|epoch| self.resolved_field_at(index, epoch))
            .unwrap_or_else(|| Err(crate::property::PropertyError::Unresolved { model: self.label, field: self.properties[index].field }))
    }

    /// Resolve a field by index under an explicitly held epoch.
    pub fn resolved_field_at(&'static self, index: usize, epoch: super::SchemaEpoch) -> Result<PropertyId, crate::property::PropertyError> {
        let field = &self.properties[index];
        field.resolved.get(epoch).ok_or(crate::property::PropertyError::Unresolved { model: self.label, field: field.field })
    }
}

/// Build the portable registration request for a compiled descriptor.
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

/// Decode an explicit id already validated by the derive macro.
pub(crate) fn parse_explicit_id(s: &str) -> ankurah_proto::EntityId {
    ankurah_proto::EntityId::from_base64(s).unwrap_or_else(|e| panic!("derive macro emitted an invalid explicit id {s:?}: {e}"))
}
