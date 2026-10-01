//! Static model declarations and their catalog bindings.

use super::catalog::register::{Registrant, RegistrantProperty};
use super::catalog::resolver::{resolve_selection, DescriptorResolver};
use super::catalog::{CatalogManager, SysModelRow, SysPropertyRow};
use super::cell::PerSystemOnceCell;
use super::registration::{RegistrationError, RegistrationError::Retrieval};
use super::SystemEpoch;
use ankql::ast::{Parsed, Resolved, Selection};
use ankurah_proto::{EntityId, ModelId, PropertyId, RegisterModel, RegisterProperty, SystemModel};

use crate::error::RetrievalError;

/// A compiled Model struct's field declarations and per-system catalog bindings.
#[derive(Debug)]
pub struct ModelStructDescriptor {
    /// Source-level registration label, currently the lowercased struct name.
    pub label: &'static str,
    /// Display name, initially the struct name
    pub name: &'static str,
    /// Struct property descriptors in field declaration order.
    pub properties: &'static [StructProperty],
    #[doc(hidden)]
    pub system: Option<SystemModel>,
    /// Explicit existing model binding, encoded as a 32-byte EntityId.
    pub explicit_id: Option<&'static str>,
    /// Per-build declaration identity, reserved as a future fallback key.
    pub build_id: [u8; 16],
    /// Durable model identity resolved for each system epoch.
    pub resolved: PerSystemOnceCell<ModelId>,
}

/// One active field's registration metadata and resolved identity.
#[derive(Debug)]
pub struct StructProperty {
    /// Label of the compiled model declaring this field.
    pub model_label: &'static str,
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
    /// Whether this model-property membership is optional.
    pub optional: bool,
    /// Explicit existing property binding, possibly shared across models.
    pub explicit_id: Option<&'static str>,
    /// Per-build correlation and fallback property lookup id
    pub build_id: [u8; 16],
    /// Durable property identity resolved for each system epoch.
    pub resolved: PerSystemOnceCell<PropertyId>,
}

impl StructProperty {
    /// Look up this property's binding in `epoch`.
    pub fn resolved_id(&self, epoch: SystemEpoch) -> Result<PropertyId, crate::property::PropertyError> {
        self.resolved.get(epoch).map_err(|_| crate::property::PropertyError::Unresolved { model: self.model_label, field: self.field })
    }
}

impl RegistrantProperty for StructProperty {
    fn build_id(&self) -> [u8; 16] { self.build_id }

    fn name(&self) -> &str { self.name }

    fn renamed_from(&self) -> Option<&str> { self.renamed_from }

    fn backend(&self) -> &str { self.backend }

    fn value_type(&self) -> &str { self.value_type }

    fn explicit_id(&self) -> Option<EntityId> { self.explicit_id.map(parse_explicit_id) }

    fn optional(&self) -> bool { self.optional }
}

impl ModelStructDescriptor {
    /// Return this struct's bound model identity for `epoch`.
    pub fn model_id(&self, epoch: SystemEpoch) -> Result<ModelId, RetrievalError> {
        self.resolved.get(epoch).map_err(|_| RetrievalError::UnboundDeclaration { label: self.label.into() })
    }

    pub(crate) fn registrant(&'static self, epoch: SystemEpoch) -> DescriptorRegistrant {
        DescriptorRegistrant { schema: self, epoch, model: None, properties: vec![None; self.properties.len()] }
    }

    /// Return this struct's model identity for `epoch`, binding it from the local catalog if needed; never registers.
    pub fn bind_local(&'static self, catalog: &CatalogManager, epoch: SystemEpoch) -> Result<ModelId, RetrievalError> {
        if self.resolved.get(epoch).is_err() {
            // Cached rows may be stale until the catalog's first durable answer, and an epoch's first binding is permanent.
            if !catalog.is_ready() {
                return Err(RetrievalError::NodeNotReady);
            }
            catalog.resolve_local(&mut self.registrant(epoch))?;
        }
        self.model_id(epoch)
    }

    /// Resolve names and literal types against this model, restricting the selection to its members.
    pub(crate) fn resolve_selection(
        &'static self,
        catalog: &CatalogManager,
        epoch: Option<SystemEpoch>,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError> {
        let epoch = match (self.system, epoch) {
            (Some(_), None) => SystemEpoch::BOOTSTRAP,
            (_, Some(epoch)) => epoch,
            (None, None) => return Err(RetrievalError::UnboundDeclaration { label: self.label.to_string() }),
        };
        let model = self.bind_local(catalog, epoch)?;
        let resolver = DescriptorResolver { schema: self, epoch, catalog };
        Ok(resolve_selection(&model, &resolver, selection)?.and_member_of(model))
    }

    /// The active field whose display name is `name`, if any.
    pub fn field_by_name(&self, name: &str) -> Option<&'static StructProperty> { self.properties.iter().find(|f| f.name == name) }
}

pub(crate) struct DescriptorRegistrant {
    schema: &'static ModelStructDescriptor,
    epoch: SystemEpoch,
    model: Option<EntityId>,
    properties: Vec<Option<EntityId>>,
}

impl DescriptorRegistrant {
    fn incomplete(&self) -> RegistrationError {
        Retrieval(RetrievalError::Other(format!(
            "registration of '{}' succeeded without a complete compatible catalog binding",
            self.schema.label
        )))
    }
}

impl Registrant for DescriptorRegistrant {
    type Property = StructProperty;

    fn build_id(&self) -> [u8; 16] { self.schema.build_id }

    fn label(&self) -> &str { self.schema.label }

    fn name(&self) -> &str { self.schema.name }

    fn explicit_id(&self) -> Option<EntityId> { self.schema.explicit_id.map(parse_explicit_id) }

    fn properties(&self) -> impl ExactSizeIterator<Item = &Self::Property> { self.schema.properties.iter() }

    fn set_model(&mut self, id: EntityId, model: SysModelRow) -> Result<(), RegistrationError> {
        if model.label != self.schema.label
            || self.schema.explicit_id.is_none() && model.name != self.schema.name
            || self.schema.explicit_id.is_some_and(|explicit| parse_explicit_id(explicit) != id)
        {
            return Err(self.incomplete());
        }
        self.model = Some(id);
        Ok(())
    }

    fn set_property(
        &mut self,
        index: usize,
        id: EntityId,
        property: SysPropertyRow,
        _membership_id: EntityId,
        optional: bool,
    ) -> Result<(), RegistrationError> {
        let field = &self.schema.properties[index];
        if field.explicit_id.is_some_and(|explicit| parse_explicit_id(explicit) != id)
            || field.explicit_id.is_none() && property.name != field.name
            || property.backend != field.backend
            || property.value_type != field.value_type
            || optional != field.optional
        {
            return Err(self.incomplete());
        }
        self.properties[index] = Some(id);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), RegistrationError> {
        let Some(model) = self.model else { return Err(self.incomplete()) };
        if self.properties.iter().any(Option::is_none) {
            return Err(self.incomplete());
        }
        for (field, property) in self.schema.properties.iter().zip(&self.properties) {
            let property = property.expect("checked above");
            field.resolved.set(self.epoch, PropertyId::EntityId(property));
        }
        self.schema.resolved.set(self.epoch, ModelId::EntityId(model));
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_resolution_distinguishes_unbound_from_invalid_declarations() {
        let catalog = CatalogManager::default();
        for label in ["local_resolution", "_ankurah_reserved"] {
            let schema = Box::leak(Box::new(ModelStructDescriptor {
                label,
                name: label,
                properties: &[],
                system: None,
                explicit_id: None,
                build_id: [0; 16],
                resolved: PerSystemOnceCell::per_epoch(),
            }));
            let epoch = SystemEpoch::allocate();
            let error = catalog.resolve_local(&mut schema.registrant(epoch)).expect_err("an empty catalog cannot bind");
            if label == "local_resolution" {
                assert!(
                    matches!(error, RegistrationError::Retrieval(RetrievalError::UnboundDeclaration { label: found }) if found == label)
                );
            } else {
                assert!(matches!(error, RegistrationError::ReservedCollection(found) if found == label));
            }
            assert!(schema.resolved.get(epoch).is_err());
        }
        assert_eq!(catalog.counts(), (0, 0, 0));
    }
}
