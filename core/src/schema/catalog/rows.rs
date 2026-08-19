//! The typed model shapes of the catalog's three entity collections.
//!
//! A **row** here is the persisted field set of one model definition, one
//! property definition, or one model-property membership. These structs give
//! the catalog's own data the same generated Model/View/Mutable surface
//! application data has, which is what lets the projection read the catalog
//! through ordinary live queries instead of a private state-buffer parse.
//!
//! `base = "crate"` makes the generated code address core directly rather
//! than through the `ankurah` facade, which core cannot depend on.
//! `system = "..."` pins a built-in [`ankurah_proto::SystemModel`] identity
//! and every field's [`ankurah_proto::SystemProperty`] identity, so reading
//! these rows never needs the catalog that describes them -- the
//! self-description ouroboros the catalog must not fall into. `no_ffi` omits
//! the WASM and UniFFI layers: these are core's implementation types, not
//! part of any language binding.
//!
//! The field set is the vocabulary the registration executor writes
//! (`super::super::registration`): same LWW backend, same
//! `PropertyId::System` identities. Reading and writing therefore address
//! the same properties, and neither side owns a private encoding.

use ankurah_derive::Model;
use ankurah_proto::EntityId;

/// One `_ankurah_model` row: a registered model definition. `label` is the
/// registration lookup key; `name` is the mutable display name.
#[derive(Model, Debug, Clone)]
#[model(base = "crate", system = "Model", no_ffi)]
pub struct SysModelRow {
    #[active_type(LWW)]
    pub label: String,
    #[active_type(LWW)]
    pub name: String,
}

/// One `_ankurah_property` row: a registered property definition.
/// `minted_for` records the model in whose scope the identity was allocated
/// (provenance, never a matching key) and `target_model` the referenced model
/// of an entity-reference property; both are absent for most rows.
#[derive(Model, Debug, Clone)]
#[model(base = "crate", system = "Property", no_ffi)]
pub struct SysPropertyRow {
    #[active_type(LWW)]
    pub name: String,
    #[active_type(LWW)]
    pub backend: String,
    #[active_type(LWW)]
    pub value_type: String,
    pub minted_for: Option<EntityId>,
    pub target_model: Option<EntityId>,
}

/// One `_ankurah_model_property` row: a property's membership in a model.
#[derive(Model, Debug, Clone)]
#[model(base = "crate", system = "ModelProperty", no_ffi)]
pub struct SysModelPropertyRow {
    pub model: EntityId,
    pub property: EntityId,
    pub optional: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Model;
    use ankurah_proto::SystemModel;

    /// The derive builds a system model's declared label from its variant
    /// name; core declares the same three labels as constants, and
    /// `system_model_id` resolves those. If the two ever disagreed, a query
    /// that names a catalog collection by label would resolve to nothing, so
    /// pin them together here. The rows themselves are addressed by the
    /// pinned identity below, never by the label.
    #[test]
    fn row_models_match_the_canonical_system_labels() {
        use crate::schema::system_model_id;
        assert_eq!(system_model_id(SysModelRow::descriptor().label), Some(crate::schema::model_collection()));
        assert_eq!(system_model_id(SysPropertyRow::descriptor().label), Some(crate::schema::property_collection()));
        assert_eq!(system_model_id(SysModelPropertyRow::descriptor().label), Some(crate::schema::model_property_collection()));
        assert_eq!(SysModelRow::descriptor().label, crate::schema::MODEL_COLLECTION_ID);
        assert_eq!(SysPropertyRow::descriptor().label, crate::schema::PROPERTY_COLLECTION_ID);
        assert_eq!(SysModelPropertyRow::descriptor().label, crate::schema::MODEL_PROPERTY_COLLECTION_ID);
        assert_eq!(SysModelRow::descriptor().system, Some(SystemModel::Model));
        assert_eq!(SysPropertyRow::descriptor().system, Some(SystemModel::Property));
        assert_eq!(SysModelPropertyRow::descriptor().system, Some(SystemModel::ModelProperty));
    }

    /// Every row field addresses the same built-in property the registration
    /// executor writes, at every epoch, on a node that has never registered
    /// anything. That is the whole of what makes the catalog readable before
    /// the catalog is loaded.
    #[test]
    fn row_fields_pin_the_built_in_property_identities() {
        use ankurah_proto::{PropertyId, SystemProperty};
        let epoch = crate::schema::SchemaEpoch::BOOTSTRAP;
        let bound = |descriptor: &'static crate::schema::ModelStructDescriptor, name: &str| {
            descriptor.field_by_name(name).and_then(|field| field.resolved.get(epoch))
        };
        assert_eq!(bound(SysModelRow::descriptor(), "label"), Some(PropertyId::System(SystemProperty::Label)));
        assert_eq!(bound(SysPropertyRow::descriptor(), "value_type"), Some(PropertyId::System(SystemProperty::ValueType)));
        assert_eq!(bound(SysPropertyRow::descriptor(), "minted_for"), Some(PropertyId::System(SystemProperty::MintedFor)));
        assert_eq!(bound(SysModelPropertyRow::descriptor(), "optional"), Some(PropertyId::System(SystemProperty::Optional)));
    }
}
