//! Canonical typed model shapes for the catalog's three entity collections.
//!
//! Here, a **row** means the persisted fields of one model definition,
//! property definition, or model-property membership. These structs make
//! catalog data available through the same generated Model, View, and
//! Mutable APIs as application data. Durable registration writes them through
//! a local SystemRoot context; ordinary application contexts are still
//! forbidden from mutating system collections. The generated Mutable surface
//! provides the common mechanism, while the Context authority decides who
//! may use it.
//!
//! `base = "crate"` makes generated code address core directly rather than
//! through the `::ankurah` facade. `system = "..."` pins a built-in
//! [`ankurah_proto::SystemModel`] identity, so first use never tries to
//! register the catalog row through the catalog it describes. `no_ffi`
//! deliberately omits WASM and UniFFI bindings for these private catalog
//! implementation types; choosing an alternate `base` does not itself alter
//! a model's FFI surface.

use ankurah_derive::Model;
use ankurah_proto::EntityId;

/// One `_ankurah_model` row: a registered model definition.
#[derive(Model, Debug, Clone)]
#[model(base = "crate", system = "Model", no_ffi)]
pub struct SysModelRow {
    #[active_type(LWW)]
    pub label: String,
    #[active_type(LWW)]
    pub name: String,
}

/// One `_ankurah_property` row: a registered property definition.
/// `minted_for` and `target_model` are optional in the data (folds preserve
/// absent provenance; only reference properties have targets).
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
    use ankurah_core_types::SystemModel;

    /// The derive builds each system collection label from the variant name
    /// by convention; core declares the canonical labels as constants. The
    /// two must agree, and each descriptor must pin its variant.
    #[test]
    fn row_models_match_the_canonical_system_labels() {
        assert_eq!(SysModelRow::collection().as_str(), crate::schema::MODEL_COLLECTION_ID);
        assert_eq!(SysPropertyRow::collection().as_str(), crate::schema::PROPERTY_COLLECTION_ID);
        assert_eq!(SysModelPropertyRow::collection().as_str(), crate::schema::MODEL_PROPERTY_COLLECTION_ID);
        assert_eq!(SysModelRow::descriptor().system, Some(SystemModel::Model));
        assert_eq!(SysPropertyRow::descriptor().system, Some(SystemModel::Property));
        assert_eq!(SysModelPropertyRow::descriptor().system, Some(SystemModel::ModelProperty));
    }
}
