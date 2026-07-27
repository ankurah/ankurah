//! Typed row Models for the catalog's system collections, derived like any
//! app model. `base = "crate"` repoints the generated code at core itself
//! (core cannot address itself through the `::ankurah` facade), and
//! `system = "..."` pins each model's closed built-in identity: every
//! registration path short-circuits on it, so these models never consult
//! the catalog they describe. Expect more system tables to live here over
//! time.

use ankurah_derive::Model;
use ankurah_proto::EntityId;

/// One `_ankurah_model` row: a registered model definition.
#[derive(Model, Debug, Clone)]
#[model(base = "crate", system = "Model")]
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
#[model(base = "crate", system = "Property")]
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
#[model(base = "crate", system = "ModelProperty")]
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
