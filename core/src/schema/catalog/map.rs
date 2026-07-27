use std::collections::{BTreeMap, BTreeSet};

use ankurah_proto::{self as proto, EntityId};

use crate::{
    property::backend::{LWWBackend, PropertyBackend},
    schema::{model_collection, model_property_collection, property_collection, ModelStructDescriptor},
    value::Value,
    ModelId,
};

/// A parsed model definition entity (`_ankurah_model`): the durable model
/// identity `id`, the source-level registration lookup key `label`, and the
/// current display `name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelDef {
    pub id: EntityId,
    pub label: String,
    pub name: String,
}

/// A parsed property definition entity (`_ankurah_property`). `minted_for`
/// records the model in whose lookup scope the property was allocated
/// (provenance only, never a matching key); `target_model` is the referenced
/// model for an entity-reference property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyDef {
    pub id: EntityId,
    pub minted_for: Option<EntityId>,
    pub name: String,
    pub backend: String,
    pub value_type: String,
    pub target_model: Option<EntityId>,
}

/// A parsed model-property membership entity (`_ankurah_model_property`).
/// `optional` stays `None` (treated as optional) until the `optional`
/// follow-up event arrives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPropertyMembershipDef {
    pub id: EntityId,
    pub model: EntityId,
    pub property: EntityId,
    pub optional: Option<bool>,
}

/// The durable model identity admitted for one compiled model shape. Field
/// identities are re-derived from the schema and the catalog map on demand.
#[derive(Debug, Clone)]
pub(super) struct EnsuredSchemaBinding {
    pub(super) schema: &'static ModelStructDescriptor,
    pub(super) model: EntityId,
    pub(super) confirmed: bool,
}

/// Indexed in-memory view of the catalog entities.
#[derive(Debug, Default)]
pub(super) struct CatalogMapInner {
    pub(super) properties: BTreeMap<EntityId, PropertyDef>,
    pub(super) models: BTreeMap<EntityId, ModelDef>,
    pub(super) memberships: BTreeMap<EntityId, ModelPropertyMembershipDef>,
    pub(super) by_label: BTreeMap<String, EntityId>,
    model_memberships: BTreeMap<EntityId, BTreeSet<EntityId>>,
    pub(super) names_global: BTreeMap<String, BTreeSet<EntityId>>,
}

/// Remove one id from a set-valued index, dropping emptied entries.
fn deindex<K: Ord>(index: &mut BTreeMap<K, BTreeSet<EntityId>>, key: &K, id: &EntityId) {
    if let Some(set) = index.get_mut(key) {
        set.remove(id);
        if set.is_empty() {
            index.remove(key);
        }
    }
}

impl CatalogMapInner {
    pub(super) fn clear(&mut self) { *self = Self::default(); }

    /// Fold one catalog entity state into the map, keyed by its collection.
    /// Idempotent by entity id; non-catalog collections and unparseable
    /// states are ignored.
    pub(super) fn apply_state(&mut self, collection: &ModelId, id: EntityId, state: &proto::EntityState) {
        let Some(buffer) = state.state.state_buffers.0.get("lww") else { return };
        let Ok(backend) = LWWBackend::from_state_buffer(buffer) else { return };
        let values = backend.property_values();
        let text = |field: &str| if let Some(Some(Value::String(v))) = values.get(field) { Some(v.clone()) } else { None };
        let eid = |field: &str| if let Some(Some(Value::EntityId(v))) = values.get(field) { Some(*v) } else { None };

        if *collection == model_collection() {
            let Some(label) = text("label") else { return };
            let name = text("name").unwrap_or_else(|| label.clone());
            self.upsert_model(ModelDef { id, label, name });
        } else if *collection == property_collection() {
            let (Some(name), Some(backend), Some(value_type)) = (text("name"), text("backend"), text("value_type")) else { return };
            let (minted_for, target_model) = (eid("minted_for"), eid("target_model"));
            self.upsert_property(PropertyDef { id, minted_for, name, backend, value_type, target_model });
        } else if *collection == model_property_collection() {
            let (Some(model), Some(property)) = (eid("model"), eid("property")) else { return };
            let optional = if let Some(Some(Value::Bool(v))) = values.get("optional") { Some(*v) } else { None };
            self.upsert_membership(ModelPropertyMembershipDef { id, model, property, optional });
        }
    }

    pub(super) fn upsert_model(&mut self, def: ModelDef) {
        // A relabel detaches the old label only while this model still owns
        // the index entry (another model may have claimed the label since).
        if let Some(old) = self.models.get(&def.id).filter(|old| old.label != def.label) {
            if self.by_label.get(&old.label) == Some(&def.id) {
                self.by_label.remove(&old.label);
            }
        }
        self.by_label.insert(def.label.clone(), def.id);
        self.models.insert(def.id, def);
    }

    pub(super) fn upsert_property(&mut self, mut def: PropertyDef) {
        if let Some(old) = self.properties.get(&def.id) {
            deindex(&mut self.names_global, &old.name, &old.id);
            // `minted_for` is provenance: once learned, it survives an
            // upsert that does not know it.
            def.minted_for = def.minted_for.or(old.minted_for);
        }
        self.names_global.entry(def.name.clone()).or_default().insert(def.id);
        self.properties.insert(def.id, def);
    }

    pub(super) fn upsert_membership(&mut self, def: ModelPropertyMembershipDef) {
        if let Some(old) = self.memberships.get(&def.id).filter(|old| old.model != def.model) {
            deindex(&mut self.model_memberships, &old.model, &def.id);
        }
        self.model_memberships.entry(def.model).or_default().insert(def.id);
        self.memberships.insert(def.id, def);
    }

    pub(super) fn resolve(&self, label: &str, name: &str) -> anyhow::Result<Option<EntityId>> {
        let Some(ids) = self.by_label.get(label).and_then(|model| self.model_memberships.get(model)) else { return Ok(None) };
        let found: BTreeSet<EntityId> = ids
            .iter()
            .filter_map(|id| self.properties.get(&self.memberships.get(id)?.property))
            .filter(|property| property.name == name)
            .map(|property| property.id)
            .collect();
        if found.len() > 1 {
            anyhow::bail!("property '{name}' in model '{label}' is ambiguous across durable identities {found:?}");
        }
        Ok(found.first().copied())
    }

    pub(super) fn memberships_of(&self, model: &EntityId) -> Vec<ModelPropertyMembershipDef> {
        self.model_memberships.get(model).into_iter().flatten().filter_map(|id| self.memberships.get(id).cloned()).collect()
    }

    pub(super) fn membership(&self, model: &EntityId, property: &EntityId) -> Option<ModelPropertyMembershipDef> {
        self.model_memberships.get(model)?.iter().find_map(|id| self.memberships.get(id).filter(|m| m.property == *property).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 16]) }

    fn property(id: EntityId, name: &str) -> PropertyDef {
        PropertyDef { id, minted_for: None, name: name.into(), backend: "lww".into(), value_type: "string".into(), target_model: None }
    }

    #[test]
    fn raw_name_resolution_fails_closed_on_ambiguous_memberships() {
        let (model, first, second) = (id(1), id(2), id(3));
        let mut map = CatalogMapInner::default();
        map.upsert_model(ModelDef { id: model, label: "report".to_owned(), name: "Report".to_owned() });
        map.upsert_property(property(first, "status"));
        map.upsert_property(property(second, "status"));
        map.upsert_membership(ModelPropertyMembershipDef { id: id(4), model, property: first, optional: Some(false) });
        map.upsert_membership(ModelPropertyMembershipDef { id: id(5), model, property: second, optional: Some(false) });

        let error = map.resolve("report", "status").unwrap_err();
        assert!(error.to_string().contains("ambiguous"), "{error}");
    }

    #[test]
    fn duplicate_memberships_for_one_property_are_not_name_ambiguity() {
        let (model, property_id) = (id(1), id(2));
        let mut map = CatalogMapInner::default();
        map.upsert_model(ModelDef { id: model, label: "report".to_owned(), name: "Report".to_owned() });
        map.upsert_property(property(property_id, "status"));
        map.upsert_membership(ModelPropertyMembershipDef { id: id(3), model, property: property_id, optional: Some(false) });
        map.upsert_membership(ModelPropertyMembershipDef { id: id(4), model, property: property_id, optional: Some(false) });

        assert_eq!(map.resolve("report", "status").unwrap(), Some(property_id));
    }
}
