use std::collections::{BTreeMap, BTreeSet};

use crate::ModelId;
use ankurah_proto::{self as proto, EntityId};

use crate::{
    property::backend::{LWWBackend, PropertyBackend},
    schema::{model_collection, model_property_collection, property_collection, ModelSchema},
    value::Value,
};

/// A parsed model definition entity (`_ankurah_model`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelDef {
    /// The catalog entity that durably identifies this model.
    pub id: EntityId,
    /// The source-level label used as the registration lookup key.
    pub label: String,
    /// The model's current registered display name.
    pub name: String,
}

/// A parsed property definition entity (`_ankurah_property`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyDef {
    /// The catalog entity that durably identifies this property.
    pub id: EntityId,
    /// The model in whose lookup scope this property was minted.
    pub minted_for: Option<EntityId>,
    /// The property's current registered display name.
    pub name: String,
    /// The registered state-backend identifier.
    pub backend: String,
    /// The registered logical value-type spelling.
    pub value_type: String,
    /// The referenced model for an entity-reference property.
    pub target_model: Option<EntityId>,
}

/// A parsed contract-membership entity (`_ankurah_model_property`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipDef {
    /// The catalog entity that durably identifies this membership.
    pub id: EntityId,
    /// The model participating in the membership.
    pub model: EntityId,
    /// The property participating in the membership.
    pub property: EntityId,
    /// `None` until the `optional` follow-up event arrives. Until then the
    /// membership is treated as optional.
    pub optional: Option<bool>,
}

/// Exact durable identities admitted for one compiled model shape.
#[derive(Debug, Clone)]
pub(super) struct EnsuredSchemaBinding {
    pub(super) schema: &'static ModelSchema,
    pub(super) model: EntityId,
    pub(super) fields: BTreeMap<&'static str, EntityId>,
    pub(super) confirmed: bool,
}

/// Indexed in-memory view of the catalog entities.
#[derive(Debug, Default)]
pub(super) struct CatalogMapInner {
    pub(super) properties: BTreeMap<EntityId, PropertyDef>,
    pub(super) models: BTreeMap<EntityId, ModelDef>,
    pub(super) memberships: BTreeMap<EntityId, MembershipDef>,
    pub(super) by_label: BTreeMap<String, EntityId>,
    model_memberships: BTreeMap<EntityId, BTreeSet<EntityId>>,
    pub(super) names_global: BTreeMap<String, BTreeSet<EntityId>>,
}

impl CatalogMapInner {
    pub(super) fn clear(&mut self) {
        self.properties.clear();
        self.models.clear();
        self.memberships.clear();
        self.by_label.clear();
        self.model_memberships.clear();
        self.names_global.clear();
    }

    // The live-Entity upsert/remove surface (reactor updates parsed through
    // AbstractEntity, including de-indexing on catalog entity removal)
    // returns with the read flip's reactor feed; state-buffer parsing below
    // serves the durable warm.

    pub(super) fn upsert_model(&mut self, def: ModelDef) {
        if let Some(old) = self.models.get(&def.id) {
            if old.label != def.label {
                self.by_label.remove(&old.label);
            }
        }
        self.by_label.insert(def.label.clone(), def.id);
        self.models.insert(def.id, def);
    }

    pub(super) fn upsert_property(&mut self, def: PropertyDef) {
        if let Some(old) = self.properties.get(&def.id).cloned() {
            self.deindex_property_names(&old);
        }
        self.names_global.entry(def.name.clone()).or_default().insert(def.id);
        self.properties.insert(def.id, def);
    }

    pub(super) fn upsert_membership(&mut self, def: MembershipDef) {
        if let Some(old) = self.memberships.get(&def.id) {
            if old.model != def.model {
                if let Some(set) = self.model_memberships.get_mut(&old.model) {
                    set.remove(&def.id);
                    if set.is_empty() {
                        self.model_memberships.remove(&old.model);
                    }
                }
            }
        }
        self.model_memberships.entry(def.model).or_default().insert(def.id);
        self.memberships.insert(def.id, def);
    }

    fn deindex_property_names(&mut self, def: &PropertyDef) {
        if let Some(set) = self.names_global.get_mut(&def.name) {
            set.remove(&def.id);
            if set.is_empty() {
                self.names_global.remove(&def.name);
            }
        }
    }

    pub(super) fn resolve(&self, label: &str, name: &str) -> anyhow::Result<Option<EntityId>> {
        let Some(model_id) = self.by_label.get(label) else { return Ok(None) };
        let Some(membership_ids) = self.model_memberships.get(model_id) else { return Ok(None) };
        let mut found = None;
        for membership_id in membership_ids {
            let Some(membership) = self.memberships.get(membership_id) else { continue };
            let Some(property) = self.properties.get(&membership.property) else { continue };
            if property.name != name {
                continue;
            }
            match found {
                None => found = Some(property.id),
                Some(first) if first == property.id => {}
                Some(first) => {
                    anyhow::bail!("property '{name}' in model '{label}' is ambiguous across durable identities {first} and {}", property.id)
                }
            }
        }
        Ok(found)
    }

    pub(super) fn memberships_of(&self, model: &EntityId) -> Vec<MembershipDef> {
        self.model_memberships
            .get(model)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.memberships.get(id).cloned())
            .collect()
    }

    pub(super) fn membership(&self, model: &EntityId, property: &EntityId) -> Option<MembershipDef> {
        self.model_memberships.get(model)?.iter().find_map(|id| {
            let membership = self.memberships.get(id)?;
            (membership.property == *property).then(|| membership.clone())
        })
    }
}

pub(super) fn parse_state(collection: &ModelId, id: EntityId, state: &proto::EntityState) -> Option<Entry> {
    let buffer = state.state.state_buffers.0.get("lww")?;
    let backend = LWWBackend::from_state_buffer(buffer).ok()?;
    let values = backend.property_values();
    let get_string = |field: &str| match values.get(field) {
        Some(Some(Value::String(value))) => Some(value.clone()),
        _ => None,
    };
    let get_entity_id = |field: &str| match values.get(field) {
        Some(Some(Value::EntityId(value))) => Some(*value),
        _ => None,
    };
    let get_bool = |field: &str| match values.get(field) {
        Some(Some(Value::Bool(value))) => Some(*value),
        _ => None,
    };

    if *collection == model_collection() {
        let label = get_string("label")?;
        let name = get_string("name").unwrap_or_else(|| label.clone());
        Some(Entry::Model(ModelDef { id, label, name }))
    } else if *collection == property_collection() {
        Some(Entry::Property(PropertyDef {
            id,
            minted_for: get_entity_id("minted_for"),
            name: get_string("name")?,
            backend: get_string("backend")?,
            value_type: get_string("value_type")?,
            target_model: get_entity_id("target_model"),
        }))
    } else if *collection == model_property_collection() {
        Some(Entry::Membership(MembershipDef {
            id,
            model: get_entity_id("model")?,
            property: get_entity_id("property")?,
            optional: get_bool("optional"),
        }))
    } else {
        None
    }
}

pub(super) enum Entry {
    Model(ModelDef),
    Property(PropertyDef),
    Membership(MembershipDef),
}

pub(super) fn apply_entry(map: &mut CatalogMapInner, entry: Entry) {
    match entry {
        Entry::Model(definition) => map.upsert_model(definition),
        Entry::Property(definition) => map.upsert_property(definition),
        Entry::Membership(definition) => map.upsert_membership(definition),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 16]) }

    fn property(id: EntityId, name: &str) -> PropertyDef {
        PropertyDef {
            id,
            minted_for: None,
            name: name.to_owned(),
            backend: "lww".to_owned(),
            value_type: "string".to_owned(),
            target_model: None,
        }
    }

    #[test]
    fn raw_name_resolution_fails_closed_on_ambiguous_memberships() {
        let model = id(1);
        let first = id(2);
        let second = id(3);
        let mut map = CatalogMapInner::default();
        map.upsert_model(ModelDef { id: model, label: "report".to_owned(), name: "Report".to_owned() });
        map.upsert_property(property(first, "status"));
        map.upsert_property(property(second, "status"));
        map.upsert_membership(MembershipDef { id: id(4), model, property: first, optional: Some(false) });
        map.upsert_membership(MembershipDef { id: id(5), model, property: second, optional: Some(false) });

        let error = map.resolve("report", "status").unwrap_err();
        assert!(error.to_string().contains("ambiguous"), "{error}");
    }

    #[test]
    fn duplicate_memberships_for_one_property_are_not_name_ambiguity() {
        let model = id(1);
        let property_id = id(2);
        let mut map = CatalogMapInner::default();
        map.upsert_model(ModelDef { id: model, label: "report".to_owned(), name: "Report".to_owned() });
        map.upsert_property(property(property_id, "status"));
        map.upsert_membership(MembershipDef { id: id(3), model, property: property_id, optional: Some(false) });
        map.upsert_membership(MembershipDef { id: id(4), model, property: property_id, optional: Some(false) });

        assert_eq!(map.resolve("report", "status").unwrap(), Some(property_id));
    }
}
