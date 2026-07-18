use std::collections::{BTreeMap, BTreeSet};

use ankurah_proto::{self as proto, CollectionId, EntityId, ModelId};

use crate::{
    entity::Entity,
    property::backend::{LWWBackend, PropertyBackend},
    reactor::AbstractEntity,
    schema::{model_collection, model_property_collection, property_collection, ModelSchema},
    value::Value,
};

/// A parsed model definition entity (`_ankurah_model`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelDef {
    pub id: EntityId,
    pub collection: String,
    pub name: String,
}

/// A parsed property definition entity (`_ankurah_property`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyDef {
    pub id: EntityId,
    pub minted_for: Option<EntityId>,
    pub name: String,
    pub backend: String,
    pub value_type: String,
    pub target_model: Option<EntityId>,
}

/// A parsed contract-membership entity (`_ankurah_model_property`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipDef {
    pub id: EntityId,
    pub model: EntityId,
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
    pub(super) by_collection: BTreeMap<String, ModelId>,
    model_memberships: BTreeMap<EntityId, BTreeSet<EntityId>>,
    pub(super) names_global: BTreeMap<String, BTreeSet<EntityId>>,
}

impl CatalogMapInner {
    pub(super) fn clear(&mut self) {
        self.properties.clear();
        self.models.clear();
        self.memberships.clear();
        self.by_collection.clear();
        self.model_memberships.clear();
        self.names_global.clear();
    }

    pub(super) fn upsert(&mut self, entity: &Entity) {
        let collection = AbstractEntity::collection(entity);
        let id = *AbstractEntity::id(entity);
        if collection == model_collection() {
            if let Some(def) = parse_model(entity, id) {
                self.upsert_model(def);
            }
        } else if collection == property_collection() {
            if let Some(def) = parse_property(entity, id) {
                self.upsert_property(def);
            }
        } else if collection == model_property_collection() {
            if let Some(def) = parse_membership(entity, id) {
                self.upsert_membership(def);
            }
        }
    }

    pub(super) fn remove(&mut self, collection: &CollectionId, id: &EntityId) {
        if *collection == model_collection() {
            self.remove_model(id);
        } else if *collection == property_collection() {
            self.remove_property(id);
        } else if *collection == model_property_collection() {
            self.remove_membership(id);
        }
    }

    pub(super) fn upsert_model(&mut self, def: ModelDef) {
        if let Some(old) = self.models.get(&def.id) {
            if old.collection != def.collection {
                self.by_collection.remove(&old.collection);
            }
        }
        self.by_collection.insert(def.collection.clone(), ModelId::Entity(def.id));
        self.models.insert(def.id, def);
    }

    fn remove_model(&mut self, id: &EntityId) {
        if let Some(def) = self.models.remove(id) {
            if self.by_collection.get(&def.collection) == Some(&ModelId::Entity(*id)) {
                self.by_collection.remove(&def.collection);
            }
        }
    }

    pub(super) fn upsert_property(&mut self, def: PropertyDef) {
        if let Some(old) = self.properties.get(&def.id).cloned() {
            self.deindex_property_names(&old);
        }
        self.names_global.entry(def.name.clone()).or_default().insert(def.id);
        self.properties.insert(def.id, def);
    }

    fn remove_property(&mut self, id: &EntityId) {
        if let Some(def) = self.properties.remove(id) {
            self.deindex_property_names(&def);
        }
    }

    pub(super) fn upsert_membership(&mut self, def: MembershipDef) {
        self.model_memberships.entry(def.model).or_default().insert(def.id);
        self.memberships.insert(def.id, def);
    }

    fn remove_membership(&mut self, id: &EntityId) {
        if let Some(def) = self.memberships.remove(id) {
            if let Some(set) = self.model_memberships.get_mut(&def.model) {
                set.remove(id);
                if set.is_empty() {
                    self.model_memberships.remove(&def.model);
                }
            }
        }
    }

    fn deindex_property_names(&mut self, def: &PropertyDef) {
        if let Some(set) = self.names_global.get_mut(&def.name) {
            set.remove(&def.id);
            if set.is_empty() {
                self.names_global.remove(&def.name);
            }
        }
    }

    pub(super) fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> {
        let model_id = self.by_collection.get(collection)?.entity()?;
        let membership_ids = self.model_memberships.get(&model_id)?;
        let mut found = None;
        for membership_id in membership_ids {
            let Some(membership) = self.memberships.get(membership_id) else { continue };
            let Some(property) = self.properties.get(&membership.property) else { continue };
            if property.name != name {
                continue;
            }
            match found {
                None => found = Some(property.id),
                Some(first) => {
                    tracing::warn!(
                        "catalog map holds multiple live properties named '{}' in '{}' ({} and {}); resolving to the first",
                        name,
                        collection,
                        first,
                        property.id
                    );
                    break;
                }
            }
        }
        found
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

fn field_string(entity: &Entity, field: &str) -> Option<String> {
    match AbstractEntity::value(entity, field) {
        Some(Value::String(value)) => Some(value),
        _ => None,
    }
}

fn field_entity_id(entity: &Entity, field: &str) -> Option<EntityId> {
    match AbstractEntity::value(entity, field) {
        Some(Value::EntityId(value)) => Some(value),
        _ => None,
    }
}

fn field_bool(entity: &Entity, field: &str) -> Option<bool> {
    match AbstractEntity::value(entity, field) {
        Some(Value::Bool(value)) => Some(value),
        _ => None,
    }
}

fn parse_model(entity: &Entity, id: EntityId) -> Option<ModelDef> {
    let collection = field_string(entity, "collection")?;
    let name = field_string(entity, "name").unwrap_or_else(|| collection.clone());
    Some(ModelDef { id, collection, name })
}

fn parse_property(entity: &Entity, id: EntityId) -> Option<PropertyDef> {
    Some(PropertyDef {
        id,
        minted_for: field_entity_id(entity, "minted_for"),
        name: field_string(entity, "name")?,
        backend: field_string(entity, "backend")?,
        value_type: field_string(entity, "value_type")?,
        target_model: field_entity_id(entity, "target_model"),
    })
}

fn parse_membership(entity: &Entity, id: EntityId) -> Option<MembershipDef> {
    Some(MembershipDef {
        id,
        model: field_entity_id(entity, "model")?,
        property: field_entity_id(entity, "property")?,
        optional: field_bool(entity, "optional"),
    })
}

pub(super) fn parse_state(collection: &CollectionId, id: EntityId, state: &proto::EntityState) -> Option<Entry> {
    let buffer = state.state.state_buffers.0.get("lww")?;
    let backend = LWWBackend::from_state_buffer(buffer).ok()?;
    let values = crate::property::name_keyed(backend.property_values());
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
        let collection = get_string("collection")?;
        let name = get_string("name").unwrap_or_else(|| collection.clone());
        Some(Entry::Model(ModelDef { id, collection, name }))
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
