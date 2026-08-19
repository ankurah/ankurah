use std::collections::{BTreeMap, BTreeSet};

use ankurah_proto::EntityId;

use crate::property::PropertyError;

use super::rows::{SysModelPropertyRowView, SysModelRowView, SysPropertyRowView};

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

/// A parsed model-property membership entity (`_ankurah_model_property`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPropertyMembershipDef {
    /// The catalog entity that durably identifies this membership.
    pub id: EntityId,
    /// The model participating in the membership.
    pub model: EntityId,
    /// The property participating in the membership.
    pub property: EntityId,
    /// Whether the property is optional in this model's contract. `None`
    /// only where a raw storage lookup found the field absent (the
    /// registration executor's own duplicate checks); the typed projection
    /// requires it, since registration has always written it at creation.
    /// An absent value is treated as optional.
    pub optional: Option<bool>,
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

    pub(super) fn upsert_property(&mut self, mut def: PropertyDef) {
        if let Some(old) = self.properties.get(&def.id).cloned() {
            self.deindex_property_names(&old);
            // minted_for is provenance metadata; an upsert that does not
            // know it must not erase what the catalog already learned.
            if def.minted_for.is_none() {
                def.minted_for = old.minted_for;
            }
        }
        self.names_global.entry(def.name.clone()).or_default().insert(def.id);
        self.properties.insert(def.id, def);
    }

    pub(super) fn upsert_membership(&mut self, def: ModelPropertyMembershipDef) {
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

    pub(super) fn memberships_of(&self, model: &EntityId) -> Vec<ModelPropertyMembershipDef> {
        self.model_memberships
            .get(model)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.memberships.get(id).cloned())
            .collect()
    }

    pub(super) fn membership(&self, model: &EntityId, property: &EntityId) -> Option<ModelPropertyMembershipDef> {
        self.model_memberships.get(model)?.iter().find_map(|id| {
            let membership = self.memberships.get(id)?;
            (membership.property == *property).then(|| membership.clone())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }

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
        map.upsert_membership(ModelPropertyMembershipDef { id: id(4), model, property: first, optional: Some(false) });
        map.upsert_membership(ModelPropertyMembershipDef { id: id(5), model, property: second, optional: Some(false) });

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
        map.upsert_membership(ModelPropertyMembershipDef { id: id(3), model, property: property_id, optional: Some(false) });
        map.upsert_membership(ModelPropertyMembershipDef { id: id(4), model, property: property_id, optional: Some(false) });

        assert_eq!(map.resolve("report", "status").unwrap(), Some(property_id));
    }
}
