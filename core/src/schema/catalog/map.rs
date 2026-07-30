//! The catalog's node-local materialized projection.
//!
//! Here, a **map** means the parsed, indexed read model derived from the
//! catalog's three entity collections. It accepts the catalog's generated
//! typed Views or already-resolved definitions and produces identity and
//! membership lookups for registration and runtime
//! schema resolution. It is not catalog storage, an allocator, a feed
//! lifecycle, or a cache of compiled Rust-schema bindings; the parent
//! [`super::CatalogManager`] service owns those concerns.
//!
//! This module owns consistency between each definition table and its
//! secondary indexes. Upserting an entity must remove stale label, name, or
//! model-membership entries before publishing the new ones, and set-valued
//! indexes never retain empty buckets. A malformed typed row is rejected by
//! its accessor and contributes no definition; callers log that failure and
//! continue processing unrelated rows.

use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
};

use ankurah_proto::EntityId;

use crate::property::PropertyError;

use super::rows::{SysModelPropertyRowView, SysModelRowView, SysPropertyRowView};

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

/// Indexed in-memory view of the catalog entities.
#[derive(Debug, Default)]
pub(super) struct CatalogMap {
    pub(super) properties: BTreeMap<EntityId, PropertyDef>,
    pub(super) models: BTreeMap<EntityId, ModelDef>,
    pub(super) memberships: BTreeMap<EntityId, ModelPropertyMembershipDef>,
    pub(super) by_label: BTreeMap<String, EntityId>,
    model_memberships: EntitySetIndex<EntityId>,
    names_global: EntitySetIndex<String>,
}

/// A set-valued secondary index whose empty buckets are unobservable and
/// removed eagerly. Both property-name and model-membership indexes need
/// this same maintenance invariant when an entity changes its indexed key.
#[derive(Debug)]
struct EntitySetIndex<K>(BTreeMap<K, BTreeSet<EntityId>>);

impl<K> Default for EntitySetIndex<K> {
    fn default() -> Self { Self(BTreeMap::new()) }
}

impl<K: Ord> EntitySetIndex<K> {
    fn insert(&mut self, key: K, id: EntityId) { self.0.entry(key).or_default().insert(id); }

    fn remove<Q>(&mut self, key: &Q, id: &EntityId)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let empty = self.0.get_mut(key).is_some_and(|ids| {
            ids.remove(id);
            ids.is_empty()
        });
        if empty {
            self.0.remove(key);
        }
    }

    fn get<Q>(&self, key: &Q) -> Option<&BTreeSet<EntityId>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.get(key)
    }
}

impl CatalogMap {
    pub(super) fn clear(&mut self) { *self = Self::default(); }

    pub(super) fn apply_model(&mut self, row: &SysModelRowView) -> Result<(), PropertyError> {
        self.upsert_model(ModelDef { id: row.id(), label: row.label()?, name: row.name()? });
        Ok(())
    }

    pub(super) fn apply_property(&mut self, row: &SysPropertyRowView) -> Result<(), PropertyError> {
        self.upsert_property(PropertyDef {
            id: row.id(),
            minted_for: row.minted_for()?,
            name: row.name()?,
            backend: row.backend()?,
            value_type: row.value_type()?,
            target_model: row.target_model()?,
        });
        Ok(())
    }

    pub(super) fn apply_membership(&mut self, row: &SysModelPropertyRowView) -> Result<(), PropertyError> {
        self.upsert_membership(ModelPropertyMembershipDef {
            id: row.id(),
            model: row.model()?,
            property: row.property()?,
            optional: Some(row.optional()?),
        });
        Ok(())
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
            self.names_global.remove(&old.name, &old.id);
            // `minted_for` is provenance: once learned, it survives an
            // upsert that does not know it.
            def.minted_for = def.minted_for.or(old.minted_for);
        }
        self.names_global.insert(def.name.clone(), def.id);
        self.properties.insert(def.id, def);
    }

    pub(super) fn upsert_membership(&mut self, def: ModelPropertyMembershipDef) {
        if let Some(old) = self.memberships.get(&def.id).filter(|old| old.model != def.model) {
            self.model_memberships.remove(&old.model, &def.id);
        }
        self.model_memberships.insert(def.model, def.id);
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

    pub(super) fn siblings_by_name(&self, name: &str) -> Vec<EntityId> {
        self.names_global.get(name).into_iter().flat_map(|ids| ids.iter().copied()).collect()
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
    fn name_resolution_fails_closed_on_ambiguous_memberships() {
        let (model, first, second) = (id(1), id(2), id(3));
        let mut map = CatalogMap::default();
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
        let mut map = CatalogMap::default();
        map.upsert_model(ModelDef { id: model, label: "report".to_owned(), name: "Report".to_owned() });
        map.upsert_property(property(property_id, "status"));
        map.upsert_membership(ModelPropertyMembershipDef { id: id(3), model, property: property_id, optional: Some(false) });
        map.upsert_membership(ModelPropertyMembershipDef { id: id(4), model, property: property_id, optional: Some(false) });

        assert_eq!(map.resolve("report", "status").unwrap(), Some(property_id));
    }

    #[test]
    fn moving_a_membership_removes_it_from_the_old_model_index() {
        let (old_model, new_model, property_id, membership_id) = (id(1), id(2), id(3), id(4));
        let mut map = CatalogMap::default();
        map.upsert_membership(ModelPropertyMembershipDef {
            id: membership_id,
            model: old_model,
            property: property_id,
            optional: Some(false),
        });
        map.upsert_membership(ModelPropertyMembershipDef {
            id: membership_id,
            model: new_model,
            property: property_id,
            optional: Some(false),
        });

        assert!(map.memberships_of(&old_model).is_empty());
        assert_eq!(
            map.memberships_of(&new_model),
            vec![ModelPropertyMembershipDef { id: membership_id, model: new_model, property: property_id, optional: Some(false) }]
        );
    }
}
