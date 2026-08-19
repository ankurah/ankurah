//! The catalog's lookup tables: what the projection has delivered, and what
//! registration has confirmed that it has not delivered yet.
//!
//! Three derived tables ([`Indexed`]) stand behind every lookup here, one per
//! catalog collection, keyed by catalog entity id and holding the row itself.
//! Each one OWNS the live query it indexes and rebuilds whole from that
//! query's current rows on the first read after a change, so nobody maintains
//! these tables and nothing can leave them stale: a definition that is
//! renamed, or that leaves the catalog, leaves the table with it.
//!
//! Beside them sits the OVERLAY: the definitions the allocator confirmed to a
//! registration that has already returned, which the projection delivers a
//! task hop later on a durable node and a network round trip later on an
//! ephemeral one. One sentence governs the two, and every lookup here obeys
//! it: THE DERIVED TABLE ANSWERS FIRST, and the overlay answers only for ids
//! the derived table does not carry yet. That is the right answer through a
//! rename (the derived row carries the catalog's current name, the overlay
//! the name at registration time) and before delivery (the derived table has
//! nothing, and the overlay fills), and it is why the overlay never needs
//! pruning anywhere except reset, where it is cleared with everything else.
//!
//! Lookups that are not by id -- by label, by display name, by model -- scan.
//! The tables are lent rather than cloned to scan them, and every scan answers
//! in catalog entity id order, so two nodes reading the same catalog answer
//! alike.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

use ankurah_proto::{self as proto, EntityId};
use ankurah_signals::{porcelain::SignalExt, signal::Indexed, Mut};

use crate::{
    livequery::{EntityLiveQuery, LiveQuery},
    model::View,
};

use super::rows::{SysModelPropertyRow, SysModelPropertyRowView, SysModelRow, SysModelRowView, SysPropertyRow, SysPropertyRowView};

/// One catalog collection's derived table: every row a query over that
/// collection carries, by the catalog entity id that identifies it.
type Table<R, V> = Indexed<LiveQuery<R>, EntityId, V>;

/// The live projection: one derived table per catalog collection, each
/// holding the query it derives from. Dropping this drops all three queries,
/// which is how reset tears the projection down; the next warm builds a new
/// one.
pub(super) struct CatalogProjection {
    models: Table<SysModelRowView, SysModelRow>,
    properties: Table<SysPropertyRowView, SysPropertyRow>,
    memberships: Table<SysModelPropertyRowView, SysModelPropertyRow>,
}

impl CatalogProjection {
    /// Derive the three tables from the three projection queries.
    ///
    /// Each extraction closure reads one row ONCE per rebuild and answers
    /// with the id to file it under and the row to file there, so a lookup
    /// reads plain struct fields and never re-decodes. It is also the single
    /// place a half-written row is dealt with: the row is skipped with a
    /// warning naming it, and the definitions beside it survive.
    pub(super) fn new(
        models: LiveQuery<SysModelRowView>,
        properties: LiveQuery<SysPropertyRowView>,
        memberships: LiveQuery<SysModelPropertyRowView>,
    ) -> Self {
        Self {
            models: models.index_by(|row: &SysModelRowView| entry(row)),
            properties: properties.index_by(|row: &SysPropertyRowView| entry(row)),
            memberships: memberships.index_by(|row: &SysModelPropertyRowView| entry(row)),
        }
    }

    /// The three projection queries, for a caller with something to ask the
    /// queries themselves rather than the tables they feed.
    pub(super) fn queries(&self) -> [EntityLiveQuery; 3] {
        [
            EntityLiveQuery::clone(self.models.source()),
            EntityLiveQuery::clone(self.properties.source()),
            EntityLiveQuery::clone(self.memberships.source()),
        ]
    }
}

/// One row's table entry: its catalog entity id and the row itself. The error
/// names the row, because that identity is the caller's to supply -- the
/// derived table cannot name what it could not read.
fn entry<R: View>(row: &R) -> Result<(EntityId, R::Model), String> {
    row.to_model().map(|model| (row.id(), model)).map_err(|error| format!("{} row {}: {error}", R::collection(), row.id()))
}

/// Definitions the allocator has already confirmed but the projection has not
/// yet delivered. Registration folds its response in here before it returns,
/// so a second registration in the same breath sees the first.
#[derive(Debug, Default, Clone)]
struct CatalogOverlay {
    models: BTreeMap<EntityId, SysModelRow>,
    properties: BTreeMap<EntityId, SysPropertyRow>,
    memberships: BTreeMap<EntityId, SysModelPropertyRow>,
}

/// The catalog's in-memory answer surface: the projection while one is
/// published, and the overlay always.
pub(super) struct CatalogMap {
    /// The live projection, present between a published warm and the next
    /// reset. Absent means this node has no authoritative catalog -- which is
    /// exactly what an un-ready catalog is.
    projection: RwLock<Option<CatalogProjection>>,
    /// A [`Mut`] rather than a plain lock so a reader that resolved a name
    /// through the overlay is notified when the overlay changes, which is
    /// what it already gets from the derived side.
    overlay: Mut<Arc<CatalogOverlay>>,
}

impl Default for CatalogMap {
    fn default() -> Self { Self { projection: RwLock::new(None), overlay: Mut::new(Arc::new(CatalogOverlay::default())) } }
}

impl CatalogMap {
    /// Publish a warm's projection. The one it replaces is handed back so the
    /// caller drops it (and the queries it owns) outside this lock.
    pub(super) fn install(&self, projection: CatalogProjection) -> Option<CatalogProjection> {
        self.projection.write().unwrap().replace(projection)
    }

    /// Detach the projection and drop everything registration confirmed under
    /// the departing system. Returns the detached projection for the caller to
    /// drop outside any catalog lock.
    pub(super) fn clear(&self) -> Option<CatalogProjection> {
        let projection = self.projection.write().unwrap().take();
        self.overlay.set(Arc::new(CatalogOverlay::default()));
        projection
    }

    /// The projection's three queries, or `None` while no projection is
    /// published.
    pub(super) fn queries(&self) -> Option<[EntityLiveQuery; 3]> { self.projection.read().unwrap().as_ref().map(|p| p.queries()) }

    /// Fold the allocator's resolved definitions into the overlay. Idempotent
    /// (keyed by catalog entity id), and invisible for every id the projection
    /// has already delivered.
    pub(super) fn upsert_registered(&self, models: &[proto::RegisteredModel]) {
        let mut overlay = (*self.overlay.value()).clone();
        for model in models {
            overlay.models.insert(model.id, SysModelRow { label: model.label.clone(), name: model.name.clone() });
            for property in &model.properties {
                overlay.properties.insert(
                    property.id,
                    SysPropertyRow {
                        name: property.name.clone(),
                        backend: property.backend.clone(),
                        value_type: property.value_type.clone(),
                        minted_for: property.minted_for,
                        target_model: property.target_model,
                    },
                );
                overlay.memberships.insert(
                    property.membership_id,
                    SysModelPropertyRow { model: model.id, property: property.id, optional: property.optional },
                );
            }
        }
        self.overlay.set(Arc::new(overlay));
    }

    /// Lend everything the catalog can answer from right now.
    ///
    /// The three tables are borrowed together, in one nesting order and under
    /// one read of the projection, so a lookup that needs two of them (a
    /// property addressed through a model's memberships) never takes the same
    /// locks in the opposite order.
    fn with_entries<R>(&self, f: impl FnOnce(&CatalogEntries<'_>) -> R) -> R {
        let overlay = self.overlay.value();
        let projection = self.projection.read().unwrap();
        match projection.as_ref() {
            Some(projection) => projection.models.peek_table(|models| {
                projection.properties.peek_table(|properties| {
                    projection.memberships.peek_table(|memberships| {
                        f(&CatalogEntries {
                            models: Entries::new(Some(models), &overlay.models),
                            properties: Entries::new(Some(properties), &overlay.properties),
                            memberships: Entries::new(Some(memberships), &overlay.memberships),
                        })
                    })
                })
            }),
            None => f(&CatalogEntries {
                models: Entries::new(None, &overlay.models),
                properties: Entries::new(None, &overlay.properties),
                memberships: Entries::new(None, &overlay.memberships),
            }),
        }
    }

    /// The model registered under a source label, with the catalog entity id
    /// that identifies it. Two models under one label is a contradiction the
    /// catalog cannot resolve; the lowest id wins so every node picks the same
    /// one.
    pub(super) fn model_by_label(&self, label: &str) -> Option<(EntityId, SysModelRow)> {
        self.with_entries(|entries| {
            entries.models.ordered().into_iter().find(|(_, model)| model.label == label).map(|(id, model)| (id, model.clone()))
        })
    }

    /// Whether the catalog carries a model under this durable identity.
    pub(super) fn knows_model(&self, id: &EntityId) -> bool { self.with_entries(|entries| entries.models.get(id).is_some()) }

    /// The property definition for a durable property identity.
    pub(super) fn property(&self, id: &EntityId) -> Option<SysPropertyRow> {
        self.with_entries(|entries| entries.properties.get(id).cloned())
    }

    /// Every membership registered for `model`, in membership id order.
    pub(super) fn memberships_of(&self, model: &EntityId) -> Vec<(EntityId, SysModelPropertyRow)> {
        self.with_entries(|entries| entries.memberships_of(model).map(|(id, row)| (id, row.clone())).collect())
    }

    /// The membership binding `model` and `property`, if there is one. A
    /// property bound twice (the catalog tolerates duplicate membership rows)
    /// answers with the lowest membership id, so repeated calls agree.
    pub(super) fn membership(&self, model: &EntityId, property: &EntityId) -> Option<(EntityId, SysModelPropertyRow)> {
        self.with_entries(|entries| {
            entries.memberships_of(model).find(|(_, row)| row.property == *property).map(|(id, row)| (id, row.clone()))
        })
    }

    /// The property `name` addresses within `model`'s membership set.
    /// Membership is the scope -- a property shared into this model resolves
    /// here regardless of where it was minted.
    pub(super) fn property_by_name(&self, model: &EntityId, name: &str) -> Option<(EntityId, SysPropertyRow)> {
        self.with_entries(|entries| {
            entries.memberships_of(model).find_map(|(_, membership)| {
                let property = entries.properties.get(&membership.property)?;
                (property.name == name).then(|| (membership.property, property.clone()))
            })
        })
    }

    /// Raw name resolution within a model: the durable identity `name`
    /// currently addresses there.
    ///
    /// Fails CLOSED where one name in one model reaches two durable
    /// identities: which of them the caller meant is unknowable, and picking
    /// one would silently address the wrong property.
    pub(super) fn resolve(&self, model: &EntityId, name: &str) -> anyhow::Result<Option<EntityId>> {
        self.with_entries(|entries| {
            let mut found: Option<EntityId> = None;
            for (_, membership) in entries.memberships_of(model) {
                let Some(property) = entries.properties.get(&membership.property) else { continue };
                if property.name != name {
                    continue;
                }
                match found {
                    None => found = Some(membership.property),
                    Some(first) if first == membership.property => {}
                    Some(first) => {
                        let label = entries.models.get(model).map_or_else(|| model.to_string(), |model| model.label.clone());
                        anyhow::bail!(
                            "property '{name}' in model '{label}' is ambiguous across durable identities {first} and {}",
                            membership.property
                        )
                    }
                }
            }
            Ok(found)
        })
    }

    /// Every property identity carrying display name `name`, across all
    /// models, in id order.
    pub(super) fn siblings_by_name(&self, name: &str) -> Vec<EntityId> {
        self.with_entries(|entries| entries.properties.ordered().into_iter().filter(|(_, p)| p.name == name).map(|(id, _)| id).collect())
    }

    /// TEST/INTROSPECTION: how many models, properties, and memberships the
    /// catalog currently holds.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(super) fn counts(&self) -> (usize, usize, usize) {
        self.with_entries(|entries| (entries.models.len(), entries.properties.len(), entries.memberships.len()))
    }
}

/// The three kinds of catalog row, each merged from its derived table and the
/// overlay, borrowed for the length of one lookup.
struct CatalogEntries<'a> {
    models: Entries<'a, SysModelRow>,
    properties: Entries<'a, SysPropertyRow>,
    memberships: Entries<'a, SysModelPropertyRow>,
}

impl<'a> CatalogEntries<'a> {
    /// `model`'s memberships in membership id order. Order is what makes a
    /// name that reaches two properties resolve the same way twice, and on
    /// every node.
    fn memberships_of(&self, model: &EntityId) -> impl Iterator<Item = (EntityId, &'a SysModelPropertyRow)> {
        let model = *model;
        self.memberships.ordered().into_iter().filter(move |(_, row)| row.model == model)
    }
}

/// One kind of catalog row: the derived table where the projection has
/// published one, plus the overlay entries it has not delivered.
struct Entries<'a, T> {
    derived: Option<&'a HashMap<EntityId, T>>,
    overlay: &'a BTreeMap<EntityId, T>,
}

impl<'a, T> Entries<'a, T> {
    fn new(derived: Option<&'a HashMap<EntityId, T>>, overlay: &'a BTreeMap<EntityId, T>) -> Self { Self { derived, overlay } }

    /// The row filed under `id`, the derived table first.
    fn get(&self, id: &EntityId) -> Option<&'a T> { self.derived.and_then(|table| table.get(id)).or_else(|| self.overlay.get(id)) }

    /// Every row, id-ordered, each id once and under the same precedence
    /// [`Self::get`] applies.
    fn ordered(&self) -> BTreeMap<EntityId, &'a T> {
        let mut rows: BTreeMap<EntityId, &'a T> = self.overlay.iter().map(|(id, row)| (*id, row)).collect();
        if let Some(table) = self.derived {
            rows.extend(table.iter().map(|(id, row)| (*id, row)));
        }
        rows
    }

    #[cfg(any(test, feature = "test-helpers"))]
    fn len(&self) -> usize {
        match self.derived {
            Some(table) => table.len() + self.overlay.keys().filter(|id| !table.contains_key(id)).count(),
            None => self.overlay.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{RegisteredModel, RegisteredProperty};

    fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }

    fn property(id: EntityId, membership: EntityId, name: &str) -> RegisteredProperty {
        RegisteredProperty {
            id,
            membership_id: membership,
            name: name.to_owned(),
            backend: "lww".to_owned(),
            value_type: "string".to_owned(),
            target_model: None,
            minted_for: None,
            optional: false,
        }
    }

    /// A map holding one model with the given properties, confirmed by
    /// registration and not yet delivered by any projection. The overlay is
    /// the only source a unit test can supply, and it answers through the same
    /// merge and the same scans the derived tables do.
    fn registered(properties: Vec<RegisteredProperty>) -> CatalogMap {
        let map = CatalogMap::default();
        map.upsert_registered(&[RegisteredModel { id: id(1), label: "report".to_owned(), name: "Report".to_owned(), properties }]);
        map
    }

    #[test]
    fn raw_name_resolution_fails_closed_on_ambiguous_memberships() {
        let map = registered(vec![property(id(2), id(4), "status"), property(id(3), id(5), "status")]);

        let error = map.resolve(&id(1), "status").unwrap_err();
        assert!(error.to_string().contains("ambiguous"), "{error}");
    }

    #[test]
    fn duplicate_memberships_for_one_property_are_not_name_ambiguity() {
        let map = registered(vec![property(id(2), id(3), "status"), property(id(2), id(4), "status")]);

        assert_eq!(map.resolve(&id(1), "status").unwrap(), Some(id(2)));
    }

    #[test]
    fn a_registered_definition_answers_every_lookup_before_the_projection_delivers_it() {
        let map = registered(vec![property(id(2), id(3), "status")]);

        assert_eq!(map.model_by_label("report").map(|(id, model)| (id, model.name)), Some((id(1), "Report".to_owned())));
        assert!(map.knows_model(&id(1)));
        assert_eq!(map.property(&id(2)).map(|property| property.name), Some("status".to_owned()));
        assert_eq!(map.property_by_name(&id(1), "status").map(|(id, _)| id), Some(id(2)));
        assert_eq!(map.membership(&id(1), &id(2)).map(|(id, _)| id), Some(id(3)));
        assert_eq!(map.memberships_of(&id(1)).len(), 1);
        assert_eq!(map.siblings_by_name("status"), vec![id(2)]);
        assert_eq!(map.counts(), (1, 1, 1));
    }

    #[test]
    fn reset_drops_what_registration_confirmed() {
        let map = registered(vec![property(id(2), id(3), "status")]);
        map.clear();

        assert_eq!(map.counts(), (0, 0, 0));
        assert!(map.model_by_label("report").is_none());
        assert_eq!(map.resolve(&id(1), "status").unwrap(), None);
    }
}
