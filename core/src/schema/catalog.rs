mod model;
pub(crate) mod register;
pub mod resolver;
pub use model::*;

use std::collections::HashMap;
use std::sync::OnceLock;

use ankql::ast::{Parsed, Predicate, Selection};
use ankurah_proto::{self as proto, EntityId, PropertyId};
use ankurah_signals::signal::{Calculated, Get};

use crate::{context::Context, error::RetrievalError, livequery::LiveQuery, model::View, node::MatchArgs};

const CACHED_TRUE: MatchArgs<Parsed> =
    MatchArgs { selection: Selection { predicate: Predicate::True, order_by: None, limit: None }, cached: true };

#[derive(Clone, Copy)]
enum NameSlot {
    Unique(EntityId),
    Ambiguous(EntityId, EntityId),
}

struct Tables {
    models: LiveQuery<SysModelRowView>,
    properties: LiveQuery<SysPropertyRowView>,
    memberships: LiveQuery<SysModelPropertyRowView>,
    index: Calculated<CatalogIndex>,
}

struct CatalogIndex {
    models_by_label: HashMap<String, (EntityId, SysModelRow)>,
    models_by_id: HashMap<EntityId, SysModelRow>,
    properties: HashMap<EntityId, SysPropertyRow>,
    memberships: HashMap<(EntityId, EntityId), (EntityId, SysModelPropertyRow)>,
    names: HashMap<EntityId, HashMap<String, NameSlot>>,
}

#[derive(Default)]
pub struct CatalogManager {
    tables: OnceLock<Tables>,
    pub(crate) allocator: tokio::sync::Mutex<()>,
}

fn rows<R: View + Clone + 'static>(query: &LiveQuery<R>) -> Vec<(EntityId, R::Model)> {
    query
        .get()
        .into_iter()
        .filter_map(|row| match row.to_model() {
            Ok(model) => Some((row.id(), model)),
            Err(error) => {
                tracing::warn!("skipping unreadable {} row {}: {error}", R::collection(), row.id());
                None
            }
        })
        .collect()
}

impl CatalogManager {
    pub fn inject(&self, ctx: &Context) -> Result<(), RetrievalError> {
        let models = ctx.query::<SysModelRowView>(CACHED_TRUE)?;
        let properties = ctx.query::<SysPropertyRowView>(CACHED_TRUE)?;
        let memberships = ctx.query::<SysModelPropertyRowView>(CACHED_TRUE)?;
        let index = {
            let models = models.clone();
            let memberships = memberships.clone();
            let properties = properties.clone();
            Calculated::new(move || {
                let mut models_by_label = HashMap::new();
                let mut models_by_id = HashMap::new();
                for (id, model) in rows(&models) {
                    models_by_label.entry(model.label.clone()).or_insert((id, model.clone()));
                    models_by_id.entry(id).or_insert(model);
                }

                let properties: HashMap<_, _> = rows(&properties).into_iter().collect();
                let memberships: HashMap<_, _> =
                    rows(&memberships).into_iter().map(|(id, row)| ((row.model, row.property), (id, row))).collect();
                let mut names: HashMap<EntityId, HashMap<String, NameSlot>> = HashMap::new();
                for (_, row) in memberships.values() {
                    let Some(property) = properties.get(&row.property) else { continue };
                    match names.entry(row.model).or_default().entry(property.name.clone()) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(NameSlot::Unique(row.property));
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            if let NameSlot::Unique(first) = *slot.get() {
                                if first != row.property {
                                    slot.insert(NameSlot::Ambiguous(first, row.property));
                                }
                            }
                        }
                    }
                }
                CatalogIndex { models_by_label, models_by_id, properties, memberships, names }
            })
        };

        self.tables
            .set(Tables { models, properties, memberships, index })
            .map_err(|_| RetrievalError::Other("catalog already initialized".into()))
    }

    pub async fn wait_ready(&self) -> Result<(), RetrievalError> {
        let tables = self.tables.get().ok_or_else(|| RetrievalError::Other("catalog not initialized".into()))?;
        tables.models.wait_initialized().await?;
        tables.properties.wait_initialized().await?;
        tables.memberships.wait_initialized().await?;
        Ok(())
    }

    pub async fn wait_synced(&self) -> Result<(), RetrievalError> {
        let tables = self.tables.get().ok_or_else(|| RetrievalError::Other("catalog not initialized".into()))?;
        tables.models.wait_durable_answered().await?;
        tables.properties.wait_durable_answered().await?;
        tables.memberships.wait_durable_answered().await?;
        Ok(())
    }

    pub(crate) fn is_synced(&self) -> bool {
        self.tables.get().is_some_and(|tables| {
            tables.models.is_durable_answered() && tables.properties.is_durable_answered() && tables.memberships.is_durable_answered()
        })
    }

    pub fn resolve(&self, model: &proto::ModelId, name: &str) -> Option<PropertyId> { self.try_resolve(model, name).ok().flatten() }

    pub(crate) fn try_resolve(&self, model: &proto::ModelId, name: &str) -> anyhow::Result<Option<PropertyId>> {
        let model = match model {
            proto::ModelId::EntityId(model) => model,
            proto::ModelId::System(system) => {
                return Ok(super::resolver::resolve_system_property(*system, name).map(PropertyId::System));
            }
        };
        let Some(tables) = self.tables.get() else { return Ok(None) };
        tables.index.peek_with(|index| match index.names.get(model).and_then(|names| names.get(name)) {
            None => Ok(None),
            Some(NameSlot::Unique(id)) => Ok(Some(PropertyId::EntityId(*id))),
            Some(NameSlot::Ambiguous(first, second)) => {
                anyhow::bail!("property '{name}' in model {model} is ambiguous across durable identities {first} and {second}")
            }
        })
    }

    pub fn property_by_id(&self, id: &EntityId) -> Option<SysPropertyRow> {
        self.tables.get()?.index.peek_with(|index| index.properties.get(id).cloned())
    }

    pub fn property_by_name(&self, model: &EntityId, name: &str) -> Option<(EntityId, SysPropertyRow)> {
        let tables = self.tables.get()?;
        tables.index.peek_with(|index| {
            let id = match index.names.get(model)?.get(name)? {
                NameSlot::Unique(id) => *id,
                NameSlot::Ambiguous(..) => return None,
            };
            index.properties.get(&id).cloned().map(|row| (id, row))
        })
    }

    pub fn model_by_label(&self, label: &str) -> Option<(EntityId, SysModelRow)> {
        self.tables.get()?.index.peek_with(|index| index.models_by_label.get(label).cloned())
    }

    pub fn model_by_id(&self, id: &EntityId) -> Option<SysModelRow> {
        self.tables.get()?.index.peek_with(|index| index.models_by_id.get(id).cloned())
    }

    pub fn membership(&self, model: &EntityId, property: &EntityId) -> Option<(EntityId, SysModelPropertyRow)> {
        self.tables.get()?.index.peek_with(|index| index.memberships.get(&(*model, *property)).cloned())
    }

    pub fn model_id_for(&self, label: &str) -> Option<proto::ModelId> {
        crate::schema::system_model_id(label).or_else(|| self.model_by_label(label).map(|(id, _)| proto::ModelId::EntityId(id)))
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub fn counts(&self) -> (usize, usize, usize) {
        match self.tables.get() {
            Some(tables) => tables.index.peek_with(|index| (index.models_by_label.len(), index.properties.len(), index.memberships.len())),
            None => (0, 0, 0),
        }
    }
}
