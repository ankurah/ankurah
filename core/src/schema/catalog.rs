mod model;
pub mod register;
pub mod resolver;
pub use model::*;

use crate::internal::prelude::*;
use std::collections::HashMap;
use std::sync::OnceLock;

use ankql::ast::{Parsed, Predicate, Selection};
use ankurah_proto::{EntityId, PropertyId, SystemProperty};
use ankurah_signals::{
    porcelain::SignalExt,
    signal::{Calculated, Get, Indexed},
};

use crate::context::Context;

const CACHED_TRUE: MatchArgs<Parsed> =
    MatchArgs { selection: Selection { predicate: Predicate::True, order_by: None, limit: None }, cached: true };

#[derive(Clone, Copy)]
enum NameSlot {
    Unique(EntityId),
    Ambiguous(EntityId, EntityId),
}

struct Tables {
    models_by_label: Indexed<LiveQuery<SysModelRowView>, String, (EntityId, SysModelRow)>,
    properties: Indexed<LiveQuery<SysPropertyRowView>, EntityId, SysPropertyRow>,
    memberships: Indexed<LiveQuery<SysModelPropertyRowView>, (EntityId, EntityId), (EntityId, SysModelPropertyRow)>,
    name_index: Calculated<HashMap<EntityId, HashMap<String, NameSlot>>>,
}

#[derive(Default)]
pub struct CatalogManager {
    tables: OnceLock<Tables>,
    pub(crate) allocator: tokio::sync::Mutex<()>,
}

fn entry<R: View>(row: &R) -> Result<(EntityId, R::Model), String> {
    row.to_model().map(|model| (row.id(), model)).map_err(|error| format!("{} row {}: {error}", R::collection(), row.id()))
}

impl CatalogManager {
    pub fn inject(&self, ctx: &Context) -> Result<(), RetrievalError> {
        let models_by_label = ctx.query::<SysModelRowView>(CACHED_TRUE)?.index_by(|row: &SysModelRowView| {
            let (id, model) = entry(row)?;
            Ok::<_, String>((model.label.clone(), (id, model)))
        });
        let properties = ctx.query::<SysPropertyRowView>(CACHED_TRUE)?.index_by(entry::<SysPropertyRowView>);
        let memberships = ctx.query::<SysModelPropertyRowView>(CACHED_TRUE)?.index_by(|view: &SysModelPropertyRowView| {
            let (id, row) = entry(view)?;
            Ok::<_, String>(((row.model, row.property), (id, row)))
        });

        let name_index = {
            let memberships = memberships.clone();
            let properties = properties.clone();
            Calculated::new(move || {
                let mut index: HashMap<EntityId, HashMap<String, NameSlot>> = HashMap::new();
                let properties = properties.get();
                for (_, row) in memberships.get().values() {
                    let Some(property) = properties.get(&row.property) else { continue };
                    match index.entry(row.model).or_default().entry(property.name.clone()) {
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
                index
            })
        };

        self.tables
            .set(Tables { models_by_label, properties, memberships, name_index })
            .map_err(|_| RetrievalError::Other("catalog already initialized".into()))
    }

    pub async fn wait_ready(&self) -> Result<(), RetrievalError> {
        let tables = self.tables.get().ok_or_else(|| RetrievalError::Other("catalog not initialized".into()))?;
        tables.models_by_label.source().wait_initialized().await?;
        tables.properties.source().wait_initialized().await?;
        tables.memberships.source().wait_initialized().await?;
        Ok(())
    }

    pub async fn wait_synced(&self) -> Result<(), RetrievalError> {
        let tables = self.tables.get().ok_or_else(|| RetrievalError::Other("catalog not initialized".into()))?;
        tables.models_by_label.source().wait_durable_answered().await?;
        tables.properties.source().wait_durable_answered().await?;
        tables.memberships.source().wait_durable_answered().await?;
        Ok(())
    }

    pub(crate) fn is_synced(&self) -> bool {
        self.tables.get().is_some_and(|tables| {
            tables.models_by_label.source().is_durable_answered()
                && tables.properties.source().is_durable_answered()
                && tables.memberships.source().is_durable_answered()
        })
    }

    pub fn resolve(&self, model: &proto::ModelId, name: &str) -> Option<PropertyId> { self.try_resolve(model, name).ok().flatten() }

    pub(crate) fn try_resolve(&self, model: &proto::ModelId, name: &str) -> anyhow::Result<Option<PropertyId>> {
        let proto::ModelId::EntityId(model) = model else {
            return Ok(SystemProperty::from_name(name).map(PropertyId::System));
        };
        let Some(tables) = self.tables.get() else { return Ok(None) };
        tables.name_index.peek_with(|index| match index.get(model).and_then(|names| names.get(name)) {
            None => Ok(None),
            Some(NameSlot::Unique(id)) => Ok(Some(PropertyId::EntityId(*id))),
            Some(NameSlot::Ambiguous(first, second)) => {
                anyhow::bail!("property '{name}' in model {model} is ambiguous across durable identities {first} and {second}")
            }
        })
    }

    pub fn property_by_id(&self, id: &EntityId) -> Option<SysPropertyRow> { self.tables.get()?.properties.peek_lookup(id) }

    pub fn property_by_name(&self, model: &EntityId, name: &str) -> Option<(EntityId, SysPropertyRow)> {
        let tables = self.tables.get()?;
        let id = tables.name_index.peek_with(|index| match index.get(model)?.get(name)? {
            NameSlot::Unique(id) => Some(*id),
            NameSlot::Ambiguous(..) => None,
        })?;
        tables.properties.peek_lookup(&id).map(|row| (id, row))
    }

    pub fn model_by_label(&self, label: &str) -> Option<(EntityId, SysModelRow)> {
        self.tables.get()?.models_by_label.peek_lookup(&label.to_owned())
    }

    pub fn membership(&self, model: &EntityId, property: &EntityId) -> Option<(EntityId, SysModelPropertyRow)> {
        self.tables.get()?.memberships.peek_lookup(&(*model, *property))
    }

    pub fn model_id_for(&self, label: &str) -> Option<proto::ModelId> {
        crate::schema::system_model_id(label).or_else(|| self.model_by_label(label).map(|(id, _)| proto::ModelId::EntityId(id)))
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub fn counts(&self) -> (usize, usize, usize) {
        match self.tables.get() {
            Some(tables) => (
                tables.models_by_label.peek_table(|t| t.len()),
                tables.properties.peek_table(|t| t.len()),
                tables.memberships.peek_table(|t| t.len()),
            ),
            None => (0, 0, 0),
        }
    }
}
