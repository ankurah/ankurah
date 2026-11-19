use std::{collections::BTreeSet, sync::Arc};

use ankql::ast::{Resolved, Selection};
use ankurah_core::error::RetrievalError;
use ankurah_core::selection::ordering::OrderBoundary;
use ankurah_proto::{Attested, EntityId, EntityState, StateFragment};
use ankurah_storage_common::{materialization_plan::MaterializationPlan, selection::StateSelection};

use crate::{database::Database, error::sled_error};

/// Join materialization keys before loading state. The one-materialization
/// indexed path stays in SledModelStore.
pub(super) async fn fetch(database: Arc<Database>, selection: Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    fetch_after(database, selection, None).await
}

pub(super) async fn fetch_after(
    database: Arc<Database>,
    selection: Selection<Resolved>,
    boundary: Option<&OrderBoundary>,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    let boundary = boundary.cloned();
    tokio::task::spawn_blocking(move || {
        let mut selected = StateSelection::new(&selection).after(boundary.as_ref())?;
        let plan = MaterializationPlan::new(&selection);
        let mut materializations = std::collections::BTreeMap::new();
        for model in &plan.models {
            let ids = match database.materialization(model)? {
                Some(tree) => entity_ids(&tree)?,
                None => BTreeSet::new(),
            };
            materializations.insert(*model, ids);
        }
        let all = if plan.needs_all_entities() { entity_ids(&database.entities_tree)? } else { BTreeSet::new() };
        for id in plan.candidate_ids(&materializations, all) {
            if let Some(bytes) = database.entities_tree.get(id.to_bytes()).map_err(sled_error)? {
                let fragment: StateFragment = bincode::deserialize(bytes.as_ref())?;
                selected.push(Attested::<EntityState>::from_parts(id, fragment))?;
            }
        }
        Ok(selected.finish())
    })
    .await?
}

fn entity_ids(tree: &sled::Tree) -> Result<BTreeSet<EntityId>, RetrievalError> {
    tree.iter()
        .keys()
        .map(|key| {
            let bytes = key.map_err(sled_error)?;
            let bytes = bytes.as_ref().try_into().map_err(RetrievalError::storage)?;
            Ok(EntityId::from_bytes(bytes))
        })
        .collect()
}
