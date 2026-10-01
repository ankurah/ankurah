use std::collections::{BTreeMap, BTreeSet};

use ankql::ast::{Resolved, Selection};
use ankurah_core::error::RetrievalError;
use ankurah_proto::{Attested, EntityId, EntityState};
use ankurah_storage_common::{materialization_plan::MaterializationPlan, selection::select_states};
use futures::StreamExt;
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};

use super::{entity_state_from_object, registered_materialization_name};
use crate::database::Database;
use crate::util::{cb_future::cb_future, cb_stream::cb_stream, object::Object, require::WBGRequire};

/// Read membership keys and their matching states in one IndexedDB snapshot.
pub(crate) async fn fetch(database: &Database, selection: &Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    let plan = MaterializationPlan::new(selection);
    let mut names = Vec::new();
    for model in &plan.models {
        names.push((*model, registered_materialization_name(database, model).await?));
    }
    let db = database.get_connection().await;
    SendWrapper::new(async move {
        let stores = js_sys::Array::of2(&JsValue::from_str("materializations"), &JsValue::from_str("entities"));
        let transaction = db.transaction_with_str_sequence(&stores).require("create joined read transaction")?;
        let materialized = transaction.object_store("materializations").require("get materializations store")?;
        let entities = transaction.object_store("entities").require("get entities store")?;
        let mut materializations = BTreeMap::new();
        for (model, name) in names {
            let ids = match name {
                Some(name) => entity_ids(&materialized, &format!("{name}\0")).await?,
                None => BTreeSet::new(),
            };
            materializations.insert(model, ids);
        }
        let all = if plan.needs_all_entities() { entity_ids(&entities, "").await? } else { BTreeSet::new() };
        let mut states = Vec::new();
        for id in plan.candidate_ids(&materializations, all) {
            let request = entities.get(&JsValue::from_str(&id.to_base64())).require("get joined entity")?;
            cb_future(&request, "success", "error").await.require("await joined entity")?;
            let value = request.result().require("get joined entity result")?;
            if !value.is_null() && !value.is_undefined() {
                states.push(entity_state_from_object(id, &Object::new(value))?);
            }
        }
        select_states(states, selection)
    }).await
}

async fn entity_ids(store: &web_sys::IdbObjectStore, prefix: &str) -> Result<BTreeSet<EntityId>, RetrievalError> {
    let range = web_sys::IdbKeyRange::bound(&JsValue::from_str(prefix), &JsValue::from_str(&format!("{prefix}\u{ffff}")))
        .require("create materialization key range")?;
    let request = store.open_key_cursor_with_range(&range).require("scan materialization keys")?;
    let mut stream = cb_stream(&request, "success", "error");
    let mut ids = BTreeSet::new();
    while let Some(result) = stream.next().await {
        let value = result.require("materialization cursor error")?;
        if value.is_null() || value.is_undefined() { break; }
        let cursor = value.dyn_into::<web_sys::IdbCursor>().require("cast materialization cursor")?;
        let key = cursor.key().require("get materialization key")?.as_string().require("materialization key is string")?;
        let id = key.strip_prefix(prefix).require("materialization key prefix")?;
        ids.insert(EntityId::from_base64(id).map_err(RetrievalError::storage)?);
        cursor.continue_().require("advance materialization cursor")?;
    }
    Ok(ids)
}
