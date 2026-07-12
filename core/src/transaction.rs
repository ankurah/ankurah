use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ankurah_proto::{self as proto, EntityId};

use crate::error::RetrievalError;
use crate::policy::AccessDenied;
use crate::{
    context::TContext,
    entity::Entity,
    error::MutationError,
    model::{Model, MutableBorrow},
};

use append_only_vec::AppendOnlyVec;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct Transaction {
    pub(crate) dyncontext: Arc<dyn TContext + Send + Sync + 'static>,
    pub(crate) id: proto::TransactionId,
    pub(crate) entities: AppendOnlyVec<Entity>,
    pub(crate) alive: Arc<AtomicBool>,
    /// Genesis events frozen eagerly by `create`, keyed by their derived
    /// entity id. Commit emits each before any post-create Update.
    pub(crate) genesis_events: std::sync::RwLock<std::collections::BTreeMap<EntityId, proto::Event>>,
    /// Entity IDs that were created in this transaction via create().
    /// Kept as an independent invariant check against `genesis_events` so a
    /// phantom empty-head entity can never be promoted into a creation.
    pub(crate) created_entity_ids: std::sync::RwLock<std::collections::HashSet<EntityId>>,
    /// Exact compiled schema shapes used to create, fetch, or edit entities
    /// in this transaction. Commit reasserts only these shapes: replaying a
    /// process-global cache would let one failed, incompatible declaration
    /// poison unrelated later transactions for the same collection.
    pub(crate) schemas: std::sync::RwLock<Vec<&'static crate::schema::ModelSchema>>,
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl Transaction {
    #[wasm_bindgen(js_name = "commit")]
    pub async fn js_commit(self) -> Result<(), JsValue> {
        let _ = self.dyncontext.commit_local_trx(&self).await?;
        Ok(())
    }
}

impl Transaction {
    pub(crate) fn new(dyncontext: Arc<dyn TContext + Send + Sync + 'static>) -> Self {
        Self {
            dyncontext,
            id: proto::TransactionId::new(),
            entities: AppendOnlyVec::new(),
            alive: Arc::new(AtomicBool::new(true)),
            genesis_events: std::sync::RwLock::new(std::collections::BTreeMap::new()),
            created_entity_ids: std::sync::RwLock::new(std::collections::HashSet::new()),
            schemas: std::sync::RwLock::new(Vec::new()),
        }
    }

    fn record_schema(&self, schema: &'static crate::schema::ModelSchema) {
        let mut schemas = self.schemas.write().unwrap();
        if !schemas.iter().any(|known| **known == *schema) {
            schemas.push(schema);
        }
    }

    pub(crate) fn add_entity(&self, entity: Entity) -> &Entity {
        let index = self.entities.push(entity);
        &self.entities[index]
    }

    pub async fn create<'rec, 'trx: 'rec, M: Model>(&'trx self, model: &M) -> Result<MutableBorrow<'rec, M::Mutable>, MutationError> {
        // RFC 5.2 (specs/model-property-metadata/rfc.md) model first-use: ensure M is registered BEFORE creating
        // the entity. A failed reassertion proceeds only when this exact shape
        // is already fully and compatibly bound; a never-registered, missing,
        // or incompatible field fails here.
        self.dyncontext.ensure_registered(M::schema()).await?;

        // Initial values are written to a provisional, writable entity which
        // is deliberately not resident: its id does not exist until these
        // operations are frozen into the genesis preimage.
        let provisional = self.dyncontext.create_provisional_entity(M::collection(), self.alive.clone());
        model.initialize_new_entity(&provisional)?;
        provisional.resolve_pending_keys()?;

        let system = self
            .dyncontext
            .system_id()
            .ok_or_else(|| MutationError::General("cannot create an entity before the context has joined or created a system".into()))?;
        let genesis = proto::Event::genesis(provisional.model_id()?, Some(system), provisional.extract_operations()?);

        // Insert only the derived-id primary. It remains empty until commit;
        // the returned transaction fork starts at genesis so later edits have
        // the correct parent and are extracted separately.
        let entity = self.dyncontext.create_transaction_entity(M::collection(), &genesis, self.alive.clone())?;
        self.dyncontext.check_write(&entity)?;
        self.record_schema(M::schema());

        // Store the already-extracted genesis exactly once. Commit must never
        // ask the final transaction entity to reconstruct these operations.
        if self.genesis_events.write().unwrap().insert(entity.id, genesis).is_some() {
            return Err(MutationError::AlreadyExists);
        }
        if !self.created_entity_ids.write().unwrap().insert(entity.id) {
            return Err(MutationError::AlreadyExists);
        }

        let entity_ref = self.add_entity(entity);
        Ok(MutableBorrow::new(entity_ref))
    }
    fn get_trx_entity(&self, id: &EntityId) -> Option<&Entity> { self.entities.iter().find(|e| e.id == *id) }
    pub async fn get<'rec, 'trx: 'rec, M: Model>(&'trx self, id: &EntityId) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        // RFC 5.2 model first-use on the mutating fetch-to-edit path.
        // Fetching does not mint identity, so a strict registration failure
        // is deferred here (warn only); commit_local_trx enforces it before
        // any write lands (plan decision 16).
        if let Err(e) = self.dyncontext.ensure_registered(M::schema()).await {
            tracing::warn!("registration unavailable on fetch-to-edit for '{}'; commit will enforce: {}", M::collection(), e);
        }
        match self.get_trx_entity(id) {
            Some(entity) => {
                self.record_schema(M::schema());
                Ok(MutableBorrow::new(entity))
            }
            None => {
                // go fetch the entity from the context
                let retrieved_entity = self.dyncontext.get_entity(*id, &M::collection(), false).await?;
                // double check to make sure somebody didn't add the entity to the trx during the await
                // because we're forking the entity, we need to make sure we aren't adding the same entity twice
                if let Some(entity) = self.get_trx_entity(&retrieved_entity.id) {
                    // if this happens, I don't think we want to refresh the entity, because it's already snapshotted in the trx
                    // and we should leave it that way to honor the consistency model
                    self.record_schema(M::schema());
                    Ok(MutableBorrow::new(entity))
                } else {
                    let entity = self.add_entity(retrieved_entity.snapshot(self.alive.clone()));
                    self.record_schema(M::schema());
                    Ok(MutableBorrow::new(entity))
                }
            }
        }
    }
    pub fn edit<'rec, 'trx: 'rec, M: Model>(&'trx self, entity: &Entity) -> Result<MutableBorrow<'rec, M::Mutable>, AccessDenied> {
        // RFC 5.2 model first-use on the edit path. This entry is
        // SYNCHRONOUS (it is what the derive-generated `View::edit` calls, so
        // it cannot become async without touching derive and every
        // `.edit()?` call site), so it overlays the compiled schema via the
        // cheap sync cache rather than issuing the durable async
        // registration. The durable write still happens on the async mutating
        // entries (`create`, `get`); the entity being edited was itself
        // created or fetched through one of those, so the model reaches the
        // durable catalog. This overlay guarantees local resolution here and
        // now.
        self.dyncontext.cache_compiled(M::schema());
        if let Some(entity) = self.get_trx_entity(&entity.id) {
            self.record_schema(M::schema());
            return Ok(MutableBorrow::new(entity));
        }
        self.dyncontext.check_write(entity)?;
        self.record_schema(M::schema());

        Ok(MutableBorrow::new(self.add_entity(entity.snapshot(self.alive.clone()))))
    }

    #[must_use]
    pub async fn commit(self) -> Result<(), MutationError> {
        let _ = self.dyncontext.commit_local_trx(&self).await?;
        Ok(())
    }

    /// Commits the transaction and returns the events that were created.
    /// This is primarily useful for testing DAG structures.
    #[cfg(feature = "test-helpers")]
    #[must_use]
    pub async fn commit_and_return_events(self) -> Result<Vec<ankurah_proto::Event>, MutationError> {
        self.dyncontext.commit_local_trx(&self).await
    }

    pub fn rollback(self) {
        // Mark transaction as no longer alive
        self.alive.store(false, Ordering::Release);
        // The transaction will be dropped without committing
    }

    // TODO: Implement delete functionality after core query/edit operations are stable
    // For now, "removal" from result sets is handled by edits that cause entities to no longer match queries
    /*
    pub async fn delete<'rec, 'trx: 'rec, M: Model>(
        &'trx self,
        id: impl Into<ID>,
    ) -> Result<(), crate::error::RetrievalError> {
        let id = id.into();
        let entity = self.fetch_entity(id, M::collection()).await?;
        let entity = Arc::new(entity.clone());
        self.node.delete_entity(entity).await?;
        Ok(())
    }
    */
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Mark transaction as no longer alive when dropped
        self.alive.store(false, Ordering::Release);
        // how do we want to do the rollback?
    }
}

#[cfg(feature = "uniffi")]
#[uniffi::export]
impl Transaction {
    /// Commit the transaction (UniFFI version - uses Arc<Self>)
    /// Simply borrows self and calls commit_local_trx - the alive flag prevents double commits
    #[uniffi::method(name = "commit")]
    pub async fn uniffi_commit(self: Arc<Self>) -> Result<(), MutationError> {
        let _ = self.dyncontext.commit_local_trx(&self).await?;
        Ok(())
    }

    /// Rollback the transaction (UniFFI version)
    #[uniffi::method(name = "rollback")]
    pub fn uniffi_rollback(&self) { self.alive.store(false, Ordering::Release); }
}
