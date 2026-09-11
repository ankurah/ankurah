use crate::internal::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ankurah_proto::EntityId;

use crate::context::DynContextInner;
use crate::entity::ProvisionalEntity;
use crate::model::{Model, MutableBorrow};

use append_only_vec::AppendOnlyVec;

pub(crate) mod commit;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct Transaction {
    pub(crate) dyncontext: Arc<dyn DynContextInner + Send + Sync + 'static>,
    pub(crate) id: proto::TransactionId,
    pub(crate) entities: AppendOnlyVec<Entity>,
    /// Prevents concurrent get/edit calls from appending separate snapshots of the same EntityId
    /// to `entities`; AppendOnlyVec makes appends thread-safe, but does not enforce uniqueness.
    snapshot_creation_lock: Mutex<()>,
    pub(crate) alive: Arc<AtomicBool>,
    /// Each created entity's genesis.
    pub(crate) genesis_events: std::sync::RwLock<std::collections::BTreeMap<EntityId, PendingGenesis>>,
}

#[derive(Clone)]
pub(crate) struct PendingGenesis {
    pub(crate) event: proto::Event,
    /// Supplies the expected model id for commit-time membership checks,
    /// even when remote registration has completed before the local catalog catches up.
    pub(crate) schema: &'static ModelStructDescriptor,
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
    pub(crate) fn new(dyncontext: Arc<dyn DynContextInner + Send + Sync + 'static>) -> Self {
        Self {
            dyncontext,
            id: proto::TransactionId::new(),
            entities: AppendOnlyVec::new(),
            snapshot_creation_lock: Mutex::new(()),
            alive: Arc::new(AtomicBool::new(true)),
            genesis_events: std::sync::RwLock::new(std::collections::BTreeMap::new()),
        }
    }

    pub(crate) fn add_entity(&self, entity: Entity) -> &Entity {
        let index = self.entities.push(entity);
        &self.entities[index]
    }

    /// Mint an entity after registering its model in the current system epoch.
    pub async fn create<'rec, 'trx: 'rec, M: Model>(&'trx self, model: &M) -> Result<MutableBorrow<'rec, M::Mutable>, MutationError> {
        let (model_id, epoch) = self.dyncontext.schema_resolver().ensure_registered(M::descriptor()).await?;

        let mut provisional = ProvisionalEntity::new();
        model.initialize_new_entity(&mut provisional, model_id, epoch)?;
        let system = self.dyncontext.system_id().ok_or(MutationError::SystemNotReady)?;
        let genesis = proto::Event::genesis(M::collection(), Some(system), proto::AuthorId::Unknown, provisional.extract_operations()?);

        let entity = self.dyncontext.create_entity(M::collection(), &genesis, self.alive.clone())?;
        self.dyncontext.check_write(&entity)?;

        if self.genesis_events.write().unwrap().insert(entity.id, PendingGenesis { event: genesis, schema: M::descriptor() }).is_some() {
            return Err(MutationError::AlreadyExists);
        }

        let entity_ref = self.add_entity(entity);
        Ok(MutableBorrow::new(entity_ref))
    }
    fn get_trx_entity(&self, id: &EntityId) -> Option<&Entity> { self.entities.iter().find(|e| e.id == *id) }

    /// Retrieve an entity for editing, registering the model locally or remotely if needed.
    /// Reuses this transaction's existing snapshot when present.
    pub async fn get<'rec, 'trx: 'rec, M: Model>(&'trx self, id: &EntityId) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        self.dyncontext.schema_resolver().ensure_registered(M::descriptor()).await?;
        let entity = match self.get_trx_entity(id) {
            Some(entity) => entity,
            None => {
                let retrieved_entity = self.dyncontext.get_entity(&M::collection(), *id, false).await?;
                if let Some(entity) = self.get_trx_entity(id) {
                    entity
                } else {
                    self.dyncontext.check_write(&retrieved_entity).map_err(RetrievalError::AccessDenied)?;
                    let _guard = self.snapshot_creation_lock.lock().unwrap();
                    self.get_trx_entity(id).unwrap_or_else(|| self.add_entity(retrieved_entity.snapshot(self.alive.clone())))
                }
            }
        };
        Ok(MutableBorrow::new(entity))
    }

    /// Edit an entity using local model bindings; fails if registration is needed.
    /// Reuses this transaction's existing snapshot when present.
    /// Local edits may outlive node halt; committing them cannot.
    pub fn edit<'rec, 'trx: 'rec, M: Model>(&'trx self, source: &Entity) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        self.dyncontext.schema_resolver().bind_descriptor_local(M::descriptor(), source)?;
        let entity = if let Some(entity) = self.get_trx_entity(&source.id) {
            entity
        } else {
            self.dyncontext.check_write(source).map_err(RetrievalError::AccessDenied)?;
            let _guard = self.snapshot_creation_lock.lock().unwrap();
            self.get_trx_entity(&source.id).unwrap_or_else(|| self.add_entity(source.snapshot(self.alive.clone())))
        };
        Ok(MutableBorrow::new(entity))
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
