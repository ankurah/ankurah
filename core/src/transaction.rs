use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ankurah_proto::{self as proto, EntityId};

use crate::error::RetrievalError;
use crate::policy::AccessDenied;
use crate::{
    context::TContext,
    entity::{Entity, ProvisionalEntity},
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
    /// Each `create()`'s minted genesis, by entity id -- the only copy until
    /// commit persists it; the entity cannot regenerate it. The entity itself
    /// contributes at most one Update to the commit: the edits made after
    /// `create()`, if any.
    pub(crate) genesis_events: std::sync::RwLock<std::collections::BTreeMap<EntityId, proto::Event>>,
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
        }
    }

    pub(crate) fn add_entity(&self, entity: Entity) -> &Entity {
        let index = self.entities.push(entity);
        &self.entities[index]
    }

    /// Mint an entity from `model`'s initial values and return it under the id
    /// its own genesis event derives.
    ///
    /// An entity cannot be created before the node knows its system root: the
    /// genesis binds the root into the id, so there is no id to hand back until
    /// the root exists. On a node that has neither created nor joined a system
    /// this refuses with [`MutationError::SystemNotReady`], which a caller can
    /// retry once the handshake with a durable peer has established the system.
    pub async fn create<'rec, 'trx: 'rec, M: Model>(&'trx self, model: &M) -> Result<MutableBorrow<'rec, M::Mutable>, MutationError> {
        // First-use registration: the new entity's membership asserts the
        // model's durable identity, so it must exist before the entity does.
        let model_id = self.dyncontext.ensure_registered(M::descriptor()).await?;

        // The initial values are staged in a vessel that has no identity of its
        // own: the entity id does not exist until these operations have been
        // frozen into the genesis preimage. Each field stages under the
        // durable identity its descriptor cell resolved for this node's
        // current epoch (populated by the registration gate above).
        let epoch = self.dyncontext.schema_epoch().ok_or(MutationError::SystemNotReady)?;
        let mut provisional = ProvisionalEntity::new();
        model.initialize_new_entity(&mut provisional, model_id, epoch).map_err(|e| MutationError::General(Box::new(e)))?;
        let system = self.dyncontext.system_id().ok_or(MutationError::SystemNotReady)?;
        let genesis = proto::Event::genesis(M::collection(), Some(system), proto::AuthorId::Unknown, provisional.extract_operations()?);

        // Insert the resident primary under the derived id, and take the
        // transaction entity whose baseline is that genesis, so later edits
        // parent onto it and are extracted separately.
        let entity = self.dyncontext.create_transaction_entity(M::collection(), &genesis, self.alive.clone())?;
        self.dyncontext.check_write(&entity)?;

        // Store the already-extracted genesis exactly once. Commit must never
        // ask this entity to reconstruct those operations.
        if self.genesis_events.write().unwrap().insert(entity.id, genesis).is_some() {
            return Err(MutationError::AlreadyExists);
        }

        let entity_ref = self.add_entity(entity);
        Ok(MutableBorrow::new(entity_ref))
    }
    fn get_trx_entity(&self, id: &EntityId) -> Option<&Entity> { self.entities.iter().find(|e| e.id == *id) }
    pub async fn get<'rec, 'trx: 'rec, M: Model>(&'trx self, id: &EntityId) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        match self.get_trx_entity(id) {
            Some(entity) => Ok(MutableBorrow::new(entity)),
            None => {
                // go fetch the entity from the context
                let retrieved_entity = self.dyncontext.get_entity(*id, &M::collection(), false).await?;
                // double check to make sure somebody didn't add the entity to the trx during the await
                // because we're forking the entity, we need to make sure we aren't adding the same entity twice
                if let Some(entity) = self.get_trx_entity(&retrieved_entity.id) {
                    // if this happens, I don't think we want to refresh the entity, because it's already snapshotted in the trx
                    // and we should leave it that way to honor the consistency model
                    Ok(MutableBorrow::new(entity))
                } else {
                    Ok(MutableBorrow::new(self.add_entity(retrieved_entity.snapshot(self.alive.clone()))))
                }
            }
        }
    }
    pub fn edit<'rec, 'trx: 'rec, M: Model>(&'trx self, entity: &Entity) -> Result<MutableBorrow<'rec, M::Mutable>, AccessDenied> {
        if let Some(entity) = self.get_trx_entity(&entity.id) {
            return Ok(MutableBorrow::new(entity));
        }
        self.dyncontext.check_write(entity)?;

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
