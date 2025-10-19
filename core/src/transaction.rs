use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ankurah_proto::Event;
use ankurah_proto::{self as proto, EntityId};

use crate::entity::EntityTransaction;
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
pub struct Transaction {
    pub(crate) dyncontext: Option<Arc<dyn TContext + Send + Sync + 'static>>,
    id: proto::TransactionId,
    entities: AppendOnlyVec<Entity>,
    pub(crate) alive: Arc<AtomicBool>,
    pub(crate) entity_trx: Mutex<Option<EntityTransaction>>,
}

#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl Transaction {
    #[wasm_bindgen(js_name = "commit")]
    pub async fn js_commit(mut self) -> Result<(), JsValue> {
        let context = self.dyncontext.take().expect("Transaction already consumed");
        context.commit_local_trx(self).await?;
        Ok(())
    }
}

impl Transaction {
    pub(crate) fn new(dyncontext: Arc<dyn TContext + Send + Sync + 'static>, weak_entity_set: crate::entity::WeakEntitySet) -> Self {
        Self {
            dyncontext: Some(dyncontext),
            id: proto::TransactionId::new(),
            entities: AppendOnlyVec::new(),
            alive: Arc::new(AtomicBool::new(true)),
            entity_trx: Mutex::new(Some(EntityTransaction::new(weak_entity_set))),
        }
    }

    pub(crate) fn add_entity(&self, entity: Entity) -> &Entity {
        let index = self.entities.push(entity);
        &self.entities[index]
    }

    /// Consumes the transaction and returns the transaction id and a list of entities and events to be committed
    pub(crate) fn into_parts(mut self) -> Result<(proto::TransactionId, Vec<(Entity, Event)>, EntityTransaction), MutationError> {
        let mut events = Vec::new();
        for entity in self.entities.iter() {
            if let Some(event) = entity.generate_commit_event()? {
                events.push((entity.clone(), event));
            }
        }
        let entity_trx = self.entity_trx.get_mut().unwrap().take().expect("EntityTransaction already taken");
        Ok((self.id.clone(), events, entity_trx))
    }

    pub async fn create<'rec, 'trx: 'rec, M: Model>(&'trx self, model: &M) -> Result<MutableBorrow<'rec, M::Mutable>, MutationError> {
        let context = self.dyncontext.as_ref().expect("Transaction already consumed");
        let mut entity_trx_guard = self.entity_trx.lock().unwrap();
        let entity_trx = entity_trx_guard.as_mut().expect("EntityTransaction already consumed");
        let entity = context.create_entity(M::collection(), entity_trx);
        model.initialize_new_entity(&entity);
        context.check_write(&entity)?;

        let entity_ref = self.add_entity(entity);
        Ok(MutableBorrow::new(entity_ref))
    }
    fn get_trx_entity(&self, id: &EntityId) -> Option<&Entity> { self.entities.iter().find(|e| e.id == *id) }
    pub async fn get<'rec, 'trx: 'rec, M: Model>(&'trx self, id: &EntityId) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        match self.get_trx_entity(id) {
            Some(entity) => Ok(MutableBorrow::new(entity)),
            None => {
                // go fetch the entity from the context
                let retrieved_entity =
                    self.dyncontext.as_ref().expect("Transaction already consumed").get_entity(*id, &M::collection(), false).await?;
                // double check to make sure somebody didn't add the entity to the trx during the await
                // because we're forking the entity, we need to make sure we aren't adding the same entity twice
                if let Some(entity) = self.get_trx_entity(&retrieved_entity.id) {
                    // if this happens, I don't think we want to refresh the entity, because it's already snapshotted in the trx
                    // and we should leave it that way to honor the consistency model
                    Ok(MutableBorrow::new(entity))
                } else {
                    Ok(MutableBorrow::new(self.add_entity(retrieved_entity.branch(self.alive.clone()))))
                }
            }
        }
    }
    pub fn edit<'rec, 'trx: 'rec, M: Model>(&'trx self, entity: &Entity) -> Result<MutableBorrow<'rec, M::Mutable>, AccessDenied> {
        if let Some(entity) = self.get_trx_entity(&entity.id) {
            return Ok(MutableBorrow::new(entity));
        }
        self.dyncontext.as_ref().expect("Transaction already consumed").check_write(entity)?;

        Ok(MutableBorrow::new(self.add_entity(entity.branch(self.alive.clone()))))
    }

    #[must_use]
    pub async fn commit(mut self) -> Result<(), MutationError> {
        let context = self.dyncontext.take().expect("Transaction already consumed");
        context.commit_local_trx(self).await
    }

    pub fn rollback(self) {
        tracing::info!("trx.rollback");
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
