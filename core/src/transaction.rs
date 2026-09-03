use crate::internal::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ankurah_proto::EntityId;

use crate::context::TContext;
use crate::entity::ProvisionalEntity;
use crate::model::{Model, MutableBorrow};

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
    /// Each created entity's frozen genesis and compiled model declaration.
    pub(crate) genesis_events:
        std::sync::RwLock<std::collections::BTreeMap<EntityId, (proto::Event, &'static crate::schema::ModelStructDescriptor)>>,
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

    /// Mint an entity after registering its model in the current system epoch.
    pub async fn create<'rec, 'trx: 'rec, M: Model>(&'trx self, model: &M) -> Result<MutableBorrow<'rec, M::Mutable>, MutationError> {
        let (model_id, epoch) = self.dyncontext.ensure_registered(M::descriptor()).await?;

        let mut provisional = ProvisionalEntity::new();
        model.initialize_new_entity(&mut provisional, model_id, epoch)?;
        let system = self.dyncontext.system_id().ok_or(MutationError::SystemNotReady)?;
        if self.dyncontext.schema_epoch() != Some(epoch) {
            return Err(crate::schema::registration::RegistrationError::SystemChanged.into());
        }
        let genesis = proto::Event::genesis(M::collection(), Some(system), proto::AuthorId::Unknown, provisional.extract_operations()?);

        let entity = self.dyncontext.create_transaction_entity(M::collection(), &genesis, epoch, self.alive.clone())?;
        self.dyncontext.check_write(&entity)?;

        if self.genesis_events.write().unwrap().insert(entity.id, (genesis, M::descriptor())).is_some() {
            return Err(MutationError::AlreadyExists);
        }

        let entity_ref = self.add_entity(entity);
        Ok(MutableBorrow::new(entity_ref))
    }
    fn get_trx_entity(&self, id: &EntityId) -> Option<&Entity> { self.entities.iter().find(|e| e.id == *id) }
    pub async fn get<'rec, 'trx: 'rec, M: Model>(&'trx self, id: &EntityId) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        let (_, epoch) = self.dyncontext.ensure_registered(M::descriptor()).await?;
        match self.get_trx_entity(id) {
            Some(entity) if entity.schema_epoch() == epoch => Ok(MutableBorrow::new(entity)),
            Some(_) => Err(crate::schema::registration::RegistrationError::SystemChanged.into()),
            None => {
                let retrieved_entity = self.dyncontext.get_entity(&M::collection(), *id, false).await?;
                if retrieved_entity.schema_epoch() != epoch {
                    return Err(crate::schema::registration::RegistrationError::SystemChanged.into());
                }
                // Reuse an entity inserted while the fetch was in flight.
                if let Some(entity) = self.get_trx_entity(&retrieved_entity.id) {
                    if entity.schema_epoch() != epoch {
                        return Err(crate::schema::registration::RegistrationError::SystemChanged.into());
                    }
                    Ok(MutableBorrow::new(entity))
                } else {
                    Ok(MutableBorrow::new(self.add_entity(retrieved_entity.snapshot(self.alive.clone()))))
                }
            }
        }
    }
    pub fn edit<'rec, 'trx: 'rec, M: Model>(&'trx self, entity: &Entity) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        self.dyncontext.bind_descriptor(M::descriptor(), entity.schema_epoch())?;
        if let Some(entity) = self.get_trx_entity(&entity.id) {
            return Ok(MutableBorrow::new(entity));
        }
        self.dyncontext.check_write(entity).map_err(RetrievalError::AccessDenied)?;

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
