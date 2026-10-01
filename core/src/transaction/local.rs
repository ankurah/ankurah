use crate::internal::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ankurah_proto::{Attested, EntityId, EntityState, Event};

use crate::context::{ContextAuth, DynContextInner};
use crate::entity::{LocalTrxEntity, RemoteTrxEntity};
use crate::model::{Model, MutableBorrow};
use crate::node::event_admissibility::check_genesis_membership;
use crate::policy::ContextPolicy;
use crate::reactor::ChangeNotification;
use crate::retrieval::{LocalEventGetter, LocalStateGetter};
use crate::storage::StorageTransaction;
use crate::util::retry::retry_on;

use append_only_vec::AppendOnlyVec;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct Transaction {
    pub(crate) dyncontext: Arc<dyn DynContextInner + Send + Sync + 'static>,
    pub(crate) id: proto::TransactionId,
    pub(crate) entities: AppendOnlyVec<LocalTrxEntity>,
    /// Prevents concurrent get/edit calls from appending separate snapshots of the same EntityId
    /// to `entities`; AppendOnlyVec makes appends thread-safe, but does not enforce uniqueness.
    snapshot_creation_lock: Mutex<()>,
    pub(crate) alive: Arc<AtomicBool>,
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
        }
    }

    pub(crate) fn add_entity(&self, entity: LocalTrxEntity) -> &LocalTrxEntity {
        let index = self.entities.push(entity);
        &self.entities[index]
    }

    /// Mint an entity after registering its model in the current system epoch.
    pub async fn create<'rec, 'trx: 'rec, M: Model>(&'trx self, model: &M) -> Result<MutableBorrow<'rec, M::Mutable>, MutationError> {
        let (model_id, epoch) = self.dyncontext.schema_resolver().ensure_registered(M::descriptor()).await?;

        let system = self.dyncontext.system_id().ok_or(MutationError::SystemNotReady)?;
        let entity = LocalTrxEntity::new(Some(system), proto::AuthorId::Unknown, epoch, self.alive.clone());
        model.initialize_new_entity(&entity, model_id, epoch)?;
        self.dyncontext.check_write(&entity.read())?;

        let entity_ref = self.add_entity(entity);
        Ok(MutableBorrow::new(entity_ref)?)
    }
    fn get_trx_entity(&self, id: &EntityId) -> Option<&LocalTrxEntity> { self.entities.iter().find(|e| e.assigned_id() == Some(*id)) }

    /// Retrieve an entity for editing, registering the model locally or remotely if needed.
    /// Reuses this transaction's existing snapshot when present.
    pub async fn get<'rec, 'trx: 'rec, M: Model>(&'trx self, id: &EntityId) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        let (model, _) = self.dyncontext.schema_resolver().ensure_registered(M::descriptor()).await?;
        let source = match self.get_trx_entity(id) {
            Some(entity) => entity.read(),
            None => self.dyncontext.get_entity(*id, false).await?,
        };
        self.authorize_edit(&model, &source)?;
        let entity = {
            let _guard = self.snapshot_creation_lock.lock().unwrap();
            match self.get_trx_entity(id) {
                Some(entity) => entity,
                None => self.add_entity(LocalTrxEntity::edit(&source, proto::AuthorId::Unknown, self.alive.clone())?),
            }
        };
        MutableBorrow::new(entity)
    }

    /// Edit a committed entity using local model bindings; fails if registration is needed.
    /// Reuses this transaction's existing snapshot when present.
    /// Local edits may outlive node halt; committing them cannot.
    pub fn edit<'rec, 'trx: 'rec, M: Model>(&'trx self, source: &Entity) -> Result<MutableBorrow<'rec, M::Mutable>, RetrievalError> {
        let resident = Entity::from_resident(source.resident()?);
        let model = self.dyncontext.schema_resolver().bind_descriptor_local(M::descriptor(), &resident)?;
        self.authorize_edit(&model, &resident)?;
        let entity = if let Some(entity) = self.get_trx_entity(&resident.id()) {
            entity
        } else {
            let _guard = self.snapshot_creation_lock.lock().unwrap();
            match self.get_trx_entity(&resident.id()) {
                Some(entity) => entity,
                None => self.add_entity(LocalTrxEntity::edit(&resident, proto::AuthorId::Unknown, self.alive.clone())?),
            }
        };
        MutableBorrow::new(entity)
    }

    /// Check that the typed view applies and this context may edit the entity.
    fn authorize_edit(&self, model: &ModelId, entity: &Entity) -> Result<(), RetrievalError> {
        if !entity.has_membership(model) {
            return Err(RetrievalError::MissingComponent { entity_id: entity.id(), model_id: *model });
        }
        self.dyncontext.check_write(entity)?;
        Ok(())
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

impl Transaction {
    /// Validate and commit the transaction, then publish its entity changes.
    /// Privileged contexts bypass policy, not epoch or event-validity checks.
    /// The public `commit` reaches this through the context, which supplies the typed node and its auth.
    pub(crate) async fn commit_with<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        auth: &ContextAuth<SessionSet<PA::ContextData>>,
    ) -> Result<Vec<Event>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let epoch = node.system.require_system_ready()?;

        if self.alive.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return Err(MutationError::General("Transaction already committed or rolled back".into()));
        }

        let policy = ContextPolicy::new(&node.policy_agent, auth.clone());

        let mut entity_events = Vec::new();
        // Prepare once, outside the retry loop: every attempt re-applies and relays these exact events.
        for entity in self.entities.iter() {
            if entity.system_epoch() != epoch {
                return Err(MutationError::ForeignEntity);
            }
            let events = entity.prepare_events()?;
            for event in &events {
                event.payload.validate_structure()?;
                check_genesis_membership(&event.payload)?;
            }
            if !events.is_empty() {
                entity_events.push((entity, events));
            } else {
                // An unchanged edit still returns its views to the resident entity.
                entity.rollback();
            }
        }

        let event_getter = LocalEventGetter::new(node.storage.clone(), node.durable);
        let state_getter = LocalStateGetter::new(node.storage.clone());
        let mut relayed = false;
        retry_on!(MutationError::WriteConflict, {
            let mut storage_trx = node.storage.transaction();
            let mut attested_events = Vec::new();
            let mut forks = Vec::new();
            for (entity, events) in &entity_events {
                // TODO(#509): Finalize the existing transaction fork; rebase it only on write conflicts.
                // A subscription echo may already have committed this creation locally.
                let entity_after = RemoteTrxEntity::for_event(&node.entities, &state_getter, &event_getter, &events[0].payload).await?;
                let entity_before = entity_after.snapshot();
                let after = entity_after.read();
                for event in events {
                    let expected_head = entity_after.head();
                    let mut attested = event.clone();
                    entity_after
                        .apply_event(&event_getter, &mut attested, |event| policy.check_write_event(node, &entity_before, &after, event))
                        .await?;
                    storage_trx.add_events(std::slice::from_ref(&attested)).await?;
                    let state = EntityState { entity_id: entity.id(), state: entity_after.to_state()? };
                    let attestation = policy.attest_state(node, &state);
                    storage_trx.set_state(&expected_head, &Attested::opt(state, attestation)).await?;

                    attested_events.push(attested);
                }
                forks.push((entity, entity_after));
            }

            if !relayed {
                if let ContextAuth::Sessions(sessions) = auth {
                    node.relay_to_required_peers(&sessions.write_credential()?, self.id.clone(), &attested_events).await?;
                }
                relayed = true;
            }
            let publication = node.commit_publication_lock.lock().await;
            storage_trx.commit().await?.committed()?;
            let mut changes = Vec::new();
            for (entity, fork) in forks {
                let change = fork.commit(&node.entities, &event_getter).await?;
                entity.committed(change.entity())?;
                if !change.events().is_empty() {
                    changes.push(change);
                }
            }
            drop(publication);
            node.reactor.notify_change(changes).await;
            Ok(attested_events.into_iter().map(|event| event.payload).collect())
        })
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Mark transaction as no longer alive when dropped
        self.alive.store(false, Ordering::Release);
        for entity in self.entities.iter() {
            entity.rollback();
        }
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
    pub fn uniffi_rollback(&self) {
        self.alive.store(false, Ordering::Release);
        for entity in self.entities.iter() {
            entity.rollback();
        }
    }
}
