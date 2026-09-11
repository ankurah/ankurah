use crate::internal::prelude::*;
use crate::{connector::PeerConnectionError, error::NodeHaltReason};
use ankurah_proto::{Attested, EntityState, Event};
use ankurah_signals::{Mut, Read, Wait};
use anyhow::{anyhow, Result};
use proto::PropertyId;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock, RwLock,
};
use tokio::sync::Notify;

use crate::collectionset::CollectionSet;
use crate::entity::WeakEntitySet;
use crate::property::{Property, PropertyError};
use crate::retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents};
use crate::{property::backend::LWWBackend, value::Value};
pub const SYSTEM_COLLECTION_ID: &str = "_ankurah_system";
pub const PROTECTED_COLLECTIONS: &[&str] = &[SYSTEM_COLLECTION_ID];

/// Tracks the local system root and system-scoped runtime state.
pub struct SystemManager<SE>(Arc<Inner<SE>>);
impl<SE> Clone for SystemManager<SE> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SystemEpochError {
    #[error("system is not ready")]
    NotReady,
    #[error("node halted: {0}")]
    Halted(#[from] NodeHaltReason),
}

impl From<SystemEpochError> for MutationError {
    fn from(error: SystemEpochError) -> Self {
        match error {
            SystemEpochError::NotReady => Self::SystemNotReady,
            SystemEpochError::Halted(error) => Self::NodeHalted(error),
        }
    }
}

struct Inner<SE> {
    collectionset: CollectionSet<SE>,
    entities: WeakEntitySet,
    durable: bool,
    root: RwLock<Option<Attested<EntityState>>>,
    items: RwLock<Vec<Entity>>,
    loaded: OnceLock<bool>,
    loading: Notify,
    system_ready: RwLock<bool>,
    allow_system_replacement: AtomicBool,
    node_state: Mut<NodeState>,
    /// Serializes initial system creation/adoption, never ordinary node operations.
    root_write: Arc<tokio::sync::Mutex<()>>,
}

impl<SE> SystemManager<SE>
where SE: StorageEngine + Send + Sync + 'static
{
    pub(crate) fn new(collections: CollectionSet<SE>, entities: WeakEntitySet, durable: bool) -> Self {
        let me = Self(Arc::new(Inner {
            collectionset: collections,
            entities,
            durable,
            items: RwLock::new(Vec::new()),
            root: RwLock::new(None),
            loaded: OnceLock::new(),
            loading: Notify::new(),
            system_ready: RwLock::new(false),
            allow_system_replacement: AtomicBool::new(false),
            node_state: Mut::new(NodeState::Uninitialized),
            root_write: Arc::new(tokio::sync::Mutex::new(())),
        }));
        {
            let me = me.clone();
            crate::task::spawn(async move {
                if let Err(e) = me.load_system_catalog().await {
                    me.halt(NodeHaltReason::SystemLoad(e.to_string()));
                }
            });
        }
        me
    }

    pub fn root(&self) -> Option<Attested<EntityState>> { self.0.root.read().unwrap().as_ref().map(|r| r.clone()) }

    /// The system root's entity id, which every non-root genesis binds into
    /// its own id. `None` until this node has created or adopted a system.
    pub fn root_id(&self) -> Option<proto::EntityId> { self.0.root.read().unwrap().as_ref().map(|r| r.payload.entity_id) }

    pub fn items(&self) -> Vec<Entity> { self.0.items.read().unwrap().clone() }

    /// Get a storage collection after local system metadata loads.
    pub async fn collection(&self, id: &CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.wait_loaded().await?;
        // TODO - update the system catalog to create an entity for this collection
        self.0.collectionset.get(id).await
    }

    /// Whether a system is initialized and the node has not halted.
    pub fn is_system_ready(&self) -> bool { self.check_not_halted().is_ok() && *self.0.system_ready.read().unwrap() }

    /// This node's immutable system epoch, absent until a system is ready or after a hard reset.
    pub fn system_epoch(&self) -> Option<SystemEpoch> { self.0.system_ready.read().unwrap().then_some(self.0.entities.system_epoch()) }

    pub(crate) fn require_system_ready(&self) -> Result<SystemEpoch, SystemEpochError> {
        self.check_not_halted()?;
        self.system_epoch().ok_or(SystemEpochError::NotReady)
    }

    pub(crate) fn set_allow_system_replacement(&self, allow: bool) { self.0.allow_system_replacement.store(allow, Ordering::Release); }

    pub(crate) fn node_state(&self) -> Read<NodeState> { self.0.node_state.read() }

    /// Publish successful initialization without reviving a halted node.
    pub(crate) fn mark_running(&self) {
        self.0.node_state.update(|state| {
            if matches!(state, NodeState::Startup) {
                *state = NodeState::Running;
            }
        });
    }

    /// Reject terminal halt without blocking the work that initializes this node.
    pub(crate) fn check_not_halted(&self) -> Result<(), NodeHaltReason> {
        self.0.node_state.with(|state| state.halt_reason().cloned().map_or(Ok(()), Err))
    }

    /// Publish terminal halt; services observe the state and stop their own work.
    /// In-flight storage work may still finish.
    pub(crate) fn halt(&self, halt_reason: NodeHaltReason) -> NodeHaltReason {
        let halt_reason = self.0.node_state.update(|state| match state {
            NodeState::Halted(reason) => reason.clone(),
            NodeState::Uninitialized | NodeState::Startup | NodeState::Running => {
                *state = NodeState::Halted(halt_reason.clone());
                halt_reason
            }
        });
        self.0.loading.notify_waiters();
        halt_reason
    }

    /// Publish readiness after creating, adopting, or loading the persisted system root.
    fn mark_system_ready(&self) {
        self.0.node_state.update(|state| {
            *self.0.system_ready.write().unwrap() = true;
            if matches!(state, NodeState::Uninitialized) {
                *state = NodeState::Startup;
            }
        });
    }

    /// Wait for system initialization, or return the reason the node halted.
    pub async fn wait_system_ready(&self) -> Result<(), NodeHaltReason> {
        self.node_state()
            .wait_for(|state| match state {
                NodeState::Uninitialized => None,
                NodeState::Startup | NodeState::Running => Some(Ok(())),
                NodeState::Halted(reason) => Some(Err(reason.clone())),
            })
            .await
    }

    /// Create the system root on a durable node.
    pub async fn create(&self) -> Result<()> {
        if !self.0.durable {
            return Err(anyhow!("Only durable nodes can create a new system"));
        }

        self.wait_loaded().await?;
        let _root_write = self.0.root_write.lock().await;
        self.check_not_halted()?;

        {
            let items = self.0.items.read().unwrap();
            if !items.is_empty() {
                return Err(anyhow!("System root already exists"));
            }
        }

        // TODO - see if we can use the Model derive macro for a SysCatalogItem model rather than doing this manually
        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        // The root genesis alone has no parent system to bind.
        let mut provisional = crate::entity::ProvisionalEntity::new();
        provisional.add_membership(proto::ModelId::System(proto::SystemModel::System));
        let lww_backend = provisional.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        lww_backend.set(PropertyId::System(proto::SystemProperty::Item), proto::sys::Item::SysRoot.into_value()?);

        let event = proto::Event::genesis(collection_id.clone(), None, proto::AuthorId::Unknown, provisional.extract_operations()?);
        let system_entity = self.0.entities.create_root(collection_id.clone(), event.entity_id);

        let event_getter = LocalEventGetter::new(storage.clone(), true);
        event_getter.stage_event(event.clone());

        system_entity.apply_event(&event_getter, &event).await?;
        let attested_event: Attested<Event> = event.clone().into();
        event_getter.commit_event(&attested_event).await?;
        let attested_state: Attested<EntityState> = system_entity.to_entity_state()?.into();
        storage.set_state(attested_state.clone()).await?;

        self.0.items.write().unwrap().push(system_entity);
        *self.0.root.write().unwrap() = Some(attested_state);

        self.mark_system_ready();

        Ok(())
    }

    /// Adopt a system once; a different system halts this node and optionally wipes its storage.
    pub async fn adopt_system(&self, state: Attested<EntityState>) -> Result<(), PeerConnectionError> {
        self.wait_loaded().await?;
        if self.0.durable {
            return Err(PeerConnectionError::InvalidSystem("durable nodes create their own system".into()));
        }
        if state.payload.collection != CollectionId::fixed_name(SYSTEM_COLLECTION_ID) || state.payload.state.head.is_empty() {
            return Err(PeerConnectionError::InvalidSystem("expected a materialized system root".into()));
        }
        let candidate =
            crate::entity::TemporaryEntity::new(state.payload.entity_id, state.payload.collection.clone(), &state.payload.state)
                .map_err(|error| PeerConnectionError::InvalidSystem(error.to_string()))?;
        let item = proto::sys::Item::from_value(crate::selection::filter::Filterable::value(
            &candidate,
            &PropertyId::System(proto::SystemProperty::Item),
        ))
        .map_err(|error| PeerConnectionError::InvalidSystem(error.to_string()))?;
        if !matches!(item, proto::sys::Item::SysRoot) {
            return Err(PeerConnectionError::InvalidSystem("expected a system root item".into()));
        }

        let root_write = self.0.root_write.clone().lock_owned().await;
        self.check_not_halted()?;
        if let Some(current) = self.root_id() {
            let offered = state.payload.entity_id;
            if current == offered {
                self.mark_system_ready();
                return Ok(());
            }
            let reset = self.0.allow_system_replacement.load(Ordering::Acquire);
            let reason = self.halt(NodeHaltReason::SystemReplacement { current, proposed: offered });
            drop(root_write);
            if reset {
                let system = self.clone();
                let (finished, completion) = tokio::sync::oneshot::channel();
                // Halting can cancel the connection; the storage wipe must finish independently.
                crate::task::spawn(async move {
                    let result = system.hard_reset().await.map_err(|error| PeerConnectionError::SystemReset(error.to_string()));
                    if let Err(error) = &result {
                        tracing::error!(%error, "System replacement storage wipe failed");
                    }
                    let _ = finished.send(result);
                });
                completion.await.expect("system reset task panicked")?;
            }
            return Err(reason.into());
        }

        // Finish initial adoption even if its connection is canceled.
        let system = self.clone();
        let (finished, completion) = tokio::sync::oneshot::channel();
        crate::task::spawn(async move {
            let result = async { system.0.collectionset.get(&state.payload.collection).await?.set_state(state.clone()).await }.await;
            if result.is_ok() {
                *system.0.root.write().unwrap() = Some(state);
                system.mark_system_ready();
            }
            drop(root_write);
            let result = match result {
                Ok(_) => Ok(()),
                Err(error) => Err(system.halt(NodeHaltReason::SystemLoad(error.to_string())).into()),
            };
            let _ = finished.send(result);
        });
        completion.await.expect("system adoption task panicked")
    }

    /// Delete all collections, including the system catalog, and clear local system metadata.
    /// Does not restart the node or reset its reactor/livequeries.
    /// Stop outstanding storage work before wiping; await completion before reusing the store.
    pub async fn hard_reset(&self) -> Result<()> {
        self.0.collectionset.delete_all_collections().await?;
        self.0.items.write().unwrap().clear();
        *self.0.root.write().unwrap() = None;
        *self.0.system_ready.write().unwrap() = false;
        Ok(())
    }

    /// Whether the persisted system root and items have loaded; independent of schema catalog readiness.
    pub fn is_loaded(&self) -> bool { self.0.loaded.get().is_some() }

    /// Wait for local system loading, or return the node's terminal halt.
    pub async fn wait_loaded(&self) -> Result<(), NodeHaltReason> {
        loop {
            let notified = self.0.loading.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.check_not_halted()?;
            if self.is_loaded() {
                return Ok(());
            }

            notified.await;
        }
    }

    async fn load_system_catalog(&self) -> Result<()> {
        if self.is_loaded() {
            return Err(anyhow!("System catalog already loaded"));
        }

        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        let mut entities = Vec::new();
        let mut root_state = None;

        let state_getter = LocalStateGetter::new(storage.clone());
        let event_getter = LocalEventGetter::new(storage.clone(), self.0.durable);

        for state in
            storage.fetch_states(&ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }).await?
        {
            let (_entity_changed, entity) = self
                .0
                .entities
                .with_state(&state_getter, &event_getter, state.payload.entity_id, collection_id.clone(), state.payload.state.clone())
                .await?;
            let lww_backend = entity.get_backend::<LWWBackend>()?;
            let item = proto::sys::Item::from_value(lww_backend.get(&PropertyId::System(proto::SystemProperty::Item)))?;
            if let proto::sys::Item::SysRoot = &item {
                if state.payload.state.head.is_empty() {
                    return Err(anyhow!("persisted system root has no head"));
                }
                if root_state.is_some() {
                    return Err(anyhow!("multiple persisted system roots"));
                }
                root_state = Some(state);
            }
            entities.push(entity);
        }

        {
            let mut items = self.0.items.write().unwrap();
            items.extend(entities);
        }

        let has_root = root_state.is_some();
        {
            let mut root = self.0.root.write().expect("Root lock poisoned");
            *root = root_state;
        }

        if has_root {
            self.mark_system_ready();
        }

        self.0.loaded.set(has_root).expect("Loading flag already set");
        self.0.loading.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{node::Node, policy::PermissiveAgent, storage::StorageCollection, test_utils::TestStorage};

    #[tokio::test]
    async fn adoption_waits_for_the_initial_persisted_root_load() {
        let storage = Arc::new(TestStorage::default());
        let seed = Node::new_durable(storage.clone(), PermissiveAgent::new());
        seed.system.create().await.unwrap();
        let root = seed.system.root_id().unwrap();
        drop(seed);

        let table = storage.table(&CollectionId::fixed_name(SYSTEM_COLLECTION_ID));
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        *table.hold_fetch.lock().unwrap() = Some((entered, release_rx));
        let offered = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        offered.system.create().await.unwrap();
        let node = Node::new(storage, PermissiveAgent::new());
        entered_rx.await.unwrap();
        let adoption = node.system.adopt_system(offered.system.root().unwrap());
        tokio::pin!(adoption);
        assert!(futures::poll!(&mut adoption).is_pending());
        release.send(()).unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), adoption).await.unwrap().unwrap_err();
        assert!(matches!(error, PeerConnectionError::NodeHalted(NodeHaltReason::SystemReplacement { current, .. }) if current == root));
        assert!(node.system.is_loaded());
        assert!(!node.system.is_system_ready());
        assert_eq!(node.system.root_id(), Some(root));
        assert!(node.state().value().halt_reason().is_some());
        assert!(table.get_state(root).await.is_ok());
    }

    #[tokio::test]
    async fn cancelled_connection_does_not_cancel_the_storage_wipe() {
        let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        server.system.create().await.unwrap();
        let storage = Arc::new(TestStorage::default());
        let client = Node::new(storage.clone(), PermissiveAgent::new());
        client.system.adopt_system(server.system.root().unwrap()).await.unwrap();
        let replacement = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        replacement.system.create().await.unwrap();
        client.set_allow_system_replacement(true);

        let mut adoption = Box::pin(client.system.adopt_system(replacement.system.root().unwrap()));
        assert!(futures::poll!(&mut adoption).is_pending());
        assert!(client.state().value().halt_reason().is_some());
        drop(adoption);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while client.system.root_id().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let reopened = Node::new(storage, PermissiveAgent::new());
        reopened.system.wait_loaded().await.unwrap();
        assert!(reopened.system.root_id().is_none());
        reopened.system.adopt_system(replacement.system.root().unwrap()).await.unwrap();
        assert_eq!(reopened.system.root_id(), replacement.system.root_id());
    }

    #[tokio::test]
    async fn unreadable_persisted_root_fails_readiness_instead_of_hanging() {
        let storage = Arc::new(TestStorage::default());
        let seed = Node::new_durable(storage.clone(), PermissiveAgent::new());
        seed.system.create().await.unwrap();
        let mut root = seed.system.root().unwrap();
        root.payload.state.state_buffers.0.insert("lww".into(), vec![0xff]);
        storage.table(&root.payload.collection).set_state(root).await.unwrap();
        drop(seed);

        let node = Node::new_durable(storage, PermissiveAgent::new());
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), node.system.wait_system_ready()).await.unwrap().unwrap_err();
        assert!(matches!(error, NodeHaltReason::SystemLoad(_)));
        assert_eq!(node.context_async(crate::policy::DEFAULT_CONTEXT).await.err(), Some(error.clone()));
        assert_eq!(node.system.wait_loaded().await, Err(error));
    }

    #[tokio::test]
    async fn cancelled_connection_does_not_leave_a_half_adopted_system() {
        let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        server.system.create().await.unwrap();
        let storage = Arc::new(TestStorage::default());
        let client = Node::new(storage.clone(), PermissiveAgent::new());
        client.system.wait_loaded().await.unwrap();
        let table = storage.table(&CollectionId::fixed_name(SYSTEM_COLLECTION_ID));
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        *table.hold_set_state.lock().unwrap() = Some((entered, release_rx));

        let mut adoption = Box::pin(client.system.adopt_system(server.system.root().unwrap()));
        assert!(futures::poll!(&mut adoption).is_pending());
        tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx).await.unwrap().unwrap();
        drop(adoption);
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), client.system.wait_system_ready()).await.unwrap().unwrap();
        assert_eq!(client.system.root_id(), server.system.root_id());
        assert!(table.get_state(server.system.root_id().unwrap()).await.is_ok());
        assert!(client.state().value().halt_reason().is_none());
    }
}

impl Property for proto::sys::Item {
    const VALUE_TYPE: &'static str = "string";

    fn into_value(&self) -> std::result::Result<Option<Value>, crate::property::PropertyError> {
        Ok(Some(Value::String(
            serde_json::to_string(self).map_err(|_| PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })?,
        )))
    }

    fn from_value(value: Option<Value>) -> std::result::Result<Self, crate::property::PropertyError> {
        if let Some(Value::String(string)) = value {
            let item: proto::sys::Item = serde_json::from_str(&string)
                .map_err(|_| PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })?;
            Ok(item)
        } else {
            Err(PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })
        }
    }
}
