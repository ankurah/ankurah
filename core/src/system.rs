use crate::internal::prelude::*;
use ankurah_proto::{Attested, EntityState, Event};
use anyhow::{anyhow, Result};
use proto::PropertyId;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, OnceLock, RwLock,
};
use tokio::sync::Notify;
use tracing::{error, warn};

use crate::collectionset::CollectionSet;
use crate::entity::WeakEntitySet;
use crate::livequery::LiveQueryRegistry;
use crate::notice_info;
use crate::property::{Property, PropertyError};
use crate::reactor::Reactor;
use crate::retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents};
use crate::{property::backend::LWWBackend, value::Value};
pub const SYSTEM_COLLECTION_ID: &str = "_ankurah_system";
pub const PROTECTED_COLLECTIONS: &[&str] = &[SYSTEM_COLLECTION_ID];

/// Tracks the local system root and system-scoped runtime state.
pub struct SystemManager<SE>(Arc<Inner<SE>>);
impl<SE> Clone for SystemManager<SE> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct Inner<SE> {
    collectionset: CollectionSet<SE>,
    collection_map: RwLock<BTreeMap<CollectionId, Entity>>,
    entities: WeakEntitySet,
    durable: bool,
    root: RwLock<Option<Attested<EntityState>>>,
    items: RwLock<Vec<Entity>>,
    loaded: OnceLock<()>,
    loading: Notify,
    system_ready: RwLock<bool>,
    system_ready_notify: Arc<Notify>,
    pending_joins: Arc<AtomicUsize>,
    /// Current resolution generation, absent until the system is ready.
    schema_epoch: Arc<RwLock<Option<SchemaEpoch>>>,
    /// Serializes create, join, reset, and remote work tied to the current root.
    root_write: tokio::sync::Mutex<()>,
    reactor: Reactor,
    /// Queries that must survive a system reset.
    live_queries: LiveQueryRegistry,
}

impl<SE> SystemManager<SE>
where SE: StorageEngine + Send + Sync + 'static
{
    pub(crate) fn new(
        collections: CollectionSet<SE>,
        entities: WeakEntitySet,
        reactor: Reactor,
        durable: bool,
        schema_epoch: Arc<RwLock<Option<SchemaEpoch>>>,
        live_queries: LiveQueryRegistry,
    ) -> Self {
        let me = Self(Arc::new(Inner {
            collectionset: collections,
            entities,
            durable,
            items: RwLock::new(Vec::new()),
            root: RwLock::new(None),
            loaded: OnceLock::new(),
            loading: Notify::new(),
            collection_map: RwLock::new(BTreeMap::new()),
            system_ready: RwLock::new(false),
            system_ready_notify: Arc::new(Notify::new()),
            pending_joins: Arc::new(AtomicUsize::new(0)),
            schema_epoch,
            root_write: tokio::sync::Mutex::new(()),
            reactor,
            live_queries,
        }));
        {
            let me = me.clone();
            crate::task::spawn(async move {
                if let Err(e) = me.load_system_catalog().await {
                    error!("Failed to load system catalog: {}", e);
                }
            });
        }
        me
    }

    pub fn root(&self) -> Option<Attested<EntityState>> { self.0.root.read().unwrap().as_ref().map(|r| r.clone()) }

    /// The system root's entity id, which every non-root genesis binds into
    /// its own id. `None` until this node has created or joined a system.
    pub fn root_id(&self) -> Option<proto::EntityId> { self.0.root.read().unwrap().as_ref().map(|r| r.payload.entity_id) }

    pub fn items(&self) -> Vec<Entity> { self.0.items.read().unwrap().clone() }

    /// Get a storage collection after local system metadata loads.
    pub async fn collection(&self, id: &CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.wait_loaded().await;
        // TODO - update the system catalog to create an entity for this collection
        self.0.collectionset.get(id).await
    }

    /// Returns true if we've successfully initialized or joined a system
    pub fn is_system_ready(&self) -> bool { *self.0.system_ready.read().unwrap() && self.0.pending_joins.load(Ordering::Acquire) == 0 }

    pub(crate) fn is_current_root(&self, state: &Attested<EntityState>) -> bool {
        self.is_system_ready()
            && self.0.root.read().unwrap().as_ref().is_some_and(|root| root.payload.state.head == state.payload.state.head)
    }

    /// This node's current schema epoch, absent until a system is ready.
    pub fn schema_epoch(&self) -> Option<SchemaEpoch> {
        if self.0.pending_joins.load(Ordering::Acquire) != 0 {
            return None;
        }
        *self.0.schema_epoch.read().unwrap()
    }

    pub(crate) async fn lock_root_state(&self) -> tokio::sync::MutexGuard<'_, ()> { self.0.root_write.lock().await }

    /// Hide readiness until every scheduled join has settled.
    pub(crate) fn begin_join(&self) -> PendingJoin { PendingJoin::new(self.0.pending_joins.clone(), self.0.system_ready_notify.clone()) }

    /// Publish a fresh epoch before the node becomes ready.
    fn mark_system_ready(&self) {
        {
            let mut ready = self.0.system_ready.write().unwrap();
            if !*ready {
                *self.0.schema_epoch.write().unwrap() = Some(SchemaEpoch::allocate());
                *ready = true;
            }
        }
        self.0.system_ready_notify.notify_waiters();
    }

    /// Waits until this node has a system root
    pub async fn wait_system_ready(&self) {
        loop {
            if self.is_system_ready() {
                return;
            }
            let notified = self.0.system_ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_system_ready() {
                return;
            }
            notified.await;
        }
    }

    /// Create the system root on a durable node.
    pub async fn create(&self) -> Result<()> {
        if !self.0.durable {
            return Err(anyhow!("Only durable nodes can create a new system"));
        }

        self.wait_loaded().await;
        let _root_write = self.0.root_write.lock().await;

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

        {
            let mut items = self.0.items.write().unwrap();
            items.push(system_entity);
        }
        *self.0.root.write().unwrap() = Some(attested_state);

        self.mark_system_ready();

        Ok(())
    }

    /// Joins an existing system. This should only be called by ephemeral nodes.
    pub async fn join_system(&self, state: Attested<EntityState>) -> Result<(), MutationError> {
        let pending = self.begin_join();
        self.join_with_guard(state, pending).await
    }

    pub(crate) async fn join_with_guard(&self, state: Attested<EntityState>, _pending: PendingJoin) -> Result<(), MutationError> {
        self.wait_loaded().await;

        if self.0.durable {
            warn!("Durable node attempted to join system - this is not allowed");
            return Err(MutationError::General(Box::new(std::io::Error::other("Durable nodes cannot join an existing system"))));
        }

        let _root_write = self.0.root_write.lock().await;

        if let Some(root) = self.root() {
            if root.payload.state.head == state.payload.state.head {
                notice_info!("Found matching root - Node is part of the same system");
                self.mark_system_ready();
                return Ok(());
            }
            tracing::warn!("Mismatched root state during join: local={:?}, remote={:?}", root, state.payload.state.head);
            tracing::info!("Resetting storage to replace mismatched root");
            self.hard_reset_inner().await.map_err(|e| MutationError::General(Box::new(std::io::Error::other(e.to_string()))))?;
        }

        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        self.0.collectionset.get(&collection_id).await?.set_state(state.clone()).await?;
        {
            let mut root = self.0.root.write().expect("Root lock poisoned");
            *root = Some(state);
        }
        self.mark_system_ready();
        Ok(())
    }

    /// Delete every collection and clear all system-scoped runtime state.
    pub async fn hard_reset(&self) -> Result<()> {
        let _root_write = self.0.root_write.lock().await;
        self.hard_reset_inner().await
    }

    async fn hard_reset_inner(&self) -> Result<()> {
        {
            let mut system_ready = self.0.system_ready.write().unwrap();
            *self.0.schema_epoch.write().unwrap() = None;
            *system_ready = false;
        }

        let _live_query_reset = self.0.live_queries.begin_system_reset();
        let _reactor_reset = self.0.reactor.begin_system_reset().await;
        self.0.collectionset.delete_all_collections().await?;

        self.0.items.write().unwrap().clear();
        *self.0.root.write().unwrap() = None;
        self.0.collection_map.write().unwrap().clear();

        Ok(())
    }

    /// Returns true if the local system catalog is loaded
    pub fn is_loaded(&self) -> bool { self.0.loaded.get().is_some() }

    /// Wait for the local system catalog to be loaded
    pub async fn wait_loaded(&self) {
        loop {
            if self.is_loaded() {
                return;
            }

            let notified = self.0.loading.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.is_loaded() {
                return;
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
            let lww_backend = entity.get_backend::<LWWBackend>().expect("LWW Backend should exist");
            if let Some(value) = lww_backend.get(&PropertyId::System(proto::SystemProperty::Item)) {
                let item = proto::sys::Item::from_value(Some(value)).expect("Invalid sys item");

                if let proto::sys::Item::SysRoot = &item {
                    root_state = Some(state);
                }
                entities.push(entity);
            }
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

        if has_root && self.0.durable {
            self.mark_system_ready();
        }

        self.0.loaded.set(()).expect("Loading flag already set");
        self.0.loading.notify_waiters();
        Ok(())
    }
}

pub(crate) struct PendingJoin {
    count: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl PendingJoin {
    fn new(count: Arc<AtomicUsize>, notify: Arc<Notify>) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self { count, notify }
    }
}

impl Drop for PendingJoin {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
        self.notify.notify_waiters();
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
