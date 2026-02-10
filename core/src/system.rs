use ankurah_proto::{self as proto, Attested, CollectionId, EntityState, Event};
use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock, RwLock};
use tokio::sync::Notify;
use tracing::{error, warn};

use crate::collectionset::CollectionSet;
use crate::entity::{Entity, WeakEntitySet};
use crate::error::MutationError;
use crate::error::RetrievalError;
use crate::notice_info;
use crate::policy::PolicyAgent;
use crate::property::{Property, PropertyError};
use crate::reactor::Reactor;
use crate::retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents};
use crate::storage::{StorageCollectionWrapper, StorageEngine};
use crate::{property::backend::LWWBackend, value::Value};
pub const SYSTEM_COLLECTION_ID: &str = "_ankurah_system";
pub const PROTECTED_COLLECTIONS: &[&str] = &[SYSTEM_COLLECTION_ID];

/// System catalog manager for storing various metadata about the system
/// * root clock
/// * valid collections (TODO)
/// * property definitions (TODO)

pub struct SystemManager<SE, PA>(Arc<Inner<SE, PA>>);
impl<SE, PA> Clone for SystemManager<SE, PA> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct Inner<SE, PA> {
    collectionset: CollectionSet<SE>,
    collection_map: RwLock<BTreeMap<CollectionId, Entity>>,
    entities: WeakEntitySet,
    durable: bool,
    root: RwLock<Option<Attested<EntityState>>>,
    items: RwLock<Vec<Entity>>,
    loaded: OnceLock<()>,
    loading: Notify,
    system_ready: RwLock<bool>,
    system_ready_notify: Notify,
    reactor: Reactor,
    _phantom: PhantomData<PA>,
}

impl<SE, PA> SystemManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn new(collections: CollectionSet<SE>, entities: WeakEntitySet, reactor: Reactor, durable: bool) -> Self {
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
            system_ready_notify: Notify::new(),
            reactor,
            _phantom: PhantomData,
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

    pub fn items(&self) -> Vec<Entity> { self.0.items.read().unwrap().clone() }

    /// get an existing collection if it's defined in the system catalog, else insert a SysItem::Collection
    /// then return collections.get to get the StorageCollectionWrapper
    pub async fn collection(&self, id: &CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.wait_loaded().await;
        // TODO - update the system catalog to create an entity for this collection

        // Return the collection wrapper
        self.0.collectionset.get(id).await
    }

    /// Returns true if we've successfully initialized or joined a system
    pub fn is_system_ready(&self) -> bool { *self.0.system_ready.read().unwrap() }

    /// Waits until we've successfully initialized or joined a system
    pub async fn wait_system_ready(&self) {
        if !self.is_system_ready() {
            self.0.system_ready_notify.notified().await;
        }
    }

    /// Creates a new system root. This should only be called once per system by durable nodes
    /// The rest of the nodes must "join" this system.
    pub async fn create(&self) -> Result<()> {
        if !self.0.durable {
            return Err(anyhow!("Only durable nodes can create a new system"));
        }

        // Wait for local system catalog to be loaded
        self.wait_loaded().await;

        {
            let items = self.0.items.read().unwrap();
            if !items.is_empty() {
                return Err(anyhow!("System root already exists"));
            }
        }

        // TODO - see if we can use the Model derive macro for a SysCatalogItem model rather than doing this manually
        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        let system_entity = self.0.entities.create(collection_id.clone());

        let lww_backend = system_entity.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        lww_backend.set("item".into(), proto::sys::Item::SysRoot.into_value()?);

        let event = system_entity.generate_commit_event()?.ok_or(anyhow!("Expected event"))?;

        // Stage the event, apply, then commit
        let event_getter = LocalEventGetter::new(storage.clone());
        event_getter.stage_event(event.clone());

        // Apply the creation event so LWW values are tagged with event_id before serialization.
        system_entity.apply_event(&event_getter, &event).await?;
        let attested_event: Attested<Event> = event.clone().into();
        event_getter.commit_event(&attested_event).await?;
        // Now get the entity state after the head is updated
        let attested_state: Attested<EntityState> = system_entity.to_entity_state()?.into();
        storage.set_state(attested_state.clone()).await?;

        // Update our system state
        let mut items = self.0.items.write().unwrap();
        items.push(system_entity);
        *self.0.root.write().unwrap() = Some(attested_state);

        // Mark system as ready and notify waiters
        *self.0.system_ready.write().unwrap() = true;
        self.0.system_ready_notify.notify_waiters();

        Ok(())
    }

    /// Joins an existing system. This should only be called by ephemeral nodes.
    pub async fn join_system(&self, state: Attested<EntityState>) -> Result<(), MutationError> {
        // Wait for catalog to be loaded before proceeding
        self.wait_loaded().await;

        // If node is durable, fail - durable nodes should not join an existing system
        if self.0.durable {
            warn!("Durable node attempted to join system - this is not allowed");
            return Err(MutationError::General(Box::new(std::io::Error::other("Durable nodes cannot join an existing system"))));
        }

        let root_state = self.root();

        // If we have a matching root, we're already in sync - just mark ready and return
        if let Some(root) = root_state {
            if root.payload.state.head == state.payload.state.head {
                notice_info!("Found matching root - Node is part of the same system");
                *self.0.system_ready.write().unwrap() = true;
                self.0.system_ready_notify.notify_waiters();
                return Ok(());
            }
            tracing::warn!("Mismatched root state during join: local={:?}, remote={:?}", root, state.payload.state.head);

            // Only reset storage if we have a root that needs to be replaced
            tracing::info!("Resetting storage to replace mismatched root");
            // Drop locks before reset
            {
                let mut root = self.0.root.write().expect("Root lock poisoned");
                *root = None;
            }
            self.hard_reset().await.map_err(|e| MutationError::General(Box::new(std::io::Error::other(e.to_string()))))?;
        }

        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        // Set the state
        storage.set_state(state.clone()).await?;

        // Set root and mark system as ready
        {
            let mut root = self.0.root.write().expect("Root lock poisoned");
            *root = Some(state);
        }
        *self.0.system_ready.write().unwrap() = true;
        self.0.system_ready_notify.notify_waiters();

        Ok(())
    }

    /// Resets all storage by deleting all collections, including the system collection.
    /// This is used when an ephemeral node needs to join a system with a different root.
    /// **This is a destructive operation and should be used with extreme caution.**
    pub async fn hard_reset(&self) -> Result<()> {
        // Delete all collections from storage
        self.0.collectionset.delete_all_collections().await?;

        // Reset our state
        {
            let mut items = self.0.items.write().unwrap();
            items.clear();
        }
        {
            let mut root = self.0.root.write().unwrap();
            *root = None;
        }
        {
            let mut collection_map = self.0.collection_map.write().unwrap();
            collection_map.clear();
        }
        {
            let mut system_ready = self.0.system_ready.write().unwrap();
            *system_ready = false;
        }

        // Reset the reactor state to notify subscriptions
        self.0.reactor.system_reset();

        Ok(())
    }

    /// Returns true if the local system catalog is loaded
    pub fn is_loaded(&self) -> bool { self.0.loaded.get().is_some() }

    /// Waits for the local system catalog to be loaded
    pub async fn wait_loaded(&self) {
        if !self.is_loaded() {
            self.0.loading.notified().await;
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
        let event_getter = LocalEventGetter::new(storage.clone());

        for state in
            storage.fetch_states(&ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }).await?
        {
            let (_entity_changed, entity) =
                self.0.entities.with_state(&state_getter, &event_getter, state.payload.entity_id, collection_id.clone(), state.payload.state.clone()).await?;
            let lww_backend = entity.get_backend::<LWWBackend>().expect("LWW Backend should exist");
            if let Some(value) = lww_backend.get(&"item".to_string()) {
                let item = proto::sys::Item::from_value(Some(value)).expect("Invalid sys item");

                if let proto::sys::Item::SysRoot = &item {
                    root_state = Some(state);
                }
                entities.push(entity);
            }
        }

        // Update our system state
        {
            let mut items = self.0.items.write().unwrap();
            items.extend(entities);
        }

        // If we loaded a system root and we're a durable node, we're ready
        let has_root = root_state.is_some();
        {
            let mut root = self.0.root.write().expect("Root lock poisoned");
            *root = root_state;
        }

        // Only mark ready if we're a durable node and found a root
        // Ephemeral nodes must explicitly join via join_system()
        if has_root && self.0.durable {
            *self.0.system_ready.write().unwrap() = true;
            self.0.system_ready_notify.notify_waiters();
        }

        // Set loaded state and notify waiters
        self.0.loaded.set(()).expect("Loading flag already set");
        self.0.loading.notify_waiters();
        Ok(())
    }
}

impl Property for proto::sys::Item {
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
