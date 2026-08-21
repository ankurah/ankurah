use ankurah_proto::{self as proto, Attested, CollectionId, EntityState, Event};
use anyhow::{anyhow, Result};
use proto::PropertyId;
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
use crate::schema::SchemaEpoch;
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
    /// This node's current schema epoch: the resolution generation every
    /// cell read under this node passes explicitly. Assigned from the
    /// process-wide allocator on each not-ready-to-ready transition; absent
    /// while no system is ready. Shared with the node's `WeakEntitySet`,
    /// which stamps materializing entities from it.
    schema_epoch: Arc<RwLock<Option<SchemaEpoch>>>,
    /// Serializes the two writers of the durable root: a join's persist and
    /// the reset that deletes it. A persist decides whether to write under
    /// this lock, so a reset can never land between that decision and the
    /// write and leave the wiped storage holding the root it just deleted.
    root_write: tokio::sync::Mutex<()>,
    reactor: Reactor,
    _phantom: PhantomData<PA>,
}

impl<SE, PA> SystemManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn new(
        collections: CollectionSet<SE>,
        entities: WeakEntitySet,
        reactor: Reactor,
        durable: bool,
        schema_epoch: Arc<RwLock<Option<SchemaEpoch>>>,
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
            system_ready_notify: Notify::new(),
            schema_epoch,
            root_write: tokio::sync::Mutex::new(()),
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

    /// The system root's entity id, which every non-root genesis binds into
    /// its own id. `None` until this node has created or joined a system.
    pub fn root_id(&self) -> Option<proto::EntityId> { self.0.root.read().unwrap().as_ref().map(|r| r.payload.entity_id) }

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

    /// This node's current schema epoch: `None` until a system is ready.
    /// Every descriptor-cell read under this node passes this value; it is
    /// never read from the global allocator, because several resident nodes
    /// in one process each hold their own current epoch.
    pub fn schema_epoch(&self) -> Option<SchemaEpoch> { *self.0.schema_epoch.read().unwrap() }

    /// Publish system readiness. On the not-ready-to-ready flip this node's
    /// fresh [`SchemaEpoch`] is assigned BEFORE readiness is observable, so
    /// anything admitted after readiness reads a present epoch; a redundant
    /// call on an already-ready node assigns nothing (the epoch must not
    /// shift under running code). Every become-ready site funnels here so no
    /// transition can miss the assignment; hard_reset clears the epoch with
    /// readiness, which is what makes a reset-and-rejoin re-resolve instead
    /// of reading the previous system's identities.
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

    /// Waits until this node has a system: a root exists and an epoch is
    /// assigned. It says nothing about the catalog's fill; queries that need
    /// catalog names defer behind the catalog's own sync instead.
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

        // Stage the root's initial values in a vessel with no identity of its
        // own, then freeze them into its genesis: the root is the one entity
        // whose genesis carries `system: None`, because there is no system
        // above it to bind.
        let mut provisional = crate::entity::ProvisionalEntity::new();
        provisional.add_membership(proto::ModelId::System(proto::SystemModel::System));
        let lww_backend = provisional.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        lww_backend.set(PropertyId::System(proto::SystemProperty::Item), proto::sys::Item::SysRoot.into_value()?);

        let event = proto::Event::genesis(collection_id.clone(), None, proto::AuthorId::Unknown, provisional.extract_operations()?);
        let system_entity = self.0.entities.create_root(collection_id.clone(), event.entity_id);

        // Stage the event, apply, then commit
        let event_getter = LocalEventGetter::new(storage.clone(), true);
        event_getter.stage_event(event.clone());

        // Apply the creation event so LWW values are tagged with event_id before serialization.
        system_entity.apply_event(&event_getter, &event).await?;
        let attested_event: Attested<Event> = event.clone().into();
        event_getter.commit_event(&attested_event).await?;
        // Now get the entity state after the head is updated
        let attested_state: Attested<EntityState> = system_entity.to_entity_state()?.into();
        storage.set_state(attested_state.clone()).await?;

        // Update our system state
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
                self.mark_system_ready();
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

        // A stored root means "joined once"; it does not certify sync.
        // Whether this node has heard from the system is a per-process,
        // per-query fact (durable_version), re-established at every start,
        // so a restart after a crash here serves nothing stale -- raw
        // resolution defers behind the catalog's first durable answer either
        // way.
        {
            let mut root = self.0.root.write().expect("Root lock poisoned");
            *root = Some(state.clone());
        }
        self.mark_system_ready();
        self.persist_root(state).await.map_err(|e| MutationError::General(Box::new(std::io::Error::other(e.to_string()))))?;

        Ok(())
    }

    /// Write the joined root, conditional on this node still holding exactly
    /// the root that was joined: a reset, or a join of a different system,
    /// has already decided what this node's root is. Holds the root-write
    /// lock across the check and the write.
    async fn persist_root(&self, state: Attested<EntityState>) -> Result<()> {
        let _write = self.0.root_write.lock().await;
        let still_ours = self
            .root()
            .is_some_and(|root| root.payload.entity_id == state.payload.entity_id && root.payload.state.head == state.payload.state.head);
        if !still_ours {
            return Ok(());
        }
        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        self.0.collectionset.get(&collection_id).await?.set_state(state).await?;
        Ok(())
    }

    /// Resets all storage by deleting all collections, including the system collection.
    /// This is used when an ephemeral node needs to join a system with a different root.
    /// **This is a destructive operation and should be used with extreme caution.**
    pub async fn hard_reset(&self) -> Result<()> {
        // Exclude a join's pending root persist for the whole reset: it
        // decides whether to write under this same lock, so it either wrote
        // before the wipe (and the wipe removes it) or finds the root gone
        // afterwards and writes nothing.
        let _root_write = self.0.root_write.lock().await;

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
            // The epoch leaves with readiness, cleared under the SAME
            // readiness write lock that `mark_system_ready` assigns them
            // under (same nesting order): clearing them in two separate
            // lock scopes would let a concurrent become-ready transition
            // interleave between the writes and be clobbered into a
            // permanently ready-with-no-epoch node. Cell entries made
            // under the departing epoch are permanently inert, and the
            // next become-ready transition allocates a fresh epoch.
            let mut system_ready = self.0.system_ready.write().unwrap();
            *self.0.schema_epoch.write().unwrap() = None;
            *system_ready = false;
        }

        // Reset the reactor state to notify subscriptions. Standing queries
        // (the catalog's included) survive the reset: their resultsets are
        // emptied here and refill from the new system's rows.
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
            self.mark_system_ready();
        }

        // Set loaded state and notify waiters
        self.0.loaded.set(()).expect("Loading flag already set");
        self.0.loading.notify_waiters();
        Ok(())
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
