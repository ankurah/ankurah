use ankurah_proto::{self as proto, Attested, CollectionId, EntityState, Event};
use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Serializes system-epoch transitions and initial storage load. Reset's
    /// catalog drain guarantee is only meaningful when no second reset/join
    /// can bypass it and delete or republish state concurrently.
    lifecycle: tokio::sync::Mutex<()>,
    /// Remains set across cancellation or deletion failure so the next
    /// lifecycle operation resumes reset before reading or publishing a root.
    /// Access is serialized by `lifecycle`; atomic storage provides interior
    /// mutability without a second lock.
    reset_incomplete: AtomicBool,
    reactor: Reactor,
    /// Installed by `CatalogManager::start`. Reset has two phases because the
    /// catalog owns asynchronous relay work while SystemManager owns storage:
    /// begin invalidates and drains old catalog effects before deletion,
    /// finish clears catalog state after system/reactor reset, and resume
    /// re-arms durable catalog maintenance after the replacement root is ready.
    catalog_reset_hook: RwLock<Option<CatalogResetHook>>,
    _phantom: PhantomData<PA>,
}

type CatalogResetFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[derive(Clone)]
struct CatalogResetHook {
    begin: Arc<dyn Fn() -> CatalogResetFuture + Send + Sync>,
    finish: Arc<dyn Fn() + Send + Sync>,
    resume: Arc<dyn Fn() + Send + Sync>,
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
            lifecycle: tokio::sync::Mutex::new(()),
            reset_incomplete: AtomicBool::new(false),
            reactor,
            catalog_reset_hook: RwLock::new(None),
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
        loop {
            // notify_waiters stores no permit. Arm the waiter before reading
            // readiness so a transition between the check and await cannot
            // strand reconstruction or join callers.
            let notified = self.0.system_ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_system_ready() {
                return;
            }
            notified.await;
        }
    }

    /// Install the catalog reset barrier (called by `CatalogManager::start`).
    /// SystemManager remains the sole owner of destructive storage deletion.
    pub(crate) fn set_catalog_reset_hook(
        &self,
        begin: Arc<dyn Fn() -> CatalogResetFuture + Send + Sync>,
        finish: Arc<dyn Fn() + Send + Sync>,
        resume: Arc<dyn Fn() + Send + Sync>,
    ) {
        *self.0.catalog_reset_hook.write().unwrap() = Some(CatalogResetHook { begin, finish, resume });
    }

    /// Creates a new system root. This should only be called once per system by durable nodes
    /// The rest of the nodes must "join" this system.
    pub async fn create(&self) -> Result<()> {
        if !self.0.durable {
            return Err(anyhow!("Only durable nodes can create a new system"));
        }

        // Wait for local system catalog to be loaded
        self.wait_loaded().await;
        let _lifecycle = self.0.lifecycle.lock().await;
        if self.0.reset_incomplete.load(Ordering::Acquire) {
            self.hard_reset_locked().await?;
        }

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
        lww_backend.set(crate::property::PropertyKey::name("item"), proto::sys::Item::SysRoot.into_value()?);

        // The system root is a fresh entity: its commit event is a genesis
        // (empty head), so the D2-2 stamp is exactly 1 and nothing resolves.
        let event_getter = LocalEventGetter::new(storage.clone(), true);
        let event = system_entity.generate_commit_event()?.ok_or(anyhow!("Expected event"))?;

        // Stage the event, apply, then commit
        event_getter.stage_event(event.clone());

        // Apply the creation event so LWW values are tagged with event_id before serialization.
        system_entity.apply_event(&event_getter, &event).await?;
        let attested_event: Attested<Event> = event.clone().into();
        event_getter.commit_event(&attested_event).await?;
        // Now get the entity state after the head is updated.
        // DOCUMENTED SAFE BYPASS (D2-6, REV 5 section G, alongside
        // join_system's below): this raw set_state skips the shared
        // persist funnel, so the root's applied-set and persist marker
        // simply LAG: a later redelivery of the genesis walks to its
        // no-op instead of skipping O(1), and its first funnel persist
        // writes redundantly instead of eliding. Lagging is the safe
        // direction (a marker may never LEAD storage); runs once at
        // system creation.
        let attested_state: Attested<EntityState> = system_entity.to_entity_state()?.into();
        storage.set_state(attested_state.clone()).await?;

        // Update our system state
        let mut items = self.0.items.write().unwrap();
        items.push(system_entity);
        *self.0.root.write().unwrap() = Some(attested_state);

        // Mark system as ready and notify waiters
        *self.0.system_ready.write().unwrap() = true;
        self.0.system_ready_notify.notify_waiters();
        if let Some(hook) = self.0.catalog_reset_hook.read().unwrap().clone() {
            (hook.resume)();
        }

        Ok(())
    }

    /// Joins an existing system. This should only be called by ephemeral nodes.
    pub async fn join_system(&self, state: Attested<EntityState>) -> Result<(), MutationError> {
        // Wait for catalog to be loaded before proceeding
        self.wait_loaded().await;
        let _lifecycle = self.0.lifecycle.lock().await;
        if self.0.reset_incomplete.load(Ordering::Acquire) {
            self.hard_reset_locked().await.map_err(|e| MutationError::General(Box::new(std::io::Error::other(e.to_string()))))?;
        }

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
                if let Some(hook) = self.0.catalog_reset_hook.read().unwrap().clone() {
                    (hook.resume)();
                }
                return Ok(());
            }
            tracing::warn!("Mismatched root state during join: local={:?}, remote={:?}", root, state.payload.state.head);

            // Only reset storage if we have a root that needs to be replaced
            tracing::info!("Resetting storage to replace mismatched root");
            // Keep the old root until hard_reset clears it after the drain and
            // deletion barrier. If this join future is canceled mid-barrier,
            // a retry must still observe the mismatch and resume the retained
            // drain rather than installing the new root into a half-reset node.
            self.hard_reset_locked().await.map_err(|e| MutationError::General(Box::new(std::io::Error::other(e.to_string()))))?;
        }

        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        // Structural sanity on the trust anchor (M4, REV 5 section K): the
        // root state's head-generation annotation must cover exactly its
        // head, or the adopted root could never stamp a commit. This is the
        // one wire-state ingress that does not route through the shared
        // state-apply, so the check is inlined; the VALUES are adopted
        // inside the same trust envelope as the root itself (this path only
        // runs on ephemeral nodes).
        if !state.payload.state.head_generations.matches_head(&state.payload.state.head) {
            return Err(MutationError::Ingest(crate::error::IngestError::Lineage(crate::error::LineageRejection::HeadGenerationsMismatch)));
        }

        // Entity-mediated adoption (D1 M4): the root state materializes
        // through with_state like every other state feed, so the resident
        // set and the reactor see the root this node just adopted.
        // Validation semantics are UNCHANGED on this trust-bootstrap path
        // (admission unification is #274's jurisdiction), and the peer's
        // attested bytes persist verbatim below: re-attesting the trust
        // anchor locally would swap its provenance.
        let state_getter = LocalStateGetter::new(storage.clone());
        let event_getter = LocalEventGetter::new(storage.clone(), self.0.durable);
        let (_, root_entity) = self
            .0
            .entities
            .with_state(&state_getter, &event_getter, state.payload.entity_id, collection_id, state.payload.state.clone())
            .await?;

        // Set the state.
        // DOCUMENTED SAFE BYPASS (D2-6; REV 5 section G): the verbatim
        // persist of the peer's attested bytes skips the shared persist
        // funnel, so the adopted root's applied-set and persist marker
        // LAG: redeliveries walk instead of skipping O(1) and the first
        // funnel persist writes redundantly instead of eliding. Lagging
        // is the safe direction (a marker may never LEAD storage);
        // preserves attestation provenance.
        storage.set_state(state.clone()).await?;

        // Set root and mark system as ready
        {
            let mut root = self.0.root.write().expect("Root lock poisoned");
            *root = Some(state);
        }
        *self.0.system_ready.write().unwrap() = true;
        self.0.system_ready_notify.notify_waiters();
        if let Some(hook) = self.0.catalog_reset_hook.read().unwrap().clone() {
            (hook.resume)();
        }

        // Notification flows for uniformity with every other state feed;
        // pre-ready nothing can be subscribed, so this is a no-op today.
        let change = crate::changes::EntityChange::new(root_entity, Vec::new())?;
        self.0.reactor.notify_change(vec![change]).await;

        Ok(())
    }

    /// Resets all storage by deleting all collections, including the system collection.
    /// Ephemeral nodes use this when joining a different system; durable nodes
    /// may use it before creating a replacement root in place.
    /// **This is a destructive operation and should be used with extreme caution.**
    pub async fn hard_reset(&self) -> Result<()> {
        let _lifecycle = self.0.lifecycle.lock().await;
        self.hard_reset_locked().await
    }

    /// Reset while the caller owns `lifecycle`. Kept separate so a mismatched
    /// `join_system` can reset atomically without recursively locking.
    async fn hard_reset_locked(&self) -> Result<()> {
        // The reset epoch bump comes FIRST (D2-6): from this instant, every
        // persist that captured the previous epoch stamps a marker that is
        // never trusted, so no persist the successor system needs can be
        // elided on the dead system's testimony. Memory-only; nothing
        // persisted.
        self.0.entities.bump_reset_epoch();

        // The purge (REV 5 section D.1, the one-id-one-system invariant):
        // every entry in the resident map at reset time belongs to the dead
        // system by definition, so clear the map, taking only the map's own
        // lock (no entity locks, no lock-order hazard, no sweep). Stale
        // residents become unreachable from ingest immediately, before the
        // storage wipe below; holders of strong references keep their
        // frozen snapshots. This also drops the accumulated dead weak
        // entries.
        self.0.entities.purge();

        self.0.reset_incomplete.store(true, Ordering::Release);
        // Close readiness before the catalog barrier so no concurrent caller
        // can treat the system as usable while its prior epoch is draining.
        *self.0.system_ready.write().unwrap() = false;

        // Invalidate the old catalog generation, tear down its live queries,
        // and wait for responses already admitted at schema ingress to finish
        // applying. The hook is cloned out of the lock before await.
        let catalog_reset_hook = self.0.catalog_reset_hook.read().unwrap().clone();
        if let Some(hook) = &catalog_reset_hook {
            (hook.begin)().await;
        }

        // Delete all collections from storage. Even a failed deletion must
        // release the catalog's resetting latch so callers are not stranded;
        // system/reactor clearing remains success-only.
        if let Err(error) = self.0.collectionset.delete_all_collections().await {
            if let Some(hook) = &catalog_reset_hook {
                (hook.finish)();
            }
            return Err(error.into());
        }

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

        // Reset the reactor state to notify subscriptions
        self.0.reactor.system_reset();

        // Only after storage, system state, and reactor state are clear may
        // the catalog finish its reset and permit a new warm generation.
        if let Some(hook) = catalog_reset_hook {
            (hook.finish)();
        }
        // No await exists between successful deletion, system/reactor clear,
        // catalog finish, and this store. Cancellation therefore leaves the
        // flag set exactly for incomplete transitions.
        self.0.reset_incomplete.store(false, Ordering::Release);

        Ok(())
    }

    /// Returns true if the local system catalog is loaded
    pub fn is_loaded(&self) -> bool { self.0.loaded.get().is_some() }

    /// Waits for the local system catalog to be loaded
    pub async fn wait_loaded(&self) {
        loop {
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
        let _lifecycle = self.0.lifecycle.lock().await;
        if self.0.reset_incomplete.load(Ordering::Acquire) {
            self.hard_reset_locked().await?;
        }
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
            if let Some(value) = lww_backend.get(&crate::property::PropertyKey::name("item")) {
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
            if let Some(hook) = self.0.catalog_reset_hook.read().unwrap().clone() {
                (hook.resume)();
            }
        }

        // Set loaded state and notify waiters
        self.0.loaded.set(()).expect("Loading flag already set");
        self.0.loading.notify_waiters();
        Ok(())
    }
}

impl Property for proto::sys::Item {
    // JSON in a string register. System entities never enter the catalog
    // (bootstrap exemption), so this documents the serialization only.
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
