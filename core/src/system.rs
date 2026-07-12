use ankurah_proto::{self as proto, Attested, CollectionId, EntityState, Event};
use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::{atomic::AtomicBool, Arc, OnceLock, RwLock};
use tokio::sync::Notify;
use tracing::error;

use crate::collectionset::CollectionSet;
use crate::entity::{Entity, WeakEntitySet};
use crate::error::MutationError;
use crate::error::RetrievalError;
use crate::notice_info;
use crate::policy::PolicyAgent;
use crate::property::{Property, PropertyError};
use crate::reactor::Reactor;
use crate::retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents};
use crate::storage::{StorageCollectionWrapper, StorageEngine, SystemRootClaim};
use crate::{
    property::backend::{LWWBackend, PropertyBackend},
    value::Value,
};
pub const SYSTEM_COLLECTION_ID: &str = "_ankurah_system";
/// Collections that are not mutable through ordinary transactions: the
/// system collection and the metadata catalog (RFC section 4 in specs/model-property-metadata/rfc.md). Consulted
/// by the commit path (durable nodes refuse CommitTransaction events
/// targeting these) and by CollectionSet::get's reserved-prefix check.
pub const PROTECTED_COLLECTIONS: &[&str] = &[
    SYSTEM_COLLECTION_ID,
    crate::schema::MODEL_COLLECTION_ID,
    crate::schema::PROPERTY_COLLECTION_ID,
    crate::schema::MODEL_PROPERTY_COLLECTION_ID,
];

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
    local_node_id: proto::NodeId,
    /// Root identity, founder, and join state share one lock so two concurrent
    /// first presences cannot both observe an empty root and become durable.
    root: RwLock<Option<RootBinding>>,
    /// Serializes durable root creation across the complete async persistence
    /// path. Without this, two concurrent `create()` calls can both pass the
    /// empty-catalog check and persist different founders/roots.
    create_lock: tokio::sync::Mutex<()>,
    items: RwLock<Vec<Entity>>,
    loaded: OnceLock<()>,
    /// A completed-but-failed storage warm is latched so waiters wake but all
    /// root-sensitive operations remain fail-closed until hard reset.
    load_error: RwLock<Option<String>>,
    loading: Notify,
    system_ready: RwLock<bool>,
    system_ready_notify: Notify,
    reactor: Reactor,
    /// Installed by `CatalogManager::start`. `hard_reset` invokes it to flush
    /// the in-memory catalog map, which the SystemManager cannot reach
    /// directly (the Node owns the CatalogManager). RFC 5.2 requires the
    /// catalog map to be flushed alongside the state hard_reset clears,
    /// because allocated catalog ids belong to one system's allocator.
    catalog_reset_hook: RwLock<Option<Arc<dyn Fn() + Send + Sync>>>,
    _phantom: PhantomData<PA>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootStatus {
    /// Loaded from an ephemeral node's cache, but not yet re-joined.
    StoredUnjoined,
    /// A verified first presence reserved this root before durable routing was
    /// enabled. `needs_reset` is filled in by storage loading when reservation
    /// wins the race with startup.
    Reserved {
        needs_reset: bool,
    },
    /// Join persistence failed and destructive cleanup owns this reservation.
    /// New presences must not attach until cleanup completes.
    Aborting,
    Ready,
}

#[derive(Clone)]
struct RootBinding {
    proof: proto::SystemRootProof,
    founder: proto::NodeId,
    status: RootStatus,
}

/// Result of atomically checking and reserving a durable peer's root proof.
pub(crate) enum RootReservation {
    /// This peer won first join; persist the proof asynchronously.
    StartJoin(proto::SystemRootProof),
    /// The same root/founder was already reserved or pinned.
    AlreadyPinned,
    /// A different root is already reserved or pinned, or a durable receiver
    /// cannot join another node's system.
    Conflict,
}

/// Capability returned when an exact pending reservation is synchronously
/// transitioned to `Aborting`. Holding the complete proof makes the later
/// async cleanup conditional on that reservation generation.
pub(crate) struct RootJoinAbort {
    proof: proto::SystemRootProof,
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
        local_node_id: proto::NodeId,
    ) -> Self {
        let me = Self(Arc::new(Inner {
            collectionset: collections,
            entities,
            durable,
            local_node_id,
            items: RwLock::new(Vec::new()),
            root: RwLock::new(None),
            create_lock: tokio::sync::Mutex::new(()),
            loaded: OnceLock::new(),
            load_error: RwLock::new(None),
            loading: Notify::new(),
            collection_map: RwLock::new(BTreeMap::new()),
            system_ready: RwLock::new(false),
            system_ready_notify: Notify::new(),
            reactor,
            catalog_reset_hook: RwLock::new(None),
            _phantom: PhantomData,
        }));
        {
            let me = me.clone();
            crate::task::spawn(async move {
                if let Err(e) = me.load_system_catalog().await {
                    error!("Failed to load system catalog: {}", e);
                    *me.0.load_error.write().unwrap() = Some(e.to_string());
                    me.mark_loaded();
                }
            });
        }
        me
    }

    pub fn root(&self) -> Option<Attested<EntityState>> { self.0.root.read().unwrap().as_ref().map(|root| root.proof.state.clone()) }

    /// The genesis-backed proof advertised in Presence. A root is exposed as
    /// soon as it is atomically reserved; `is_system_ready` remains false until
    /// its event and state are durable locally.
    pub(crate) fn root_proof(&self) -> Option<proto::SystemRootProof> {
        self.0.root.read().unwrap().as_ref().map(|root| root.proof.clone())
    }

    /// The pinned system identity without cloning its complete state payload.
    pub fn root_id(&self) -> Option<proto::EntityId> { self.0.root.read().unwrap().as_ref().map(|root| root.proof.entity_id()) }

    /// The durable node identity anchored in the pinned system root.
    pub fn founder(&self) -> Option<proto::NodeId> { self.0.root.read().unwrap().as_ref().map(|root| root.founder) }

    /// Verify the complete, immutable RFC-1 root proof and return the founder
    /// encoded by its genesis. Nothing in the carried state is trusted: it must
    /// equal the deterministic materialization of the content-addressed
    /// genesis, including per-property provenance and the singleton head.
    fn verify_root_proof(proof: &proto::SystemRootProof) -> Result<proto::NodeId> {
        if !proof.state.attestations.is_empty() {
            return Err(anyhow!("system root state must not carry attestation envelopes"));
        }
        let genesis = &proof.genesis;
        genesis.validate_structure().map_err(|e| anyhow!("invalid root genesis structure: {e}"))?;

        let expected_model = crate::schema::well_known_model_id(SYSTEM_COLLECTION_ID).expect("system collection has a well-known model id");
        if genesis.model != expected_model {
            return Err(anyhow!("root genesis uses a non-system model"));
        }
        if !matches!(&genesis.body, proto::EventBody::Genesis { system: None, .. }) {
            return Err(anyhow!("system root must be a root genesis with system=None"));
        }
        if genesis.operations().0.len() != 1 {
            return Err(anyhow!("system root genesis must contain only the lww backend"));
        }
        let lww_operations = genesis.operations().0.get("lww").ok_or_else(|| anyhow!("system root genesis has no lww operations"))?;

        let backend = LWWBackend::new();
        backend.apply_operations_with_event(lww_operations, genesis.id())?;
        let item = proto::sys::Item::from_value(backend.get(&crate::property::PropertyKey::name("item")))?;
        let proto::sys::Item::SysRoot { founder } = item else {
            return Err(anyhow!("system root genesis does not contain a SysRoot record"));
        };

        let expected_state = EntityState {
            entity_id: genesis.entity_id,
            model: genesis.model,
            state: proto::State {
                state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_owned(), backend.to_state_buffer()?)])),
                head: proto::Clock::from(vec![genesis.id()]),
            },
        };
        if proof.state.payload != expected_state {
            return Err(anyhow!("system root state is not the exact genesis materialization"));
        }
        Ok(founder)
    }

    /// Idempotently complete a persisted proposal. The engine-level claim
    /// stores the full verified proof, so a loser or restart can safely finish
    /// these exact writes after the original creator crashes.
    async fn persist_root_proof(
        storage: &StorageCollectionWrapper,
        proof: &proto::SystemRootProof,
        durable: bool,
    ) -> Result<(), MutationError> {
        let event_getter = LocalEventGetter::new(storage.clone(), durable);
        let genesis: Attested<Event> = proof.genesis.clone().into();
        event_getter.commit_event(&genesis).await?;
        storage.set_state(proof.state.clone()).await?;
        Ok(())
    }

    /// Validate and atomically reserve a durable peer's root before the peer is
    /// inserted into durable routing. This is the first-join serialization
    /// point: once one root is `Reserved`, a different root can only connect as
    /// non-durable.
    pub(crate) fn reserve_root_from_presence(&self, proof: &proto::SystemRootProof, claimant: proto::NodeId) -> Result<RootReservation> {
        let founder = Self::verify_root_proof(proof)?;
        if founder != claimant {
            return Err(anyhow!("root founder does not match presence node id"));
        }

        let incoming_id = proof.entity_id();
        let mut root = self.0.root.write().unwrap();
        let Some(existing) = root.as_mut() else {
            if self.0.durable {
                return Ok(RootReservation::Conflict);
            }
            *root = Some(RootBinding { proof: proof.clone(), founder, status: RootStatus::Reserved { needs_reset: false } });
            return Ok(RootReservation::StartJoin(proof.clone()));
        };

        if existing.proof.entity_id() == incoming_id && existing.founder == founder {
            return match existing.status {
                RootStatus::StoredUnjoined if !self.0.durable => {
                    existing.proof = proof.clone();
                    existing.status = RootStatus::Reserved { needs_reset: false };
                    Ok(RootReservation::StartJoin(proof.clone()))
                }
                RootStatus::Reserved { .. } | RootStatus::Ready => Ok(RootReservation::AlreadyPinned),
                RootStatus::Aborting => Ok(RootReservation::Conflict),
                RootStatus::StoredUnjoined => Ok(RootReservation::Conflict),
            };
        }

        // An ephemeral cache is not a live pin until it re-joins. Preserve the
        // historical ability to switch systems, but serialize the replacement
        // synchronously so only this proof can become durable.
        if !self.0.durable && existing.status == RootStatus::StoredUnjoined {
            *existing = RootBinding { proof: proof.clone(), founder, status: RootStatus::Reserved { needs_reset: true } };
            return Ok(RootReservation::StartJoin(proof.clone()));
        }

        Ok(RootReservation::Conflict)
    }

    pub fn items(&self) -> Vec<Entity> { self.0.items.read().unwrap().clone() }

    /// get an existing collection if it's defined in the system catalog, else insert a SysItem::Collection
    /// then return collections.get to get the StorageCollectionWrapper
    pub async fn collection(&self, id: &CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.wait_loaded().await;
        if let Some(error) = self.0.load_error.read().unwrap().clone() {
            return Err(RetrievalError::Other(format!("system catalog failed to load: {error}")));
        }
        // TODO - update the system catalog to create an entity for this collection

        // Return the collection wrapper
        self.0.collectionset.get(id).await
    }

    /// Returns true if we've successfully initialized or joined a system
    pub fn is_system_ready(&self) -> bool { *self.0.system_ready.read().unwrap() }

    /// Waits until we've successfully initialized or joined a system
    pub async fn wait_system_ready(&self) {
        if self.is_system_ready() {
            return;
        }
        let notified = self.0.system_ready_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_system_ready() {
            notified.await;
        }
    }

    /// Install the catalog-map flush hook (called by `CatalogManager::start`).
    /// `hard_reset` invokes it so the in-memory catalog map is cleared
    /// together with the state this manager resets; the SystemManager cannot
    /// see the CatalogManager (the Node holds it), so it holds this handle.
    pub(crate) fn set_catalog_reset_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.0.catalog_reset_hook.write().unwrap() = Some(hook);
    }

    /// Creates a new system root. This should only be called once per system by durable nodes
    /// The rest of the nodes must "join" this system.
    pub async fn create(&self) -> Result<()> {
        if !self.0.durable {
            return Err(anyhow!("Only durable nodes can create a new system"));
        }

        // Wait for local system catalog to be loaded
        self.wait_loaded().await;
        if let Some(error) = self.0.load_error.read().unwrap().clone() {
            return Err(anyhow!("system catalog failed to load: {error}"));
        }
        let _create_guard = self.0.create_lock.lock().await;

        let has_items = !self.0.items.read().unwrap().is_empty();
        if has_items || self.0.root.read().unwrap().is_some() {
            return Err(anyhow!("System root already exists"));
        }

        // TODO - see if we can use the Model derive macro for a SysCatalogItem model rather than doing this manually
        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        let create_alive = Arc::new(AtomicBool::new(true));
        let provisional = self.0.entities.create_provisional(collection_id.clone(), create_alive.clone());
        let lww_backend = provisional.get_backend::<LWWBackend>().expect("LWW Backend should exist");
        lww_backend
            .set(crate::property::PropertyKey::name("item"), proto::sys::Item::SysRoot { founder: self.0.local_node_id }.into_value()?);
        let model = crate::schema::well_known_model_id(SYSTEM_COLLECTION_ID).expect("system collection has a well-known model id");
        let event = Event::genesis(model, None, provisional.extract_operations()?);
        let transaction_entity = self.0.entities.create_transaction_entity(collection_id.clone(), &event, create_alive)?;
        let system_entity = match &transaction_entity.kind {
            crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
            crate::entity::EntityKind::Primary => return Err(anyhow!("system root creation did not produce a transaction fork")),
        };

        // `snapshot_after_genesis` already contains the exact materialized
        // genesis state while the resident primary remains empty. Persist the
        // complete proof as the recoverable CAS proposal before any root data:
        // a loser or restart can finish these identical writes if this task
        // crashes at any later point.
        let attested_state: Attested<EntityState> = transaction_entity.to_entity_state()?.into();
        let proof = proto::SystemRootProof { genesis: event.clone(), state: attested_state };
        Self::verify_root_proof(&proof)?;
        match self.0.collectionset.claim_system_root(&proof).await? {
            SystemRootClaim::Claimed => {}
            SystemRootClaim::Existing(existing) => {
                Self::verify_root_proof(&existing)?;
                Self::persist_root_proof(&storage, &existing, true).await?;
                self.0.entities.remove_if_phantom(&event.entity_id);
                return Err(anyhow!("System root already claimed as {}", existing.entity_id()));
            }
        }

        if let Err(error) = Self::persist_root_proof(&storage, &proof, true).await {
            // Keep the full proof claim: unlike a bare id, it is a recoverable
            // proposal. Clearing it could race a live writer and would make a
            // crash between claim and persistence permanently ambiguous.
            self.0.entities.remove_if_phantom(&event.entity_id);
            return Err(error.into());
        }

        // The proof is durable; now advance the resident primary. The event is
        // already stored, so lineage lookup can ground the application.
        let event_getter = LocalEventGetter::new(storage.clone(), true);
        system_entity.apply_event(&event_getter, &event).await?;

        // Update our system state
        let mut items = self.0.items.write().unwrap();
        items.push(system_entity);
        *self.0.root.write().unwrap() = Some(RootBinding { proof, founder: self.0.local_node_id, status: RootStatus::Ready });

        // Mark system as ready and notify waiters
        *self.0.system_ready.write().unwrap() = true;
        self.0.system_ready_notify.notify_waiters();

        Ok(())
    }

    /// Persist a root that was already verified and atomically reserved by
    /// [`Self::reserve_root_from_presence`]. The reservation remains pinned
    /// throughout any storage reset, so a racing, different founder can never
    /// enter durable routing during the async portion of join.
    pub(crate) async fn finish_reserved_join(&self, proof: proto::SystemRootProof) -> Result<(), MutationError> {
        self.wait_loaded().await;
        if self.0.durable {
            return Err(MutationError::General(Box::new(std::io::Error::other("Durable nodes cannot join an existing system"))));
        }
        Self::verify_root_proof(&proof).map_err(MutationError::from)?;

        let reservation_needs_reset = {
            let root = self.0.root.read().unwrap();
            match root.as_ref() {
                Some(RootBinding { proof: reserved, status: RootStatus::Reserved { needs_reset }, .. })
                    if reserved.entity_id() == proof.entity_id() =>
                {
                    *needs_reset
                }
                _ => {
                    return Err(MutationError::General(Box::new(std::io::Error::other("system root reservation was lost before join"))));
                }
            }
        };
        let needs_reset = self.0.load_error.read().unwrap().is_some() || reservation_needs_reset;

        if needs_reset {
            tracing::info!("Resetting storage to replace mismatched reserved root");
            self.clear_local_state(false).await?;
        }

        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;
        match self.0.collectionset.claim_system_root(&proof).await? {
            SystemRootClaim::Claimed => {}
            SystemRootClaim::Existing(existing) if existing == proof => {}
            SystemRootClaim::Existing(existing) => {
                return Err(MutationError::General(Box::new(std::io::Error::other(format!(
                    "storage is already claimed by system root {}",
                    existing.entity_id()
                )))));
            }
        }
        Self::persist_root_proof(&storage, &proof, false).await?;

        {
            let mut root = self.0.root.write().unwrap();
            match root.as_mut() {
                Some(binding)
                    if binding.proof.entity_id() == proof.entity_id() && matches!(binding.status, RootStatus::Reserved { .. }) =>
                {
                    binding.proof = proof;
                    binding.status = RootStatus::Ready;
                }
                _ => {
                    return Err(MutationError::General(Box::new(std::io::Error::other("system root reservation changed during join"))));
                }
            }
        }
        *self.0.system_ready.write().unwrap() = true;
        self.0.system_ready_notify.notify_waiters();
        notice_info!("Persisted verified system root and completed join");
        Ok(())
    }

    /// Synchronously fence an exact pending join before any peer cleanup. New
    /// presences see `Aborting` and cannot attach to a reservation whose async
    /// storage cleanup is about to run.
    pub(crate) fn begin_abort_reserved_join(&self, root_id: proto::EntityId) -> Option<RootJoinAbort> {
        let mut root = self.0.root.write().unwrap();
        let binding = root.as_mut()?;
        if binding.proof.entity_id() != root_id || !matches!(binding.status, RootStatus::Reserved { .. }) {
            return None;
        }
        binding.status = RootStatus::Aborting;
        Some(RootJoinAbort { proof: binding.proof.clone() })
    }

    /// Destructively clear every artifact accepted while `abort` was the
    /// pending root, then remove only that exact fenced reservation.
    pub(crate) async fn finish_abort_reserved_join(&self, abort: RootJoinAbort) -> Result<(), MutationError> {
        self.clear_local_state(false).await?;
        let mut root = self.0.root.write().unwrap();
        match root.as_ref() {
            Some(binding) if binding.status == RootStatus::Aborting && binding.proof == abort.proof => {
                *root = None;
                Ok(())
            }
            _ => Err(MutationError::General(Box::new(std::io::Error::other("system root reservation changed during abort cleanup")))),
        }
    }

    /// Resets all storage by deleting all collections, including the system collection.
    /// This is used when an ephemeral node needs to join a system with a different root.
    /// **This is a destructive operation and should be used with extreme caution.**
    pub async fn hard_reset(&self) -> Result<()> { self.clear_local_state(true).await.map_err(anyhow::Error::from) }

    /// Clear system-owned storage and caches. A reserved first-join root can be
    /// preserved while the old system's storage is deleted; this is what keeps
    /// the reservation race-free across the async reset.
    async fn clear_local_state(&self, clear_root: bool) -> Result<(), MutationError> {
        // Delete all collections from storage
        self.0.collectionset.delete_all_collections().await?;

        // Reset our state
        {
            let mut items = self.0.items.write().unwrap();
            items.clear();
        }
        if clear_root {
            let mut root = self.0.root.write().unwrap();
            *root = None;
        }
        {
            let mut collection_map = self.0.collection_map.write().unwrap();
            collection_map.clear();
        }

        // Flush the in-memory catalog map (RFC 5.2): allocated catalog ids
        // belong to one system, so a node re-joining a different system must
        // re-register against that system's allocator; a stale map would leak
        // the old system's ids. Runs after
        // collection_map.clear() and before reactor.system_reset(), which the
        // catalog's own reactor subscriptions also observe. The hook clears
        // maps and drops subscriptions but never deletes storage collections
        // (that is delete_all_collections above).
        let catalog_reset_hook = self.0.catalog_reset_hook.read().unwrap().clone();
        if let Some(hook) = catalog_reset_hook {
            hook();
        }

        {
            let mut system_ready = self.0.system_ready.write().unwrap();
            *system_ready = false;
        }
        *self.0.load_error.write().unwrap() = None;

        // Reset the reactor state to notify subscriptions
        self.0.reactor.system_reset();

        Ok(())
    }

    /// Returns true if the local system catalog is loaded
    pub fn is_loaded(&self) -> bool { self.0.loaded.get().is_some() }

    /// Waits for the local system catalog to be loaded
    pub async fn wait_loaded(&self) {
        if self.is_loaded() {
            return;
        }
        let notified = self.0.loading.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_loaded() {
            notified.await;
        }
    }

    fn mark_loaded(&self) {
        let _ = self.0.loaded.set(());
        self.0.loading.notify_waiters();
    }

    async fn load_system_catalog(&self) -> Result<()> {
        if self.is_loaded() {
            return Err(anyhow!("System catalog already loaded"));
        }

        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        // A claim is a complete verified proposal, not a bare id. If the
        // original creator crashed after CAS, finish its exact idempotent
        // writes before scanning. Never steal or clear an absent-state claim:
        // doing so would race a live slow writer.
        let claimed_proof = self.0.collectionset.system_root_claim().await?;
        if let Some(proof) = &claimed_proof {
            Self::verify_root_proof(proof)?;
            Self::persist_root_proof(&storage, proof, self.0.durable).await?;
        }

        let states =
            storage.fetch_states(&ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }).await?;
        let mut item_states = Vec::new();
        let mut root_states = Vec::new();
        for state in &states {
            let Some(buffer) = state.payload.state.state_buffers.0.get("lww") else { continue };
            let backend = LWWBackend::from_state_buffer(buffer)?;
            let Some(value) = backend.get(&crate::property::PropertyKey::name("item")) else { continue };
            let item = proto::sys::Item::from_value(Some(value))?;
            if matches!(item, proto::sys::Item::SysRoot { .. }) {
                root_states.push(state.clone());
            }
            item_states.push(state.clone());
        }

        // Validate every root candidate before mutating the in-memory catalog.
        // Multiple distinct valid roots are corruption and fail closed; the old
        // last-wins behavior silently selected whichever scan row came last.
        let mut valid_roots = BTreeMap::<proto::EntityId, RootBinding>::new();
        for state in root_states {
            let event_id = proto::EventId::from_bytes(state.payload.entity_id.to_bytes());
            let Some(genesis) = storage.get_events(vec![event_id]).await?.into_iter().next() else {
                error!("Ignoring persisted system root whose genesis event is missing");
                continue;
            };
            let proof = proto::SystemRootProof { genesis: genesis.payload, state };
            match Self::verify_root_proof(&proof) {
                Ok(founder) => {
                    valid_roots.insert(
                        proof.entity_id(),
                        RootBinding {
                            proof,
                            founder,
                            status: if self.0.durable && founder == self.0.local_node_id {
                                RootStatus::Ready
                            } else {
                                RootStatus::StoredUnjoined
                            },
                        },
                    );
                }
                Err(error) => error!("Ignoring persisted system root without a valid self-certifying proof: {error}"),
            }
        }
        if valid_roots.len() > 1 {
            return Err(anyhow!("storage contains {} distinct valid system roots", valid_roots.len()));
        }
        let loaded_root = valid_roots.into_values().next();

        match (&claimed_proof, &loaded_root) {
            (Some(claimed), Some(loaded)) if claimed == &loaded.proof => {}
            (Some(claimed), Some(loaded)) => {
                return Err(anyhow!("system-root claim {} conflicts with stored root {}", claimed.entity_id(), loaded.proof.entity_id()));
            }
            (Some(claimed), None) => {
                return Err(anyhow!("system-root claim {} could not be materialized", claimed.entity_id()));
            }
            (None, Some(loaded)) => match self.0.collectionset.claim_system_root(&loaded.proof).await? {
                SystemRootClaim::Claimed => {}
                SystemRootClaim::Existing(existing) if existing == loaded.proof => {}
                SystemRootClaim::Existing(existing) => {
                    return Err(anyhow!("stored root {} lost claim reconciliation to {}", loaded.proof.entity_id(), existing.entity_id()));
                }
            },
            (None, None) => {}
        }

        let state_getter = LocalStateGetter::new(storage.clone());
        let event_getter = LocalEventGetter::new(storage.clone(), self.0.durable);
        let mut entities = Vec::new();
        for state in item_states {
            let (_entity_changed, entity) = self
                .0
                .entities
                .with_state(&state_getter, &event_getter, state.payload.entity_id, collection_id.clone(), state.payload.state.clone())
                .await?;
            entities.push(entity);
        }

        let mut durable_root_ready = false;
        {
            let mut root = self.0.root.write().expect("Root lock poisoned");
            match root.as_mut() {
                // A first presence may reserve while the asynchronous storage
                // load is in flight. Never overwrite it; only tell the pending
                // join whether replacing the loaded cache requires a reset.
                Some(RootBinding { proof, status: RootStatus::Reserved { needs_reset }, .. }) => {
                    *needs_reset = loaded_root.as_ref().is_some_and(|loaded| loaded.proof.entity_id() != proof.entity_id());
                }
                Some(_) => {}
                None => {
                    durable_root_ready = self.0.durable
                        && loaded_root
                            .as_ref()
                            .is_some_and(|binding| binding.founder == self.0.local_node_id && binding.status == RootStatus::Ready);
                    if self.0.durable && loaded_root.as_ref().is_some_and(|binding| binding.founder != self.0.local_node_id) {
                        error!(
                            "Durable node key does not match the founder recorded in persisted system root; reopen with the persisted signing key"
                        );
                    }
                    *root = loaded_root;
                }
            }
        }

        self.0.items.write().unwrap().extend(entities);

        // Only a durable whose persisted key matches the proven founder is
        // ready after load. Ephemeral nodes explicitly complete first join.
        if durable_root_ready {
            *self.0.system_ready.write().unwrap() = true;
            self.0.system_ready_notify.notify_waiters();
        }

        self.mark_loaded();
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
