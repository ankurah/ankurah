use ankurah_proto::{self as proto, Attested, CollectionId, EntityState, Event};
use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, OnceLock, RwLock,
};
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
    /// Serializes system-epoch transitions and initial storage load. Reset's
    /// catalog drain guarantee is only meaningful when no second reset/join
    /// can bypass it and delete or republish state concurrently.
    lifecycle: tokio::sync::Mutex<()>,
    /// Remains set across cancellation or deletion failure so the next
    /// lifecycle operation resumes reset before reading or publishing a root.
    /// Access is serialized by `lifecycle`; atomic storage provides interior
    /// mutability without a second lock.
    reset_incomplete: AtomicBool,
    /// Whether the incomplete reset intended to drop the root binding. A
    /// resumed reset must not clear a pinned first-join reservation that the
    /// original (cancelled) clear deliberately preserved.
    reset_incomplete_clear_root: AtomicBool,
    reactor: Reactor,
    /// Installed by `CatalogManager::start`. Reset has two phases because the
    /// catalog owns asynchronous relay work while SystemManager owns storage:
    /// begin invalidates and drains old catalog effects before deletion,
    /// finish clears catalog state after system/reactor reset, and resume
    /// re-arms durable catalog maintenance after the replacement root is ready.
    catalog_reset_hook: RwLock<Option<CatalogResetHook>>,
    /// Node-owned peer teardown/rebinding, installed after Node
    /// construction. See [`PeerSessionsReset`] for the two modes.
    peer_reset_hook: RwLock<Option<Arc<dyn Fn(PeerSessionsReset) + Send + Sync>>>,
    /// Shared-storage epoch this manager loaded/created/joined against. The
    /// actual gate and monotonic epoch live in CollectionSet's per-engine
    /// fence, shared across independently-built Nodes using the same Arc<SE>.
    bound_storage_epoch: AtomicU64,
    resetting: AtomicBool,
    destructive_resetting: AtomicBool,
    _phantom: PhantomData<PA>,
}

type CatalogResetFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// How a system transition treats this Node's registered peer sessions.
pub(crate) enum PeerSessionsReset {
    /// An old system is being replaced or destroyed: drain every session and
    /// wake its pending waiters, preserving only a not-yet-ready connection
    /// whose pending root is the newly Reserved root (the authenticated
    /// channel that triggered the switch).
    Drain { preserved_pending_root: Option<proto::EntityId> },
    /// A first owner is being published over previously EMPTY storage
    /// (create, or a first-join claim). No prior system existed, so nothing a
    /// live session asserted has been invalidated: ready sessions are rebound
    /// to the fresh generation in place, not-yet-ready sessions are left for
    /// `promote_peers_for_root` to re-arm, and the pending founder keeps its
    /// old token until promotion.
    Rebind { fresh_generation: Arc<AtomicBool> },
}

#[derive(Clone)]
struct CatalogResetHook {
    begin: Arc<dyn Fn() -> CatalogResetFuture + Send + Sync>,
    finish: Arc<dyn Fn() + Send + Sync>,
    resume: Arc<dyn Fn() + Send + Sync>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootStatus {
    /// Loaded from an ephemeral node's cache, but not yet re-joined.
    StoredUnjoined,
    /// A verified first presence reserved this root before durable routing was
    /// enabled. `needs_reset` (and the exact root the reset would replace) is
    /// filled in by storage loading when reservation wins the race with
    /// startup.
    Reserved {
        needs_reset: bool,
        /// The loaded root this reservation replaces, when `needs_reset`.
        /// Join re-validates against the storage claim under the exclusive
        /// writer; deletion while STALE is permitted only when the claim is
        /// exactly this recorded root (a new owner installed meanwhile must
        /// never be erased by a queued replacement join).
        replaces: Option<proto::EntityId>,
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

/// Cancellation-safe reset marker. Drop releases the exclusive gate before
/// publishing `resetting = false`, so a connector can never observe reset as
/// finished while the destructive writer is still held.
struct ResetActivity<'a> {
    guard: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
    resetting: &'a AtomicBool,
}

struct FlagActivity<'a>(&'a AtomicBool);

impl<'a> FlagActivity<'a> {
    fn new(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Release);
        Self(flag)
    }
}

impl Drop for FlagActivity<'_> {
    fn drop(&mut self) { self.0.store(false, Ordering::Release); }
}

impl<'a> ResetActivity<'a> {
    fn new(guard: tokio::sync::OwnedRwLockWriteGuard<()>, resetting: &'a AtomicBool) -> Self {
        resetting.store(true, Ordering::Release);
        Self { guard: Some(guard), resetting }
    }
}

impl Drop for ResetActivity<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.resetting.store(false, Ordering::Release);
    }
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
        let storage_epoch = collections.storage_epoch();
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
            lifecycle: tokio::sync::Mutex::new(()),
            reset_incomplete: AtomicBool::new(false),
            reset_incomplete_clear_root: AtomicBool::new(false),
            reactor,
            catalog_reset_hook: RwLock::new(None),
            peer_reset_hook: RwLock::new(None),
            bound_storage_epoch: AtomicU64::new(storage_epoch),
            resetting: AtomicBool::new(false),
            destructive_resetting: AtomicBool::new(false),
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

    pub fn root(&self) -> Option<Attested<EntityState>> {
        self.is_storage_generation_current().then(|| self.0.root.read().unwrap().as_ref().map(|root| root.proof.state.clone())).flatten()
    }

    /// The genesis-backed proof advertised in Presence. A root is exposed as
    /// soon as it is atomically reserved; `is_system_ready` remains false until
    /// its event and state are durable locally.
    pub(crate) fn root_proof(&self) -> Option<proto::SystemRootProof> {
        self.is_storage_generation_current().then(|| self.0.root.read().unwrap().as_ref().map(|root| root.proof.clone())).flatten()
    }

    /// The pinned system identity without cloning its complete state payload.
    pub fn root_id(&self) -> Option<proto::EntityId> {
        self.is_storage_generation_current().then(|| self.0.root.read().unwrap().as_ref().map(|root| root.proof.entity_id())).flatten()
    }

    /// The durable node identity anchored in the pinned system root.
    pub fn founder(&self) -> Option<proto::NodeId> {
        self.is_storage_generation_current().then(|| self.0.root.read().unwrap().as_ref().map(|root| root.founder)).flatten()
    }

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
        if !self.is_storage_generation_current() {
            return Ok(RootReservation::Conflict);
        }
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
            *root =
                Some(RootBinding { proof: proof.clone(), founder, status: RootStatus::Reserved { needs_reset: false, replaces: None } });
            return Ok(RootReservation::StartJoin(proof.clone()));
        };

        if existing.proof.entity_id() == incoming_id && existing.founder == founder {
            return match existing.status {
                RootStatus::StoredUnjoined if !self.0.durable => {
                    existing.proof = proof.clone();
                    existing.status = RootStatus::Reserved { needs_reset: false, replaces: None };
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
            let replaces = Some(existing.proof.entity_id());
            *existing = RootBinding { proof: proof.clone(), founder, status: RootStatus::Reserved { needs_reset: true, replaces } };
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
    pub fn is_system_ready(&self) -> bool { self.is_storage_generation_current() && *self.0.system_ready.read().unwrap() }

    pub(crate) fn is_storage_generation_current(&self) -> bool {
        self.0.bound_storage_epoch.load(Ordering::Acquire) == self.0.collectionset.storage_epoch()
    }

    /// Whether a first-join reservation (or its cleanup) is still pending.
    /// Such a manager is MID-TRANSITION, not terminally stale: the join either
    /// rebinds it to the current epoch and publishes readiness, or the
    /// reservation is cleared and staleness becomes final.
    fn has_pending_reservation(&self) -> bool {
        matches!(
            self.0.root.read().unwrap().as_ref().map(|binding| binding.status),
            Some(RootStatus::Reserved { .. }) | Some(RootStatus::Aborting)
        )
    }

    /// Waits until we've successfully initialized or joined a system
    pub async fn wait_system_ready(&self) {
        loop {
            if self.is_system_ready() || (!self.is_storage_generation_current() && !self.has_pending_reservation()) {
                return;
            }

            // Arm both notifications before the second check. A reset on a
            // different Node has no access to this manager's local Notify, so
            // the engine-shared epoch notification is what prevents a stale
            // manager from sleeping forever. A stale manager whose reservation
            // is still pending keeps waiting: its queued join re-validates
            // against storage under the exclusive writer and either rebinds
            // this manager or clears the reservation (both notify).
            let ready = self.0.system_ready_notify.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            let epoch_notify = self.0.collectionset.storage_epoch_notify();
            let epoch_changed = epoch_notify.notified();
            tokio::pin!(epoch_changed);
            epoch_changed.as_mut().enable();

            if self.is_system_ready() || (!self.is_storage_generation_current() && !self.has_pending_reservation()) {
                return;
            }
            tokio::select! {
                _ = ready => {},
                _ = epoch_changed => {},
            }
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

    pub(crate) fn set_peer_reset_hook(&self, hook: Arc<dyn Fn(PeerSessionsReset) + Send + Sync>) {
        *self.0.peer_reset_hook.write().unwrap() = Some(hook);
    }

    pub(crate) fn is_resetting(&self) -> bool { self.0.resetting.load(Ordering::Acquire) }

    pub(crate) fn is_destructive_resetting(&self) -> bool { self.0.destructive_resetting.load(Ordering::Acquire) }

    /// Fence a transaction to the exact resident/system generation in which
    /// it began. The returned guard must live across every durable write.
    pub(crate) async fn guard_generation(
        &self,
        generation: &Arc<AtomicBool>,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, MutationError> {
        let guard = self.0.collectionset.storage_read_lease().await;
        if !self.is_storage_generation_current()
            || !self.0.entities.is_current_generation(generation)
            || !*self.0.system_ready.read().unwrap()
        {
            return Err(MutationError::SystemReset);
        }
        Ok(guard)
    }

    async fn guard_generation_allow_unready(
        &self,
        generation: &Arc<AtomicBool>,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, MutationError> {
        let guard = self.0.collectionset.storage_read_lease().await;
        if !self.is_storage_generation_current() || !self.0.entities.is_current_generation(generation) {
            return Err(MutationError::SystemReset);
        }
        Ok(guard)
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
            self.resume_incomplete_reset().await?;
        }
        if let Some(error) = self.0.load_error.read().unwrap().clone() {
            return Err(anyhow!("system catalog failed to load: {error}"));
        }
        let _create_guard = self.0.create_lock.lock().await;
        let old_generation = self.0.entities.system_generation();
        let _create_activity = ResetActivity::new(self.0.collectionset.storage_write_lease().await, &self.0.resetting);
        if !self.is_storage_generation_current() || !self.0.entities.is_current_generation(&old_generation) {
            return Err(MutationError::SystemReset.into());
        }

        let has_items = !self.0.items.read().unwrap().is_empty();
        if has_items || self.0.root.read().unwrap().is_some() {
            return Err(anyhow!("System root already exists"));
        }

        // Publishing a brand-new storage owner is a shared generation
        // transition. Do it under the writer before constructing resident
        // handles, so independently-built managers over this Arc<SE> become
        // stale and only this winner binds the fresh token.
        let (new_storage_epoch, fresh_generation) = self.0.collectionset.advance_storage_epoch();
        self.0.bound_storage_epoch.store(new_storage_epoch, Ordering::Release);
        self.publish_first_owner_with_generation(true, fresh_generation);
        self.0.collectionset.publish_storage_epoch();

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
        if let Some(hook) = self.0.catalog_reset_hook.read().unwrap().clone() {
            (hook.resume)();
        }

        Ok(())
    }

    /// Persist a root that was already verified and atomically reserved by
    /// [`Self::reserve_root_from_presence`]. The reservation remains pinned
    /// throughout any storage reset, so a racing, different founder can never
    /// enter durable routing during the async portion of join.
    pub(crate) async fn finish_reserved_join(&self, proof: proto::SystemRootProof) -> Result<(), MutationError> {
        self.wait_loaded().await;
        let _lifecycle = self.0.lifecycle.lock().await;
        if self.0.reset_incomplete.load(Ordering::Acquire) {
            self.resume_incomplete_reset().await?;
        }
        if self.0.durable {
            return Err(MutationError::General(Box::new(std::io::Error::other("Durable nodes cannot join an existing system"))));
        }
        Self::verify_root_proof(&proof).map_err(MutationError::from)?;

        // Advisory pre-read: a replacement join must drain the catalog BEFORE
        // taking the exclusive storage gate, because draining waits for
        // admitted catalog effects whose application may need a storage read
        // lease. The values read here are stable (load has completed and a
        // pinned reservation's needs_reset never changes); the authoritative
        // re-read still happens under the gate below.
        let advisory_needs_reset = self.0.load_error.read().unwrap().is_some()
            || self.0.root.read().unwrap().as_ref().is_some_and(|binding| {
                binding.proof.entity_id() == proof.entity_id() && matches!(binding.status, RootStatus::Reserved { needs_reset: true, .. })
            });
        let catalog_reset_hook = self.0.catalog_reset_hook.read().unwrap().clone();
        if advisory_needs_reset {
            if let Some(hook) = &catalog_reset_hook {
                (hook.begin)().await;
            }
        }

        // Serialize the complete switch -- invalidation, deletion, root
        // proof persistence, and publication. This prevents hard_reset or an
        // old in-flight writer from interleaving between deletion and the new
        // root becoming ready.
        let _reset_activity = ResetActivity::new(self.0.collectionset.storage_write_lease().await, &self.0.resetting);
        let result = async {
            let (reservation_needs_reset, reservation_replaces) = {
                let root = self.0.root.read().unwrap();
                match root.as_ref() {
                    Some(RootBinding { proof: reserved, status: RootStatus::Reserved { needs_reset, replaces }, .. })
                        if reserved.entity_id() == proof.entity_id() =>
                    {
                        (*needs_reset, *replaces)
                    }
                    _ => {
                        return Err(MutationError::General(Box::new(std::io::Error::other(
                            "system root reservation was lost before join",
                        ))));
                    }
                }
            };
            let needs_reset = self.0.load_error.read().unwrap().is_some() || reservation_needs_reset;

            // The reservation is manager-local and still pinned, so staleness
            // alone must not doom this join: a sibling manager's abort cleanup
            // (or reset) may have advanced the shared epoch while this join
            // waited for the writer. Storage truth decides instead. Every
            // owner installation advances the epoch UNDER this same writer,
            // so a claim observed while NOT stale cannot belong to a new
            // owner; while stale, only the exact root this reservation was
            // created to replace may be cleared.
            let stale = !self.is_storage_generation_current();
            let current_claim = self.0.collectionset.system_root_claim().await?;
            let claim_allows = match &current_claim {
                None => true,
                Some(claim) if claim.entity_id() == proof.entity_id() => true,
                Some(_) if needs_reset && !stale => true,
                Some(claim) if needs_reset && reservation_replaces == Some(claim.entity_id()) => true,
                Some(_) => false,
            };
            if !claim_allows {
                return Err(MutationError::General(Box::new(std::io::Error::other(format!(
                    "storage is already claimed by system root {}",
                    current_claim.map(|claim| claim.entity_id().to_string()).unwrap_or_default()
                )))));
            }
            let _destructive_reset = needs_reset.then(|| FlagActivity::new(&self.0.destructive_resetting));

            if needs_reset {
                tracing::info!("Resetting storage to replace mismatched reserved root");
                self.clear_local_state_locked(false, catalog_reset_hook.as_ref()).await?;
            } else if stale {
                // Same-root fast path raced a sibling clear: rebind this
                // manager to the current epoch (becoming the new shared
                // owner) before persisting. Storage was cleared by whoever
                // advanced, so there is nothing to delete here.
                let (new_storage_epoch, fresh_generation) = self.0.collectionset.advance_storage_epoch();
                self.0.bound_storage_epoch.store(new_storage_epoch, Ordering::Release);
                self.publish_first_owner_with_generation(false, fresh_generation);
                self.0.collectionset.publish_storage_epoch();
            }

            let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
            let storage = self.0.collectionset.get(&collection_id).await?;
            match self.0.collectionset.claim_system_root(&proof).await? {
                SystemRootClaim::Claimed => {
                    if !needs_reset && !stale {
                        // A successful first claim establishes a new shared
                        // storage owner just like create(). Managers that
                        // reserved a competing root in the old empty epoch
                        // must be stale before this writer is released.
                        let (new_storage_epoch, fresh_generation) = self.0.collectionset.advance_storage_epoch();
                        self.0.bound_storage_epoch.store(new_storage_epoch, Ordering::Release);
                        self.publish_first_owner_with_generation(false, fresh_generation);
                        self.0.collectionset.publish_storage_epoch();
                    }
                }
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
            if let Some(hook) = self.0.catalog_reset_hook.read().unwrap().clone() {
                (hook.resume)();
            }
            notice_info!("Persisted verified system root and completed join");
            Ok(())
        }
        .await;
        if result.is_err() && advisory_needs_reset {
            // The drain ran but its reset never reached `clear_local_state_locked`
            // (or failed before finishing). Release the catalog latch so later
            // lifecycle operations are not stranded behind `resetting`.
            if let Some(hook) = &catalog_reset_hook {
                (hook.finish)();
            }
        }
        result
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
        // Serialize with the other lifecycle transitions so a second reset or
        // join cannot interleave between this drain and the deletion below,
        // and so the catalog's begin/finish phases cannot cross-pair between
        // two concurrent destructive operations on this manager.
        let _lifecycle = self.0.lifecycle.lock().await;
        // Drain BEFORE the exclusive gate: admitted catalog effects may need a
        // storage read lease to complete, which the write lease would block.
        let catalog_reset_hook = self.0.catalog_reset_hook.read().unwrap().clone();
        if let Some(hook) = &catalog_reset_hook {
            (hook.begin)().await;
        }
        let _reset_activity = ResetActivity::new(self.0.collectionset.storage_write_lease().await, &self.0.resetting);
        let _destructive_reset = FlagActivity::new(&self.0.destructive_resetting);
        let result = async {
            // Another Node using this exact storage engine may have reset and
            // created a new system while this abort waited for the writer.
            // In that case this manager may invalidate only its local caches;
            // it must never advance the epoch or delete the new owner's rows.
            if !self.is_storage_generation_current() {
                self.invalidate_local_state(true, None);
                if let Some(hook) = &catalog_reset_hook {
                    (hook.finish)();
                }
                return Ok(());
            }
            if self.0.collectionset.system_root_claim().await?.is_some_and(|claimed| claimed != abort.proof) {
                // A different manager won and published while this abort was
                // queued. Even on a legacy/non-advanced epoch, never erase a
                // durable claim that does not belong to this reservation.
                self.invalidate_local_state(true, None);
                if let Some(hook) = &catalog_reset_hook {
                    (hook.finish)();
                }
                return Ok(());
            }
            self.clear_local_state_locked(false, catalog_reset_hook.as_ref()).await?;
            let cleared = {
                let mut root = self.0.root.write().unwrap();
                match root.as_ref() {
                    Some(binding) if binding.status == RootStatus::Aborting && binding.proof == abort.proof => {
                        *root = None;
                        true
                    }
                    _ => false,
                }
            };
            // Readiness waiters ride through a pending reservation even while
            // stale; clearing it is the transition that makes their staleness
            // final, so they must be woken here.
            self.0.system_ready_notify.notify_waiters();
            if cleared {
                Ok(())
            } else {
                Err(MutationError::General(Box::new(std::io::Error::other("system root reservation changed during abort cleanup"))))
            }
        }
        .await;
        if result.is_err() {
            // Pair the drain above with a latch release even when cleanup
            // errored before (or inside) the locked clear. `finish` is
            // idempotent, so a second call after the locked clear is safe.
            if let Some(hook) = &catalog_reset_hook {
                (hook.finish)();
            }
        }
        result
    }

    /// Resets all storage by deleting all collections, including the system collection.
    /// Ephemeral nodes use this when joining a different system; durable nodes
    /// may use it before creating a replacement root in place.
    /// **This is a destructive operation and should be used with extreme caution.**
    pub async fn hard_reset(&self) -> Result<()> {
        let _lifecycle = self.0.lifecycle.lock().await;
        self.clear_local_state(true).await.map_err(anyhow::Error::from)
    }

    /// Resume a reset that a cancelled or failed earlier transition left
    /// incomplete. Caller holds `lifecycle`.
    async fn resume_incomplete_reset(&self) -> Result<(), MutationError> {
        let clear_root = self.0.reset_incomplete_clear_root.load(Ordering::Acquire);
        self.clear_local_state(clear_root).await?;
        if !clear_root {
            // Complete a cancelled abort's intent: its cleanup owned the
            // reservation, and nothing else will ever remove an Aborting
            // binding whose cleanup task died.
            {
                let mut root = self.0.root.write().unwrap();
                if matches!(root.as_ref(), Some(binding) if binding.status == RootStatus::Aborting) {
                    *root = None;
                }
            }
            self.0.system_ready_notify.notify_waiters();
        }
        Ok(())
    }

    /// Clear system-owned storage and caches. A reserved first-join root can be
    /// preserved while the old system's storage is deleted; this is what keeps
    /// the reservation race-free across the async reset. Caller holds
    /// `lifecycle`; the catalog drain runs BEFORE the exclusive storage gate is
    /// taken, because draining waits for admitted catalog effects whose
    /// application may itself need a storage read lease.
    async fn clear_local_state(&self, clear_root: bool) -> Result<(), MutationError> {
        self.0.reset_incomplete.store(true, Ordering::Release);
        self.0.reset_incomplete_clear_root.store(clear_root, Ordering::Release);
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

        let _reset_activity = ResetActivity::new(self.0.collectionset.storage_write_lease().await, &self.0.resetting);
        let _destructive_reset = FlagActivity::new(&self.0.destructive_resetting);
        self.clear_local_state_locked(clear_root, catalog_reset_hook.as_ref()).await
    }

    /// Clear while the caller owns the reset writer AND has already run the
    /// catalog drain (`hook.begin`). Root reservation status decides whether
    /// one pending new-founder PeerState is preserved; old ready peers and all
    /// other sessions are always drained. Exactly one `hook.finish` runs on
    /// every path, only after storage deletion settles, so the catalog cannot
    /// warm a new generation against rows that are still being deleted.
    async fn clear_local_state_locked(&self, clear_root: bool, catalog_reset_hook: Option<&CatalogResetHook>) -> Result<(), MutationError> {
        self.0.reset_incomplete.store(true, Ordering::Release);
        self.0.reset_incomplete_clear_root.store(clear_root, Ordering::Release);
        let (new_storage_epoch, fresh_generation) = self.0.collectionset.advance_storage_epoch();
        self.0.bound_storage_epoch.store(new_storage_epoch, Ordering::Release);
        let preserved_pending_root = if clear_root {
            None
        } else {
            self.0
                .root
                .read()
                .unwrap()
                .as_ref()
                .and_then(|binding| matches!(binding.status, RootStatus::Reserved { .. }).then(|| binding.proof.entity_id()))
        };

        self.invalidate_local_state_with_generation(clear_root, preserved_pending_root, Some(fresh_generation));
        self.0.collectionset.publish_storage_epoch();

        // Delete all collections from storage. Even a failed deletion must
        // release the catalog's resetting latch so callers are not stranded;
        // the incomplete-reset latch stays set for the retry.
        if let Err(error) = self.0.collectionset.delete_all_collections().await {
            if let Some(hook) = catalog_reset_hook {
                (hook.finish)();
            }
            return Err(error);
        }

        if let Some(hook) = catalog_reset_hook {
            (hook.finish)();
        }
        // No await exists between successful deletion and this store, so
        // cancellation leaves the flag set exactly for incomplete transitions.
        self.0.reset_incomplete.store(false, Ordering::Release);

        Ok(())
    }

    /// Invalidate state owned solely by this manager. This contains no await
    /// and never touches shared storage, so a stale manager can safely use it
    /// after losing the engine epoch without erasing the succeeding system.
    fn invalidate_local_state(&self, clear_root: bool, preserved_pending_root: Option<proto::EntityId>) {
        self.invalidate_local_state_with_generation(clear_root, preserved_pending_root, None);
    }

    fn invalidate_local_state_with_generation(
        &self,
        clear_root: bool,
        preserved_pending_root: Option<proto::EntityId>,
        fresh_generation: Option<Arc<AtomicBool>>,
    ) {
        self.invalidate_local_state_inner(clear_root, fresh_generation.clone(), PeerSessionsReset::Drain { preserved_pending_root });
    }

    /// A first-owner publication over EMPTY storage: no prior system existed,
    /// so live peer sessions are rebound rather than drained. See
    /// [`PeerSessionsReset::Rebind`].
    fn publish_first_owner_with_generation(&self, clear_root: bool, fresh_generation: Arc<AtomicBool>) {
        self.invalidate_local_state_inner(clear_root, Some(fresh_generation.clone()), PeerSessionsReset::Rebind { fresh_generation });
    }

    fn invalidate_local_state_inner(&self, clear_root: bool, fresh_generation: Option<Arc<AtomicBool>>, peers: PeerSessionsReset) {
        // Publish unready and invalidate all resident handles before any
        // destructive await. A failure to delete therefore remains fail
        // closed instead of leaving old strong handles live.
        *self.0.system_ready.write().unwrap() = false;
        match fresh_generation {
            Some(generation) => self.0.entities.system_reset_to(generation),
            None => self.0.entities.system_reset(),
        }
        if let Some(hook) = self.0.peer_reset_hook.read().unwrap().clone() {
            hook(peers);
        }

        self.0.items.write().unwrap().clear();
        if clear_root {
            *self.0.root.write().unwrap() = None;
        }
        self.0.collection_map.write().unwrap().clear();

        *self.0.load_error.write().unwrap() = None;

        // Reset the reactor state to notify subscriptions. Catalog map
        // flushing is NOT done here: the three-phase catalog hook (begin
        // drain / finish / resume) is driven by the owning lifecycle
        // operation, which sequences `finish` after any storage deletion.
        self.0.reactor.system_reset();
        self.0.system_ready_notify.notify_waiters();
    }

    /// Returns true if the local system catalog is loaded
    pub fn is_loaded(&self) -> bool { self.0.loaded.get().is_some() }

    /// Waits for the local system catalog to be loaded
    pub async fn wait_loaded(&self) {
        loop {
            // notify_waiters stores no permit. Arm the waiter before reading
            // the flag so a transition between the check and await cannot
            // strand a caller.
            let notified = self.0.loading.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_loaded() {
                return;
            }
            notified.await;
        }
    }

    fn mark_loaded(&self) {
        let _ = self.0.loaded.set(());
        self.0.loading.notify_waiters();
    }

    async fn load_system_catalog(&self) -> Result<()> {
        let _lifecycle = self.0.lifecycle.lock().await;
        if self.0.reset_incomplete.load(Ordering::Acquire) {
            self.resume_incomplete_reset().await?;
        }
        if self.is_loaded() {
            return Err(anyhow!("System catalog already loaded"));
        }

        let system_generation = self.0.entities.system_generation();
        let _reset_guard = self.guard_generation_allow_unready(&system_generation).await?;

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
                Some(RootBinding { proof, status: RootStatus::Reserved { needs_reset, replaces }, .. }) => {
                    let mismatched = loaded_root.as_ref().filter(|loaded| loaded.proof.entity_id() != proof.entity_id());
                    *needs_reset = mismatched.is_some();
                    *replaces = mismatched.map(|loaded| loaded.proof.entity_id());
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
            if let Some(hook) = self.0.catalog_reset_hook.read().unwrap().clone() {
                (hook.resume)();
            }
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
