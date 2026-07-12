//! The in-memory catalog map and its maintenance
//! (specs/model-property-metadata/rfc.md section 5.2).
//!
//! Every node keeps a live view of the three catalog collections
//! (`_ankurah_model`, `_ankurah_property`, `_ankurah_model_property`) so
//! that property references resolve locally. The map is maintained two
//! ways, mirroring how the two node kinds already replicate:
//!
//! - DURABLE nodes have the catalog in local storage. Once the system is
//!   ready the manager warms the map by scanning the three collections
//!   (`fetch_states` with `Predicate::True`, the same move
//!   `SystemManager::load_system_catalog` makes for the system collection),
//!   then keeps it fresh with a POLICY-FREE reactor subscription: the map
//!   is node infrastructure like `SystemManager` (which reads storage with
//!   no policy). Mutation stays gated by `check_event` in the executor and
//!   remote access stays gated server-side, so a policy-free local read is
//!   sound.
//! - EPHEMERAL nodes get the catalog through the ordinary subscription
//!   relay. The first `context`/`context_async` call drives
//!   [`CatalogManager::ensure_subscribed`], which stands up three
//!   [`EntityLiveQuery`]s with `Predicate::True` over the catalog
//!   collections; their reactor updates feed the same map. Catalog
//!   visibility on an ephemeral node therefore follows that node's user
//!   credentials.
//!
//! Catalog entities are SYSTEM MODELS (RFC section 4): they are read
//! through the raw `Entity`/backend interface, never a `View`, because
//! deriving a `Model` for a catalog collection would be the
//! self-description ouroboros the RFC forbids. Live entities are parsed
//! through the `AbstractEntity::value` accessor; storage states are parsed
//! through `LWWBackend::from_state_buffer` + `property_values`, exactly as
//! `registration::catalog_entity_values` does.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use ankurah_proto::{self as proto, CollectionId, EntityId, QueryId};
use ankurah_signals::{porcelain::subscribe::SubscriptionGuard, Subscribe};
use tokio::sync::Notify;
use tracing::{debug, error, warn};

use crate::{
    collectionset::CollectionSet,
    entity::{Entity, WeakEntitySet},
    livequery::EntityLiveQuery,
    node::{Node, WeakNode},
    policy::PolicyAgent,
    reactor::{AbstractEntity, GapFetcher, MembershipChange, Reactor, ReactorSubscription, ReactorUpdate},
    resultset::EntityResultSet,
    storage::StorageEngine,
    util::request_fence::{RequestFence, RequestLease, RequestValidity},
};

use super::{model_collection, model_property_collection, property_collection, registration::RegistrationError, ModelSchema};

/// A short grace period for a connected catalog relay to deliver its initial
/// snapshots. The catalog remains a cache: expiry falls back to local state,
/// while the live relay subscriptions stay installed and may populate the map
/// later.
const REMOTE_CATALOG_WARM_GRACE: Duration = Duration::from_secs(2);

/// Runs a rollback closure unless a successful initialization disarms it.
/// Keeping this guard inside the node-owned initialization task releases the
/// first-call latch on construction failure and lets generation invalidation
/// leave any newer claimant untouched.
struct RollbackGuard<F: FnOnce()> {
    rollback: Option<F>,
}

impl<F: FnOnce()> RollbackGuard<F> {
    fn new(rollback: F) -> Self { Self { rollback: Some(rollback) } }
    fn disarm(&mut self) { self.rollback = None; }
}

impl<F: FnOnce()> Drop for RollbackGuard<F> {
    fn drop(&mut self) {
        if let Some(rollback) = self.rollback.take() {
            rollback();
        }
    }
}

mod map;
use map::{apply_entry, parse_state, CatalogMapInner, EnsuredSchemaBinding, Entry};
pub use map::{MembershipDef, ModelDef, PropertyDef};

// -- gap fetcher ------------------------------------------------------------

/// Catalog queries are `Predicate::True` with no LIMIT, so no gap ever
/// forms; this fetcher is never asked to fill one.
struct NoopGapFetcher;

#[async_trait::async_trait]
impl GapFetcher<Entity> for NoopGapFetcher {
    async fn fetch_gap(
        &self,
        _collection_id: &CollectionId,
        _selection: &ankql::ast::Selection,
        _last_entity: Option<&Entity>,
        _gap_size: usize,
    ) -> Result<Vec<Entity>, crate::error::RetrievalError> {
        Ok(Vec::new())
    }
}

// -- manager ----------------------------------------------------------------

/// The three catalog collections warmed and maintained by this manager.
fn catalog_collections() -> [CollectionId; 3] { [model_collection(), property_collection(), model_property_collection()] }

/// Maintains the in-memory catalog map for a node. Held by `Node` beside
/// `SystemManager`; mirrors its `<SE, PA>` generics.
pub struct CatalogManager<SE, PA>(Arc<CatalogInner<SE, PA>>)
where PA: PolicyAgent;

impl<SE, PA> Clone for CatalogManager<SE, PA>
where PA: PolicyAgent
{
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct CatalogInner<SE, PA>
where PA: PolicyAgent
{
    collectionset: CollectionSet<SE>,
    /// Held per the `CatalogManager::new` contract (mirrors SystemManager).
    /// The durable warm reads LWW state buffers directly (like
    /// `registration::catalog_entity_values`) rather than resident entities,
    /// and the ephemeral path uses LiveQueries, so this is currently unused;
    /// retained for parity and for a future warm that materializes resident
    /// entities.
    #[allow(dead_code)]
    entities: WeakEntitySet,
    reactor: Reactor,
    durable: bool,
    /// The allocator mutex (RFC 5.1 executor discipline): the registration
    /// executor serializes every RegisterSchema execution on this lock and
    /// upserts its allocations into the map synchronously before releasing
    /// it, because the reactor-fed map alone lags commit and consecutive
    /// registrations must never double-allocate.
    allocator: tokio::sync::Mutex<()>,
    map: RwLock<CatalogMapInner>,
    ready: RwLock<bool>,
    ready_notify: Notify,
    /// Durable: the reactor subscription that keeps the map fresh; dropping
    /// it unsubscribes. Ephemeral: unused.
    durable_sub: RwLock<Option<(ReactorSubscription, SubscriptionGuard)>>,
    /// Ephemeral: the live queries + their reactor-update guards. Held so
    /// the subscriptions stay alive and drop cleanly.
    ephemeral_queries: RwLock<Vec<(EntityLiveQuery, SubscriptionGuard)>>,
    /// Monotonic catalog-warm generation plus the ephemeral first-call-wins
    /// latch. Reset invalidates the generation before clearing the catalog so
    /// neither a detached ephemeral warm nor a slow durable startup warm can
    /// publish afterward.
    setup_state: RwLock<CatalogSetupState>,
    /// Wakes detached ephemeral warm tasks when reset invalidates their
    /// generation, so they promptly remove in-flight relay entries rather
    /// than waiting for a response or the grace deadline.
    setup_changed: Notify,
    /// Collections whose registration has been ENSURED for this process
    /// (RFC 5.2 model first-use latch). Latched on a successful durable
    /// execution or a successful forwarded RegisterSchema (the response
    /// consumed into the map). A strict error (executor/policy refusal, or
    /// a never-registered offline error) does NOT latch. Cleared by
    /// `reset` (allocated ids belong to one system and must not survive
    /// hard_reset).
    /// Exact compiled-schema bindings successfully checked in this process,
    /// grouped by collection. Collection-only latching is insufficient: two
    /// model declarations can use different identities for the same local
    /// field name, and display-name changes must not erase an established
    /// binding.
    ensured: RwLock<BTreeMap<String, Vec<EnsuredSchemaBinding>>>,
    /// The manager stays generic over the node's PolicyAgent for its
    /// Node-taking methods (ensure_registered, ensure_subscribed).
    _pa: std::marker::PhantomData<PA>,
}

#[derive(Debug, Default)]
struct CatalogSetupState {
    generation: u64,
    ephemeral_active: bool,
    /// While true, `ensure_subscribed` may wait but cannot claim a new warm.
    /// SystemManager clears it only after storage and reactor reset finish.
    resetting: bool,
    /// Quiescing owner fence for initial relay responses. Reset invalidates it
    /// before storage deletion and waits for responses already admitted at
    /// schema ingress to finish NodeApplier.
    ephemeral_fence: Option<RequestFence>,
    /// Quiescing owner fence for the current durable storage warm. The warm
    /// retains one lease from before its first storage access through
    /// subscription/readiness publication, so reset can invalidate the
    /// generation and drain it before deleting storage.
    durable_fence: Option<RequestFence>,
    /// Owner fence for schema registration in the current system epoch.
    /// It remains absent while no system is ready and is rearmed only by the
    /// ready hook after startup or reset. Both allocator execution and
    /// forwarded-response folding retain leases across their map effects.
    registration_fence: Option<RequestFence>,
    /// Invalidated owners retained until reset finishes. `hard_reset` is
    /// cancellation-safe at the barrier: a retry while `resetting` clones and
    /// drains the same fences instead of bypassing work whose first waiter was
    /// canceled.
    draining_fences: Vec<RequestFence>,
    /// A durable hard reset drops its reactor subscription. Once the new
    /// system root is ready, one warm must attach the current generation.
    durable_resume_pending: bool,
}

impl<SE, PA> CatalogInner<SE, PA>
where PA: PolicyAgent
{
    /// Resolve a compiled alias through identities retained at registration.
    /// This applies to ordinary and explicit fields alike: both must survive a
    /// later display-name change. Multiple admitted declarations mapping the
    /// same local name to different ids are ambiguous and fail closed.
    fn resolve_property(&self, collection: &str, name: &str) -> Option<EntityId> {
        let matching_bindings: Vec<_> = self
            .ensured
            .read()
            .unwrap()
            .get(collection)
            .into_iter()
            .flat_map(|bindings| bindings.iter())
            .filter_map(|binding| binding.fields.get(name).copied().map(|property| (binding.model, property)))
            .collect();
        if matching_bindings.is_empty() {
            return self.map.read().unwrap().resolve(collection, name);
        }

        let map = self.map.read().unwrap();
        let mut candidates = BTreeSet::new();
        for (model, property) in matching_bindings {
            if map.membership(&model, &property).is_some() && map.properties.contains_key(&property) {
                candidates.insert(property);
            }
        }

        if candidates.len() != 1 {
            tracing::warn!(
                "ensured schemas map compiled property name '{}.{}' to {} ids; refusing ambiguous resolution",
                collection,
                name,
                candidates.len()
            );
            return None;
        }

        candidates.iter().next().copied()
    }
}

impl<SE, PA> crate::schema::CatalogResolver for CatalogInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> { self.resolve_property(collection, name) }
    fn name_for(&self, id: &EntityId) -> Option<String> { self.map.read().unwrap().properties.get(id).map(|def| def.name.clone()) }
    fn model_id_for(&self, collection: &str) -> Option<EntityId> { self.map.read().unwrap().by_collection.get(collection).copied() }
    fn canonical_value_type(&self, id: &EntityId) -> Option<String> {
        self.map.read().unwrap().properties.get(id).map(|def| def.value_type.clone())
    }
}

impl<SE, PA> CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Stored catalog definition states needed to describe `models` on the
    /// wire. Returns model ids whose definition could not be loaded so the
    /// connection can make them eligible for a later announcement retry.
    pub(crate) async fn definition_states_for_models(
        &self,
        models: &[EntityId],
    ) -> (Vec<proto::Attested<proto::EntityState>>, Vec<EntityId>) {
        if models.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let (Ok(model_col), Ok(property_col), Ok(membership_col)) = (
            self.0.collectionset.get(&model_collection()).await,
            self.0.collectionset.get(&property_collection()).await,
            self.0.collectionset.get(&model_property_collection()).await,
        ) else {
            return (Vec::new(), models.to_vec());
        };

        let mut states = Vec::new();
        let mut failed = Vec::new();
        for model in models {
            match model_col.get_state(*model).await {
                Ok(state) => states.push(state),
                Err(error) => {
                    warn!("Cannot load model definition {} for wire announcement: {}", model.to_base64_short(), error);
                    failed.push(*model);
                    continue;
                }
            }
            for membership in self.memberships_of(model) {
                if let Ok(state) = property_col.get_state(membership.property).await {
                    states.push(state);
                }
                if let Ok(state) = membership_col.get_state(membership.id).await {
                    states.push(state);
                }
            }
        }
        (states, failed)
    }

    pub(crate) fn new(collections: CollectionSet<SE>, entities: WeakEntitySet, reactor: Reactor, durable: bool) -> Self {
        Self(Arc::new(CatalogInner {
            collectionset: collections,
            entities,
            reactor,
            durable,
            allocator: tokio::sync::Mutex::new(()),
            map: RwLock::new(CatalogMapInner::default()),
            ready: RwLock::new(false),
            ready_notify: Notify::new(),
            durable_sub: RwLock::new(None),
            ephemeral_queries: RwLock::new(Vec::new()),
            setup_state: RwLock::new(CatalogSetupState::default()),
            setup_changed: Notify::new(),
            ensured: RwLock::new(BTreeMap::new()),
            _pa: std::marker::PhantomData,
        }))
    }

    /// Called right after the `NodeInner` Arc exists (beside
    /// `policy_agent.on_node_ready`). It installs the hard-reset/readiness
    /// hooks. Durable nodes arm a warm that the system-ready hook launches;
    /// ephemeral catalog setup remains driven by `ensure_subscribed`.
    pub(crate) fn start(&self, node: WeakNode<SE, PA>) {
        let Some(strong) = node.upgrade() else { return };

        // Install the hard-reset flush hook on the system manager so
        // SystemManager::hard_reset can clear the catalog in-place (it does
        // not hold the CatalogManager directly).
        {
            let begin_manager = self.clone();
            let finish_manager = self.clone();
            let resume_manager = self.clone();
            strong.system.set_catalog_reset_hook(
                Arc::new(move || {
                    let manager = begin_manager.clone();
                    Box::pin(async move { manager.begin_reset().await })
                }),
                Arc::new(move || finish_manager.finish_reset()),
                Arc::new(move || resume_manager.resume_after_system_ready()),
            );
        }

        if self.0.durable {
            // A durable node may remain deliberately uninitialized. Do not
            // spawn a task that owns the managers while waiting indefinitely
            // for a system root. Instead, arm exactly one warm and let
            // SystemManager's create/load-ready transition call the hook.
            self.0.setup_state.write().unwrap().durable_resume_pending = true;
        }

        // Every ready system epoch gets one registration fence, on either
        // node kind. If loading/joining won the race before hook installation,
        // this claims the missed transition; otherwise SystemManager calls it.
        if strong.system.is_system_ready() {
            self.resume_after_system_ready();
        }
    }

    /// Run one generation's durable warm and always release readiness for a
    /// still-current generation. The system-ready hook launches both startup
    /// and post-reset generations; no task waits indefinitely for a root.
    async fn run_durable_warm(&self, generation: u64, _lease: RequestLease) {
        if let Err(e) = self.warm_and_subscribe_durable(generation).await {
            error!("CatalogManager durable warm failed: {}", e);
            // Readiness must still latch: ingress resolution
            // (Node::resolve_model_wait) parks on it, and a permanently
            // un-ready catalog would turn one failed warm into a hang.
            // With a partial map, later resolutions reject loudly instead,
            // which is the retryable failure mode we want.
            let setup = self.0.setup_state.read().unwrap();
            if setup.generation == generation {
                self.mark_ready();
            }
        }
    }

    /// Durable path: warm the map by scanning the catalog collections that
    /// ALREADY exist in storage (never materializing empty ones), then
    /// attach a policy-free, fetch-free reactor subscription so future
    /// registrations -- executed locally or relayed from peers -- keep the map
    /// fresh, then mark ready.
    ///
    /// A schema-less durable node has no catalog collections yet; it warms
    /// nothing, subscribes without conjuring empty `_ankurah_*` trees, and is
    /// immediately ready with an empty (correct) map. The trees appear only
    /// when a real registration commits.
    async fn warm_and_subscribe_durable(&self, generation: u64) -> Result<(), crate::error::RetrievalError> {
        if self.0.setup_state.read().unwrap().generation != generation {
            return Ok(());
        }

        // Attach the incremental reactor subscription BEFORE the storage
        // scan, so a registration committing mid-warm is never missed: on
        // every commit path set_state precedes notify_change, so the scan can
        // never read anything OLDER than what the listener concurrently
        // applied, and upserts are idempotent by entity id. Registered
        // fetch-free (add_query_no_fetch) so watching a not-yet-existing
        // catalog collection does not materialize it; the wildcard watcher
        // still routes every future change (local registration or relayed
        // catalog event) to the listener, which applies it to the map.
        let catalog = catalog_collections();
        let subscription = self.0.reactor.subscribe();
        let guard = {
            // CatalogInner retains this guard, so the callback must not retain
            // CatalogInner in return. Otherwise durable_sub forms a permanent
            // CatalogInner -> guard -> callback -> CatalogInner cycle.
            let weak = Arc::downgrade(&self.0);
            subscription.subscribe(move |update: ReactorUpdate| {
                if let Some(inner) = weak.upgrade() {
                    let me = CatalogManager(inner);
                    // Serialize the generation check and map update with
                    // reset, which takes the write side before clearing.
                    let setup = me.0.setup_state.read().unwrap();
                    if setup.generation == generation {
                        me.apply_reactor_update(update);
                    }
                }
            })
        };

        for collection in &catalog {
            let resultset = EntityResultSet::empty();
            let gap_fetcher: Arc<dyn GapFetcher<Entity>> = Arc::new(NoopGapFetcher);
            self.0
                .reactor
                .add_query_no_fetch(
                    subscription.id(),
                    QueryId::new(),
                    collection.clone(),
                    ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None },
                    resultset,
                    gap_fetcher,
                )
                .map_err(|e| crate::error::RetrievalError::Other(format!("catalog add_query failed: {e}")))?;
        }

        // Now merge the storage scan into the LIVE map (never a wholesale
        // replace, which would clobber entries the listener applied while we
        // were scanning). Only the catalog collections that already exist are
        // read; `get` (and thus fetch_states) would otherwise materialize
        // empty `_ankurah_*` trees on every schema-less durable node.
        // Propagate a listing failure rather than silently treating it as "no
        // collections exist" (which would warm nothing and come up empty even
        // though data is present). `start` catches this, logs it, and still
        // latches readiness, so resolution rejects loudly instead of hanging.
        let existing = self.0.collectionset.engine_collections().await?;
        for collection in &catalog {
            if !existing.contains(collection) {
                continue;
            }
            let storage = self.0.collectionset.get(collection).await?;
            let states = storage
                .fetch_states(&ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None })
                .await?;
            let setup = self.0.setup_state.read().unwrap();
            if setup.generation != generation {
                return Ok(());
            }
            let mut map = self.0.map.write().unwrap();
            for state in states {
                if let Some(entry) = parse_state(collection, state.payload.entity_id, &state.payload) {
                    apply_entry(&mut map, entry);
                }
            }
        }

        // Keep the generation read lock through publication/readiness. Reset
        // either invalidates first (and this task drops its local guard) or
        // runs afterward and clears the just-published state.
        let setup = self.0.setup_state.read().unwrap();
        if setup.generation != generation {
            return Ok(());
        }
        *self.0.durable_sub.write().unwrap() = Some((subscription, guard));
        self.mark_ready();
        Ok(())
    }

    /// Ephemeral path: on the first call, stand up three
    /// [`EntityLiveQuery`]s (`Predicate::True`) over the catalog
    /// collections, feed their reactor updates into the map, wait for all
    /// three to initialize, and mark ready. The winning caller launches that
    /// setup as a node-owned task, then waits like every concurrent caller.
    /// Cancelling `context_async` therefore cannot cancel the shared warm or
    /// drop half-created remote subscriptions. A rollback guard in the task
    /// releases the latch if construction itself fails so a later caller can
    /// retry.
    pub async fn ensure_subscribed(&self, cdata: PA::ContextData, node: &Node<SE, PA>) {
        if self.0.durable {
            return; // durable nodes warm from storage, never subscribe via relay
        }

        let mut launched_generation = None;
        loop {
            // `finish_reset` releases catalog locks before a replacement root
            // is joined. Do not let a woken waiter claim that rootless gap and
            // launch relay work against old peers; readiness is awaited with
            // no setup lock held. A reset racing after this wait closes
            // readiness before taking setup and invalidates any claim that
            // won first.
            if !node.system.is_system_ready() {
                node.system.wait_system_ready().await;
                continue;
            }
            // Enable the waiter before inspecting either readiness or the
            // claim latch. The setup task may complete on another executor
            // turn immediately after it is spawned.
            let notified = self.0.ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_catalog_ready() {
                return;
            }
            if let Some(generation) = launched_generation {
                let setup = self.0.setup_state.read().unwrap();
                if !setup.ephemeral_active || setup.generation != generation {
                    // This caller's detached setup rolled its claim back or a
                    // hard reset invalidated it. Do not immediately reclaim
                    // and spin on a deterministic constructor failure; return
                    // without claiming readiness and let a later call (or an
                    // already-waiting non-claimant) retry.
                    return;
                }
            }

            let claimed = {
                let mut setup = self.0.setup_state.write().unwrap();
                if !node.system.is_system_ready() || setup.resetting || setup.ephemeral_active {
                    None
                } else {
                    setup.generation = setup.generation.wrapping_add(1);
                    setup.ephemeral_active = true;
                    let fence = RequestFence::new();
                    setup.ephemeral_fence = Some(fence.clone());
                    Some((setup.generation, fence))
                }
            };
            if let Some((generation, fence)) = claimed {
                launched_generation = Some(generation);
                let manager = self.clone();
                let node = node.clone();
                let cdata = cdata.clone();
                crate::task::spawn(async move {
                    manager.initialize_ephemeral_subscriptions(cdata, node, generation, fence).await;
                });
            }
            if self.is_catalog_ready() {
                return;
            }
            let (setup_active, resetting) = {
                let setup = self.0.setup_state.read().unwrap();
                (setup.ephemeral_active, setup.resetting)
            };
            if resetting {
                notified.await;
                continue;
            }
            if !setup_active {
                // The task this caller launched failed construction. Return
                // the context without claiming readiness; a subsequent call
                // can retry. A concurrent non-claimant loops and becomes the
                // next claimant, preserving first-call race behavior.
                if launched_generation.is_some() {
                    return;
                }
                continue;
            }
            notified.await;
        }
    }

    async fn initialize_ephemeral_subscriptions(&self, cdata: PA::ContextData, node: Node<SE, PA>, generation: u64, fence: RequestFence) {
        // Own the complete setup attempt, not just its local fetches and wire
        // responses. Reset wakes the invalidation selects below, then waits
        // for this lease so relay entries are discarded before deletion even
        // when async selection resolution raced their registration.
        let Some(_setup_lease) = fence.try_acquire() else { return };
        let rollback_manager = self.clone();
        let mut claim_guard = RollbackGuard::new(move || {
            let mut setup = rollback_manager.0.setup_state.write().unwrap();
            if setup.ephemeral_active && setup.generation == generation && !rollback_manager.is_catalog_ready() {
                setup.ephemeral_active = false;
                if let Some(fence) = setup.ephemeral_fence.take() {
                    fence.invalidate();
                }
                drop(setup);
                rollback_manager.0.ready_notify.notify_waiters();
            }
        });

        let mut queries = Vec::with_capacity(3);
        for collection in catalog_collections() {
            // cached: true -- the catalog subscription is a CACHE (maintainer
            // ruling 2026-07-06): it accelerates resolution and enables
            // offline use; registration is the source of truth for any
            // doubt. The cached query activates from local storage
            // immediately (catalog entities persisted by earlier sessions),
            // then merges the relay snapshot and live updates as they land.
            let args = crate::node::MatchArgs {
                selection: ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None },
                cached: true,
            };
            let request_validity = RequestValidity::fenced(fence.clone());
            let lq = match EntityLiveQuery::new_weak_node_with_request_validity(
                &node,
                collection.clone(),
                args,
                cdata.clone(),
                request_validity,
            ) {
                Ok(lq) => lq,
                Err(e) => {
                    error!("CatalogManager ephemeral subscribe to {} failed: {}", collection, e);
                    // The claim guard rolls the latch back and wakes waiters.
                    return;
                }
            };
            queries.push(lq);
        }

        // Cached activation makes the local catalog immediately usable
        // offline, but it is not a remote snapshot. When a durable peer is
        // connected, wait until each relay subscription has applied its
        // initial response before calling the map ready; otherwise a dynamic
        // binding could misclassify a valid property from the authority as
        // unknown during this window. If the relay is offline or fails, keep
        // the cache's offline-enabler semantics and proceed with local state.
        {
            let invalidated = self.0.setup_changed.notified();
            tokio::pin!(invalidated);
            invalidated.as_mut().enable();
            if self.0.setup_state.read().unwrap().generation != generation {
                Self::discard_ephemeral_queries(&node, &queries);
                return;
            }
            let initialized = futures::future::join_all(queries.iter().map(|lq| lq.wait_initialized()));
            tokio::pin!(initialized);
            tokio::select! {
                _ = &mut initialized => {}
                _ = &mut invalidated => {
                    Self::discard_ephemeral_queries(&node, &queries);
                    return;
                }
            }
        }

        if let Some(relay) = &node.subscription_relay {
            let remote_warm = futures::future::join_all(
                queries
                    .iter()
                    .map(|lq| async { lq.wait_initial_query_ready().await && relay.wait_established_or_offline(lq.query_id()).await }),
            );
            tokio::pin!(remote_warm);
            let grace = futures_timer::Delay::new(REMOTE_CATALOG_WARM_GRACE);
            tokio::pin!(grace);
            let invalidated = self.0.setup_changed.notified();
            tokio::pin!(invalidated);
            invalidated.as_mut().enable();
            if self.0.setup_state.read().unwrap().generation != generation {
                Self::discard_ephemeral_queries(&node, &queries);
                return;
            }
            tokio::select! {
                _ = &mut remote_warm => {}
                _ = &mut grace => {
                    warn!("catalog relay did not establish within {:?}; continuing from local cache", REMOTE_CATALOG_WARM_GRACE);
                }
                _ = &mut invalidated => {
                    Self::discard_ephemeral_queries(&node, &queries);
                    return;
                }
            }
        }

        // Serialize the final generation check and all publication with reset.
        // If reset won, dropping `queries` tears down the obsolete relay
        // subscriptions and the generation-aware rollback leaves any newer
        // claimant untouched.
        let setup = self.0.setup_state.read().unwrap();
        if !setup.ephemeral_active || setup.generation != generation {
            drop(setup);
            Self::discard_ephemeral_queries(&node, &queries);
            return;
        }

        // Install map listeners only after the current generation wins its
        // publication fence. A stale remote response is applied through the
        // node's global reactor and would otherwise be visible to a newer
        // generation's listener even though the obsolete listener itself was
        // generation-gated. Subscribing before the resultset scan closes the
        // usual listener/scan race: an update lands either in the callback or
        // in the resultset we seed immediately afterward (possibly both,
        // which is an idempotent upsert).
        let retained: Vec<_> = queries
            .into_iter()
            .map(|lq| {
                // CatalogInner retains the LQ and this guard. Capture only a
                // weak pointer so the callback cannot complete a retain cycle.
                let weak = Arc::downgrade(&self.0);
                let guard = lq.reactor_subscription().subscribe(move |update: ReactorUpdate| {
                    let Some(inner) = weak.upgrade() else { return };
                    let me = CatalogManager(inner);
                    // Hold the generation read lock through map mutation.
                    // Reset takes the write lock before invalidating and
                    // clearing, so an old callback either finishes before the
                    // clear or observes a mismatched generation afterward.
                    let setup = me.0.setup_state.read().unwrap();
                    if setup.ephemeral_active && setup.generation == generation {
                        me.apply_reactor_update(update);
                    }
                });
                (lq, guard)
            })
            .collect();

        // Seed the map from the resultsets in case any Initial items predated
        // our listener.
        {
            let mut map = self.0.map.write().unwrap();
            for (lq, _) in &retained {
                let resultset = lq.resultset();
                let read = resultset.read();
                for (_, entity) in read.iter_entities() {
                    map.upsert(entity);
                }
            }
        }

        *self.0.ephemeral_queries.write().unwrap() = retained;
        self.mark_ready();
        claim_guard.disarm();
    }

    fn discard_ephemeral_queries(node: &Node<SE, PA>, queries: &[EntityLiveQuery]) {
        if let Some(relay) = &node.subscription_relay {
            for query in queries {
                relay.unsubscribe_predicate(query.query_id());
            }
        }
    }

    /// Ingest catalog definition states shipped on a wire envelope (#330):
    /// parse each into its def and
    /// upsert the in-memory map, exactly like the storage warm. Map-only -- a
    /// cache warm; the durable catalog entities still replicate through the
    /// ordinary subscription paths. States whose model id is not a well-known
    /// catalog collection are ignored (defense in depth; the sender only
    /// ships catalog entities).
    pub(crate) fn ingest_wire_states(&self, states: &[proto::Attested<proto::EntityState>]) {
        if states.is_empty() {
            return;
        }
        let mut map = self.0.map.write().unwrap();
        for state in states {
            let Some(collection) = crate::schema::well_known_collection(&state.payload.model) else { continue };
            if !crate::schema::is_catalog_collection(&collection) {
                continue;
            }
            match parse_state(&collection, state.payload.entity_id, &state.payload) {
                Some(Entry::Model(def)) => {
                    // The `schema` envelope field is untrusted and a wider
                    // ingress than the durable subscription it shortcuts, so a
                    // wire model def gets two guards beyond parse_state's shape
                    // check:
                    //  - it must not name a reserved collection. No legitimate
                    //    catalog entity describes an `_ankurah_*` collection
                    //    (the well-known ids have no catalog entity), so such a
                    //    def could only be an attempt to route ordinary traffic
                    //    into a protected collection.
                    //  - an existing model id keeps its collection, and a
                    //    collection already mapped to one id cannot be rebound
                    //    to another. Either mutation would redirect subsequent
                    //    body traffic through poisoned routing metadata. The
                    //    display name remains mutable.
                    if def.collection.starts_with(crate::schema::RESERVED_COLLECTION_PREFIX) {
                        warn!("ignoring shipped model def {} naming reserved collection '{}'", def.id, def.collection);
                    } else if map.models.get(&def.id).is_some_and(|existing| existing.collection != def.collection) {
                        warn!(
                            "ignoring shipped model def {} changing immutable collection from '{}' to '{}'",
                            def.id,
                            map.models.get(&def.id).map(|existing| existing.collection.as_str()).unwrap_or("<unknown>"),
                            def.collection
                        );
                    } else if map.by_collection.get(&def.collection).map_or(false, |existing| *existing != def.id) {
                        warn!("ignoring shipped model def {} rebinding collection '{}'", def.id, def.collection);
                    } else {
                        map.upsert_model(def);
                    }
                }
                Some(Entry::Property(def)) => {
                    // Allocation fixes a property's provenance and canonical
                    // backend/type pair. Only its display name and reference
                    // target are mutable metadata.
                    if let Some(existing) = map.properties.get(&def.id) {
                        if existing.minted_for != def.minted_for || existing.backend != def.backend || existing.value_type != def.value_type
                        {
                            warn!("ignoring shipped property def {} changing immutable provenance/backend/value_type", def.id);
                            continue;
                        }
                    }
                    map.upsert_property(def);
                }
                Some(Entry::Membership(def)) => {
                    // A membership entity is the stable (model, property)
                    // edge. Its optionality may change, but neither endpoint
                    // may be rewritten by an envelope cache warm.
                    if let Some(existing) = map.memberships.get(&def.id) {
                        if existing.model != def.model || existing.property != def.property {
                            warn!("ignoring shipped membership def {} changing immutable endpoints", def.id);
                            continue;
                        }
                    }
                    map.upsert_membership(def);
                }
                None => {}
            }
        }
    }

    /// Apply one reactor update to the map: Remove drops, everything else
    /// upserts. Idempotent (keyed by entity id).
    fn apply_reactor_update(&self, update: ReactorUpdate) {
        {
            let mut map = self.0.map.write().unwrap();
            for item in update.items {
                let is_remove = item.predicate_relevance.iter().any(|(_, change)| matches!(change, MembershipChange::Remove));
                if is_remove {
                    map.remove(&AbstractEntity::collection(&item.entity), AbstractEntity::id(&item.entity));
                } else {
                    map.upsert(&item.entity);
                }
            }
        }
    }

    // -- readiness ----------------------------------------------------------

    pub fn is_catalog_ready(&self) -> bool { *self.0.ready.read().unwrap() }

    /// Whether this manager belongs to a durable node (warms from storage)
    /// as opposed to an ephemeral one (warms by subscription). The
    /// resolution deferral branches on this (resolve.rs).
    pub(crate) fn is_durable(&self) -> bool { self.0.durable }

    pub async fn wait_catalog_ready(&self) {
        // `Notify::notify_waiters` wakes only waiters REGISTERED at that
        // moment (it stores no permit), so the `Notified` future must be
        // created BEFORE the readiness check: checking first and creating the
        // future after would lose a `mark_ready` that lands in between and
        // hang this waiter (and its query) forever. Loop because `reset` can
        // flip readiness back off between the wake and our re-check.
        loop {
            let notified = self.0.ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_catalog_ready() {
                return;
            }
            notified.await;
        }
    }

    /// Snapshot the current system epoch's schema-registration owner. It is
    /// absent before a root is ready and throughout hard reset; callers must
    /// still acquire a lease immediately before applying epoch-bound effects.
    pub(crate) fn registration_validity(&self) -> Option<RequestValidity> {
        self.0.setup_state.read().unwrap().registration_fence.clone().map(RequestValidity::fenced)
    }

    /// Wait for the durable catalog warm without letting reset strand an old
    /// allocator request. The caller acquires the validity lease after this
    /// returns, closing the ready-to-reset race atomically at the fence.
    pub(crate) async fn wait_catalog_ready_if_current(&self, validity: &RequestValidity) -> bool {
        loop {
            let notified = self.0.ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !validity.is_current() {
                return false;
            }
            if self.is_catalog_ready() {
                return true;
            }
            notified.await;
        }
    }

    /// Registration wait that is canceled by hard reset. The ordinary wait
    /// intentionally follows readiness across lifecycle changes, but an
    /// allocator operation is bound to one originating epoch: after reset it
    /// must return instead of sleeping forever for the successor catalog or
    /// continuing against its identities.
    pub(crate) async fn wait_catalog_ready_at_epoch(
        &self,
        node: &Node<SE, PA>,
        expected_epoch: u64,
    ) -> Result<(), crate::error::RetrievalError> {
        loop {
            let notified = self.0.ready_notify.notified();
            let reset_fence = node.entities.reset_fence_read().await;
            if node.entities.reset_epoch() != expected_epoch {
                return Err(crate::error::RequestError::ConnectionLost.into());
            }
            if self.is_catalog_ready() {
                return Ok(());
            }
            drop(reset_fence);
            notified.await;
        }
    }

    fn mark_ready(&self) {
        *self.0.ready.write().unwrap() = true;
        self.0.ready_notify.notify_waiters();
    }

    /// Begin SystemManager's reset barrier. Invalidate the generation and all
    /// epoch owners synchronously, tear down old live queries before waiting,
    /// then drain durable warming, ephemeral setup/local/wire application, and
    /// schema-registration effects. Storage deletion cannot begin until this
    /// returns.
    async fn begin_reset(&self) {
        let (draining_fences, ephemeral_queries, durable_sub) = {
            let mut setup = self.0.setup_state.write().unwrap();
            if !setup.resetting {
                setup.resetting = true;
                setup.generation = setup.generation.wrapping_add(1);
            }
            setup.ephemeral_active = false;
            if let Some(fence) = setup.ephemeral_fence.take() {
                fence.invalidate();
                setup.draining_fences.push(fence);
            }
            if let Some(fence) = setup.durable_fence.take() {
                fence.invalidate();
                setup.draining_fences.push(fence);
            }
            if let Some(fence) = setup.registration_fence.take() {
                fence.invalidate();
                setup.draining_fences.push(fence);
            }
            let ephemeral_queries = std::mem::take(&mut *self.0.ephemeral_queries.write().unwrap());
            let durable_sub = self.0.durable_sub.write().unwrap().take();
            *self.0.ready.write().unwrap() = false;
            (setup.draining_fences.clone(), ephemeral_queries, durable_sub)
        };

        // Dropping a live query synchronously removes its relay entry and
        // schedules the peer unsubscribe. Do this before waiting so no newer
        // response or stream is admitted merely because teardown was delayed
        // behind an already-running response.
        drop(ephemeral_queries);
        drop(durable_sub);
        self.0.setup_changed.notify_waiters();
        self.0.ready_notify.notify_waiters();

        for fence in draining_fences {
            fence.wait_drained().await;
        }
    }

    /// Finish SystemManager's reset only after storage, system state, and the
    /// reactor have been cleared. This is the point where a new ephemeral
    /// setup may claim the next generation.
    fn finish_reset(&self) {
        let mut setup = self.0.setup_state.write().unwrap();
        self.0.map.write().unwrap().clear();
        *self.0.ready.write().unwrap() = false;
        // Allocations belong to one system and must not survive hard_reset
        // (RFC 5.2): a node re-joining a different system must re-register
        // everything against the new system's allocator.
        self.0.ensured.write().unwrap().clear();
        setup.draining_fences.clear();
        setup.resetting = false;
        setup.durable_resume_pending = self.0.durable;
        // Wake any ensure_subscribed waiters so they observe the cleared
        // latch instead of sleeping on a readiness that will never come, and
        // cancel the detached owner so it removes its relay attempts before a
        // held stale response can reach NodeApplier.
        drop(setup);
        self.0.setup_changed.notify_waiters();
        self.0.ready_notify.notify_waiters();
        debug!("CatalogManager reset (map cleared, not ready)");
    }

    /// Re-arm epoch-bound catalog work after `SystemManager` has published a
    /// ready root. Every node kind gets exactly one registration fence; a
    /// durable node also claims its one pending storage warm.
    fn resume_after_system_ready(&self) {
        let durable_claim = {
            let mut setup = self.0.setup_state.write().unwrap();
            if setup.resetting {
                return;
            }
            if setup.registration_fence.is_none() {
                setup.registration_fence = Some(RequestFence::new());
            }
            if !self.0.durable || !setup.durable_resume_pending {
                return;
            }
            setup.durable_resume_pending = false;
            let fence = RequestFence::new();
            let lease = fence.try_acquire().expect("a newly-created durable warm fence must admit its owner");
            setup.durable_fence = Some(fence);
            (setup.generation, lease)
        };
        let (generation, lease) = durable_claim;
        let me = self.clone();
        crate::task::spawn(async move { me.run_durable_warm(generation, lease).await });
    }

    // -- public lookup API (cheap clones) -----------------------------------

    /// The property addressed by `name` in `collection`: prefer retained exact
    /// bindings for admitted ordinary or explicit fields, fail closed if those
    /// bindings disagree, and otherwise consult the current display-name map.
    pub fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> { self.0.resolve_property(collection, name) }

    /// A weak handle to this catalog as a name-to-id resolver, stamped onto
    /// entities at assembly for the sync read path (the PropertyKey amendment,
    /// #289). Replaces the old per-collection `SchemaBinding` push: identity is
    /// carried by the [`crate::property::PropertyKey`], not by a binding
    /// injected into a property backend.
    pub(crate) fn resolver_weak(&self) -> std::sync::Weak<dyn crate::schema::CatalogResolver> {
        // Downgrade to the concrete Weak first, then let the return type coerce
        // it to the trait object (CoerceUnsized on Weak); annotating the local
        // as the dyn type instead would wrongly force `downgrade`'s parameter.
        let weak = Arc::downgrade(&self.0);
        weak
    }

    /// Test-only probe for detecting catalog ownership cycles after a node is
    /// dropped. The closure owns only a weak pointer and therefore does not
    /// affect the lifetime it observes.
    #[cfg(feature = "test-helpers")]
    pub fn liveness_probe(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let weak = Arc::downgrade(&self.0);
        move || weak.upgrade().is_some()
    }

    pub fn property_by_id(&self, id: &EntityId) -> Option<PropertyDef> { self.0.map.read().unwrap().properties.get(id).cloned() }

    pub fn model_by_collection(&self, collection: &str) -> Option<ModelDef> {
        let map = self.0.map.read().unwrap();
        let id = map.by_collection.get(collection)?;
        map.models.get(id).cloned()
    }

    /// INGRESS resolution for the wire envelope (#330): the collection a
    /// model-definition id routes to. Well-known (system/catalog) ids first
    /// -- the bootstrap base case, answerable on a stone-cold node -- then
    /// the catalog map. `None` means the model is unknown here, which after
    /// descriptor shipping is a protocol violation the caller rejects loudly.
    pub fn collection_for_model(&self, model: &EntityId) -> Option<CollectionId> {
        if let Some(collection) = crate::schema::well_known_collection(model) {
            return Some(collection);
        }
        let map = self.0.map.read().unwrap();
        map.models.get(model).map(|def| CollectionId::fixed_name(&def.collection))
    }

    /// EGRESS resolution for the wire envelope (#330): the model-definition
    /// id stamped on events/states for `collection`. Well-knowns first, then
    /// the catalog map. `None` for an unregistered user collection -- the
    /// commit path runs registration before event generation, so a miss
    /// there is a bug, not a race.
    pub fn model_id_for(&self, collection: &str) -> Option<EntityId> {
        crate::schema::well_known_model_id(collection).or_else(|| self.0.map.read().unwrap().by_collection.get(collection).copied())
    }

    pub fn membership(&self, model: &EntityId, property: &EntityId) -> Option<MembershipDef> {
        self.0.map.read().unwrap().membership(model, property)
    }

    pub fn memberships_of(&self, model: &EntityId) -> Vec<MembershipDef> { self.0.map.read().unwrap().memberships_of(model) }

    /// Property ids sharing display name `name` across ALL contracts (the
    /// map's global name index, which also backs [`Self::property_by_name`]).
    pub fn siblings_by_name(&self, name: &str) -> Vec<EntityId> {
        self.0.map.read().unwrap().names_global.get(name).into_iter().flat_map(|s| s.iter().copied()).collect()
    }

    // -- registration lifecycle --------------------------------------------

    // -- allocator support (RFC 5.1 executor discipline) ---------------------

    /// Serialize a registration execution. The executor holds this across
    /// its whole lookup/allocate/commit/upsert sequence.
    pub(crate) async fn lock_allocator(&self) -> tokio::sync::MutexGuard<'_, ()> { self.0.allocator.lock().await }

    /// The property lookup key (RFC 5.1 as amended 2026-07-10): (minting
    /// model, current name). Backend and value_type left the key with the
    /// canonical value_type ruling: a same-name registration with a different
    /// type is a COMPATIBILITY question against the found definition, never a
    /// second identity. Used by the executor's upsert and the rename hint
    /// pre-pass.
    pub fn property_by_name(&self, model: &EntityId, name: &str) -> Option<PropertyDef> {
        let map = self.0.map.read().unwrap();
        map.names_global.get(name)?.iter().find_map(|id| {
            let p = map.properties.get(id)?;
            (p.minted_for == Some(*model) && p.name == name).then(|| p.clone())
        })
    }

    /// Derive the exact binding an already-populated catalog proves for this
    /// compiled declaration.
    ///
    /// Ordinary fields use the allocator's lookup scope `(minting model,
    /// current name)`, not any same-named membership. That distinction keeps
    /// explicit sharing explicit. An explicit model id must itself be the
    /// collection's live model; a compatible ordinary model must be the one
    /// indexed by the collection. Every field then needs a live membership and
    /// a compatible immutable backend/type pair.
    fn compatible_binding(&self, schema: &'static ModelSchema, confirmed: bool) -> Option<EnsuredSchemaBinding> {
        let map = self.0.map.read().unwrap();
        let model = match schema.explicit_id {
            Some(id) => {
                let id = super::local::parse_explicit_id(id);
                let def = map.models.get(&id)?;
                if def.collection != schema.collection || map.by_collection.get(schema.collection) != Some(&id) {
                    return None;
                }
                id
            }
            None => *map.by_collection.get(schema.collection)?,
        };

        let mut fields = BTreeMap::new();
        for field in schema.properties {
            let id = match field.explicit_id {
                Some(id) => super::local::parse_explicit_id(id),
                None => {
                    let mut matches =
                        map.properties.values().filter(|def| def.minted_for == Some(model) && def.name == field.name).map(|def| def.id);
                    let id = matches.next()?;
                    if matches.next().is_some() {
                        return None;
                    }
                    id
                }
            };
            if map.membership(&model, &id).is_none() {
                return None;
            }
            let def = map.properties.get(&id)?;
            if def.backend != field.backend || !super::registration::value_types_compatible(&def.value_type, field.value_type) {
                return None;
            }
            fields.insert(field.name, id);
        }
        Some(EnsuredSchemaBinding { schema, model, fields, confirmed })
    }

    /// Build the confirmed binding from the allocator's response itself.
    /// Registration results are the only race-free authority for the ids this
    /// exact request resolved; reconstructing them from mutable display names
    /// after the response could observe a concurrent rename or name reuse.
    fn registered_binding(
        &self,
        schema: &'static ModelSchema,
        models: &[proto::RegisteredModel],
        properties: &[proto::RegisteredProperty],
        memberships: &[proto::RegisteredMembership],
    ) -> Option<EnsuredSchemaBinding> {
        let model_def = models.iter().find(|model| model.collection == schema.collection)?;
        let model = model_def.id;
        if schema.explicit_id.is_some_and(|id| super::local::parse_explicit_id(id) != model) {
            return None;
        }

        let mut fields = BTreeMap::new();
        for field in schema.properties {
            let property = match field.explicit_id {
                Some(id) => {
                    let id = super::local::parse_explicit_id(id);
                    properties.iter().find(|property| property.id == id)?
                }
                None => properties.iter().find(|property| property.model == model && property.name == field.name)?,
            };
            if property.backend != field.backend
                || !super::registration::value_types_compatible(&property.value_type, field.value_type)
                || !memberships.iter().any(|membership| membership.model == model && membership.property == property.id)
            {
                return None;
            }
            fields.insert(field.name, property.id);
        }

        Some(EnsuredSchemaBinding { schema, model, fields, confirmed: true })
    }

    /// Record an exact binding proven from an already-compatible catalog.
    /// This is the safe no-peer fallback when the allocator cannot be reached.
    pub(crate) fn bind_compatible_schema(&self, schema: &'static ModelSchema) -> bool {
        // Bind proof and publication to one ready system epoch. Reset either
        // invalidates before admission (fail closed) or waits for this lease
        // before clearing, so old ids cannot be stored after the clear.
        let Some(validity) = self.registration_validity() else { return false };
        let Some(_lease) = validity.try_acquire() else { return false };
        let Some(binding) = self.compatible_binding(schema, false) else { return false };
        self.store_binding(binding);
        true
    }

    fn store_binding(&self, binding: EnsuredSchemaBinding) {
        let mut ensured = self.0.ensured.write().unwrap();
        let bindings = ensured.entry(binding.schema.collection.to_string()).or_default();
        if let Some(existing) = bindings.iter_mut().find(|known| *known.schema == *binding.schema) {
            // Confirmation belongs to the exact ids returned by the
            // allocator. A later local proof may not replace those ids while
            // inheriting their confirmation; only another confirmed result
            // can replace a confirmed binding.
            if binding.confirmed || !existing.confirmed {
                *existing = binding;
            }
        } else {
            bindings.push(binding);
        }
    }

    /// Automatic schema use (mutation or predicate) tries the allocator first.
    /// A policy or executor refusal is always strict. Only the explicit
    /// no-durable-peer case may proceed from locally proven exact identities.
    pub(crate) async fn ensure_schema_for_use(
        &self,
        node: &Node<SE, PA>,
        cdata: &PA::ContextData,
        schema: &'static ModelSchema,
    ) -> Result<(), RegistrationError> {
        match self.ensure_registered(node, cdata, schema).await {
            Ok(()) => Ok(()),
            Err(error @ RegistrationError::NoDurablePeer(_)) if self.bind_compatible_schema(schema) => {
                tracing::warn!(
                    "schema reassertion for fully bound collection '{}' has no durable peer; proceeding with proven canonical identities: {}",
                    schema.collection,
                    error
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// The canonical value_type of a property-definition id, if the map knows
    /// it (the CatalogManager-side twin of
    /// [`crate::schema::CatalogResolver::canonical_value_type`]; rfc.md
    /// 5.6 as amended 2026-07-10). The resolution pass casts comparison
    /// literals to this type so predicate evaluation and the reactor's
    /// watcher index collate in the type the backends store.
    pub(crate) fn canonical_value_type_of(&self, id: &EntityId) -> Option<String> {
        self.0.map.read().unwrap().properties.get(id).map(|def| def.value_type.clone())
    }

    /// Fold resolved definitions into the map: the executor calls this
    /// synchronously post-commit (before releasing the allocator mutex),
    /// and `ensure_registered` calls it with a SchemaRegistered response so
    /// binding proceeds ahead of the catalog subscription (RFC 5.2).
    /// Idempotent (keyed by entity id); the reactor later re-delivers the
    /// same entities harmlessly.
    pub fn upsert_registered(
        &self,
        models: &[proto::RegisteredModel],
        properties: &[proto::RegisteredProperty],
        memberships: &[proto::RegisteredMembership],
    ) {
        let mut map = self.0.map.write().unwrap();
        for m in models {
            map.upsert_model(ModelDef { id: m.id, collection: m.collection.clone(), name: m.name.clone() });
        }
        for p in properties {
            map.upsert_property(PropertyDef {
                id: p.id,
                minted_for: Some(p.model),
                name: p.name.clone(),
                backend: p.backend.clone(),
                value_type: p.value_type.clone(),
                target_model: p.target_model,
            });
        }
        for ms in memberships {
            map.upsert_membership(MembershipDef { id: ms.id, model: ms.model, property: ms.property, optional: Some(ms.optional) });
        }
    }

    /// RFC 5.2 model first-use registration ("ensure registration"). Called
    /// on mutating paths before a write and by typed predicate reads before
    /// name resolution. An existing schema resolves to a no-op plan, so the
    /// common read-path case emits nothing and skips the policy verb while the
    /// response feeds the map. Fast-returns only if this exact compiled schema
    /// shape is already ensured in this process, then durably registers:
    ///
    /// - DURABLE node: execute the registration locally
    ///   ([`Node::execute_schema_registration`], which updates the map
    ///   itself under the allocator mutex); latch on Ok.
    /// - EPHEMERAL node with a durable peer: forward RegisterSchema, consume
    ///   the SchemaRegistered response into the map (binding and id-keyed
    ///   writes proceed immediately, ahead of the catalog subscription);
    ///   latch on Ok.
    /// - EPHEMERAL node with NO durable peer: registration is impossible
    ///   without the allocator, so this returns
    ///   [`RegistrationError::NoDurablePeer`] without latching. The automatic
    ///   caller may proceed only if the local catalog proves the exact model
    ///   and every field's compatible canonical binding.
    ///
    /// Every error path returns WITHOUT latching, so a later attempt
    /// retries.
    pub async fn ensure_registered(
        &self,
        node: &Node<SE, PA>,
        cdata: &PA::ContextData,
        schema: &'static ModelSchema,
    ) -> Result<(), RegistrationError> {
        let collection = schema.collection.to_string();
        // Snapshot and enter the epoch before consulting the latch. Checking
        // first would allow reset to clear the latch and install a new fence
        // between the stale boolean and our admission (an ABA false success).
        let validity = self.registration_validity().ok_or(RegistrationError::SystemNotReady)?;
        let initial_lease = validity.try_acquire().ok_or(RegistrationError::SystemNotReady)?;
        if self.is_schema_ensured(schema) {
            return Ok(());
        }
        let registration_epoch = node.entities.reset_epoch();

        let (models, properties, memberships) = super::registration_request(schema);

        if node.durable {
            // A durable node executes registration itself (no forwarding);
            // the executor upserts the map before returning. Retain one
            // outer lease across the executor and exact-schema latch. It must
            // be snapshotted before execution: reacquiring afterward could
            // grab a post-reset fence and fold old definitions into the new
            // epoch (an ABA error).
            let _lease = initial_lease;
            // Re-enter the reset fence before latching: if execution
            // straddled a reset, the reset hook already cleared the old map
            // and this old-system completion must not mark the successor
            // system as registered.
            let (models, properties, memberships) =
                node.execute_schema_registration_at_epoch(cdata, models, properties, memberships, registration_epoch).await?;
            let _reset_fence = node.entities.reset_fence_read().await;
            if node.entities.reset_epoch() != registration_epoch {
                return Err(RegistrationError::Retrieval(crate::error::RequestError::ConnectionLost.into()));
            }
            self.mark_schema_ensured(schema, &models, &properties, &memberships)?;
            return Ok(());
        }

        // A forwarded request may be arbitrarily slow. Do not make reset wait
        // for the network; response admission reacquires this same old fence
        // and rejects it before schema ingestion if reset invalidated it.
        drop(initial_lease);

        // Ephemeral: forward to a connected durable peer. There is no offline
        // registration queue because only the durable allocator may mint ids.
        match node.get_durable_peers().first().copied() {
            Some(peer) => {
                let body = proto::NodeRequestBody::RegisterSchema { models, properties, memberships };
                if !validity.is_current() {
                    return Err(RegistrationError::SystemNotReady);
                }
                match node.request_if_current(peer, cdata, body, validity).await {
                    Ok(response) => {
                        // Response admission acquired the registration owner
                        // before schema ingestion. Retain that same lease
                        // through the response-body map fold and exact-schema
                        // latch, so reset clears either before or after the
                        // complete effect, never between them.
                        let (body, _lease) = response.into_parts();
                        let proto::NodeResponseBody::SchemaRegistered { models, properties, memberships } = body else {
                            return match body {
                                proto::NodeResponseBody::Error(e) => {
                                    Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(e)))
                                }
                                other => Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                                    "unexpected response to RegisterSchema: {other}"
                                )))),
                            };
                        };
                        // The response is the fast path into the map (RFC
                        // 5.2): fold it in on ack so binding proceeds now.
                        // The response-delivery epoch check protects its own
                        // wire-schema cache warm; this second check is the
                        // actual caller mutation boundary for the resolved
                        // definitions and ensured latch.
                        let _reset_fence = node.entities.reset_fence_read().await;
                        if node.entities.reset_epoch() != registration_epoch {
                            return Err(RegistrationError::Retrieval(crate::error::RequestError::ConnectionLost.into()));
                        }
                        self.upsert_registered(&models, &properties, &memberships);
                        self.mark_schema_ensured(schema, &models, &properties, &memberships)?;
                        Ok(())
                    }
                    Err(e) => Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!("{e:?}")))),
                }
            }
            None => Err(RegistrationError::NoDurablePeer(collection)),
        }
    }

    /// Whether this collection's registration is latched (durably executed
    /// or forwarded successfully) this process.
    pub fn is_ensured(&self, collection: &str) -> bool {
        self.0.ensured.read().unwrap().get(collection).is_some_and(|bindings| bindings.iter().any(|binding| binding.confirmed))
    }

    pub(crate) fn is_schema_ensured(&self, schema: &ModelSchema) -> bool {
        self.0
            .ensured
            .read()
            .unwrap()
            .get(schema.collection)
            .is_some_and(|bindings| bindings.iter().any(|known| known.confirmed && *known.schema == *schema))
    }

    pub(crate) fn has_schema_binding(&self, schema: &ModelSchema) -> bool {
        self.0.ensured.read().unwrap().get(schema.collection).is_some_and(|bindings| bindings.iter().any(|known| *known.schema == *schema))
    }

    fn mark_schema_ensured(
        &self,
        schema: &'static ModelSchema,
        models: &[proto::RegisteredModel],
        properties: &[proto::RegisteredProperty],
        memberships: &[proto::RegisteredMembership],
    ) -> Result<(), RegistrationError> {
        let binding = self.registered_binding(schema, models, properties, memberships).ok_or_else(|| {
            RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                "registration of '{}' succeeded without a complete compatible catalog binding",
                schema.collection
            )))
        })?;
        self.store_binding(binding);
        Ok(())
    }

    /// TEST/INTROSPECTION: number of parsed entities of each kind
    /// (models, properties, memberships).
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn counts(&self) -> (usize, usize, usize) {
        let map = self.0.map.read().unwrap();
        (map.models.len(), map.properties.len(), map.memberships.len())
    }
}
