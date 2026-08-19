//! The in-memory catalog map and its maintenance
//!
//! Every node keeps an in-memory view of the three catalog collections
//! (`_ankurah_model`, `_ankurah_property`, `_ankurah_model_property`),
//! because every query and every typed accessor turns a name into a durable
//! identity through it, and neither can afford a storage round trip.
//!
//! The map fills ONE way on both node kinds: the catalog feeds itself. Three
//! ordinary live queries over the typed row models ([`rows`]) are indexed by
//! catalog entity id, and the tables they derive ([`map`]) are what every
//! lookup here answers from. On a durable node those queries read local
//! storage; on an ephemeral one they read local storage AND subscribe through
//! the relay to a durable peer, which is also how catalog changes get PUSHED
//! to ephemerals -- there is no separate replication mechanism for schema.
//! Registration additionally folds its own resolved definitions in
//! synchronously ([`CatalogManager::upsert_registered`]), which is what lets
//! consecutive registrations observe each other before the reactor has
//! delivered anything.
//!
//! Readiness is the projection running over the LOCAL store, and nothing
//! here waits on the network to claim it. A node whose store holds no catalog
//! rows is ready with an empty map, and a node whose rows are stale is ready
//! with those: the two are the same epistemic state, and in both a miss is
//! honest and heals when the durable's rows arrive. A caller who needs the
//! PUSHED catalog rather than the stored one waits for
//! [`CatalogManager::wait_catalog_synced`], which is a different claim and
//! says so.
//!
//! These are PRE-READY queries. The map is what makes the system ready, so
//! gating the queries that fill it on system readiness would deadlock the
//! gate they feed. They are therefore built with the immediate raw
//! constructors, carry no credential, and address only built-in identities:
//! the row models pin their `ModelId` and every `PropertyId` at compile time,
//! so they resolve on a stone-cold node, at the bootstrap epoch, without
//! consulting the catalog they are filling.
//!
//! Reads of these three collections bypass the [`PolicyAgent`] entirely, on
//! both sides of the wire (a node's own admission and a durable's serving
//! handlers): the catalog is readable to connected peers as a documented
//! 0.10 property. Catalog WRITES remain exactly as protected as they were --
//! registration is the only writer, and every event it emits still passes
//! `check_event`.

use std::sync::{Arc, RwLock};

use ankurah_proto::{self as proto, EntityId, PropertyId, SystemProperty};
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::{
    error::RetrievalError,
    livequery::{EntityLiveQuery, LiveQuery},
    node::{MatchArgs, Node, WeakNode},
    policy::PolicyAgent,
    session::SessionSet,
    storage::StorageEngine,
    util::request_fence::{RequestFence, RequestLease, RequestValidity},
    ModelId,
};

use super::{registration::RegistrationError, ModelStructDescriptor};

mod map;
pub mod rows;
use map::{CatalogMap, CatalogProjection};
use rows::{SysModelPropertyRow, SysModelRow, SysModelRowView, SysPropertyRow};

/// The catalog lookup result for one compiled model shape: the model's
/// durable id plus each field's, in the descriptor's declaration order,
/// each matched by name against catalog rows. A binding exists only long
/// enough to resolve the descriptor's cells -- the cells are the one home
/// of resolved identities.
struct CatalogBinding {
    model: EntityId,
    fields: Vec<EntityId>,
}

impl CatalogBinding {
    fn resolve_cells(&self, schema: &'static super::ModelStructDescriptor, epoch: super::SchemaEpoch) {
        debug_assert_eq!(self.fields.len(), schema.properties.len());
        // Field cells first, model cell last: the registration gate's
        // fast path probes only the model cell, so the model entry is the
        // publication point and must not become visible while any field
        // cell is still empty (a concurrent gate would report success and
        // then read an unresolved field).
        for (field, id) in schema.properties.iter().zip(self.fields.iter()) {
            field.resolved.set(epoch, PropertyId::EntityId(*id));
        }
        schema.resolved.set(epoch, ModelId::EntityId(self.model));
    }
}

// -- manager ----------------------------------------------------------------

/// How far a warm attempt got. A superseded attempt found its generation
/// already bumped by a reset and published nothing; retrying it would only
/// race the reset that replaced it.
enum WarmOutcome {
    Published,
    Superseded,
}

/// The first retry delay after a failed warm, doubling to [`WARM_RETRY_MAX`].
const WARM_RETRY_MIN: std::time::Duration = std::time::Duration::from_millis(50);
const WARM_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// Wait for one projection query's first resultset.
///
/// The wait is over LOCAL storage and is bounded by that read, so there is no
/// timeout here and nothing to tear down: the query answers, or it fails and
/// says why. A failure is the warm's failure -- publishing readiness on a
/// query that will never populate would publish an empty catalog as
/// authoritative -- and the retry loop tries again.
async fn wait_projection<R: crate::model::View>(query: &LiveQuery<R>, model: ModelId) -> Result<(), RetrievalError> {
    query
        .wait_initialized()
        .await
        .map_err(|error| RetrievalError::Other(format!("catalog projection over '{model}' failed to initialize: {error}")))
}

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
    /// The owning node, installed by `start` (weak: the node owns the
    /// manager, never the reverse). Registration executes and forwards
    /// through it.
    node: RwLock<Option<WeakNode<SE, PA>>>,
    durable: bool,
    /// The allocator mutex: the registration
    /// executor serializes every RegisterSchema execution on this lock and
    /// upserts its allocations into the map synchronously before releasing
    /// it, so consecutive registrations observe each other and never
    /// double-allocate.
    allocator: tokio::sync::Mutex<()>,
    map: CatalogMap,
    ready: RwLock<bool>,
    ready_notify: Notify,
    /// Why the most recent warm attempt failed, while the retry loop backs
    /// off. A published warm clears it. This is the honest reading of an
    /// un-ready catalog: not "still starting", but "this went wrong".
    warm_failure: RwLock<Option<String>>,
    /// Monotonic catalog-warm generation. Reset invalidates the generation
    /// before clearing the catalog, so a warm that started beforehand cannot
    /// publish pre-reset rows into the post-reset map.
    setup_state: RwLock<CatalogSetupState>,
    /// Wakes a warm that is backing off between retries when reset
    /// invalidates its generation, so it abandons promptly instead of holding
    /// reset's drain for the rest of its delay.
    setup_changed: Notify,
    /// The manager stays generic over the node's PolicyAgent for its
    /// Node-taking methods (ensure_registered, the projection queries).
    _pa: std::marker::PhantomData<PA>,
}

#[derive(Debug, Default)]
struct CatalogSetupState {
    generation: u64,
    /// While true, no new warm may be claimed. SystemManager clears it only
    /// after storage and reactor reset finish.
    resetting: bool,
    /// Quiescing owner fence for the current warm. The warm retains one lease
    /// from before its first read through projection/readiness publication, so
    /// reset can invalidate the generation and drain it before deleting
    /// storage.
    warm_fence: Option<RequestFence>,
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
    /// A hard reset drops the projection. Once the new system root is ready,
    /// one warm must attach the current generation.
    warm_resume_pending: bool,
}

// The name-to-id lookup behind `CatalogManager::resolve`. The wider catalog
// metadata surface (model listing, reverse name lookups) becomes the
// CatalogResolver trait with the storage-engine read flip. Compiled
// declarations do not pass through here: their identities live on the
// descriptor cells; this is raw (name-addressed) resolution against the
// catalog's current display names.
impl<SE, PA> CatalogInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn resolve_model_property(&self, model: &proto::ModelId, name: &str) -> anyhow::Result<Option<PropertyId>> {
        let proto::ModelId::EntityId(id) = model else {
            return Ok(SystemProperty::from_name(name).map(PropertyId::System));
        };
        if !self.map.knows_model(id) {
            return Ok(None);
        }
        Ok(self.map.resolve(id, name)?.map(PropertyId::EntityId))
    }
}

impl<SE, PA> CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn new(durable: bool) -> Self {
        Self(Arc::new(CatalogInner {
            node: RwLock::new(None),
            durable,
            allocator: tokio::sync::Mutex::new(()),
            map: CatalogMap::default(),
            ready: RwLock::new(false),
            ready_notify: Notify::new(),
            warm_failure: RwLock::new(None),
            setup_state: RwLock::new(CatalogSetupState::default()),
            setup_changed: Notify::new(),
            _pa: std::marker::PhantomData,
        }))
    }

    /// Called right after the `NodeInner` Arc exists (beside
    /// `policy_agent.on_node_ready`). It installs the hard-reset/readiness
    /// hooks and arms the one warm that the system-ready hook launches.
    pub(crate) fn start(&self, node: WeakNode<SE, PA>) {
        let Some(strong) = node.upgrade() else { return };
        *self.0.node.write().unwrap() = Some(node);

        // Install the hard-reset flush hook on the system manager so
        // SystemManager::hard_reset can clear the catalog in-place (it does
        // not hold the CatalogManager directly), and the readiness gate that
        // makes a ready system mean a loaded catalog.
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
            let gate_manager = self.clone();
            strong.system.set_catalog_ready_gate(Arc::new(move || {
                let manager = gate_manager.clone();
                Box::pin(async move { manager.wait_catalog_ready().await })
            }));
            let synced_manager = self.clone();
            strong.system.set_catalog_synced_gate(Arc::new(move || {
                let manager = synced_manager.clone();
                Box::pin(async move { manager.wait_catalog_synced().await })
            }));
        }

        // A node may remain deliberately uninitialized. Do not spawn a task
        // that owns the managers while waiting indefinitely for a system
        // root. Instead, arm exactly one warm and let SystemManager's
        // create/load/join ready transition call the hook.
        self.0.setup_state.write().unwrap().warm_resume_pending = true;

        // Every ready system epoch gets one registration fence, on either
        // node kind. If loading/joining won the race before hook installation,
        // this claims the missed transition; otherwise SystemManager calls it.
        if strong.system.is_system_ready() {
            self.resume_after_system_ready();
        }
    }

    /// Drive warm attempts until one publishes or a reset supersedes them.
    ///
    /// A failed warm does NOT mark readiness. Readiness is the claim that the
    /// map is authoritative, and a node whose catalog never loaded cannot
    /// honestly make it: a query resolved against an empty map would report
    /// a model that exists as unknown, which is a wrong answer rather than a
    /// delay. So the failure is recorded and warned about, and the attempt
    /// repeats on a widening backoff until it succeeds, the node is dropped,
    /// or a reset takes over.
    async fn run_warm(&self, generation: u64, lease: RequestLease, validity: RequestValidity) {
        let mut delay = WARM_RETRY_MIN;
        loop {
            match self.warm(generation).await {
                // Published: the map answers from this node's store, and the
                // peer subscriptions that keep it current go out behind that.
                Ok(WarmOutcome::Published) => {
                    // The warm's own lease covered its storage reads and ends
                    // here; each attachment takes its own, so a reset waiting
                    // on a peer that never answers is not a thing that happens.
                    drop(lease);
                    return self.attach_projection_relays(generation, validity).await;
                }
                // Superseded: a reset already replaced this generation, and
                // retrying would only race the warm it armed.
                Ok(WarmOutcome::Superseded) => return,
                Err(error) => {
                    warn!("CatalogManager warm failed; the catalog stays unready and retries in {delay:?}: {error}");
                    *self.0.warm_failure.write().unwrap() = Some(error.to_string());
                }
            }

            // Reset invalidates the generation and then drains this warm's
            // fence, so waking on that change is what keeps a backing-off
            // retry from holding the reset barrier for a whole delay.
            let changed = self.0.setup_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.0.setup_state.read().unwrap().generation != generation {
                return;
            }
            futures::future::select(changed, futures_timer::Delay::new(delay)).await;
            if self.0.setup_state.read().unwrap().generation != generation {
                return;
            }
            delay = (delay * 2).min(WARM_RETRY_MAX);
        }
    }

    /// One warm attempt: open the three catalog projection queries, derive
    /// the lookup tables from them, publish those, and mark the map ready.
    ///
    /// The queries are the same on both node kinds; what differs is who else
    /// answers them. Both read local storage, which is what readiness rests
    /// on. An ephemeral node's ALSO subscribe to a durable peer, which is how
    /// catalog changes are pushed to it -- behind readiness, one request at a
    /// time ([`Self::attach_projection_relays`]).
    ///
    /// A node with no catalog rows warms nothing and is ready with an empty
    /// map, which is the correct answer for a system that has registered
    /// nothing -- and the honest one for a node whose peer has not answered
    /// yet, whose misses heal when it does.
    async fn warm(&self, generation: u64) -> Result<WarmOutcome, RetrievalError> {
        if self.0.setup_state.read().unwrap().generation != generation {
            return Ok(WarmOutcome::Superseded);
        }
        let node = self.node().ok_or_else(|| RetrievalError::Other("catalog warm ran without a node".to_owned()))?;

        // One collection at a time, in a fixed order, and each query's own
        // first read is over local storage -- that is the whole of what
        // readiness waits for.
        //
        // The models query also puts its subscribe request on the wire here,
        // rather than waiting for the other two: nothing is in flight ahead
        // of it, and it is the request that starts this node hearing from the
        // system. The other two follow it one at a time
        // ([`Self::attach_projection_relays`]).
        //
        // The warm holds its lease across all of it, and that is the point:
        // this is where the queries touch storage (their activation scans the
        // catalog collections), so reset must not begin deleting collections
        // until it is over. A reset that lands meanwhile invalidates the
        // generation and then waits here, which is the barrier working.
        let models = self.projection_query::<SysModelRowView>(&node, crate::schema::model_collection())?;
        models.subscribe_remote(&node, SessionSet::new())?;
        wait_projection(&models, crate::schema::model_collection()).await?;
        let properties = self.open_projection(&node, crate::schema::property_collection()).await?;
        let memberships = self.open_projection(&node, crate::schema::model_property_collection()).await?;

        // The held setup guard excludes a reset's generation bump (a setup
        // write) between this check and projection/readiness publication.
        let setup = self.0.setup_state.read().unwrap();
        if setup.generation != generation {
            return Ok(WarmOutcome::Superseded);
        }
        let replaced = self.0.map.install(CatalogProjection::new(models, properties, memberships));
        *self.0.warm_failure.write().unwrap() = None;
        self.mark_ready();
        // Outside the setup guard: dropping a projection unsubscribes the
        // queries it owns, which must not run under catalog setup
        // synchronization.
        drop(setup);
        drop(replaced);
        Ok(WarmOutcome::Published)
    }

    /// Open ONE catalog collection's projection query and wait for its first
    /// resultset. Its subscribe request goes out later, one at a time
    /// ([`Self::attach_projection_relays`]).
    async fn open_projection<R>(&self, node: &Node<SE, PA>, model: proto::ModelId) -> Result<LiveQuery<R>, RetrievalError>
    where R: crate::model::View + Clone + Send + Sync + 'static {
        let query = self.projection_query::<R>(node, model)?;
        wait_projection(&query, model).await?;
        Ok(query)
    }

    /// One catalog projection query: every row of one catalog collection.
    ///
    /// It is raw (it names its collection, not a compiled declaration) and
    /// pre-ready by construction: the immediate constructor admits inline
    /// rather than waiting on a registration gate, and `Predicate::True`
    /// names no property, so nothing here consults the map it is about to
    /// fill. It is CACHED, which is what makes local storage answer it on
    /// both node kinds while the relay's subscription refreshes it; the two
    /// are not alternatives. Its remote subscription waits for
    /// [`Self::attach_projection_relays`] rather than going out as the query
    /// starts. The node reference is weak because the node owns this manager;
    /// a strong one would keep the node alive through its own catalog. The
    /// query carries no credential, which is what the catalog read exemption
    /// makes sufficient.
    fn projection_query<R: crate::model::View>(&self, node: &Node<SE, PA>, model: proto::ModelId) -> Result<LiveQuery<R>, RetrievalError> {
        let args = MatchArgs {
            selection: ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None },
            cached: true,
        };
        Ok(EntityLiveQuery::new_weak_node_local(node, model, args, SessionSet::new())?.map::<R>())
    }

    /// Put the rest of the projection's subscribe requests on the wire, each
    /// one after the request before it was answered.
    ///
    /// This is how catalog changes reach an ephemeral node, and it runs AFTER
    /// readiness rather than before it: readiness is the projection over this
    /// node's own store, and a node whose peer has not answered is unhelpful,
    /// not unready.
    ///
    /// One at a time, because a subscribe request is sent when its query's
    /// storage read finishes: three at once would reach the wire in whatever
    /// order those reads landed, and a node's traffic must be a function of
    /// the node's inputs, not of how a disk race went. The warm already sent
    /// the models query's request -- nothing was in flight ahead of it -- so
    /// this waits for that answer before sending the next. On a durable node
    /// there is no relay and every query answers for itself, so this walks
    /// through without sending anything.
    ///
    /// The lease is taken per request and released while waiting, so a reset
    /// drains this promptly instead of waiting out a peer that may never
    /// answer.
    async fn attach_projection_relays(&self, generation: u64, validity: RequestValidity) {
        let Some(queries) = self.0.map.queries() else { return };
        for (index, query) in queries.iter().enumerate() {
            if index > 0 {
                let Some(node) = self.node() else { return };
                let Some(_lease) = validity.try_acquire() else { return };
                if self.0.setup_state.read().unwrap().generation != generation {
                    return;
                }
                if let Err(error) = query.subscribe_remote(&node, SessionSet::new()) {
                    warn!("catalog projection could not subscribe to a durable peer: {error}");
                    return;
                }
            }
            if let Err(error) = query.wait_remote_answered().await {
                warn!("catalog projection subscription failed; this node keeps serving what it has stored: {error}");
                return;
            }
        }
    }

    // -- readiness ----------------------------------------------------------

    /// The owning node, while it lives: present from `start` (called in
    /// `Node::build`) until the node is dropped.
    pub(crate) fn node(&self) -> Option<Node<SE, PA>> { self.0.node.read().unwrap().clone()?.upgrade() }

    /// Whether the catalog map is authoritative for the current system epoch.
    pub fn is_catalog_ready(&self) -> bool { *self.0.ready.read().unwrap() }

    /// Why the most recent warm attempt failed, while the retry loop is
    /// backing off. An un-ready catalog with no failure here is one whose
    /// first warm has simply not finished.
    pub fn catalog_warm_failure(&self) -> Option<String> { self.0.warm_failure.read().unwrap().clone() }

    /// Wait until the catalog map is authoritative for the current system epoch.
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

    /// Wait until a durable peer has ANSWERED this node's catalog projection:
    /// the relay's confirmation that a peer took all three subscriptions and
    /// their rows are applied.
    ///
    /// This is a stronger claim than readiness and a different one. Readiness
    /// says the projection is running over what this node has stored;
    /// this says the catalog it holds came from the system's authority.
    /// A caller that must see definitions another node registered waits here.
    /// On a node with no relay -- a durable one, which IS that authority --
    /// its own projection answers it, so this completes with readiness.
    ///
    /// A reset replaces the projection with a fresh one; a wait outstanding
    /// across a reset picks up the replacement rather than waiting out queries
    /// that reset has already torn down.
    pub async fn wait_catalog_synced(&self) -> Result<(), RetrievalError> {
        loop {
            // Register for the readiness change BEFORE reading the
            // projection, for `wait_catalog_ready`'s reason and one more: a
            // reset between the read and the wait would otherwise leave this
            // waiting on queries nothing will ever answer.
            let changed = self.0.ready_notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let Some(queries) = self.0.map.queries() else {
                changed.await;
                continue;
            };
            let answered = async move {
                for query in queries {
                    query.wait_remote_answered().await?;
                }
                Ok(())
            };
            match futures::future::select(std::pin::pin!(answered), changed).await {
                futures::future::Either::Left((answered, _)) => return answered,
                futures::future::Either::Right(((), _)) => continue,
            }
        }
    }

    /// Snapshot the current system epoch's schema-registration owner. It is
    /// absent before a root is ready and throughout hard reset; callers must
    /// still acquire a lease immediately before applying epoch-bound effects.
    pub(crate) fn registration_validity(&self) -> Option<RequestValidity> {
        self.0.setup_state.read().unwrap().registration_fence.clone().map(RequestValidity::fenced)
    }

    /// Wait for the catalog warm without letting reset strand an old
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

    fn mark_ready(&self) {
        *self.0.ready.write().unwrap() = true;
        self.0.ready_notify.notify_waiters();
    }

    /// Begin SystemManager's reset barrier. Invalidate the generation and all
    /// epoch owners synchronously, then drain the warm (its retries and its
    /// storage reads) and schema-registration effects. Storage deletion
    /// cannot begin until this returns.
    async fn begin_reset(&self) {
        let draining_fences = {
            let mut setup = self.0.setup_state.write().unwrap();
            if !setup.resetting {
                setup.resetting = true;
                setup.generation = setup.generation.wrapping_add(1);
            }
            if let Some(fence) = setup.warm_fence.take() {
                fence.invalidate();
                setup.draining_fences.push(fence);
            }
            if let Some(fence) = setup.registration_fence.take() {
                fence.invalidate();
                setup.draining_fences.push(fence);
            }
            *self.0.ready.write().unwrap() = false;
            setup.draining_fences.clone()
        };

        self.0.setup_changed.notify_waiters();
        self.0.ready_notify.notify_waiters();

        for fence in draining_fences {
            fence.wait_drained().await;
        }
    }

    /// Finish SystemManager's reset only after storage, system state, and the
    /// reactor have been cleared. This is the point where the next
    /// become-ready transition may claim a fresh warm.
    fn finish_reset(&self) {
        let mut setup = self.0.setup_state.write().unwrap();
        // Detach the drained projection and drop what registration confirmed
        // under the departing system, but drop the projection itself after the
        // setup guard releases, so unsubscribe teardown never runs under
        // catalog setup synchronization. The next generation's warm builds a
        // new one.
        let projection = self.0.map.clear();
        *self.0.ready.write().unwrap() = false;
        *self.0.warm_failure.write().unwrap() = None;
        // Resolved identities belong to one system, but there is nothing to
        // clear here: they live on the descriptor cells keyed by the OLD
        // epoch, which nothing reads once SystemManager clears the node's
        // held epoch. Re-joining allocates a fresh epoch and re-resolves
        // everything against the new system's allocator.
        setup.draining_fences.clear();
        setup.resetting = false;
        setup.warm_resume_pending = true;
        // Wake readiness waiters so they observe the cleared state instead of
        // sleeping on a readiness that will never come.
        drop(setup);
        drop(projection);
        self.0.setup_changed.notify_waiters();
        self.0.ready_notify.notify_waiters();
        debug!("CatalogManager reset (map cleared, not ready)");
    }

    /// Re-arm epoch-bound catalog work after `SystemManager` has published a
    /// ready root. Every node kind gets exactly one registration fence, and
    /// claims its one pending warm.
    fn resume_after_system_ready(&self) {
        let claim = {
            let mut setup = self.0.setup_state.write().unwrap();
            if setup.resetting {
                return;
            }
            if setup.registration_fence.is_none() {
                setup.registration_fence = Some(RequestFence::new());
            }
            if !setup.warm_resume_pending {
                return;
            }
            setup.warm_resume_pending = false;
            let fence = RequestFence::new();
            let lease = fence.try_acquire().expect("a newly-created warm fence must admit its owner");
            let validity = RequestValidity::fenced(fence.clone());
            setup.warm_fence = Some(fence);
            (setup.generation, lease, validity)
        };
        let (generation, lease, validity) = claim;
        let me = self.clone();
        crate::task::spawn(async move { me.run_warm(generation, lease, validity).await });
    }

    // -- public lookup API (cheap clones) -----------------------------------

    /// The property addressed by `name` in `model`: prefer retained exact
    /// bindings for admitted ordinary or explicit fields, fail closed if those
    /// bindings disagree, and otherwise consult the current display-name map.
    pub fn resolve(&self, model: &proto::ModelId, name: &str) -> Option<PropertyId> {
        self.0.resolve_model_property(model, name).ok().flatten()
    }

    /// Test-only probe for detecting catalog ownership cycles after a node is
    /// dropped. The closure owns only a weak pointer and therefore does not
    /// affect the lifetime it observes.
    #[cfg(feature = "test-helpers")]
    pub fn liveness_probe(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let weak = Arc::downgrade(&self.0);
        move || weak.upgrade().is_some()
    }

    /// Return the current catalog definition for a durable property identity.
    pub fn property_by_id(&self, id: &EntityId) -> Option<SysPropertyRow> { self.0.map.property(id) }

    /// Return the model currently registered under a source label, with the
    /// catalog entity id that identifies it.
    ///
    /// This is catalog metadata, not a storage-engine materialization lookup.
    pub fn model_by_label(&self, label: &str) -> Option<(EntityId, SysModelRow)> { self.0.map.model_by_label(label) }

    /// Whether this catalog recognizes a wire model identity. System models
    /// are the bootstrap base case, answerable on a stone-cold node; allocated
    /// ids must already be present in the catalog map. A miss after descriptor
    /// shipping is a protocol violation.
    pub fn knows_model(&self, model: &proto::ModelId) -> bool {
        match model {
            proto::ModelId::System(_) => true,
            proto::ModelId::EntityId(id) => self.0.map.knows_model(id),
        }
    }

    /// Catalog lookup for the model identity currently bound to a declared
    /// model label. This is schema information, not the authoritative
    /// storage materialization reverse map; wire egress must ask the storage
    /// engine for that mapping.
    pub fn model_id_for(&self, label: &str) -> Option<proto::ModelId> {
        crate::schema::system_model_id(label).or_else(|| self.0.map.model_by_label(label).map(|(id, _)| proto::ModelId::EntityId(id)))
    }

    /// Return the membership connecting `model` and `property`, if present,
    /// with the catalog entity id that identifies it.
    pub fn membership(&self, model: &EntityId, property: &EntityId) -> Option<(EntityId, SysModelPropertyRow)> {
        self.0.map.membership(model, property)
    }

    /// Return all property memberships currently registered for `model`, each
    /// with the catalog entity id that identifies it.
    pub fn memberships_of(&self, model: &EntityId) -> Vec<(EntityId, SysModelPropertyRow)> { self.0.map.memberships_of(model) }

    /// Property ids sharing display name `name` across ALL contracts.
    pub fn siblings_by_name(&self, name: &str) -> Vec<EntityId> { self.0.map.siblings_by_name(name) }

    // -- registration lifecycle --------------------------------------------

    // -- allocator support ---------------------

    /// Serialize a registration execution. The executor holds this across
    /// its whole lookup/allocate/commit/upsert sequence.
    pub(crate) async fn lock_allocator(&self) -> tokio::sync::MutexGuard<'_, ()> { self.0.allocator.lock().await }

    /// The property lookup key: (minting
    /// model, current name). Backend and value_type left the key with the
    /// canonical value_type ruling: a same-name registration with a different
    /// type is a COMPATIBILITY question against the found definition, never a
    /// second identity. Used by the executor's upsert and the rename hint
    /// pre-pass.
    /// The property `name` currently addresses within `model`'s membership
    /// set, with the catalog entity id that identifies it. Membership is the
    /// lookup scope -- a property shared into this model resolves here
    /// regardless of where it was minted; `minted_for` is provenance
    /// metadata, never a matching key.
    pub fn property_by_name(&self, model: &EntityId, name: &str) -> Option<(EntityId, SysPropertyRow)> {
        self.0.map.property_by_name(model, name)
    }

    /// Build the proven binding from the allocator's response itself.
    /// Registration results are the only race-free authority for the ids this
    /// exact request resolved; reconstructing them from mutable display names
    /// after the response could observe a concurrent rename or name reuse.
    fn registered_binding(&self, schema: &'static ModelStructDescriptor, models: &[proto::RegisteredModel]) -> Option<CatalogBinding> {
        let model_def = models.iter().find(|model| model.label == schema.label)?;
        let model = model_def.id;
        if schema.explicit_id.is_some_and(|id| super::compiled::parse_explicit_id(id) != model) {
            return None;
        }

        let mut fields = Vec::with_capacity(schema.properties.len());
        for field in schema.properties {
            // A nested response row IS the membership; matching it proves
            // the field is bound to this model.
            let property = match field.explicit_id {
                Some(id) => {
                    let id = super::compiled::parse_explicit_id(id);
                    model_def.properties.iter().find(|property| property.id == id)?
                }
                None => model_def.properties.iter().find(|property| property.name == field.name)?,
            };
            if property.backend != field.backend || !super::registration::value_types_compatible(&property.value_type, field.value_type) {
                return None;
            }
            fields.push(property.id);
        }

        Some(CatalogBinding { model, fields })
    }

    /// Derive the exact binding an already-populated catalog proves for this
    /// compiled declaration.
    ///
    /// Ordinary fields resolve within the model's MEMBERSHIP SET by current
    /// name (the same scope the allocator uses; a property shared into the
    /// model counts, and an ambiguous name fails the proof). An explicit
    /// model id must itself be the label's live model. Every field then
    /// needs a compatible immutable backend/type pair.
    fn compatible_binding(&self, schema: &'static ModelStructDescriptor) -> Option<CatalogBinding> {
        let map = &self.0.map;
        let (label_model, _) = map.model_by_label(schema.label)?;
        let model = match schema.explicit_id {
            Some(id) => {
                let id = super::compiled::parse_explicit_id(id);
                if label_model != id {
                    return None;
                }
                id
            }
            None => label_model,
        };

        let mut fields = Vec::with_capacity(schema.properties.len());
        for field in schema.properties {
            let id = match field.explicit_id {
                Some(id) => super::compiled::parse_explicit_id(id),
                None => map.resolve(&model, field.name).ok().flatten()?,
            };
            if map.membership(&model, &id).is_none() {
                return None;
            }
            let def = map.property(&id)?;
            if def.backend != field.backend || !super::registration::value_types_compatible(&def.value_type, field.value_type) {
                return None;
            }
            fields.push(id);
        }
        Some(CatalogBinding { model, fields })
    }

    /// Resolve the descriptor's cells from a proven binding, under the
    /// epoch the gate snapshotted at entry. The first entry per epoch is
    /// final ("no takesies-backsies"): a reset racing this gate leaves the
    /// entries tagged with the old epoch, where nothing ever reads them,
    /// and duplicate appends from concurrent gates are benign.
    pub(crate) fn bind_compatible_schema(&self, schema: &'static ModelStructDescriptor, epoch: crate::schema::SchemaEpoch) -> bool {
        let Some(proof) = self.compatible_binding(schema) else { return false };
        proof.resolve_cells(schema, epoch);
        true
    }

    /// This node's current schema epoch, from the system manager it belongs
    /// to. Absent while no system is ready. The gate snapshots this ONCE at
    /// entry, so one logical operation resolves under one epoch.
    pub(crate) fn schema_epoch(&self) -> Option<super::SchemaEpoch> { self.node().and_then(|node| node.system.schema_epoch()) }

    /// Raw name resolution with error transparency: an ambiguous name
    /// surfaces its error instead of flattening to `None` (the public
    /// [`Self::resolve`] keeps its lossy Option shape for existing callers).
    pub(crate) fn try_resolve(&self, model: &proto::ModelId, name: &str) -> anyhow::Result<Option<PropertyId>> {
        self.0.resolve_model_property(model, name)
    }

    /// Automatic schema use (mutation or predicate) tries the allocator first.
    /// A policy or executor refusal is always strict. Only the explicit
    /// no-durable-peer case may proceed from locally proven exact identities.
    pub(crate) async fn ensure_schema_for_use(
        &self,
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
    ) -> Result<(proto::ModelId, super::SchemaEpoch), RegistrationError> {
        // A built-in is already resolved at every epoch, including the
        // bootstrap epoch a pre-system node stamps its entities with.
        if let Some(system) = schema.system {
            return Ok((proto::ModelId::System(system), self.schema_epoch().unwrap_or(super::SchemaEpoch::BOOTSTRAP)));
        }
        let epoch = self.schema_epoch().ok_or(RegistrationError::SystemNotReady)?;
        // The snapshot epoch returns WITH the identity: one logical
        // operation (ensure, field resolution, entity stamp) must observe
        // exactly one epoch, so callers use this pair instead of re-reading
        // the node's epoch after the await.
        match self.ensure_registered_at(cdata, schema, epoch).await {
            Ok(()) => schema.resolved.get(epoch).map(|model| (model, epoch)).ok_or_else(|| {
                RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                    "registration of '{}' did not retain its exact model identity",
                    schema.label
                )))
            }),
            Err(error @ RegistrationError::NoDurablePeer(_)) if self.bind_compatible_schema(schema, epoch) => {
                tracing::warn!(
                    "schema reassertion for fully bound collection '{}' has no durable peer; proceeding with proven canonical identities: {}",
                    schema.label,
                    error
                );
                schema.resolved.get(epoch).map(|model| (model, epoch)).ok_or_else(|| {
                    RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                        "compatible binding for '{}' did not retain its exact model identity",
                        schema.label
                    )))
                })
            }
            // A known label whose compiled shape could not be proven bound is
            // an unconfirmed schema, not an unregistered collection; saying
            // "never registered" for it would be false.
            Err(RegistrationError::NoDurablePeer(label)) if self.model_by_label(schema.label).is_some() => {
                Err(RegistrationError::UnconfirmedSchema(label))
            }
            Err(error) => Err(error),
        }
    }

    /// Fold resolved definitions into the map's overlay: the executor calls
    /// this synchronously post-commit (before releasing the allocator mutex),
    /// and `ensure_registered` calls it with a SchemaRegistered response so
    /// binding proceeds ahead of the projection.
    ///
    /// What lands here is what ONE registration was told, so the projection's
    /// rows outrank it for every id it has delivered -- a definition renamed
    /// since is read from the catalog, not from this. Idempotent (keyed by
    /// catalog entity id); the projection later delivers the same rows and
    /// simply takes over answering for them.
    pub fn upsert_registered(&self, models: &[proto::RegisteredModel]) { self.0.map.upsert_registered(models) }

    /// TEST ONLY: seed deterministic catalog rows and admit their exact
    /// compiled-schema binding without writing catalog entities.
    ///
    /// Deterministic protocol simulators forge stable entity, model, and
    /// property identities so two runs have byte-identical traces. They cannot
    /// use the production allocator, whose fresh ids would intentionally
    /// differ between runs. This helper keeps the compiled binding complete
    /// (every field proven against the supplied rows) while making the
    /// fixture's deliberate storage bypass explicit.
    #[cfg(feature = "test-helpers")]
    pub fn seed_registered_schema(
        &self,
        schema: &'static ModelStructDescriptor,
        models: &[proto::RegisteredModel],
    ) -> Result<(), RegistrationError> {
        let epoch = self.schema_epoch().ok_or(RegistrationError::SystemNotReady)?;
        self.upsert_registered(models);
        self.resolve_registered_cells(schema, models, epoch)
    }

    /// Ensure registration. Called by explicit [`crate::context::Context::register_model`]
    /// and, through [`Self::ensure_schema_for_use`], by first-use
    /// registration on mutating and typed-read paths. An existing schema
    /// resolves to a no-op plan, so re-asserting emits nothing and skips the
    /// policy verb while the response feeds the map. Fast-returns only if
    /// the descriptor's cells are already resolved for this node's current
    /// epoch, then durably registers:
    ///
    /// - DURABLE node: execute the registration locally
    ///   ([`Node::execute_schema_registration`], which updates the map
    ///   itself under the allocator mutex); resolve the cells on Ok.
    /// - EPHEMERAL node with a durable peer: forward RegisterSchema and
    ///   consume the SchemaRegistered response into the map; resolve the
    ///   cells on Ok.
    /// - EPHEMERAL node with NO durable peer: registration is impossible
    ///   without the allocator, so this returns
    ///   [`RegistrationError::NoDurablePeer`] and resolves nothing. The
    ///   automatic caller may proceed only if the local catalog proves the
    ///   exact model and every field's compatible canonical binding.
    ///
    /// Every error path resolves nothing, so a later attempt retries.
    pub async fn ensure_registered(
        &self,
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
    ) -> Result<(), RegistrationError> {
        let epoch = self.schema_epoch().ok_or(RegistrationError::SystemNotReady)?;
        self.ensure_registered_at(cdata, schema, epoch).await
    }

    /// [`Self::ensure_registered`] under a caller-snapshotted epoch, so one
    /// logical operation (ensure + cell reads) observes exactly one epoch.
    async fn ensure_registered_at(
        &self,
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
        epoch: super::SchemaEpoch,
    ) -> Result<(), RegistrationError> {
        // A built-in's identities are compile-time constants, so there is
        // nothing to allocate and nobody to ask. It answers before the
        // registration fence exists, which is what lets the catalog's own row
        // models be used on a node whose system is not ready yet.
        if schema.system.is_some() {
            return Ok(());
        }
        let collection = schema.label.to_string();
        // Snapshot and enter the registration fence before consulting the
        // cells. Checking first would allow reset to install a new fence
        // between the stale check and our admission (an ABA false success).
        let validity = self.registration_validity().ok_or(RegistrationError::SystemNotReady)?;
        let initial_lease = validity.try_acquire().ok_or(RegistrationError::SystemNotReady)?;
        if schema.resolved.get(epoch).is_some() {
            return Ok(());
        }

        let request_model = proto::RegisterModel::from(schema);

        if self.0.durable {
            // A durable node executes registration itself (no forwarding);
            // the executor upserts the map before returning. Retain one
            // outer lease across the executor and the cell resolution. It
            // must be snapshotted before execution: reacquiring afterward
            // could grab a post-reset fence and fold old definitions into
            // the new epoch (an ABA error).
            let _lease = initial_lease;
            let models = self.register_schema(cdata, vec![request_model]).await?;
            self.resolve_registered_cells(schema, &models, epoch)?;
            return Ok(());
        }

        // A forwarded request may be arbitrarily slow. Do not make reset wait
        // for the network; response admission reacquires this same old fence
        // and rejects it before schema ingestion if reset invalidated it.
        drop(initial_lease);

        // Ephemeral: forward to a connected durable peer. There is no offline
        // registration queue because only the durable allocator may mint ids.
        let node = self.node().ok_or(RegistrationError::SystemNotReady)?;
        match node.get_durable_peers().first().copied() {
            Some(peer) => {
                let body = proto::NodeRequestBody::RegisterSchema { models: vec![request_model] };
                if !validity.is_current() {
                    return Err(RegistrationError::SystemNotReady);
                }
                match node.request(peer, cdata, body).await {
                    Ok(body) => {
                        // The response applies only while this attempt is
                        // still wanted: recheck before folding so reset
                        // clears either before or after the complete effect,
                        // never between them.
                        if !validity.is_current() {
                            return Err(RegistrationError::SystemNotReady);
                        }
                        let proto::NodeResponseBody::SchemaRegistered { models } = body else {
                            return match body {
                                proto::NodeResponseBody::Error(e) => {
                                    Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(e)))
                                }
                                other => Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                                    "unexpected response to RegisterSchema: {other}"
                                )))),
                            };
                        };
                        // The response is the fast path into the map: fold it in on ack so binding proceeds now.
                        self.upsert_registered(&models);
                        self.resolve_registered_cells(schema, &models, epoch)?;
                        Ok(())
                    }
                    Err(e) => Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!("{e:?}")))),
                }
            }
            None => Err(RegistrationError::NoDurablePeer(collection)),
        }
    }

    /// Resolve the descriptor's cells from the allocator's response.
    fn resolve_registered_cells(
        &self,
        schema: &'static ModelStructDescriptor,
        models: &[proto::RegisteredModel],
        epoch: super::SchemaEpoch,
    ) -> Result<(), RegistrationError> {
        let binding = self.registered_binding(schema, models).ok_or_else(|| {
            RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                "registration of '{}' succeeded without a complete compatible catalog binding",
                schema.label
            )))
        })?;
        binding.resolve_cells(schema, epoch);
        Ok(())
    }

    /// TEST/INTROSPECTION: number of parsed entities of each kind
    /// (models, properties, memberships).
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn counts(&self) -> (usize, usize, usize) { self.0.map.counts() }
}
