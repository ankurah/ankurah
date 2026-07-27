//! The in-memory catalog map and its maintenance.
//!
//! Every node keeps an in-memory view of the three catalog collections
//! (`_ankurah_model`, `_ankurah_property`, `_ankurah_model_property`). In
//! this write-only phase the map serves the catalog's own maintenance
//! (registration duplicate checks and allocation), filling one of two ways:
//!
//! - DURABLE nodes feed it from a policy-free reactor subscription over the
//!   three collections: the initial fetch is the warm scan, and every later
//!   catalog commit arrives through the same listener. The map is node
//!   infrastructure like `SystemManager`; mutation stays gated by
//!   `check_event` in the executor.
//! - EPHEMERAL nodes fold only SchemaRegistered responses to their own
//!   forwarded registrations; relayed catalog subscriptions (and the
//!   credential-scoped visibility they imply) return with the read flip.
//!
//! The map parses raw state buffers, never typed Views: resolving the
//! catalog's own rows through catalog resolution would be circular.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use ankql::ast::{Predicate, Selection};
use ankurah_proto::{self as proto, EntityId, PropertyId, RegisteredModel, SystemProperty};
use tokio::sync::Notify;
use tracing::{debug, error};

use crate::{
    entity::Entity,
    error::RetrievalError,
    node::{Node, WeakNode},
    policy::PolicyAgent,
    reactor::{GapFetcher, ReactorSubscription, ReactorUpdate},
    resultset::EntityResultSet,
    storage::StorageEngine,
    util::request_fence::{RequestFence, RequestValidity},
};

use super::{registration::RegistrationError, ModelStructDescriptor};

mod map;
use map::{CatalogMapInner, EnsuredSchemaBinding};

pub mod rows;
pub use map::{ModelDef, ModelPropertyMembershipDef, PropertyDef};

/// A registration failure with a plain-text cause.
fn reg_err(msg: String) -> RegistrationError { RegistrationError::Retrieval(RetrievalError::Other(msg)) }

/// The catalog feed's gap fetcher: never called (limit-less selections have
/// no gaps); it satisfies the query-registration signature without borrowing
/// a credentialed fetcher.
struct NoopGapFetcher;

#[async_trait::async_trait]
impl GapFetcher<Entity> for NoopGapFetcher {
    async fn fetch_gap(&self, _: &proto::CollectionId, _: &Selection, _: Option<&Entity>, _: usize) -> Result<Vec<Entity>, RetrievalError> {
        Ok(Vec::new())
    }
}

/// Maintains the in-memory catalog map for a node. Held by `Node` beside
/// `SystemManager`; mirrors its `<SE, PA>` generics.
pub struct CatalogManager<SE, PA: PolicyAgent>(Arc<CatalogInner<SE, PA>>);

impl<SE, PA: PolicyAgent> Clone for CatalogManager<SE, PA> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct CatalogInner<SE, PA: PolicyAgent> {
    /// The owning node, installed by `start` (weak: the node owns the
    /// manager, never the reverse). Registration executes and forwards
    /// through it.
    node: RwLock<Option<WeakNode<SE, PA>>>,
    durable: bool,
    /// The registration executor serializes every RegisterSchema on this
    /// lock and upserts its allocations before releasing it, so consecutive
    /// registrations observe each other and never double-allocate.
    allocator: tokio::sync::Mutex<()>,
    map: RwLock<CatalogMapInner>,
    ready: RwLock<bool>,
    ready_notify: Notify,
    /// Warm generation and epoch fences: reset invalidates the generation
    /// before clearing the catalog, so a slow warm cannot publish afterward.
    setup_state: RwLock<CatalogSetupState>,
    /// Exact compiled-schema bindings admitted in this process, by
    /// collection. Latched only on success; reset clears the latch
    /// (allocated ids belong to one system). Collection-only latching would
    /// be insufficient: two declarations can bind one label with two shapes.
    ensured: RwLock<BTreeMap<String, Vec<EnsuredSchemaBinding>>>,
    /// The durable map feed. Dropping either half unsubscribes, so reset
    /// tears the feed down by clearing this and the next warm re-establishes
    /// it. The listener holds a Weak of this inner (a strong ref would cycle
    /// through the guard's callback and leak).
    durable_feed: RwLock<Option<(ReactorSubscription, ankurah_signals::SubscriptionGuard)>>,
}

#[derive(Debug, Default)]
struct CatalogSetupState {
    generation: u64,
    /// While true, no new warm may be claimed. SystemManager clears it only
    /// after storage and reactor reset finish.
    resetting: bool,
    /// Quiescing owner fence for the current durable warm: one lease held
    /// from before its first storage access through readiness publication,
    /// so reset can invalidate and drain it before deleting storage.
    durable_fence: Option<RequestFence>,
    /// Owner fence for schema registration in the current system epoch:
    /// absent until a root is ready, rearmed only by the ready hook. Both
    /// execution and response folding retain leases across their map effects.
    registration_fence: Option<RequestFence>,
    /// Invalidated owners retained until reset finishes, so a canceled and
    /// retried `hard_reset` drains the same fences instead of bypassing
    /// work whose first waiter was canceled.
    draining_fences: Vec<RequestFence>,
    /// A durable hard reset drops its reactor subscription. Once the new
    /// system root is ready, one warm must attach the current generation.
    durable_resume_pending: bool,
}

impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> CatalogManager<SE, PA> {
    pub(crate) fn new(durable: bool) -> Self {
        Self(Arc::new(CatalogInner {
            node: RwLock::new(None),
            durable,
            allocator: tokio::sync::Mutex::new(()),
            map: RwLock::new(CatalogMapInner::default()),
            ready: RwLock::new(false),
            ready_notify: Notify::new(),
            setup_state: RwLock::new(CatalogSetupState::default()),
            ensured: RwLock::new(BTreeMap::new()),
            durable_feed: RwLock::new(None),
        }))
    }

    /// Called right after the `NodeInner` Arc exists (beside
    /// `policy_agent.on_node_ready`); installs the hard-reset/readiness hooks
    /// and, on durable nodes, arms the warm the system-ready hook launches.
    pub(crate) fn start(&self, node: WeakNode<SE, PA>) {
        let Some(strong) = node.upgrade() else { return };
        *self.0.node.write().unwrap() = Some(node);

        // SystemManager::hard_reset clears the catalog through these hooks;
        // it does not hold the CatalogManager directly.
        let (begin, finish, resume) = (self.clone(), self.clone(), self.clone());
        strong.system.set_catalog_reset_hook(
            Arc::new(move || {
                let manager = begin.clone();
                Box::pin(async move { manager.begin_reset().await })
            }),
            Arc::new(move || finish.finish_reset()),
            Arc::new(move || resume.resume_after_system_ready()),
        );

        if self.0.durable {
            // A durable node may stay deliberately uninitialized: spawn no
            // root-waiting task that owns the managers, just arm exactly one
            // warm for SystemManager's ready transition to launch.
            self.0.setup_state.write().unwrap().durable_resume_pending = true;
        }

        // If loading/joining won the race before hook installation, claim
        // the missed ready transition; otherwise SystemManager calls it.
        if strong.system.is_system_ready() {
            self.resume_after_system_ready();
        }
    }

    /// Durable path: subscribe the map to the three catalog collections and
    /// mark ready. The reactor subscription IS the warm: registering each
    /// `Predicate::True` query runs the local scan through the listener, and
    /// every later catalog commit (local or remote funnel) arrives the same
    /// way. The executor still folds its own commits synchronously under the
    /// allocator mutex (the feed is asynchronous, and consecutive
    /// registrations must observe their predecessors); folds are idempotent
    /// by entity id, so the overlap is harmless. The subscription carries no
    /// credentials by construction (policy lives in EntityLiveQuery above
    /// the reactor). A schema-less durable node finds no rows and is
    /// immediately ready with an empty, correct map.
    async fn warm_durable(&self, generation: u64) -> Result<(), RetrievalError> {
        if self.0.setup_state.read().unwrap().generation != generation {
            return Ok(());
        }
        let node = self.node().ok_or_else(|| RetrievalError::Other("catalog warm ran without a node".to_owned()))?;

        // Listener first, then queries: the queries' own fetches deliver the
        // existing rows through this listener, so nothing can be missed
        // between scan and subscribe. Gated on the warm's generation so a
        // reset (which bumps the generation before clearing the map) mutes
        // any straggler delivery from a torn-down feed.
        let subscription = node.reactor.subscribe();
        use ankurah_signals::Subscribe;
        let weak = Arc::downgrade(&self.0);
        let guard = subscription.subscribe(move |update: ReactorUpdate| {
            if let Some(inner) = weak.upgrade().filter(|inner| inner.setup_state.read().unwrap().generation == generation) {
                CatalogManager(inner).apply_reactor_update(update);
            }
        });

        let everything = Selection { predicate: Predicate::True, order_by: None, limit: None };
        let (sub, noop) = (subscription.id(), Arc::new(NoopGapFetcher));
        for label in [super::MODEL_COLLECTION_ID, super::PROPERTY_COLLECTION_ID, super::MODEL_PROPERTY_COLLECTION_ID] {
            let (qid, cid) = (proto::QueryId::new(), proto::CollectionId::fixed_name(label));
            node.reactor
                .add_query_and_notify(sub, qid, cid, everything.clone(), &node, EntityResultSet::empty(), noop.clone(), ())
                .await
                .map_err(|e| RetrievalError::Other(format!("catalog feed query failed: {e}")))?;
        }

        // The held setup guard excludes a reset's generation bump (a setup
        // write) between this check and feed/readiness publication.
        let setup = self.0.setup_state.read().unwrap();
        if setup.generation != generation {
            return Ok(());
        }
        *self.0.durable_feed.write().unwrap() = Some((subscription, guard));
        self.mark_ready();
        Ok(())
    }

    /// Fold one reactor update into the map, keyed by entity collection.
    /// Idempotent by entity id, so racing the executor's synchronous fold is
    /// harmless. Membership removes are ignored: catalog rows never leave a
    /// `Predicate::True` selection, and reset synthetics are muted by the
    /// listener's generation gate.
    fn apply_reactor_update(&self, update: ReactorUpdate) {
        let mut map = self.0.map.write().unwrap();
        for item in update.items {
            let Some(model) = crate::schema::system_model_id(item.entity.collection().as_str()) else { continue };
            if let Ok(state) = item.entity.to_entity_state() {
                map.apply_state(&model, item.entity.id(), &state);
            }
        }
    }

    // -- readiness ----------------------------------------------------------

    /// The owning node, from `start` (called in `Node::build`) until drop.
    pub(crate) fn node(&self) -> Option<Node<SE, PA>> { self.0.node.read().unwrap().clone()?.upgrade() }

    /// Whether the catalog map is authoritative for the current system epoch.
    pub fn is_catalog_ready(&self) -> bool { *self.0.ready.read().unwrap() }

    /// Wait until [`Self::is_catalog_ready`].
    pub async fn wait_catalog_ready(&self) { self.ready_unless(|| false).await; }

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
        self.ready_unless(|| !validity.is_current()).await
    }

    /// Readiness wait, abandoned whenever `cancelled` answers true. The
    /// `Notified` future must exist BEFORE each check: `notify_waiters`
    /// wakes only waiters registered at that moment (no permit is stored),
    /// so checking first would lose a `mark_ready` landing in between and
    /// hang this waiter forever. Loop because reset can flip readiness back
    /// off between wake and re-check; reset notifies too, so `cancelled` is
    /// re-polled promptly.
    async fn ready_unless(&self, cancelled: impl Fn() -> bool) -> bool {
        loop {
            let notified = self.0.ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if cancelled() {
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

    /// Begin SystemManager's reset barrier: invalidate the generation and
    /// all epoch owners synchronously, then drain durable warming and
    /// registration effects. Storage deletion waits for this to return.
    async fn begin_reset(&self) {
        let draining_fences = {
            let mut setup = self.0.setup_state.write().unwrap();
            if !setup.resetting {
                setup.resetting = true;
                setup.generation = setup.generation.wrapping_add(1);
            }
            for fence in [setup.durable_fence.take(), setup.registration_fence.take()].into_iter().flatten() {
                fence.invalidate();
                setup.draining_fences.push(fence);
            }
            *self.0.ready.write().unwrap() = false;
            setup.draining_fences.clone()
        };

        self.0.ready_notify.notify_waiters();

        for fence in draining_fences {
            fence.wait_drained().await;
        }
    }

    /// Finish SystemManager's reset only after storage, system state, and
    /// the reactor have been cleared; the next ready transition claims a
    /// fresh warm from here.
    fn finish_reset(&self) {
        let mut setup = self.0.setup_state.write().unwrap();
        // Feed teardown before map clear: dropping the handles unsubscribes,
        // and the next generation's warm re-establishes them.
        *self.0.durable_feed.write().unwrap() = None;
        self.0.map.write().unwrap().clear();
        *self.0.ready.write().unwrap() = false;
        // Allocations belong to one system: a node re-joining a different
        // system must re-register everything against the new allocator.
        self.0.ensured.write().unwrap().clear();
        setup.draining_fences.clear();
        setup.resetting = false;
        setup.durable_resume_pending = self.0.durable;
        drop(setup);
        // Wake waiters to observe the cleared state rather than sleeping on
        // a readiness that will never come.
        self.0.ready_notify.notify_waiters();
        debug!("CatalogManager reset (map cleared, not ready)");
    }

    /// Re-arm epoch-bound catalog work after `SystemManager` has published a
    /// ready root. Every node kind gets exactly one registration fence; a
    /// durable node also claims its one pending storage warm.
    fn resume_after_system_ready(&self) {
        let (generation, lease) = {
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
        let me = self.clone();
        crate::task::spawn(async move {
            let _lease = lease;
            if let Err(e) = me.warm_durable(generation).await {
                error!("CatalogManager durable warm failed: {}", e);
                // Readiness must still latch even on failure: registration's
                // wait parks on it, and one failed warm must not become a
                // hang. Later registrations reject loudly on their storage
                // double-checks instead, the retryable failure mode we want.
                // The held setup guard excludes a reset's generation bump
                // between the check and the latch.
                let setup = me.0.setup_state.read().unwrap();
                if setup.generation == generation {
                    me.mark_ready();
                }
            }
        });
    }

    // -- public lookup API (cheap clones) -----------------------------------

    /// The property addressed by `name` in `model`. An admitted explicit
    /// compiled id pins its identity through display renames and fails
    /// closed (None) when admitted declarations disagree; ordinary names
    /// consult the current display-name index.
    pub fn resolve(&self, model: &proto::ModelId, name: &str) -> Option<PropertyId> {
        let proto::ModelId::EntityId(id) = model else {
            return SystemProperty::from_name(name).map(PropertyId::System);
        };
        let map = self.0.map.read().unwrap();
        let label = map.models.get(id)?.label.clone();
        let mut pinned = BTreeSet::new();
        for binding in self.0.ensured.read().unwrap().get(label.as_str()).into_iter().flatten().filter(|b| b.model == *id) {
            let Some(explicit) = binding.schema.properties.iter().find(|f| f.name == name).and_then(|f| f.explicit_id) else { continue };
            let property = super::compiled::parse_explicit_id(explicit);
            if map.membership(id, &property).is_some() && map.properties.contains_key(&property) {
                pinned.insert(property);
            }
        }
        match (pinned.len(), pinned.first()) {
            (0, _) => map.resolve(&label, name).ok().flatten().map(PropertyId::EntityId),
            (1, Some(property)) => Some(PropertyId::EntityId(*property)),
            _ => None,
        }
    }

    /// Test-only probe for catalog ownership cycles after a node drop; the
    /// closure holds only a weak pointer, never extending what it observes.
    #[cfg(feature = "test-helpers")]
    pub fn liveness_probe(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let weak = Arc::downgrade(&self.0);
        move || weak.upgrade().is_some()
    }

    /// Return the current catalog definition for a durable property identity.
    pub fn property_by_id(&self, id: &EntityId) -> Option<PropertyDef> { self.0.map.read().unwrap().properties.get(id).cloned() }

    /// The model currently registered under a source label (catalog
    /// metadata, not a storage-engine materialization lookup).
    pub fn model_by_label(&self, label: &str) -> Option<ModelDef> {
        let map = self.0.map.read().unwrap();
        map.models.get(map.by_label.get(label)?).cloned()
    }

    /// The model id currently bound to a declared label. Schema information
    /// only, not the authoritative storage materialization reverse map; wire
    /// egress must ask the storage engine for that.
    pub fn model_id_for(&self, label: &str) -> Option<proto::ModelId> {
        crate::schema::system_model_id(label)
            .or_else(|| self.0.map.read().unwrap().by_label.get(label).copied().map(proto::ModelId::EntityId))
    }

    /// The runtime identity admitted for this exact compiled schema shape.
    /// Unlike `model_id_for`, this never performs a name lookup: the identity
    /// comes from the registration response (or a proven compatible binding).
    pub fn model_id_for_schema(&self, schema: &ModelStructDescriptor) -> Option<proto::ModelId> {
        self.0.ensured.read().unwrap().get(schema.label)?.iter().find(|b| *b.schema == *schema).map(|b| proto::ModelId::EntityId(b.model))
    }

    /// Return the membership connecting `model` and `property`, if present.
    pub fn membership(&self, model: &EntityId, property: &EntityId) -> Option<ModelPropertyMembershipDef> {
        self.0.map.read().unwrap().membership(model, property)
    }

    /// Return all property memberships currently registered for `model`.
    pub fn memberships_of(&self, model: &EntityId) -> Vec<ModelPropertyMembershipDef> { self.0.map.read().unwrap().memberships_of(model) }

    /// Property ids sharing display name `name` across ALL contracts (the
    /// map's global name index, which also backs [`Self::property_by_name`]).
    pub fn siblings_by_name(&self, name: &str) -> Vec<EntityId> {
        self.0.map.read().unwrap().names_global.get(name).into_iter().flat_map(|s| s.iter().copied()).collect()
    }

    // -- registration lifecycle --------------------------------------------

    /// Serialize a registration execution. The executor holds this across
    /// its whole lookup/allocate/commit/upsert sequence.
    pub(crate) async fn lock_allocator(&self) -> tokio::sync::MutexGuard<'_, ()> { self.0.allocator.lock().await }

    /// The property `name` currently addresses within `model`'s membership
    /// set. Membership is the whole lookup scope: `minted_for` is
    /// provenance, never a matching key, and backend/value_type are no part
    /// of the key either (a same-name registration with a different type is
    /// a compatibility question, never a second identity).
    pub fn property_by_name(&self, model: &EntityId, name: &str) -> Option<PropertyDef> {
        let map = self.0.map.read().unwrap();
        map.memberships_of(model).into_iter().find_map(|m| map.properties.get(&m.property).filter(|p| p.name == name).cloned())
    }

    /// Build the confirmed binding from the allocator's response itself: the
    /// response is the only race-free authority for the ids this exact
    /// request resolved (display names can rename or be reused concurrently).
    fn registered_binding(&self, schema: &'static ModelStructDescriptor, models: &[RegisteredModel]) -> Option<EnsuredSchemaBinding> {
        let model_def = models.iter().find(|model| model.label == schema.label)?;
        if schema.explicit_id.is_some_and(|id| super::compiled::parse_explicit_id(id) != model_def.id) {
            return None;
        }
        for field in schema.properties {
            // A nested response row IS the membership; matching it proves
            // the field is bound to this model.
            match field.explicit_id {
                Some(id) => model_def.properties.iter().find(|p| p.id == super::compiled::parse_explicit_id(id)),
                None => model_def.properties.iter().find(|p| p.name == field.name),
            }
            .filter(|p| p.backend == field.backend && super::registration::value_types_compatible(&p.value_type, field.value_type))?;
        }
        Some(EnsuredSchemaBinding { schema, model: model_def.id, confirmed: true })
    }

    /// Derive the exact binding an already-populated catalog proves for this
    /// compiled declaration: ordinary fields resolve by current name within
    /// the model's MEMBERSHIP SET (the allocator's own scope; ambiguity
    /// fails the proof), an explicit model id must be the label's live
    /// model, and every field needs a compatible immutable backend/type pair.
    fn compatible_binding(&self, schema: &'static ModelStructDescriptor, confirmed: bool) -> Option<EnsuredSchemaBinding> {
        let map = self.0.map.read().unwrap();
        // `by_label` maps a label to the model whose CURRENT label it is, so
        // one index check covers liveness and label match for an explicit id.
        let model = match schema.explicit_id {
            Some(id) => super::compiled::parse_explicit_id(id),
            None => *map.by_label.get(schema.label)?,
        };
        if map.by_label.get(schema.label) != Some(&model) {
            return None;
        }
        for field in schema.properties {
            let id = match field.explicit_id {
                Some(id) => super::compiled::parse_explicit_id(id),
                None => map.resolve(schema.label, field.name).ok().flatten()?,
            };
            map.membership(&model, &id)?;
            map.properties.get(&id).filter(|def| {
                def.backend == field.backend && super::registration::value_types_compatible(&def.value_type, field.value_type)
            })?;
        }
        Some(EnsuredSchemaBinding { schema, model, confirmed })
    }

    /// Record an exact binding proven from an already-compatible catalog.
    /// This is the safe no-peer fallback when the allocator cannot be reached.
    pub(crate) fn bind_compatible_schema(&self, schema: &'static ModelStructDescriptor) -> bool {
        // Bind proof and publication to one ready system epoch. Reset either
        // invalidates before admission (fail closed) or waits for this lease
        // before clearing, so old ids cannot be stored after the clear.
        let Some(_lease) = self.registration_validity().and_then(|validity| validity.try_acquire()) else { return false };
        let Some(binding) = self.compatible_binding(schema, false) else { return false };
        self.store_binding(binding);
        true
    }

    fn store_binding(&self, binding: EnsuredSchemaBinding) {
        let mut ensured = self.0.ensured.write().unwrap();
        let bindings = ensured.entry(binding.schema.label.to_string()).or_default();
        match bindings.iter_mut().find(|known| *known.schema == *binding.schema) {
            // Confirmation belongs to the exact ids the allocator returned:
            // a later local proof may not replace them while inheriting
            // their confirmation; only another confirmed result may.
            Some(existing) if binding.confirmed || !existing.confirmed => *existing = binding,
            Some(_) => {}
            None => bindings.push(binding),
        }
    }

    /// The exact model identity retained for `schema`, or a loud failure
    /// naming the `step` that should have retained it.
    fn retained_model_id(&self, schema: &ModelStructDescriptor, step: &str) -> Result<proto::ModelId, RegistrationError> {
        self.model_id_for_schema(schema).ok_or_else(|| reg_err(format!("{step} of '{}' lost its exact model identity", schema.label)))
    }

    /// Automatic schema use (mutation or predicate) tries the allocator first.
    /// A policy or executor refusal is always strict. Only the explicit
    /// no-durable-peer case may proceed from locally proven exact identities.
    pub(crate) async fn ensure_schema_for_use(
        &self,
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
    ) -> Result<proto::ModelId, RegistrationError> {
        match self.ensure_registered(cdata, schema).await {
            Ok(()) => self.retained_model_id(schema, "registration"),
            Err(error @ RegistrationError::NoDurablePeer(_)) if self.bind_compatible_schema(schema) => {
                tracing::warn!("fully bound '{}' has no durable peer; proceeding on proven identities: {}", schema.label, error);
                self.retained_model_id(schema, "compatible binding")
            }
            // A known label whose compiled shape could not be proven bound
            // is an unconfirmed schema; "never registered" would be false.
            Err(RegistrationError::NoDurablePeer(label)) if self.model_by_label(&label).is_some() => {
                Err(RegistrationError::UnconfirmedSchema(label))
            }
            Err(error) => Err(error),
        }
    }

    /// Fold resolved definitions into the map: called by the executor
    /// synchronously post-commit (before releasing the allocator mutex) and
    /// by `ensure_registered` with a SchemaRegistered response, so binding
    /// proceeds ahead of the feed. Idempotent by entity id; the reactor's
    /// later re-delivery is harmless.
    pub fn upsert_registered(&self, models: &[RegisteredModel]) {
        let mut map = self.0.map.write().unwrap();
        for m in models {
            map.upsert_model(ModelDef { id: m.id, label: m.label.clone(), name: m.name.clone() });
            for p in &m.properties {
                map.upsert_property(PropertyDef {
                    id: p.id,
                    minted_for: p.minted_for,
                    name: p.name.clone(),
                    backend: p.backend.clone(),
                    value_type: p.value_type.clone(),
                    target_model: p.target_model,
                });
                map.upsert_membership(ModelPropertyMembershipDef {
                    id: p.membership_id,
                    model: m.id,
                    property: p.id,
                    optional: Some(p.optional),
                });
            }
        }
    }

    /// TEST ONLY: seed deterministic catalog rows and admit their exact
    /// compiled-schema binding without writing catalog entities. Protocol
    /// simulators need stable identities for byte-identical traces, which
    /// the allocator's fresh ULIDs cannot give them; the binding stays
    /// complete (every field proven against the supplied rows) while the
    /// fixture's storage bypass stays explicit.
    #[cfg(feature = "test-helpers")]
    pub fn seed_registered_schema(
        &self,
        schema: &'static ModelStructDescriptor,
        models: &[RegisteredModel],
    ) -> Result<(), RegistrationError> {
        self.upsert_registered(models);
        self.mark_schema_ensured(schema, models)
    }

    /// Ensure registration: the entry point for explicit
    /// `Context::register_model` and, via `ensure_schema_for_use`, first-use
    /// registration on mutating and typed-read paths. Fast-returns only when
    /// this exact compiled shape is already ensured in this process. DURABLE
    /// nodes execute locally (the executor updates the map under the
    /// allocator mutex); EPHEMERAL nodes forward RegisterSchema to a durable
    /// peer and consume the response into the map, and with NO peer fail
    /// with NoDurablePeer (only the durable allocator may allocate ids, so
    /// there is no offline queue). Re-assertion resolves to a no-op plan
    /// whose response still feeds the map. Success latches; every error path
    /// returns WITHOUT latching, so a later attempt retries.
    pub async fn ensure_registered(
        &self,
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
    ) -> Result<(), RegistrationError> {
        // Enter the epoch before consulting the latch: checking first would
        // let reset clear the latch and install a new fence between the
        // stale boolean and our admission (an ABA false success).
        let validity = self.registration_validity().ok_or(RegistrationError::SystemNotReady)?;
        let initial_lease = validity.try_acquire().ok_or(RegistrationError::SystemNotReady)?;
        if self.is_schema_ensured(schema) {
            return Ok(());
        }

        let request_model = proto::RegisterModel::from(schema);

        if self.0.durable {
            // Retain one outer lease across the executor and latch,
            // snapshotted before execution: reacquiring afterward could grab
            // a post-reset fence and fold old definitions into the new epoch.
            let _lease = initial_lease;
            let models = self.register_schema(cdata, vec![request_model]).await?;
            self.mark_schema_ensured(schema, &models)?;
            return Ok(());
        }

        // A forwarded request may be arbitrarily slow, and reset must not
        // wait on the network: the rechecks below reject a stale response
        // instead, after reset invalidates this epoch's fence.
        drop(initial_lease);
        let still_current = || validity.is_current().then_some(()).ok_or(RegistrationError::SystemNotReady);
        let node = self.node().ok_or(RegistrationError::SystemNotReady)?;
        let Some(peer) = node.get_durable_peers().first().copied() else {
            return Err(RegistrationError::NoDurablePeer(schema.label.to_string()));
        };
        still_current()?;
        let request = proto::NodeRequestBody::RegisterSchema { models: vec![request_model] };
        let response = node.request(peer, cdata, request).await.map_err(|e| reg_err(format!("{e:?}")))?;
        // The response applies only while still wanted: recheck before
        // folding so reset clears before or after the complete effect,
        // never between.
        still_current()?;
        let models = match response {
            proto::NodeResponseBody::SchemaRegistered { models } => models,
            proto::NodeResponseBody::Error(e) => return Err(reg_err(e)),
            other => return Err(reg_err(format!("unexpected response to RegisterSchema: {other}"))),
        };
        // Fold the response in on ack so binding proceeds ahead of the feed.
        self.upsert_registered(&models);
        self.mark_schema_ensured(schema, &models)
    }

    /// Whether this collection's registration is latched (durably executed
    /// or forwarded successfully) this process.
    pub fn is_ensured(&self, collection: &str) -> bool {
        self.0.ensured.read().unwrap().get(collection).is_some_and(|bindings| bindings.iter().any(|binding| binding.confirmed))
    }

    pub(crate) fn is_schema_ensured(&self, schema: &ModelStructDescriptor) -> bool {
        self.0.ensured.read().unwrap().get(schema.label).is_some_and(|b| b.iter().any(|k| k.confirmed && *k.schema == *schema))
    }

    fn mark_schema_ensured(&self, schema: &'static ModelStructDescriptor, models: &[RegisteredModel]) -> Result<(), RegistrationError> {
        let binding = self
            .registered_binding(schema, models)
            .ok_or_else(|| reg_err(format!("registration of '{}' left no complete compatible catalog binding", schema.label)))?;
        self.store_binding(binding);
        Ok(())
    }

    /// TEST/INTROSPECTION: (models, properties, memberships) counts.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn counts(&self) -> (usize, usize, usize) {
        let map = self.0.map.read().unwrap();
        (map.models.len(), map.properties.len(), map.memberships.len())
    }
}
