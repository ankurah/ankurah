//! The in-memory catalog map and its maintenance
//!.
//!
//! Every node keeps an in-memory view of the three catalog collections
//! (`_ankurah_model`, `_ankurah_property`, `_ankurah_model_property`). In
//! this write-only phase the map exists for the catalog's own maintenance
//! (registration duplicate checks and allocation), and it fills two ways,
//! mirroring how the two node kinds already replicate:
//!
//! - DURABLE nodes have the catalog in local storage. Once the system is
//!   ready the manager warms the map by scanning the three collections
//!   (`fetch_states` with `Predicate::True`, the same move
//!   `SystemManager::load_system_catalog` makes for the system collection).
//!   Afterward the registration executor folds its own commits into the map
//!   synchronously under the allocator mutex; no other catalog writer
//!   exists this phase. The POLICY-FREE reactor subscription that keeps the
//!   map fresh under the read flip returns with that PR (the map is node
//!   infrastructure like `SystemManager`, which reads storage with no
//!   policy; mutation stays gated by `check_event` in the executor).
//! - EPHEMERAL nodes fill their map only by folding SchemaRegistered
//!   responses to their own forwarded registrations. The three catalog
//!   subscriptions through the ordinary relay (and the credential-scoped
//!   visibility they imply) return with the read flip.
//!
//! Catalog entities are SYSTEM MODELS: they are read
//! through the raw state-buffer interface, never a `View`, because
//! deriving a `Model` for a catalog collection would be the
//! self-description ouroboros: resolving the catalog's own rows through
//! catalog resolution would be circular. Storage states are parsed
//! through `LWWBackend::from_state_buffer` + `property_values`, exactly as
//! `registration::catalog_entity_values` does.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use crate::ModelId;
use ankurah_proto::{self as proto, EntityId, PropertyId, SystemProperty};
use tokio::sync::Notify;
use tracing::{debug, error};

use crate::{
    node::{Node, WeakNode},
    policy::PolicyAgent,
    storage::StorageEngine,
    util::request_fence::{RequestFence, RequestLease, RequestValidity},
};

use super::{model_collection, model_property_collection, property_collection, registration::RegistrationError, ModelStructDescriptor};

mod map;
use map::{apply_entry, parse_state, CatalogMapInner, EnsuredSchemaBinding};
pub use map::{ModelDef, ModelPropertyMembershipDef, PropertyDef};

// -- manager ----------------------------------------------------------------

/// The three catalog collections warmed and maintained by this manager.
fn catalog_collections() -> [ModelId; 3] { [model_collection(), property_collection(), model_property_collection()] }

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
    storage: Arc<SE>,
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
    map: RwLock<CatalogMapInner>,
    ready: RwLock<bool>,
    ready_notify: Notify,
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
    ///. Latched on a successful durable
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
    fn resolve_property(&self, model: EntityId, collection: &str, name: &str) -> anyhow::Result<Option<EntityId>> {
        let matching_properties: Vec<_> = self
            .ensured
            .read()
            .unwrap()
            .get(collection)
            .into_iter()
            .flat_map(|bindings| bindings.iter())
            .filter(|binding| binding.model == model)
            .filter_map(|binding| binding.fields.get(name).copied())
            .collect();
        if matching_properties.is_empty() {
            return self.map.read().unwrap().resolve(collection, name);
        }

        let map = self.map.read().unwrap();
        let mut candidates = BTreeSet::new();
        for property in matching_properties {
            if map.membership(&model, &property).is_some() && map.properties.contains_key(&property) {
                candidates.insert(property);
            }
        }

        if candidates.len() > 1 {
            anyhow::bail!(
                "property '{name}' in model '{}' is ambiguous across {} admitted durable identities",
                proto::ModelId::EntityId(model),
                candidates.len()
            );
        }

        Ok(candidates.iter().next().copied())
    }
}

// The name-to-id lookup behind `CatalogManager::resolve`. The wider catalog
// metadata surface (model listing, reverse name lookups) becomes the
// CatalogResolver trait in the propertyid-resolution PR.
impl<SE, PA> CatalogInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn resolve_model_property(&self, model: &proto::ModelId, name: &str) -> anyhow::Result<Option<PropertyId>> {
        let proto::ModelId::EntityId(id) = model else {
            return Ok(SystemProperty::from_name(name).map(PropertyId::System));
        };
        let Some(label) = self.map.read().unwrap().models.get(id).map(|model| model.label.clone()) else {
            return Ok(None);
        };
        Ok(self.resolve_property(*id, &label, name)?.map(PropertyId::EntityId))
    }
}

impl<SE, PA> CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn new(storage: Arc<SE>, durable: bool) -> Self {
        Self(Arc::new(CatalogInner {
            storage,
            node: RwLock::new(None),
            durable,
            allocator: tokio::sync::Mutex::new(()),
            map: RwLock::new(CatalogMapInner::default()),
            ready: RwLock::new(false),
            ready_notify: Notify::new(),
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
        *self.0.node.write().unwrap() = Some(node);

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
        if let Err(e) = self.warm_durable(generation).await {
            error!("CatalogManager durable warm failed: {}", e);
            // Readiness must still latch: registration's readiness wait
            // (`wait_catalog_ready_if_current`) parks on it, and a permanently
            // un-ready catalog would turn one failed warm into a hang.
            // With a partial map, later registrations reject loudly on their
            // storage double-checks instead, which is the retryable failure
            // mode we want.
            let setup = self.0.setup_state.read().unwrap();
            if setup.generation == generation {
                self.mark_ready();
            }
        }
    }

    /// Durable path: warm the map by scanning the catalog collections, then
    /// mark ready. Registration keeps the map fresh afterward: the executor
    /// folds its own commits synchronously under the allocator mutex, and in
    /// this write-only phase no other writer exists (peers forward
    /// RegisterSchema rather than committing catalog entities directly). The
    /// reactor-fed incremental subscription returns with the read flip.
    ///
    /// A schema-less durable node has no catalog rows yet: it warms nothing
    /// and is immediately ready with an empty (correct) map. Opening the
    /// collections creates their empty materializations on first startup,
    /// which is harmless and makes the catalog tables inspectable.
    async fn warm_durable(&self, generation: u64) -> Result<(), crate::error::RetrievalError> {
        if self.0.setup_state.read().unwrap().generation != generation {
            return Ok(());
        }

        let everything = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        for model in catalog_collections() {
            let label = match model {
                ModelId::System(system) => crate::schema::system_collection_label(system),
                ModelId::EntityId(_) => unreachable!("catalog collections are system models"),
            };
            let states = self.0.storage.collection(&proto::CollectionId::fixed_name(label)).await?.fetch_states(&everything).await?;
            let setup = self.0.setup_state.read().unwrap();
            if setup.generation != generation {
                return Ok(());
            }
            let mut map = self.0.map.write().unwrap();
            for state in states {
                if let Some(entry) = parse_state(&model, state.payload.entity_id, &state.payload) {
                    apply_entry(&mut map, entry);
                }
            }
        }

        let setup = self.0.setup_state.read().unwrap();
        if setup.generation != generation {
            return Ok(());
        }
        self.mark_ready();
        Ok(())
    }

    // -- readiness ----------------------------------------------------------

    /// The owning node, while it lives: present from `start` (called in
    /// `Node::build`) until the node is dropped.
    pub(crate) fn node(&self) -> Option<Node<SE, PA>> { self.0.node.read().unwrap().clone()?.upgrade() }

    /// Whether the catalog map is authoritative for the current system epoch.
    pub fn is_catalog_ready(&self) -> bool { *self.0.ready.read().unwrap() }

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
        let draining_fences = {
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
    /// reactor have been cleared. This is the point where a new ephemeral
    /// setup may claim the next generation.
    fn finish_reset(&self) {
        let mut setup = self.0.setup_state.write().unwrap();
        self.0.map.write().unwrap().clear();
        *self.0.ready.write().unwrap() = false;
        // Allocations belong to one system and must not survive hard_reset
        //: a node re-joining a different system must re-register
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
    pub fn property_by_id(&self, id: &EntityId) -> Option<PropertyDef> { self.0.map.read().unwrap().properties.get(id).cloned() }

    /// Return the model currently registered under a source label.
    ///
    /// This is catalog metadata, not a storage-engine materialization lookup.
    pub fn model_by_label(&self, label: &str) -> Option<ModelDef> {
        let map = self.0.map.read().unwrap();
        let id = map.by_label.get(label)?;
        map.models.get(id).cloned()
    }

    /// Whether this catalog recognizes a wire model identity. System models
    /// are the bootstrap base case, answerable on a stone-cold node; allocated
    /// ids must already be present in the catalog map. A miss after descriptor
    /// shipping is a protocol violation.
    pub fn knows_model(&self, model: &proto::ModelId) -> bool {
        match model {
            proto::ModelId::System(_) => true,
            proto::ModelId::EntityId(id) => self.0.map.read().unwrap().models.contains_key(id),
        }
    }

    /// Catalog lookup for the model definition currently bound to a declared
    /// model label. This is schema information, not the authoritative
    /// storage materialization reverse map; wire egress must ask the storage
    /// engine for that mapping.
    pub fn model_id_for(&self, label: &str) -> Option<proto::ModelId> {
        crate::schema::system_model_id(label)
            .or_else(|| self.0.map.read().unwrap().by_label.get(label).copied().map(proto::ModelId::EntityId))
    }

    /// The runtime identity admitted for this exact compiled schema shape.
    /// Unlike `model_id_for`, this never performs a name lookup: the identity
    /// comes from the registration response (or a proven compatible binding).
    pub fn model_id_for_schema(&self, schema: &ModelStructDescriptor) -> Option<proto::ModelId> {
        self.0
            .ensured
            .read()
            .unwrap()
            .get(schema.label)?
            .iter()
            .find(|binding| *binding.schema == *schema)
            .map(|binding| proto::ModelId::EntityId(binding.model))
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
    /// set. Membership is the lookup scope -- a property shared into this
    /// model resolves here regardless of where it was minted; `minted_for`
    /// is provenance metadata, never a matching key.
    pub fn property_by_name(&self, model: &EntityId, name: &str) -> Option<PropertyDef> {
        let map = self.0.map.read().unwrap();
        map.memberships_of(model)
            .into_iter()
            .find_map(|membership| map.properties.get(&membership.property).filter(|p| p.name == name).cloned())
    }

    /// Build the confirmed binding from the allocator's response itself.
    /// Registration results are the only race-free authority for the ids this
    /// exact request resolved; reconstructing them from mutable display names
    /// after the response could observe a concurrent rename or name reuse.
    fn registered_binding(
        &self,
        schema: &'static ModelStructDescriptor,
        models: &[proto::RegisteredModel],
    ) -> Option<EnsuredSchemaBinding> {
        let model_def = models.iter().find(|model| model.label == schema.label)?;
        let model = model_def.id;
        if schema.explicit_id.is_some_and(|id| super::compiled::parse_explicit_id(id) != model) {
            return None;
        }

        let mut fields = BTreeMap::new();
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
            fields.insert(field.name, property.id);
        }

        Some(EnsuredSchemaBinding { schema, model, fields, confirmed: true })
    }

    /// Derive the exact binding an already-populated catalog proves for this
    /// compiled declaration.
    ///
    /// Ordinary fields resolve within the model's MEMBERSHIP SET by current
    /// name (the same scope the allocator uses; a property shared into the
    /// model counts, and an ambiguous name fails the proof). An explicit
    /// model id must itself be the label's live model. Every field then
    /// needs a compatible immutable backend/type pair.
    fn compatible_binding(&self, schema: &'static ModelStructDescriptor, confirmed: bool) -> Option<EnsuredSchemaBinding> {
        let map = self.0.map.read().unwrap();
        let model = match schema.explicit_id {
            Some(id) => {
                let id = super::compiled::parse_explicit_id(id);
                let def = map.models.get(&id)?;
                if def.label != schema.label || map.by_label.get(schema.label) != Some(&id) {
                    return None;
                }
                id
            }
            None => *map.by_label.get(schema.label)?,
        };

        let mut fields = BTreeMap::new();
        for field in schema.properties {
            let id = match field.explicit_id {
                Some(id) => super::compiled::parse_explicit_id(id),
                None => map.resolve(schema.label, field.name).ok().flatten()?,
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

    /// Record an exact binding proven from an already-compatible catalog.
    /// This is the safe no-peer fallback when the allocator cannot be reached.
    pub(crate) fn bind_compatible_schema(&self, schema: &'static ModelStructDescriptor) -> bool {
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
        let bindings = ensured.entry(binding.schema.label.to_string()).or_default();
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
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
    ) -> Result<proto::ModelId, RegistrationError> {
        match self.ensure_registered(cdata, schema).await {
            Ok(()) => self.model_id_for_schema(schema).ok_or_else(|| {
                RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                    "registration of '{}' did not retain its exact model identity",
                    schema.label
                )))
            }),
            Err(error @ RegistrationError::NoDurablePeer(_)) if self.bind_compatible_schema(schema) => {
                tracing::warn!(
                    "schema reassertion for fully bound collection '{}' has no durable peer; proceeding with proven canonical identities: {}",
                    schema.label,
                    error
                );
                self.model_id_for_schema(schema).ok_or_else(|| {
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

    /// Fold resolved definitions into the map: the executor calls this
    /// synchronously post-commit (before releasing the allocator mutex),
    /// and `ensure_registered` calls it with a SchemaRegistered response so
    /// binding proceeds ahead of the catalog subscription.
    /// Idempotent (keyed by entity id); the reactor later re-delivers the
    /// same entities harmlessly.
    pub fn upsert_registered(&self, models: &[proto::RegisteredModel]) {
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
    /// compiled-schema binding without writing catalog entities.
    ///
    /// Deterministic protocol simulators forge stable entity, model, and
    /// property identities so two runs have byte-identical traces. They cannot
    /// use the production allocator, whose fresh ULIDs would intentionally
    /// differ between runs. This helper keeps the compiled binding complete
    /// (every field proven against the supplied rows) while making the
    /// fixture's deliberate storage bypass explicit.
    #[cfg(feature = "test-helpers")]
    pub fn seed_registered_schema(
        &self,
        schema: &'static ModelStructDescriptor,
        models: &[proto::RegisteredModel],
    ) -> Result<(), RegistrationError> {
        self.upsert_registered(models);
        self.mark_schema_ensured(schema, models)
    }

    /// Ensure registration. Called by explicit [`crate::context::Context::register_model`]
    /// and, through [`Self::ensure_schema_for_use`], by first-use
    /// registration on mutating and typed-read paths. An existing schema
    /// resolves to a no-op plan, so re-asserting emits nothing and skips the
    /// policy verb while the response feeds the map. Fast-returns only if
    /// this exact compiled schema shape is already ensured in this process,
    /// then durably registers:
    ///
    /// - DURABLE node: execute the registration locally
    ///   ([`Node::execute_schema_registration`], which updates the map
    ///   itself under the allocator mutex); latch on Ok.
    /// - EPHEMERAL node with a durable peer: forward RegisterSchema and
    ///   consume the SchemaRegistered response into the map; latch on Ok.
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
        cdata: &PA::ContextData,
        schema: &'static ModelStructDescriptor,
    ) -> Result<(), RegistrationError> {
        let collection = schema.label.to_string();
        // Snapshot and enter the epoch before consulting the latch. Checking
        // first would allow reset to clear the latch and install a new fence
        // between the stale boolean and our admission (an ABA false success).
        let validity = self.registration_validity().ok_or(RegistrationError::SystemNotReady)?;
        let initial_lease = validity.try_acquire().ok_or(RegistrationError::SystemNotReady)?;
        if self.is_schema_ensured(schema) {
            return Ok(());
        }

        let request_model = proto::RegisterModel::from(schema);

        if self.0.durable {
            // A durable node executes registration itself (no forwarding);
            // the executor upserts the map before returning. Retain one
            // outer lease across the executor and exact-schema latch. It must
            // be snapshotted before execution: reacquiring afterward could
            // grab a post-reset fence and fold old definitions into the new
            // epoch (an ABA error).
            let _lease = initial_lease;
            let models = self.register_schema(cdata, vec![request_model]).await?;
            self.mark_schema_ensured(schema, &models)?;
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
                        self.mark_schema_ensured(schema, &models)?;
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

    pub(crate) fn is_schema_ensured(&self, schema: &ModelStructDescriptor) -> bool {
        self.0
            .ensured
            .read()
            .unwrap()
            .get(schema.label)
            .is_some_and(|bindings| bindings.iter().any(|known| known.confirmed && *known.schema == *schema))
    }

    fn mark_schema_ensured(
        &self,
        schema: &'static ModelStructDescriptor,
        models: &[proto::RegisteredModel],
    ) -> Result<(), RegistrationError> {
        let binding = self.registered_binding(schema, models).ok_or_else(|| {
            RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                "registration of '{}' succeeded without a complete compatible catalog binding",
                schema.label
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
