//! The in-memory catalog map and its maintenance (work package A7;
//! specs/model-property-metadata/rfc.md section 5.2, "The catalog map
//! (AC3)").
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
    property::backend::{LWWBackend, PropertyBackend},
    reactor::{AbstractEntity, GapFetcher, MembershipChange, Reactor, ReactorSubscription, ReactorUpdate},
    resultset::EntityResultSet,
    storage::StorageEngine,
    value::Value,
};

use super::{model_collection, model_property_collection, property_collection, registration::RegistrationError, ModelSchema};

/// A parsed model definition entity (`_ankurah_model`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelDef {
    pub id: EntityId,
    pub collection: String,
    pub name: String,
}

/// A parsed property definition entity (`_ankurah_property`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyDef {
    pub id: EntityId,
    /// The model in whose scope this property was first derived (provenance,
    /// not ownership; RFC section 4).
    pub minted_for: Option<EntityId>,
    pub name: String,
    pub backend: String,
    pub value_type: String,
    /// For reference-typed properties.
    pub target_model: Option<EntityId>,
}

/// A parsed contract-membership entity (`_ankurah_model_property`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipDef {
    pub id: EntityId,
    pub model: EntityId,
    pub property: EntityId,
    /// `None` until the `optional` follow-up event arrives; a membership
    /// with no flag yet is TREATED as optional (RFC 5.4), never defaulted.
    pub optional: Option<bool>,
}

/// The catalog map's inner state. All lookups return cheap clones.
#[derive(Debug, Default)]
struct CatalogMapInner {
    properties: BTreeMap<EntityId, PropertyDef>,
    models: BTreeMap<EntityId, ModelDef>,
    memberships: BTreeMap<EntityId, MembershipDef>,
    /// collection -> model id.
    by_collection: BTreeMap<String, EntityId>,
    /// model id -> its membership ids (the contract edge set).
    model_memberships: BTreeMap<EntityId, BTreeSet<EntityId>>,
    /// property display name -> property ids, across ALL contracts. The
    /// cross-contract sibling gate (RFC 5.4 rule 4) scans this.
    names_global: BTreeMap<String, BTreeSet<EntityId>>,
}

impl CatalogMapInner {
    fn clear(&mut self) {
        self.properties.clear();
        self.models.clear();
        self.memberships.clear();
        self.by_collection.clear();
        self.model_memberships.clear();
        self.names_global.clear();
    }

    /// Upsert an entity (Initial/Add/Update). Dispatches on the collection.
    fn upsert(&mut self, entity: &Entity) {
        let collection = AbstractEntity::collection(entity);
        let id = *AbstractEntity::id(entity);
        if collection == model_collection() {
            if let Some(def) = parse_model(entity, id) {
                self.upsert_model(def);
            }
        } else if collection == property_collection() {
            if let Some(def) = parse_property(entity, id) {
                self.upsert_property(def);
            }
        } else if collection == model_property_collection() {
            if let Some(def) = parse_membership(entity, id) {
                self.upsert_membership(def);
            }
        }
    }

    /// Remove an entity by (collection, id).
    fn remove(&mut self, collection: &CollectionId, id: &EntityId) {
        if *collection == model_collection() {
            self.remove_model(id);
        } else if *collection == property_collection() {
            self.remove_property(id);
        } else if *collection == model_property_collection() {
            self.remove_membership(id);
        }
    }

    fn upsert_model(&mut self, def: ModelDef) {
        // A model's collection is its identity anchor and never changes for a
        // given id, but re-index defensively in case a prior partial entry
        // pointed elsewhere.
        if let Some(old) = self.models.get(&def.id) {
            if old.collection != def.collection {
                self.by_collection.remove(&old.collection);
            }
        }
        self.by_collection.insert(def.collection.clone(), def.id);
        self.models.insert(def.id, def);
    }

    fn remove_model(&mut self, id: &EntityId) {
        if let Some(def) = self.models.remove(id) {
            if self.by_collection.get(&def.collection) == Some(id) {
                self.by_collection.remove(&def.collection);
            }
        }
    }

    fn upsert_property(&mut self, def: PropertyDef) {
        if let Some(old) = self.properties.get(&def.id).cloned() {
            self.deindex_property_names(&old);
        }
        self.index_property_names(&def);
        self.properties.insert(def.id, def);
    }

    fn remove_property(&mut self, id: &EntityId) {
        if let Some(def) = self.properties.remove(id) {
            self.deindex_property_names(&def);
        }
    }

    fn upsert_membership(&mut self, def: MembershipDef) {
        self.model_memberships.entry(def.model).or_default().insert(def.id);
        self.memberships.insert(def.id, def);
    }

    fn remove_membership(&mut self, id: &EntityId) {
        if let Some(def) = self.memberships.remove(id) {
            if let Some(set) = self.model_memberships.get_mut(&def.model) {
                set.remove(id);
                if set.is_empty() {
                    self.model_memberships.remove(&def.model);
                }
            }
        }
    }

    fn index_property_names(&mut self, def: &PropertyDef) { self.names_global.entry(def.name.clone()).or_default().insert(def.id); }

    fn deindex_property_names(&mut self, def: &PropertyDef) {
        if let Some(set) = self.names_global.get_mut(&def.name) {
            set.remove(&def.id);
            if set.is_empty() {
                self.names_global.remove(&def.name);
            }
        }
    }

    /// The property named `name` in `collection`, resolved through the
    /// collection's model and its memberships (RFC 5.2). Authoritative.
    fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> {
        let model_id = self.by_collection.get(collection)?;
        let membership_ids = self.model_memberships.get(model_id)?;
        for mid in membership_ids {
            if let Some(membership) = self.memberships.get(mid) {
                if let Some(prop) = self.properties.get(&membership.property) {
                    if prop.name == name {
                        return Some(prop.id);
                    }
                }
            }
        }
        None
    }

    fn memberships_of(&self, model: &EntityId) -> Vec<MembershipDef> {
        self.model_memberships
            .get(model)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.memberships.get(id).cloned())
            .collect()
    }

    fn membership(&self, model: &EntityId, property: &EntityId) -> Option<MembershipDef> {
        self.model_memberships.get(model)?.iter().find_map(|id| {
            let m = self.memberships.get(id)?;
            (m.property == *property).then(|| m.clone())
        })
    }

    fn siblings_by_name(&self, name: &str) -> Vec<EntityId> {
        self.names_global.get(name).into_iter().flat_map(|s| s.iter().copied()).collect()
    }
}

// -- entity parsing ---------------------------------------------------------

fn field_string(entity: &Entity, field: &str) -> Option<String> {
    match AbstractEntity::value(entity, field) {
        Some(Value::String(s)) => Some(s),
        _ => None,
    }
}

fn field_entity_id(entity: &Entity, field: &str) -> Option<EntityId> {
    match AbstractEntity::value(entity, field) {
        Some(Value::EntityId(id)) => Some(id),
        _ => None,
    }
}

fn field_bool(entity: &Entity, field: &str) -> Option<bool> {
    match AbstractEntity::value(entity, field) {
        Some(Value::Bool(b)) => Some(b),
        _ => None,
    }
}

fn parse_model(entity: &Entity, id: EntityId) -> Option<ModelDef> {
    // `collection` is a genesis (identity) field; without it the entity is
    // not yet materialized enough to index.
    let collection = field_string(entity, "collection")?;
    // `name` is a follow-up; falls back to the collection until it arrives.
    let name = field_string(entity, "name").unwrap_or_else(|| collection.clone());
    Some(ModelDef { id, collection, name })
}

fn parse_property(entity: &Entity, id: EntityId) -> Option<PropertyDef> {
    // Genesis fields: name (the anchor), backend, value_type are always
    // present on a materialized property; name may be overwritten by a
    // rename follow-up.
    let name = field_string(entity, "name")?;
    let backend = field_string(entity, "backend")?;
    let value_type = field_string(entity, "value_type")?;
    Some(PropertyDef {
        id,
        minted_for: field_entity_id(entity, "minted_for"),
        name,
        backend,
        value_type,
        target_model: field_entity_id(entity, "target_model"),
    })
}

fn parse_membership(entity: &Entity, id: EntityId) -> Option<MembershipDef> {
    let model = field_entity_id(entity, "model")?;
    let property = field_entity_id(entity, "property")?;
    // `optional` is always a follow-up (RFC 5.4): None here means "not yet
    // known", treated optional by readers.
    Some(MembershipDef { id, model, property, optional: field_bool(entity, "optional") })
}

/// Parse a stored catalog state (durable warm path). Catalog entities are
/// LWW system models: read the "lww" state buffer directly, exactly like
/// `registration::catalog_entity_values`.
fn parse_state(collection: &CollectionId, id: EntityId, state: &proto::EntityState) -> Option<Entry> {
    let buffer = state.state.state_buffers.0.get("lww")?;
    let backend = LWWBackend::from_state_buffer(buffer).ok()?;
    // Catalog entities are name-keyed (RFC section 4 bootstrap exemption).
    let values = crate::property::name_keyed(backend.property_values());
    let get_string = |field: &str| match values.get(field) {
        Some(Some(Value::String(s))) => Some(s.clone()),
        _ => None,
    };
    let get_entity_id = |field: &str| match values.get(field) {
        Some(Some(Value::EntityId(v))) => Some(*v),
        _ => None,
    };
    let get_bool = |field: &str| match values.get(field) {
        Some(Some(Value::Bool(b))) => Some(*b),
        _ => None,
    };

    if *collection == model_collection() {
        let collection = get_string("collection")?;
        let name = get_string("name").unwrap_or_else(|| collection.clone());
        Some(Entry::Model(ModelDef { id, collection, name }))
    } else if *collection == property_collection() {
        let name = get_string("name")?;
        let backend = get_string("backend")?;
        let value_type = get_string("value_type")?;
        Some(Entry::Property(PropertyDef {
            id,
            minted_for: get_entity_id("minted_for"),
            name,
            backend,
            value_type,
            target_model: get_entity_id("target_model"),
        }))
    } else if *collection == model_property_collection() {
        let model = get_entity_id("model")?;
        let property = get_entity_id("property")?;
        Some(Entry::Membership(MembershipDef { id, model, property, optional: get_bool("optional") }))
    } else {
        None
    }
}

/// A parsed catalog entry, kind-tagged, from a storage state.
enum Entry {
    Model(ModelDef),
    Property(PropertyDef),
    Membership(MembershipDef),
}

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
    /// Ephemeral first-call-wins latch for `ensure_subscribed`. Reset by
    /// `reset` so a hard-reset node can re-subscribe.
    subscribed: RwLock<bool>,
    /// Collections whose registration has been ENSURED for this process
    /// (RFC 5.2 model first-use latch). Latched on a successful durable
    /// execution or a successful forwarded RegisterSchema (the response
    /// consumed into the map). A strict error (executor/policy refusal, or
    /// the rev 4 never-registered-offline error) does NOT latch. Cleared by
    /// `reset` (allocated ids belong to one system and must not survive
    /// hard_reset).
    ensured: RwLock<BTreeSet<String>>,
    /// collection -> compiled schema pointer, recorded by every
    /// cache_compiled call. Lets the COMMIT path close the edit-only
    /// registration gap: `Transaction::edit` is sync and cannot await a
    /// durable registration, so commit_local_trx ensure-registers any
    /// touched collection whose schema is known but not yet ensured
    /// (RFC 5.2 "durable write on first mutating use"). Pointers are
    /// 'static and system-free, so this survives reset().
    compiled_schemas: RwLock<BTreeMap<String, &'static ModelSchema>>,
    /// The manager stays generic over the node's PolicyAgent for its
    /// Node-taking methods (ensure_registered, ensure_subscribed).
    _pa: std::marker::PhantomData<PA>,
}

impl<SE, PA> crate::property::PropertyResolver for CatalogInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> { self.map.read().unwrap().resolve(collection, name) }
    fn siblings(&self, name: &str) -> Vec<EntityId> { self.map.read().unwrap().siblings_by_name(name) }
    fn name_for(&self, id: &EntityId) -> Option<String> { self.map.read().unwrap().properties.get(id).map(|def| def.name.clone()) }
    fn model_id_for(&self, collection: &str) -> Option<EntityId> { self.map.read().unwrap().by_collection.get(collection).copied() }
}

impl<SE, PA> CatalogManager<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
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
            subscribed: RwLock::new(false),
            ensured: RwLock::new(BTreeSet::new()),
            compiled_schemas: RwLock::new(BTreeMap::new()),
            _pa: std::marker::PhantomData,
        }))
    }

    /// Called right after the `NodeInner` Arc exists (beside
    /// `policy_agent.on_node_ready`). On durable nodes this spawns the
    /// warm-then-subscribe task; on ephemeral nodes it installs the
    /// hard-reset hook and otherwise waits for `ensure_subscribed`.
    pub(crate) fn start(&self, node: WeakNode<SE, PA>) {
        let Some(strong) = node.upgrade() else { return };

        // Install the hard-reset flush hook on the system manager so
        // SystemManager::hard_reset can clear the catalog in-place (it does
        // not hold the CatalogManager directly).
        {
            let me = self.clone();
            strong.system.set_catalog_reset_hook(Arc::new(move || me.reset()));
        }

        if !self.0.durable {
            return; // ephemeral: driven by ensure_subscribed from Node::context
        }

        // Capture the SystemManager (a separate Arc from NodeInner) so the
        // warm task can await system readiness WITHOUT holding the Node
        // itself alive -- the warm needs only `self` (which owns the
        // collectionset and reactor). This keeps `new_durable` droppable
        // even while the warm task is in flight.
        let system = strong.system.clone();
        drop(strong);

        let me = self.clone();
        crate::task::spawn(async move {
            system.wait_system_ready().await;
            if let Err(e) = me.warm_and_subscribe_durable().await {
                error!("CatalogManager durable warm failed: {}", e);
                // Readiness must still latch: ingress resolution
                // (Node::resolve_model_wait) parks on it, and a permanently
                // un-ready catalog would turn one failed warm into a hang.
                // With a partial map, later resolutions reject loudly
                // instead, which is the retryable failure mode we want.
                me.mark_ready();
            }
        });
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
    async fn warm_and_subscribe_durable(&self) -> Result<(), crate::error::RetrievalError> {
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
            let me = self.clone();
            subscription.subscribe(move |update: ReactorUpdate| {
                me.apply_reactor_update(update);
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

        *self.0.durable_sub.write().unwrap() = Some((subscription, guard));

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
            let mut map = self.0.map.write().unwrap();
            for state in states {
                if let Some(entry) = parse_state(collection, state.payload.entity_id, &state.payload) {
                    apply_entry(&mut map, entry);
                }
            }
        }

        self.mark_ready();
        Ok(())
    }

    /// Ephemeral path: on the first call, stand up three
    /// [`EntityLiveQuery`]s (`Predicate::True`) over the catalog
    /// collections, feed their reactor updates into the map, wait for all
    /// three to initialize, and mark ready. CONCURRENT callers wait for the
    /// claimant's outcome instead of returning early (returning with a cold
    /// catalog made the second of two racing queries fail closed, rev 4),
    /// and retry the claim themselves if the claimant rolled back.
    pub async fn ensure_subscribed(&self, cdata: PA::ContextData, node: &Node<SE, PA>) {
        if self.0.durable {
            return; // durable nodes warm from storage, never subscribe via relay
        }
        loop {
            let claimed = {
                let mut subscribed = self.0.subscribed.write().unwrap();
                if *subscribed {
                    false
                } else {
                    *subscribed = true;
                    true
                }
            };
            if claimed {
                break;
            }
            // Another caller is (or was) setting up. Wait for readiness,
            // re-checking the latch so a rollback sends us back to claim.
            // Register the Notified future BEFORE the checks (the
            // wait_catalog_ready lost-wakeup discipline).
            let notified = self.0.ready_notify.notified();
            if self.is_catalog_ready() {
                return;
            }
            if !*self.0.subscribed.read().unwrap() {
                continue; // claimant rolled back: try to claim it ourselves
            }
            notified.await;
        }

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
            let lq = match EntityLiveQuery::new_weak_node(node, collection.clone(), args, cdata.clone()) {
                Ok(lq) => lq,
                Err(e) => {
                    error!("CatalogManager ephemeral subscribe to {} failed: {}", collection, e);
                    // Roll back the latch so a later context can retry, and
                    // WAKE waiters so they observe the rollback rather than
                    // sleeping on a readiness that will never come.
                    *self.0.subscribed.write().unwrap() = false;
                    self.0.ready_notify.notify_waiters();
                    return;
                }
            };
            let guard = {
                let me = self.clone();
                lq.reactor_subscription().subscribe(move |update: ReactorUpdate| {
                    me.apply_reactor_update(update);
                })
            };
            queries.push((lq, guard));
        }

        // Wait for the initial snapshots to land, then seed the map from the
        // resultsets in case any Initial items predated our listener.
        for (lq, _) in &queries {
            lq.wait_initialized().await;
        }
        {
            let mut map = self.0.map.write().unwrap();
            for (lq, _) in &queries {
                let resultset = lq.resultset();
                let read = resultset.read();
                for (_, entity) in read.iter_entities() {
                    map.upsert(entity);
                }
            }
        }

        *self.0.ephemeral_queries.write().unwrap() = queries;
        self.mark_ready();
    }

    /// Ingest catalog definition states shipped on a wire envelope (#330
    /// once-per-connection descriptor shipping): parse each into its def and
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
                    //  - it must not REBIND a collection already mapped to a
                    //    different model id: that would hijack the egress
                    //    stamping of an existing collection. The authoritative
                    //    binding arrives through the durable subscription/warm
                    //    path, which does not pass through this guard.
                    if def.collection.starts_with(crate::schema::RESERVED_COLLECTION_PREFIX) {
                        warn!("ignoring shipped model def {} naming reserved collection '{}'", def.id, def.collection);
                    } else if map.by_collection.get(&def.collection).map_or(false, |existing| *existing != def.id) {
                        warn!("ignoring shipped model def {} rebinding collection '{}'", def.id, def.collection);
                    } else {
                        map.upsert_model(def);
                    }
                }
                Some(Entry::Property(def)) => map.upsert_property(def),
                Some(Entry::Membership(def)) => map.upsert_membership(def),
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
            if self.is_catalog_ready() {
                return;
            }
            notified.await;
        }
    }

    fn mark_ready(&self) {
        *self.0.ready.write().unwrap() = true;
        self.0.ready_notify.notify_waiters();
    }

    /// Whether a compiled schema for `collection` has been recorded this
    /// process (the binary ANTICIPATES the collection even if the catalog
    /// does not know it yet).
    pub fn has_compiled(&self, collection: &str) -> bool { self.0.compiled_schemas.read().unwrap().contains_key(collection) }

    /// Flush the map and readiness (RFC 5.2: hard_reset must flush the
    /// catalog map along with the state SystemManager clears, because
    /// allocated ids belong to one system and a node re-joining a different
    /// system must re-register against that system's allocator). Does NOT
    /// delete storage collections; that is SystemManager's job. Drops live
    /// subscriptions and clears the ephemeral latch so the next context
    /// re-subscribes; a durable node is expected to re-join/re-create the
    /// system, which re-warms.
    pub fn reset(&self) {
        self.0.map.write().unwrap().clear();
        *self.0.ready.write().unwrap() = false;
        *self.0.durable_sub.write().unwrap() = None;
        self.0.ephemeral_queries.write().unwrap().clear();
        *self.0.subscribed.write().unwrap() = false;
        // Allocations belong to one system and must not survive hard_reset
        // (RFC 5.2): a node re-joining a different system must re-register
        // everything against the new system's allocator.
        self.0.ensured.write().unwrap().clear();
        // Wake any ensure_subscribed waiters so they observe the cleared
        // latch instead of sleeping on a readiness that will never come.
        self.0.ready_notify.notify_waiters();
        debug!("CatalogManager reset (map cleared, not ready)");
    }

    // -- public lookup API (cheap clones) -----------------------------------

    /// The property named `name` in `collection` (RFC 5.2 name lookup).
    pub fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> { self.0.map.read().unwrap().resolve(collection, name) }

    /// A weak handle to this catalog as a name-to-id resolver, stamped onto
    /// entities at assembly for the sync read path (the PropertyKey amendment,
    /// #289). Replaces the old per-collection `SchemaBinding` push: identity is
    /// carried by the [`crate::property::PropertyKey`], not by a binding
    /// injected into a property backend.
    pub(crate) fn resolver_weak(&self) -> std::sync::Weak<dyn crate::property::PropertyResolver> {
        // Downgrade to the concrete Weak first, then let the return type coerce
        // it to the trait object (CoerceUnsized on Weak); annotating the local
        // as the dyn type instead would wrongly force `downgrade`'s parameter.
        let weak = Arc::downgrade(&self.0);
        weak
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
    /// cross-contract sibling gate, RFC 5.4 rule 4).
    pub fn siblings_by_name(&self, name: &str) -> Vec<EntityId> { self.0.map.read().unwrap().siblings_by_name(name) }

    // -- registration lifecycle (RFC 5.2, work package A11b; rev 4) ---------

    /// Record a compiled model's schema pointer for the commit-time
    /// registration gap (plan decision 16): `Transaction::edit` is sync and
    /// cannot await a durable registration, so `commit_local_trx`
    /// ensure-registers any touched collection whose compiled schema is
    /// recorded but not yet ensured.
    ///
    /// Rev 4 note: this used to ALSO overlay locally-derived catalog ids
    /// into the map; under allocation (RFC 5.1) ids exist only in the
    /// catalog and its registration responses, so there is nothing local to
    /// overlay. Resolution before first registration correctly reports the
    /// property as unknown or defers (RFC 5.3).
    pub fn cache_compiled(&self, schema: &'static ModelSchema) {
        self.0.compiled_schemas.write().unwrap().insert(schema.collection.to_string(), schema);
    }

    // -- allocator support (RFC 5.1 executor discipline) ---------------------

    /// Serialize a registration execution. The executor holds this across
    /// its whole lookup/allocate/commit/upsert sequence.
    pub(crate) async fn lock_allocator(&self) -> tokio::sync::MutexGuard<'_, ()> { self.0.allocator.lock().await }

    /// The property lookup key (RFC 5.1): (minting model, current name,
    /// backend, value_type). Used by the executor's upsert and the rename
    /// hint pre-pass.
    pub fn property_by_key(&self, model: &EntityId, name: &str, backend: &str, value_type: &str) -> Option<PropertyDef> {
        let map = self.0.map.read().unwrap();
        map.names_global.get(name)?.iter().find_map(|id| {
            let p = map.properties.get(id)?;
            (p.minted_for == Some(*model) && p.backend == backend && p.value_type == value_type).then(|| p.clone())
        })
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
    /// on the mutating path (create/edit) before the write, and by predicate
    /// RESOLUTION at a read path's first use of a compiled model (REN 2
    /// revised, plan decision 25b) -- an existing schema resolves to a no-op
    /// plan, so the common read-path case emits nothing and skips the policy
    /// verb while the response feeds the map. Fast-returns if the collection
    /// is already ensured this process. Records the compiled
    /// schema first (for the commit-time gap), then durably registers:
    ///
    /// - DURABLE node: execute the registration locally
    ///   ([`Node::execute_schema_registration`], which updates the map
    ///   itself under the allocator mutex); latch on Ok.
    /// - EPHEMERAL node with a durable peer: forward RegisterSchema, consume
    ///   the SchemaRegistered response into the map (binding and id-keyed
    ///   writes proceed immediately, ahead of the catalog subscription);
    ///   latch on Ok.
    /// - EPHEMERAL node with NO durable peer: the rev 4 STRICT OFFLINE rule.
    ///   Registration is impossible without the allocator, so this returns
    ///   [`RegistrationError::NoDurablePeer`] WITHOUT latching. The caller
    ///   discriminates (plan decision 16): a collection the catalog already
    ///   knows keeps writing offline against its cached binding (the
    ///   re-assert is deferrable); a NEVER-registered collection fails the
    ///   write ("connect once first").
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
        if self.0.ensured.read().unwrap().contains(&collection) {
            return Ok(());
        }

        // Record the compiled schema for the commit-time registration gap.
        self.cache_compiled(schema);

        let (models, properties, memberships) = super::registration_request(schema);

        if node.durable {
            // A durable node executes registration itself (no forwarding);
            // the executor upserts the map before returning.
            node.execute_schema_registration(cdata, models, properties, memberships).await?;
            self.0.ensured.write().unwrap().insert(collection);
            return Ok(());
        }

        // Ephemeral: forward to a connected durable peer; there is no
        // offline queue (rev 4 deleted it with derivation).
        match node.get_durable_peers().first().copied() {
            Some(peer) => {
                let body = proto::NodeRequestBody::RegisterSchema { models, properties, memberships };
                match node.request(peer, cdata, body).await {
                    Ok(proto::NodeResponseBody::SchemaRegistered { models, properties, memberships }) => {
                        // The response is the fast path into the map (RFC
                        // 5.2): fold it in on ack so binding proceeds now.
                        self.upsert_registered(&models, &properties, &memberships);
                        self.0.ensured.write().unwrap().insert(collection);
                        Ok(())
                    }
                    // A policy/executor refusal is a STRICT error: do not latch.
                    Ok(proto::NodeResponseBody::Error(e)) => Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(e))),
                    Ok(other) => Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!(
                        "unexpected response to RegisterSchema: {other}"
                    )))),
                    Err(e) => Err(RegistrationError::Retrieval(crate::error::RetrievalError::Other(format!("{e:?}")))),
                }
            }
            None => Err(RegistrationError::NoDurablePeer(collection)),
        }
    }

    /// Whether this collection's registration is latched (durably executed
    /// or forwarded successfully) this process.
    pub fn is_ensured(&self, collection: &str) -> bool { self.0.ensured.read().unwrap().contains(collection) }

    /// The compiled schema recorded for `collection`, if any and if NOT yet
    /// ensured. The commit path uses this to close the edit-only gap.
    pub(crate) fn unensured_schema_for(&self, collection: &str) -> Option<&'static ModelSchema> {
        if self.is_ensured(collection) {
            return None;
        }
        self.0.compiled_schemas.read().unwrap().get(collection).copied()
    }

    /// TEST/INTROSPECTION: number of parsed entities of each kind
    /// (models, properties, memberships).
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn counts(&self) -> (usize, usize, usize) {
        let map = self.0.map.read().unwrap();
        (map.models.len(), map.properties.len(), map.memberships.len())
    }
}

/// Apply a parsed storage `Entry` to a map (durable warm).
fn apply_entry(map: &mut CatalogMapInner, entry: Entry) {
    match entry {
        Entry::Model(def) => map.upsert_model(def),
        Entry::Property(def) => map.upsert_property(def),
        Entry::Membership(def) => map.upsert_membership(def),
    }
}
