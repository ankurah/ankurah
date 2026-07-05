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
use tracing::{debug, error};

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

use super::{model_collection, model_property_collection, property_collection};

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
    /// (collection, property display name) -> property ids. Resolution and
    /// the per-collection sibling gate index. Best-effort: kept in step with
    /// property and model arrivals; [`CatalogMapInner::resolve`] is the
    /// authoritative path (via memberships) and does not depend on it.
    display_names: BTreeMap<(String, String), BTreeSet<EntityId>>,
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
        self.display_names.clear();
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
        let collection = def.collection.clone();
        self.models.insert(def.id, def);
        // A property whose minting model just arrived may now be placeable in
        // the per-collection display-name index.
        self.reindex_display_names_for_model(&collection);
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

    fn index_property_names(&mut self, def: &PropertyDef) {
        self.names_global.entry(def.name.clone()).or_default().insert(def.id);
        if let Some(collection) = self.minting_collection(def) {
            self.display_names.entry((collection, def.name.clone())).or_default().insert(def.id);
        }
    }

    fn deindex_property_names(&mut self, def: &PropertyDef) {
        if let Some(set) = self.names_global.get_mut(&def.name) {
            set.remove(&def.id);
            if set.is_empty() {
                self.names_global.remove(&def.name);
            }
        }
        if let Some(collection) = self.minting_collection(def) {
            let key = (collection, def.name.clone());
            if let Some(set) = self.display_names.get_mut(&key) {
                set.remove(&def.id);
                if set.is_empty() {
                    self.display_names.remove(&key);
                }
            }
        }
    }

    fn minting_collection(&self, def: &PropertyDef) -> Option<String> {
        def.minted_for.and_then(|m| self.models.get(&m)).map(|m| m.collection.clone())
    }

    /// When a model arrives, place any already-known properties minted under
    /// it into the per-collection display-name index.
    fn reindex_display_names_for_model(&mut self, collection: &str) {
        let placements: Vec<(String, EntityId)> = self
            .properties
            .values()
            .filter(|p| self.minting_collection(p).as_deref() == Some(collection))
            .map(|p| (p.name.clone(), p.id))
            .collect();
        for (name, id) in placements {
            self.display_names.entry((collection.to_string(), name)).or_default().insert(id);
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
    let values = backend.property_values();
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
    _phantom: std::marker::PhantomData<PA>,
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
            map: RwLock::new(CatalogMapInner::default()),
            ready: RwLock::new(false),
            ready_notify: Notify::new(),
            durable_sub: RwLock::new(None),
            ephemeral_queries: RwLock::new(Vec::new()),
            subscribed: RwLock::new(false),
            _phantom: std::marker::PhantomData,
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
        let existing = self.0.collectionset.engine_collections().await.unwrap_or_default();
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
    /// three to initialize, and mark ready. Subsequent calls are no-ops.
    pub async fn ensure_subscribed(&self, cdata: PA::ContextData, node: &Node<SE, PA>) {
        if self.0.durable {
            return; // durable nodes warm from storage, never subscribe via relay
        }
        {
            let mut subscribed = self.0.subscribed.write().unwrap();
            if *subscribed {
                return;
            }
            *subscribed = true;
        }

        let mut queries = Vec::with_capacity(3);
        for collection in catalog_collections() {
            // cached: false -- the ephemeral catalog view is purely
            // relay-driven; a `cached: true` query would also spawn a local
            // reactor activation, adding concurrent reactor traffic that can
            // perturb unrelated subscription lifecycles during setup.
            let args = crate::node::MatchArgs {
                selection: ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None },
                cached: false,
            };
            let lq = match EntityLiveQuery::new_weak_node(node, collection.clone(), args, cdata.clone()) {
                Ok(lq) => lq,
                Err(e) => {
                    error!("CatalogManager ephemeral subscribe to {} failed: {}", collection, e);
                    // Roll back the latch so a later context can retry.
                    *self.0.subscribed.write().unwrap() = false;
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

    /// Apply one reactor update to the map: Remove drops, everything else
    /// upserts. Idempotent (keyed by entity id).
    fn apply_reactor_update(&self, update: ReactorUpdate) {
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

    // -- readiness ----------------------------------------------------------

    pub fn is_catalog_ready(&self) -> bool { *self.0.ready.read().unwrap() }

    pub async fn wait_catalog_ready(&self) {
        if !self.is_catalog_ready() {
            self.0.ready_notify.notified().await;
        }
    }

    fn mark_ready(&self) {
        *self.0.ready.write().unwrap() = true;
        self.0.ready_notify.notify_waiters();
    }

    /// Flush the map and readiness (RFC 5.2: hard_reset must flush the
    /// catalog map along with the state SystemManager clears, because
    /// derived ids are root-scoped and a node re-joining a different system
    /// must re-derive everything). Does NOT delete storage collections; that
    /// is SystemManager's job. Drops live subscriptions and clears the
    /// ephemeral latch so the next context re-subscribes; a durable node is
    /// expected to re-join/re-create the system, which re-warms.
    pub fn reset(&self) {
        self.0.map.write().unwrap().clear();
        *self.0.ready.write().unwrap() = false;
        *self.0.durable_sub.write().unwrap() = None;
        self.0.ephemeral_queries.write().unwrap().clear();
        *self.0.subscribed.write().unwrap() = false;
        debug!("CatalogManager reset (map cleared, not ready)");
    }

    // -- public lookup API (cheap clones) -----------------------------------

    /// The property named `name` in `collection` (RFC 5.2 name lookup).
    pub fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> { self.0.map.read().unwrap().resolve(collection, name) }

    pub fn property_by_id(&self, id: &EntityId) -> Option<PropertyDef> { self.0.map.read().unwrap().properties.get(id).cloned() }

    pub fn model_by_collection(&self, collection: &str) -> Option<ModelDef> {
        let map = self.0.map.read().unwrap();
        let id = map.by_collection.get(collection)?;
        map.models.get(id).cloned()
    }

    pub fn membership(&self, model: &EntityId, property: &EntityId) -> Option<MembershipDef> {
        self.0.map.read().unwrap().membership(model, property)
    }

    pub fn memberships_of(&self, model: &EntityId) -> Vec<MembershipDef> { self.0.map.read().unwrap().memberships_of(model) }

    /// Property ids sharing display name `name` across ALL contracts (the
    /// cross-contract sibling gate, RFC 5.4 rule 4).
    pub fn siblings_by_name(&self, name: &str) -> Vec<EntityId> { self.0.map.read().unwrap().siblings_by_name(name) }

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
