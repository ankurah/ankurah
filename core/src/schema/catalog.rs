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
mod map;
use map::{apply_entry, parse_state, CatalogMapInner, EnsuredSchemaBinding, Entry};
pub use map::{MembershipDef, ModelDef, PropertyDef};
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
    /// All-or-nothing per model: a definition whose property or membership
    /// states cannot all be loaded ships NOTHING for that model and reports
    /// it failed, so it is re-shipped whole later rather than latching as
    /// announced off a partial description.
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
        'models: for model in models {
            let mut model_states = Vec::new();
            match model_col.get_state(*model).await {
                Ok(state) => model_states.push(state),
                Err(error) => {
                    warn!("Cannot load model definition {} for wire announcement: {}", model.to_base64_short(), error);
                    failed.push(*model);
                    continue;
                }
            }
            for membership in self.memberships_of(model) {
                match property_col.get_state(membership.property).await {
                    Ok(state) => model_states.push(state),
                    Err(error) => {
                        warn!(
                            "Cannot load property definition {} for wire announcement of model {}: {}",
                            membership.property.to_base64_short(),
                            model.to_base64_short(),
                            error
                        );
                        failed.push(*model);
                        continue 'models;
                    }
                }
                match membership_col.get_state(membership.id).await {
                    Ok(state) => model_states.push(state),
                    Err(error) => {
                        warn!(
                            "Cannot load membership definition {} for wire announcement of model {}: {}",
                            membership.id.to_base64_short(),
                            model.to_base64_short(),
                            error
                        );
                        failed.push(*model);
                        continue 'models;
                    }
                }
            }
            states.append(&mut model_states);
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

    // -- public lookup API (cheap clones) -----------------------------------

    /// The property addressed by `name` in `collection`: prefer retained exact
    /// bindings for admitted ordinary or explicit fields, fail closed if those
    /// bindings disagree, and otherwise consult the current display-name map.
    pub fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> { self.0.resolve_property(collection, name) }

    /// A weak handle to this catalog as a name-to-id resolver, stamped onto
    /// entities at assembly for the sync read path. Replaces the old
    /// per-collection `SchemaBinding` push: identity is carried by the
    /// [`ankql::ast::PropertyId`] itself, not by a binding injected into a
    /// property backend.
    pub(crate) fn resolver_weak(&self) -> std::sync::Weak<dyn crate::schema::CatalogResolver> {
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
    /// map's global name index, which also backs [`Self::property_by_name`]).
    pub fn siblings_by_name(&self, name: &str) -> Vec<EntityId> {
        self.0.map.read().unwrap().names_global.get(name).into_iter().flat_map(|s| s.iter().copied()).collect()
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
}
