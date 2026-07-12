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
    sync::{atomic::AtomicBool, Arc, RwLock},
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
    /// property display name -> property ids, across ALL contracts. Backs
    /// the by-name allocator lookup and the name index queries.
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
    ///
    /// One live membership per (model, name) is an allocator invariant under
    /// the canonical value_type ruling (registration compat-checks, never
    /// forks), so multi-candidate election cannot arise from a well-formed
    /// catalog; a second match here means a corrupted or pre-ruling map and
    /// is worth a loud trace.
    fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> {
        let model_id = self.by_collection.get(collection)?;
        let membership_ids = self.model_memberships.get(model_id)?;
        let mut found: Option<EntityId> = None;
        for mid in membership_ids {
            if let Some(membership) = self.memberships.get(mid) {
                if let Some(prop) = self.properties.get(&membership.property) {
                    if prop.name == name {
                        match found {
                            None => found = Some(prop.id),
                            Some(first) => {
                                tracing::warn!(
                                    "catalog map holds multiple live properties named '{}' in '{}' ({} and {}); resolving to the first",
                                    name,
                                    collection,
                                    first,
                                    prop.id
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }
        found
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
    // Creation fields: name, backend, and value_type are always present on a
    // materialized property; name may be overwritten by a rename follow-up.
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
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Model(ModelDef),
    Property(PropertyDef),
    Membership(MembershipDef),
}

impl Entry {
    fn id(&self) -> EntityId {
        match self {
            Self::Model(def) => def.id,
            Self::Property(def) => def.id,
            Self::Membership(def) => def.id,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Model(_) => "model",
            Self::Property(_) => "property",
            Self::Membership(_) => "membership",
        }
    }
}

/// Strict failures at the catalog descriptor wire-cache boundary.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum WireCatalogError {
    #[error("shipped catalog state {entity} uses unknown model {model}")]
    UnknownModel { entity: EntityId, model: EntityId },
    #[error("shipped catalog state {entity} uses non-catalog model {model} ({collection})")]
    NonCatalogModel { entity: EntityId, model: EntityId, collection: String },
    #[error("shipped catalog state {entity} for {collection} is not parseable")]
    Unparseable { entity: EntityId, collection: String },
    #[error("shipped model {model} names reserved collection '{collection}'")]
    ReservedModel { model: EntityId, collection: String },
    #[error("shipped batch contains conflicting definitions for entity {entity}")]
    IntraBatchEntityConflict { entity: EntityId },
    #[error("shipped batch binds collection '{collection}' to both {first} and {second}")]
    IntraBatchCollectionConflict { collection: String, first: EntityId, second: EntityId },
    #[error("shipped batch contains duplicate membership ({model}, {property}) under both {first} and {second}")]
    IntraBatchMembershipConflict { model: EntityId, property: EntityId, first: EntityId, second: EntityId },
    #[error("shipped {incoming_kind} {entity} conflicts with existing {existing_kind} of the same id")]
    ExistingKindConflict { entity: EntityId, incoming_kind: &'static str, existing_kind: &'static str },
    #[error("shipped model {model} attempts to rebind from '{existing}' to '{incoming}'")]
    ModelRebinding { model: EntityId, existing: String, incoming: String },
    #[error("shipped model {incoming} attempts to rebind collection '{collection}' from model {existing}")]
    CollectionRebinding { collection: String, existing: EntityId, incoming: EntityId },
    #[error("shipped property {property} changes immutable registration fields")]
    PropertyRebinding { property: EntityId },
    #[error("shipped membership {membership} changes its model/property endpoints")]
    MembershipRebinding { membership: EntityId },
    #[error("shipped membership {incoming} duplicates existing membership {existing} for ({model}, {property})")]
    ExistingMembershipConflict { model: EntityId, property: EntityId, existing: EntityId, incoming: EntityId },
}

/// Parse and validate every descriptor that does not depend on current map
/// state. Exact duplicates are harmless and collapse to one entry; any
/// disagreement is a protocol error for the whole batch.
fn parse_wire_batch(states: &[proto::Attested<proto::EntityState>]) -> Result<Vec<Entry>, WireCatalogError> {
    let mut entries = BTreeMap::<EntityId, Entry>::new();
    let mut collections = BTreeMap::<String, EntityId>::new();
    let mut memberships = BTreeMap::<(EntityId, EntityId), EntityId>::new();

    for state in states {
        let entity = state.payload.entity_id;
        let model = state.payload.model;
        let collection = crate::schema::well_known_collection(&model).ok_or(WireCatalogError::UnknownModel { entity, model })?;
        if !crate::schema::is_catalog_collection(&collection) {
            return Err(WireCatalogError::NonCatalogModel { entity, model, collection: collection.as_str().to_owned() });
        }
        let entry = parse_state(&collection, entity, &state.payload)
            .ok_or_else(|| WireCatalogError::Unparseable { entity, collection: collection.as_str().to_owned() })?;

        match &entry {
            Entry::Model(def) => {
                if def.collection.starts_with(crate::schema::RESERVED_COLLECTION_PREFIX) {
                    return Err(WireCatalogError::ReservedModel { model: def.id, collection: def.collection.clone() });
                }
                if let Some(first) = collections.insert(def.collection.clone(), def.id) {
                    if first != def.id {
                        return Err(WireCatalogError::IntraBatchCollectionConflict {
                            collection: def.collection.clone(),
                            first,
                            second: def.id,
                        });
                    }
                }
            }
            Entry::Membership(def) => {
                if let Some(first) = memberships.insert((def.model, def.property), def.id) {
                    if first != def.id {
                        return Err(WireCatalogError::IntraBatchMembershipConflict {
                            model: def.model,
                            property: def.property,
                            first,
                            second: def.id,
                        });
                    }
                }
            }
            Entry::Property(_) => {}
        }

        if let Some(existing) = entries.get(&entity) {
            if existing != &entry {
                return Err(WireCatalogError::IntraBatchEntityConflict { entity });
            }
        } else {
            entries.insert(entity, entry);
        }
    }

    Ok(entries.into_values().collect())
}

fn existing_other_kind(map: &CatalogMapInner, entry: &Entry) -> Option<&'static str> {
    let id = entry.id();
    match entry {
        Entry::Model(_) => {
            map.properties.contains_key(&id).then_some("property").or_else(|| map.memberships.contains_key(&id).then_some("membership"))
        }
        Entry::Property(_) => {
            map.models.contains_key(&id).then_some("model").or_else(|| map.memberships.contains_key(&id).then_some("membership"))
        }
        Entry::Membership(_) => {
            map.models.contains_key(&id).then_some("model").or_else(|| map.properties.contains_key(&id).then_some("property"))
        }
    }
}

/// Validate against the current map and apply only after the complete entry
/// set succeeds. The caller holds the map write lock across this function,
/// closing the validation-to-mutation race.
fn validate_and_apply_wire_entries(map: &mut CatalogMapInner, entries: Vec<Entry>) -> Result<(), WireCatalogError> {
    let mut missing = Vec::new();
    for entry in entries {
        if let Some(existing_kind) = existing_other_kind(map, &entry) {
            return Err(WireCatalogError::ExistingKindConflict { entity: entry.id(), incoming_kind: entry.kind(), existing_kind });
        }
        match &entry {
            Entry::Model(def) => {
                if let Some(existing) = map.models.get(&def.id) {
                    if existing.collection != def.collection {
                        return Err(WireCatalogError::ModelRebinding {
                            model: def.id,
                            existing: existing.collection.clone(),
                            incoming: def.collection.clone(),
                        });
                    }
                    // Wire descriptor shipping is a bootstrap cache, not the
                    // schema update channel. Never let a reordered snapshot
                    // overwrite mutable fields (for example a newer model
                    // display name); authoritative reactor/storage ingestion
                    // advances existing entries with full entity history.
                    continue;
                }
                if let Some(existing) = map.by_collection.get(&def.collection) {
                    if *existing != def.id {
                        return Err(WireCatalogError::CollectionRebinding {
                            collection: def.collection.clone(),
                            existing: *existing,
                            incoming: def.id,
                        });
                    }
                }
            }
            Entry::Property(def) => {
                if let Some(existing) = map.properties.get(&def.id) {
                    if existing.minted_for != def.minted_for || existing.backend != def.backend || existing.value_type != def.value_type {
                        return Err(WireCatalogError::PropertyRebinding { property: def.id });
                    }
                    continue;
                }
            }
            Entry::Membership(def) => {
                if let Some(existing) = map.memberships.get(&def.id) {
                    if existing.model != def.model || existing.property != def.property {
                        return Err(WireCatalogError::MembershipRebinding { membership: def.id });
                    }
                    continue;
                }
                if let Some(existing) = map.membership(&def.model, &def.property) {
                    if existing.id != def.id {
                        return Err(WireCatalogError::ExistingMembershipConflict {
                            model: def.model,
                            property: def.property,
                            existing: existing.id,
                            incoming: def.id,
                        });
                    }
                }
            }
        }
        missing.push(entry);
    }

    for entry in missing {
        apply_entry(map, entry);
    }
    Ok(())
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
    /// Compiled schema shapes successfully checked in this process, grouped
    /// by collection. Collection-only latching is insufficient: two binaries
    /// (or two model types in one process) can declare different fields or
    /// types for the same collection, and each distinct declaration must pass
    /// the canonical compatibility gate at least once.
    ensured: RwLock<BTreeMap<String, Vec<&'static ModelSchema>>>,
    /// collection -> distinct compiled schema pointers, recorded by every
    /// cache_compiled call. This lets predicate resolution identify and
    /// first-use-register binary-known model declarations. Commit tracks its
    /// exact schema shapes on the transaction instead; a failed historical
    /// declaration must not poison unrelated later transactions. Pointers are
    /// 'static and system-free, so this survives reset().
    compiled_schemas: RwLock<BTreeMap<String, Vec<&'static ModelSchema>>>,
    /// The manager stays generic over the node's PolicyAgent for its
    /// Node-taking methods (ensure_registered, ensure_subscribed).
    _pa: std::marker::PhantomData<PA>,
}

impl<SE, PA> CatalogInner<SE, PA>
where PA: PolicyAgent
{
    /// Resolve a compiled explicit-id alias only after that exact schema shape
    /// has passed registration in this process. Explicit identity bypasses
    /// by-name lookup, so this takes precedence over a catalog property that
    /// merely shares the local alias. The live membership is the evidence
    /// that the registered model actually includes the bound definition.
    /// Multiple ensured declarations mapping the same local name to different
    /// ids are ambiguous and fail closed.
    fn resolve_property(&self, collection: &str, name: &str) -> Option<EntityId> {
        let ensured = self.ensured.read().unwrap().get(collection).cloned().unwrap_or_default();
        let matching_fields: Vec<_> =
            ensured.into_iter().flat_map(|schema| schema.properties.iter()).filter(|field| field.name == name).collect();
        if matching_fields.is_empty() {
            return self.map.read().unwrap().resolve(collection, name);
        }

        let map = self.map.read().unwrap();
        let mut candidates = BTreeSet::new();
        for field in matching_fields {
            match field.explicit_id {
                Some(id) => {
                    candidates.insert(super::local::parse_explicit_id(id));
                }
                None => {
                    // Only an ensured ordinary declaration makes the catalog
                    // name a candidate. An unrelated catalog property with
                    // this name must not shadow an explicit local alias.
                    if let Some(id) = map.resolve(collection, name) {
                        candidates.insert(id);
                    }
                }
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

        let id = *candidates.iter().next().expect("one property id");
        let Some(model) = map.by_collection.get(collection) else {
            return None;
        };
        if map.membership(model, &id).is_none() || !map.properties.contains_key(&id) {
            tracing::warn!("ensured explicit property alias '{}.{}' points to {} without a live catalog membership", collection, name, id);
            return None;
        }
        Some(id)
    }
}

impl<SE, PA> crate::property::PropertyResolver for CatalogInner<SE, PA>
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
            ensured: RwLock::new(BTreeMap::new()),
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
    /// once-per-connection descriptor shipping). This is an atomic, strict
    /// cache warm: the complete batch is parsed before locking the map, then
    /// validated against one map snapshot under a single write lock, and only
    /// then applied. Any malformed, non-catalog, reserved, or conflicting
    /// descriptor rejects the entire batch without mutating the map. Durable
    /// catalog entities still replicate through the ordinary subscription
    /// paths.
    pub(crate) fn ingest_wire_states(&self, states: &[proto::Attested<proto::EntityState>]) -> Result<(), WireCatalogError> {
        if states.is_empty() {
            return Ok(());
        }
        let entries = parse_wire_batch(states)?;
        let mut map = self.0.map.write().unwrap();
        validate_and_apply_wire_entries(&mut map, entries)
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

    /// Wait for catalog readiness without allowing a reset to turn an old
    /// operation into an unbounded waiter. Reset wakes `ready_notify`; the
    /// next exact-generation check then returns `SystemReset` even though the
    /// new generation's catalog may remain cold until it rejoins or creates.
    pub(crate) async fn wait_catalog_ready_in_generation(
        &self,
        node: &Node<SE, PA>,
        generation: &Arc<AtomicBool>,
    ) -> Result<(), crate::error::MutationError> {
        loop {
            // `notify_waiters` stores no permit, so register before checking
            // either readiness or generation to close the lost-wakeup gap.
            let notified = self.0.ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let _generation_guard = node.system.guard_generation(generation).await?;
                if self.is_catalog_ready() {
                    return Ok(());
                }
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

    /// The property addressed by `name` in `collection`: an ensured compiled
    /// explicit-id alias first, otherwise ordinary catalog-name lookup.
    pub fn resolve(&self, collection: &str, name: &str) -> Option<EntityId> { self.0.resolve_property(collection, name) }

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
    /// map's global name index, which also backs [`Self::property_by_name`]).
    pub fn siblings_by_name(&self, name: &str) -> Vec<EntityId> {
        self.0.map.read().unwrap().names_global.get(name).into_iter().flat_map(|s| s.iter().copied()).collect()
    }

    // -- registration lifecycle (RFC 5.2, work package A11b; rev 4) ---------

    /// Record a binary-known compiled schema for predicate first-use
    /// registration and unknown-collection classification. Transactions keep
    /// their own exact schema provenance for commit-time enforcement; this
    /// process-global cache is never replayed wholesale into a commit.
    ///
    /// Rev 4 note: this used to ALSO overlay locally-derived catalog ids
    /// into the map; under allocation (RFC 5.1) ids exist only in the
    /// catalog and its registration responses, so there is nothing local to
    /// overlay. Resolution before first registration correctly reports the
    /// property as unknown or defers (RFC 5.3).
    pub fn cache_compiled(&self, schema: &'static ModelSchema) {
        let mut compiled = self.0.compiled_schemas.write().unwrap();
        let schemas = compiled.entry(schema.collection.to_string()).or_default();
        if !schemas.iter().any(|known| **known == *schema) {
            schemas.push(schema);
        }
    }

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

    /// Whether every active field in this compiled schema already has a
    /// catalog membership with a compatible canonical definition. This is the
    /// only safe fallback after a registration reassertion is unavailable: a
    /// missing or incompatible field must fail the write rather than escape as
    /// new Name residue in a registered user collection.
    pub(crate) fn schema_is_fully_bound_compatible(&self, schema: &ModelSchema) -> bool {
        let map = self.0.map.read().unwrap();
        let Some(model) = map.by_collection.get(schema.collection).copied() else { return false };
        schema.properties.iter().all(|field| {
            // An explicit binding bypasses display-name lookup by contract:
            // the bound definition's canonical name may differ from the
            // local Rust field (and may be renamed later). Ordinary fields
            // continue to resolve through their current catalog name.
            let id = match field.explicit_id {
                Some(id) => super::local::parse_explicit_id(id),
                None => {
                    let Some(id) = map.resolve(schema.collection, field.name) else { return false };
                    id
                }
            };
            if map.membership(&model, &id).is_none() {
                return false;
            }
            let Some(def) = map.properties.get(&id) else { return false };
            def.backend == field.backend && super::registration::value_types_compatible(&def.value_type, field.value_type)
        })
    }

    /// The canonical value_type of a property-definition id, if the map knows
    /// it (the CatalogManager-side twin of
    /// [`crate::property::PropertyResolver::canonical_value_type`]; rfc.md
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
    /// on the mutating path (create/edit) before the write, and by predicate
    /// RESOLUTION at a read path's first use of a compiled model (REN 2
    /// revised, plan decision 25b) -- an existing schema resolves to a no-op
    /// plan, so the common read-path case emits nothing and skips the policy
    /// verb while the response feeds the map. Fast-returns only if this exact
    /// compiled schema shape is already ensured in this process. Records the
    /// compiled schema first for future predicate first-use resolution and
    /// diagnostics, then durably registers:
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
    ///   discriminates (plan decision 16): an unavailable reassertion is
    ///   deferrable only if every compiled field already has a compatible
    ///   canonical binding; otherwise the write fails.
    ///
    /// Every error path returns WITHOUT latching, so a later attempt
    /// retries.
    pub async fn ensure_registered(
        &self,
        node: &Node<SE, PA>,
        cdata: &PA::ContextData,
        schema: &'static ModelSchema,
    ) -> Result<(), RegistrationError> {
        let generation = node.entities.system_generation();
        self.ensure_registered_in_generation(node, cdata, schema, &generation).await
    }

    /// Generation-pinned form of [`Self::ensure_registered`]. Long-lived
    /// operations (notably transactions) must pass the token captured when
    /// they began rather than sampling the node's current token after an
    /// await. The registration round-trip deliberately runs without holding
    /// the reset read lease; its response is folded and latched only after an
    /// exact-generation lease has been reacquired.
    pub async fn ensure_registered_in_generation(
        &self,
        node: &Node<SE, PA>,
        cdata: &PA::ContextData,
        schema: &'static ModelSchema,
        generation: &Arc<AtomicBool>,
    ) -> Result<(), RegistrationError> {
        let collection = schema.collection.to_string();
        {
            // The latch is system-local. Check it only while fenced to the
            // operation's exact generation, otherwise an old transaction can
            // mistake a new system's identical schema latch for its own.
            let _generation_guard = node.system.guard_generation(generation).await?;
            if self.is_schema_ensured(schema) {
                return Ok(());
            }
        }

        // Retain this binary-known shape for later predicate first-use
        // resolution and diagnostics.
        self.cache_compiled(schema);

        let (models, properties, memberships) = super::registration_request(schema);

        if node.durable {
            // A durable node executes registration itself (no forwarding);
            // the executor upserts the map before returning. Reacquire the
            // exact-generation lease after that durable await before this
            // caller publishes its success latch.
            node.execute_schema_registration_in_generation(cdata, models, properties, memberships, generation).await?;
            let _generation_guard = node.system.guard_generation(generation).await?;
            self.mark_schema_ensured(schema);
            return Ok(());
        }

        // Ephemeral: forward to a connected durable peer; there is no
        // offline queue (rev 4 deleted it with derivation).
        match node.get_durable_peers().first().copied() {
            Some(peer) => {
                let body = proto::NodeRequestBody::RegisterSchema { models, properties, memberships };
                match node.request(peer, cdata, body).await {
                    Ok(proto::NodeResponseBody::SchemaRegistered { models, properties, memberships }) => {
                        // Do not let a delayed response from the pre-reset
                        // session repopulate the new system's map or latch.
                        // The guard is held through both synchronous writes.
                        let _generation_guard = node.system.guard_generation(generation).await?;
                        // The response is the fast path into the map (RFC
                        // 5.2): fold it in on ack so binding proceeds now.
                        self.upsert_registered(&models, &properties, &memberships);
                        self.mark_schema_ensured(schema);
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
    pub fn is_ensured(&self, collection: &str) -> bool {
        self.0.ensured.read().unwrap().get(collection).is_some_and(|schemas| !schemas.is_empty())
    }

    fn is_schema_ensured(&self, schema: &ModelSchema) -> bool {
        self.0.ensured.read().unwrap().get(schema.collection).is_some_and(|schemas| schemas.iter().any(|known| **known == *schema))
    }

    fn mark_schema_ensured(&self, schema: &'static ModelSchema) {
        let mut ensured = self.0.ensured.write().unwrap();
        let schemas = ensured.entry(schema.collection.to_string()).or_default();
        if !schemas.iter().any(|known| **known == *schema) {
            schemas.push(schema);
        }
    }

    /// Every distinct compiled schema recorded for `collection` whose exact
    /// shape is NOT yet ensured. Predicate first-use resolution uses this so
    /// one declaration cannot inherit another declaration's collection-level
    /// latch. Commit uses transaction-scoped schema provenance instead.
    pub(crate) fn unensured_schemas_for(&self, collection: &str) -> Vec<&'static ModelSchema> {
        self.0
            .compiled_schemas
            .read()
            .unwrap()
            .get(collection)
            .into_iter()
            .flat_map(|schemas| schemas.iter().copied())
            .filter(|schema| !self.is_schema_ensured(schema))
            .collect()
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

#[cfg(test)]
mod wire_ingest_tests {
    use super::*;
    use crate::property::PropertyKey;

    fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }

    fn model_state(entity_id: EntityId, collection: &str) -> proto::Attested<proto::EntityState> {
        model_state_named(entity_id, collection, collection, 0xD0)
    }

    fn model_state_named(entity_id: EntityId, collection: &str, name: &str, event: u8) -> proto::Attested<proto::EntityState> {
        let source = LWWBackend::new();
        source.set(PropertyKey::name("collection"), Some(Value::String(collection.to_owned())));
        source.set(PropertyKey::name("name"), Some(Value::String(name.to_owned())));
        let operations = source.to_operations().expect("test model operations serialize").expect("test model has writes");
        let backend = LWWBackend::new();
        backend.apply_operations_with_event(&operations, proto::EventId::from_bytes([event; 32])).expect("test model operations apply");
        let buffer = backend.to_state_buffer().expect("test model state serializes");
        let model = crate::schema::well_known_model_id(crate::schema::MODEL_COLLECTION_ID).expect("model catalog has a well-known id");
        proto::Attested::opt(
            proto::EntityState {
                entity_id,
                model,
                state: proto::State {
                    state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_owned(), buffer)])),
                    head: proto::Clock::default(),
                },
            },
            None,
        )
    }

    #[test]
    fn wire_batch_rejects_every_previously_skipped_descriptor_class() {
        let entity = id(1);

        let mut unknown = model_state(entity, "albums");
        unknown.payload.model = id(0xE0);
        assert!(matches!(
            parse_wire_batch(&[unknown]),
            Err(WireCatalogError::UnknownModel { entity: found, .. }) if found == entity
        ));

        let mut non_catalog = model_state(entity, "albums");
        non_catalog.payload.model =
            crate::schema::well_known_model_id(crate::system::SYSTEM_COLLECTION_ID).expect("system has a well-known model id");
        assert!(matches!(parse_wire_batch(&[non_catalog]), Err(WireCatalogError::NonCatalogModel { .. })));

        let mut unparseable = model_state(entity, "albums");
        unparseable.payload.state.state_buffers.0.clear();
        assert!(matches!(parse_wire_batch(&[unparseable]), Err(WireCatalogError::Unparseable { .. })));

        let reserved = model_state(entity, "_ankurah_shadow");
        assert!(matches!(parse_wire_batch(&[reserved]), Err(WireCatalogError::ReservedModel { .. })));
    }

    #[test]
    fn intra_batch_collection_conflict_rejects_the_whole_batch() {
        let result = parse_wire_batch(&[model_state(id(1), "albums"), model_state(id(2), "albums")]);
        assert!(matches!(result, Err(WireCatalogError::IntraBatchCollectionConflict { .. })));
    }

    #[test]
    fn existing_rebinding_does_not_partially_apply_valid_entries() {
        let existing = id(1);
        let mut map = CatalogMapInner::default();
        map.upsert_model(ModelDef { id: existing, collection: "albums".to_owned(), name: "albums".to_owned() });

        let entries = parse_wire_batch(&[model_state(id(2), "artists"), model_state(id(3), "albums")]).unwrap();
        let result = validate_and_apply_wire_entries(&mut map, entries);

        assert!(matches!(result, Err(WireCatalogError::CollectionRebinding { .. })));
        assert_eq!(map.by_collection.get("albums"), Some(&existing));
        assert!(!map.by_collection.contains_key("artists"), "valid earlier entries must not leak from a rejected batch");
        assert_eq!(map.models.len(), 1);
    }

    #[test]
    fn valid_batch_applies_after_complete_validation() {
        let mut map = CatalogMapInner::default();
        let entries = parse_wire_batch(&[model_state(id(1), "albums"), model_state(id(2), "artists")]).unwrap();
        validate_and_apply_wire_entries(&mut map, entries).unwrap();

        assert_eq!(map.by_collection.get("albums"), Some(&id(1)));
        assert_eq!(map.by_collection.get("artists"), Some(&id(2)));
        assert_eq!(map.models.len(), 2);
    }

    #[test]
    fn reordered_wire_snapshot_cannot_roll_back_mutable_descriptor_fields() {
        let entity = id(1);
        let newer = parse_wire_batch(&[model_state_named(entity, "albums", "Albums v2", 0xD2)]).unwrap();
        let older = parse_wire_batch(&[model_state_named(entity, "albums", "Albums v1", 0xD1)]).unwrap();
        let mut map = CatalogMapInner::default();

        validate_and_apply_wire_entries(&mut map, newer).unwrap();
        validate_and_apply_wire_entries(&mut map, older).unwrap();

        assert_eq!(map.models.get(&entity).unwrap().name, "Albums v2");
    }
}
