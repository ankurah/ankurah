use crate::ingest::PersistState;
use crate::retrieval::SuspenseEvents;
use crate::{
    changes::EntityChange,
    entity::Entity,
    error::{MutationError, RetrievalError},
    livequery::{EntityLiveQuery, LiveQuery},
    model::View,
    node::{MatchArgs, Node},
    policy::{AccessDenied, PolicyAgent},
    storage::{StorageCollectionWrapper, StorageEngine},
    transaction::Transaction,
};
use ankurah_proto::{self as proto, Attested, Clock, CollectionId, Event};
use async_trait::async_trait;
use std::sync::{atomic::AtomicBool, Arc};
use tracing::debug;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// Context is used to provide a local interface to fetch and subscribe to entities
/// with a specific ContextData. Generally this means your auth token for a specific user,
/// but ContextData is abstracted so you can use what you want.
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct Context(Arc<dyn TContext + Send + Sync + 'static>);
impl Clone for Context {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct NodeAndContext<SE, PA: PolicyAgent>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub node: Node<SE, PA>,
    pub cdata: PA::ContextData,
}

#[async_trait]
pub trait TContext {
    fn node_id(&self) -> proto::EntityId;
    /// Create a brand new entity for a transaction, and add it to the WeakEntitySet
    /// Note that this does not actually persist the entity to the storage engine
    /// It merely ensures that there are no duplicate entities with the same ID (except forked entities)
    fn create_entity(&self, collection: proto::CollectionId, trx_alive: Arc<AtomicBool>) -> Entity;
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied>;
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError>;
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity>;
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError>;
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError>;
    fn query(&self, collection_id: proto::CollectionId, args: MatchArgs) -> Result<EntityLiveQuery, RetrievalError>;
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> TContext for NodeAndContext<SE, PA> {
    fn node_id(&self) -> proto::EntityId { self.node.id }
    fn create_entity(&self, collection: proto::CollectionId, trx_alive: Arc<AtomicBool>) -> Entity {
        let primary_entity = self.node.entities.create(collection);
        primary_entity.snapshot(trx_alive)
    }
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> { self.node.policy_agent.check_write(&self.cdata, entity, None) }
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError> {
        self.get_entity(collection, id, cached).await
    }
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity> { self.node.entities.get(&id) }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.fetch_entities(collection, args).await
    }
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError> {
        use std::sync::atomic::Ordering;

        // Atomically mark transaction as no longer alive, preventing double-commit.
        // compare_exchange returns Err if the value was already false (already committed/rolled back).
        if trx.alive.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return Err(MutationError::General("Transaction already committed or rolled back".into()));
        }

        // Generate events from the transaction entities. The commit-lane
        // stamp (D2-2) reads each parent's generation from its event
        // payload, resolved staging-then-local-then-peer: on an ephemeral
        // node a snapshot-adopted head's events are not in local storage,
        // and the CachedEventGetter fetches (and caches) them from a
        // durable peer, the pre-accepted per-commit cost. The node-held
        // staging area is shared so a commit over a head whose event a
        // prior commit failure left staged-but-unstored still resolves.
        let trx_id = trx.id.clone();
        let mut entity_events = Vec::new();
        for entity in trx.entities.iter() {
            let stamp_collection = self.node.collections.get(entity.collection()).await?;
            let stamp_getter = crate::retrieval::CachedEventGetter::with_staging(
                entity.collection().clone(),
                stamp_collection,
                &self.node,
                &self.cdata,
                self.node.staging_for(entity.collection()),
            );
            if let Some(event) = entity.generate_commit_event(&stamp_getter).await? {
                // Validate creation events: if parent is empty, this is a creation event
                // and the entity must have been created in this transaction via create()
                if event.is_entity_create() {
                    let created_ids = trx.created_entity_ids.read().unwrap();
                    if !created_ids.contains(&entity.id) {
                        return Err(MutationError::General(
                            format!(
                                "Cannot commit phantom entity {}: entity has empty parent (creation event) \
                             but was not created in this transaction via create()",
                                entity.id
                            )
                            .into(),
                        ));
                    }
                }
                entity_events.push((entity.clone(), event));
            }
        }

        // Phase one (D1 M7): plan and policy-check every entity's event
        // through the shared commit-lane phase (planner ordering and
        // classification, fork-policy preview) before persisting ANY of
        // them, so a later denial leaves nothing durable (failure
        // atomicity, V7). The preview forks the CANONICAL entity (the
        // upstream resident for transaction forks), so the policy judges
        // the event against everything the node currently knows, including
        // history committed after this transaction forked; check_event
        // attestations are attached in staging.
        let mut planned = Vec::new();
        for (entity, event) in entity_events {
            let canonical = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
                crate::entity::EntityKind::Primary => entity.clone(),
            };
            let event_id = event.id();
            let group = self.node.plan_and_check_entity_group(&self.cdata, &canonical, &[Attested::opt(event, None)]).await?;
            planned.push((entity, canonical, event_id, group));
        }

        // Phase two: every event checked; make them durable. Macro-order
        // preserved from the pre-pipeline lane: commit events, advance the
        // transaction forks' heads, relay and await required peers, and
        // only then materialize onto the canonical residents, persist
        // state, and notify once. This lane deliberately reads only the
        // group's staging, getter, and collection; the plan is consumed by
        // the remote lane's executor phase two, while here the relay
        // barrier between event commit and state persist rules that
        // sequence out (see the executor module doc).
        let mut attested_events = Vec::new();
        for (_, _, event_id, group) in &planned {
            let attested =
                group.staging.get_attested(event_id).ok_or_else(|| MutationError::General("staged event evicted mid-commit".into()))?;
            group.getter.commit_event(&attested).await?;
            attested_events.push(attested);
        }

        // Update heads BEFORE relaying (makes entities visible to server echo)
        for (entity, _, event_id, _) in &planned {
            entity.commit_head(Clock::new([event_id.clone()]));
        }
        // Relay to peers and wait for confirmation
        self.node.relay_to_required_peers(&self.cdata, trx_id, &attested_events).await?;

        // All peers confirmed: materialize onto the canonical entities and
        // persist state, one change per entity, one notification for the
        // transaction.
        let mut changes: Vec<EntityChange> = Vec::new();
        for ((entity, canonical, _, group), attested_event) in planned.iter().zip(&attested_events) {
            // The event is durable and no longer staged, so the getter
            // resolves it (and any concurrent history) from storage. A
            // primary entity already carries the operations; only upstream
            // residents of transaction forks apply here.
            if matches!(&entity.kind, crate::entity::EntityKind::Transacted { .. }) {
                canonical.apply_event(&group.getter, &attested_event.payload).await?;
            }

            // The local commit lane persists through the SAME machinery as
            // every other resident persist lane (REV 5 section E):
            // NodePersist -> save_state (to_state, attest_state, set_state),
            // which this stanza previously duplicated verbatim. The lane
            // keeps its own macro-order (commit, advance forks, relay, only
            // then materialize and persist); only the persist funnel is
            // shared.
            let persist = crate::node_applier::NodePersist { node: &self.node, collection: &group.collection };
            persist.persist(canonical).await?;
            // The post-persist hook, insertion half (derivations section 5;
            // REV 5 sections E and F): the completed persist proves the
            // just-committed event covered, so a server echo redelivering
            // it is served by the O(1) skip instead of a comparison walk.
            // A failed persist inserts nothing (the ? above returns first).
            canonical.mark_applied([attested_event.payload.id()]);

            changes.push(EntityChange::new(canonical.clone(), vec![attested_event.clone()])?);
        }

        // Notify reactor of ALL changes
        self.node.reactor.notify_change(changes).await;

        Ok(attested_events.into_iter().map(|a| a.payload).collect())
    }
    fn query(&self, collection_id: proto::CollectionId, args: MatchArgs) -> Result<EntityLiveQuery, RetrievalError> {
        EntityLiveQuery::new(&self.node, collection_id, args, self.cdata.clone())
    }
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.node.system.collection(id).await
    }
}

// This whole impl is conditionalized by the wasm feature flag
#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl Context {
    #[wasm_bindgen(js_name = "node_id")]
    pub fn js_node_id(&self) -> proto::EntityId { self.0.node_id() }
}

// This impl may or may not have the wasm_bindgen attribute but the functions will always be defined
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", uniffi::export)]
impl Context {
    /// Begin a transaction.
    pub fn begin(&self) -> Transaction { Transaction::new(self.0.clone()) }
}

impl Context {
    pub fn new<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: Node<SE, PA>,
        data: PA::ContextData,
    ) -> Self {
        Self(Arc::new(NodeAndContext { node, cdata: data }))
    }

    pub fn node_id(&self) -> proto::EntityId { self.0.node_id() }

    // TODO: Fix this - arghhh async lifetimes
    // pub async fn trx<T, F, Fut>(self: &Arc<Self>, f: F) -> anyhow::Result<T>
    // where
    //     F: for<'a> FnOnce(&'a Transaction) -> Fut,
    //     Fut: std::future::Future<Output = anyhow::Result<T>>,
    // {
    //     let trx = self.begin();
    //     let result = f(&trx).await?;
    //     trx.commit().await?;
    //     Ok(result)
    // }

    pub async fn get<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        let entity = self.0.get_entity(id, &R::collection(), false).await?;
        Ok(R::from_entity(entity))
    }

    /// Get an entity, but its ok to return early if the entity is already in the local node storage
    pub async fn get_cached<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        let entity = self.0.get_entity(id, &R::collection(), true).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(&self, args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>) -> Result<Vec<R>, RetrievalError> {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        let collection_id = R::Model::collection();

        let entities = self.0.fetch_entities(&collection_id, args).await?;

        Ok(entities.into_iter().map(|e| R::from_entity(e)).collect())
    }

    pub async fn fetch_one<R: View + Clone + 'static>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<Option<R>, RetrievalError> {
        let views = self.fetch::<R>(args).await?;
        Ok(views.into_iter().next())
    }
    /// Subscribe to changes in entities matching a selection
    pub fn query<R>(&self, args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>) -> Result<LiveQuery<R>, RetrievalError>
    where R: View {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        Ok(self.0.query(R::Model::collection(), args)?.map::<R>())
    }

    /// Subscribe to changes in entities matching a selection and wait for initialization
    pub async fn query_wait<R>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<LiveQuery<R>, RetrievalError>
    where
        R: View,
    {
        let livequery = self.query::<R>(args)?;
        livequery.wait_initialized().await;
        Ok(livequery)
    }
    pub async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.0.collection(id).await
    }
}

impl<SE, PA> NodeAndContext<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Retrieve a single entity, either by cloning the resident Entity from the Node's WeakEntitySet or fetching from storage
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::EntityId,
        cached: bool,
    ) -> Result<Entity, RetrievalError> {
        debug!("Node({}).get_entity {:?}-{:?}", self.node.id, id, collection_id);

        if !self.node.durable {
            // Fetch from peers and commit first response
            match self.node.get_from_peer(collection_id, vec![id], &self.cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if let Some(local) = self.node.entities.get(&id) {
            debug!("Node({}).get_entity found local entity - returning", self.node.id);
            let state = local.to_state()?;
            let entity_id = local.id();
            self.node.policy_agent.check_read(&self.cdata, &entity_id, collection_id, &state)?;
            return Ok(local);
        }
        debug!("{}.get_entity fetching from storage", self.node);

        let collection = self.node.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                self.node.policy_agent.check_read(
                    &self.cdata,
                    &entity_state.payload.entity_id,
                    collection_id,
                    &entity_state.payload.state,
                )?;
                let state_getter = crate::retrieval::LocalStateGetter::new(collection.clone());
                let event_getter = crate::retrieval::CachedEventGetter::new(collection_id.clone(), collection, &self.node, &self.cdata);
                let (_changed, entity) = self
                    .node
                    .entities
                    .with_state(&state_getter, &event_getter, id, collection_id.clone(), entity_state.payload.state)
                    .await?;
                Ok(entity)
            }
            Err(e) => Err(e),
        }
    }
    /// Fetch a list of entities based on a selection
    pub async fn fetch_entities(&self, collection_id: &CollectionId, mut args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.node.policy_agent.can_access_collection(&self.cdata, collection_id)?;
        // Fetch raw states from storage

        args.selection.predicate = self.node.policy_agent.filter_predicate(&self.cdata, collection_id, args.selection.predicate)?;

        // Resolve types in the AST (converts literals for JSON path comparisons)
        args.selection = self.node.type_resolver.resolve_selection_types(args.selection);

        // TODO implement cached: true
        if !self.node.durable {
            // Fetch from peers and commit first response
            Ok(self.fetch_from_peer(collection_id, args.selection).await?)
        } else {
            let storage_collection = self.node.collections.get(collection_id).await?;
            let states = storage_collection.fetch_states(&args.selection).await?;

            // Convert states to entities
            let mut entities = Vec::new();
            let state_getter = crate::retrieval::LocalStateGetter::new(storage_collection.clone());
            let event_getter = crate::retrieval::CachedEventGetter::new(collection_id.clone(), storage_collection, &self.node, &self.cdata);
            for state in states {
                let (_, entity) = self
                    .node
                    .entities
                    .with_state(&state_getter, &event_getter, state.payload.entity_id, collection_id.clone(), state.payload.state)
                    .await?;
                entities.push(entity);
            }
            Ok(entities)
        }
    }

    /// Fetch entities from the first available durable peer with known_matches support
    async fn fetch_from_peer(
        &self,
        collection_id: &proto::CollectionId,
        selection: ankql::ast::Selection,
    ) -> Result<Vec<crate::entity::Entity>, RetrievalError> {
        let peer_id = self.node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        // 1. Pre-fetch known_matches from local storage
        let known_matched_entities = self.node.fetch_entities_from_local(collection_id, &selection).await?;

        let known_matches = known_matched_entities
            .iter()
            .map(|entity| proto::KnownEntity { entity_id: entity.id(), head: entity.head().clone() })
            .collect();

        // 2. Send fetch request with known_matches
        let selection_clone = selection.clone();
        match self
            .node
            .request(peer_id, &self.cdata, proto::NodeRequestBody::Fetch { collection: collection_id.clone(), selection, known_matches })
            .await?
        {
            proto::NodeResponseBody::Fetch(deltas) => {
                let collection = self.node.collections.get(collection_id).await?;
                let staging = self.node.staging_for(collection_id);
                let event_getter = crate::retrieval::CachedEventGetter::with_staging(
                    collection_id.clone(),
                    collection.clone(),
                    &self.node,
                    &self.cdata,
                    staging.clone(),
                );
                let state_getter = crate::retrieval::LocalStateGetter::new(collection);

                // 3. Apply deltas to local storage using NodeApplier
                crate::node_applier::NodeApplier::apply_deltas(&self.node, &peer_id, deltas, &staging, &event_getter, &state_getter)
                    .await?;
                // ARCHITECTURAL QUESTION: Optimize in-place mutation vs re-fetching for remote-peer-assisted operations https://github.com/ankurah/ankurah/issues/145

                // 4. Re-fetch entities from local storage after applying deltas
                self.node.fetch_entities_from_local(collection_id, &selection_clone).await
            }
            proto::NodeResponseBody::Error(e) => {
                tracing::debug!("Error from peer fetch: {}", e);
                Err(RetrievalError::Other(format!("{:?}", e)))
            }
            _ => {
                tracing::debug!("Unexpected response type from peer fetch");
                Err(RetrievalError::Other("Unexpected response type".to_string()))
            }
        }
    }
}
