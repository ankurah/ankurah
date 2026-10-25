mod common;

use ankql::ast::Predicate;
use ankurah::core::{
    entity::Entity,
    error::ValidationError,
    node::{Node as NodeAlias, NodeInner},
    policy::{AccessDenied, DefaultContext, PolicyAgent, DEFAULT_CONTEXT},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    util::Iterable,
};
use ankurah::proto::{self, Attested, EventId};
use ankurah::{Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::{empty_offer, nocache, Album, GatedConnection, Model, Peek, Pet, PetView};

#[tokio::test]
async fn peer_get_preserves_denial_and_absence_even_with_a_cached_entity() -> Result<()> {
    use ankurah::core::error::RetrievalError;

    let agent = BridgePolicyAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let context = server.context_async(DEFAULT_CONTEXT).await?;
    let trx = context.begin();
    let id = trx.create(&Pet { name: "Nori".into(), age: "3".into() }).await?.id();
    trx.commit().await?;

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let retained = reader.get::<PetView>(id).await?;
    agent.deny_read_states.lock().unwrap().insert(id);

    let model = *Pet::descriptor().bind_local(&server.catalog, server.system.system_epoch().unwrap())?.as_entity_id().unwrap();
    let missing = proto::EntityId::random();
    let response = client.request(server.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![id, missing, model] }).await?;
    // Exercise the actual wire shape, including a successful catalog bootstrap read in the same batch.
    let response: proto::NodeResponseBody = bincode::deserialize(&bincode::serialize(&response)?)?;
    match response {
        proto::NodeResponseBody::Get(results) => assert!(matches!(
            results.as_slice(),
            [proto::GetResult::AccessDenied(denied), proto::GetResult::NotFound(absent), proto::GetResult::Found(state)]
                if *denied == id && *absent == missing && state.payload.entity_id == model
        )),
        response => panic!("unexpected response: {response:?}"),
    }
    for cached in [false, true] {
        let result = if cached { reader.get_cached::<PetView>(id).await } else { reader.get::<PetView>(id).await };
        assert!(matches!(result, Err(RetrievalError::AccessDenied(_))), "peer denial must not fall back to retained state");
        let result = if cached { reader.get_cached::<PetView>(missing).await } else { reader.get::<PetView>(missing).await };
        assert!(matches!(result, Err(RetrievalError::EntityNotFound(absent)) if absent == missing));
    }
    assert_eq!(retained.name()?, "Nori", "already-returned views remain usable");
    Ok(())
}

#[tokio::test]
async fn state_read_hook_filters_fetch_cache_and_live_updates() -> Result<()> {
    use ankurah::{core::error::RetrievalError, model::View};
    use ankurah_signals::Peek;

    let agent = BridgePolicyAgent::new();
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    let context = node.context_async(DEFAULT_CONTEXT).await?;
    let trx = context.begin();
    let hidden = trx.create(&Pet { name: "Hidden".into(), age: "3".into() }).await?.id();
    let visible = trx.create(&Pet { name: "Visible".into(), age: "4".into() }).await?.id();
    trx.commit().await?;
    let retained = context.get::<PetView>(hidden).await?;
    let query = context.query_wait::<PetView>("true").await?;
    assert_eq!(query.peek().len(), 2);

    // The predicate still allows both entities; only the post-retrieval hook denies this one.
    agent.deny_read_states.lock().unwrap().insert(hidden);
    assert!(matches!(context.get_cached::<PetView>(hidden).await, Err(RetrievalError::AccessDenied(_))));
    let anonymous = ankurah::Context::new(node.clone(), ankurah::core::session::SessionSet::new());
    assert!(
        matches!(anonymous.get_cached::<PetView>(hidden).await, Err(RetrievalError::AccessDenied(_))),
        "an empty session set must not bypass policy"
    );
    assert_eq!(context.fetch::<PetView>("true").await?.iter().map(View::id).collect::<Vec<_>>(), vec![visible]);

    let trx = context.begin();
    trx.edit::<Pet>(retained.entity())?.name()?.insert(0, "Now ")?;
    trx.commit().await?;
    assert_eq!(query.peek().iter().map(View::id).collect::<Vec<_>>(), vec![visible]);
    let new_query = context.query_wait::<PetView>("true").await?;
    assert_eq!(new_query.peek().iter().map(View::id).collect::<Vec<_>>(), vec![visible]);

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let _connection = LocalProcessConnection::new(&client, &node).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let remote = reader.query_wait::<PetView>(common::nocache("true")?).await?;
    assert_eq!(remote.peek().iter().map(View::id).collect::<Vec<_>>(), vec![visible]);
    Ok(())
}

/// A responder sends nothing the read policy hides: SyncEntities reports a
/// hidden entity unavailable, and a Fetch whose offer still lists it finds it
/// gone from the result; neither pushes it.
#[tokio::test]
async fn a_responder_pushes_nothing_the_read_policy_hides() -> Result<()> {
    let agent = BridgePolicyAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    // Updates for the client stay in the gate, so the test reads what each request pushed.
    let (_conn, gate) = GatedConnection::new(&server, &client, |message| matches!(message, proto::NodeMessage::Update(_))).await;
    client.system.wait_system_ready().await?;
    let context = server.context_async(DEFAULT_CONTEXT).await?;
    let trx = context.begin();
    let hidden = trx.create(&Pet { name: "Hidden".into(), age: "3".into() }).await?.id();
    let visible = trx.create(&Pet { name: "Visible".into(), age: "4".into() }).await?.id();
    trx.commit().await?;
    let head = server.storage.get_state(hidden).await?.payload.state.head;
    agent.deny_read_states.lock().unwrap().insert(hidden);
    let selection: ankql::ast::Selection<ankql::ast::Resolved> = Predicate::MemberOf(context.resolve_model_id::<Pet>().await?).into();

    let entities = vec![proto::EntityHead { entity_id: hidden, head: proto::Clock::default() }];
    match client
        .request(server.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::SyncEntities { selection: selection.clone(), entities })
        .await?
    {
        proto::NodeResponseBody::EntitiesSynced { held, unavailable } => assert!(held.is_empty() && unavailable == [hidden]),
        response => panic!("{response:?}"),
    }

    let listed = proto::KeyedEntityHead { key: vec![], entity: proto::EntityHead { entity_id: hidden, head } };
    let inventory = proto::TreeSpanInventory { bounds: proto::TreeSpan::all(), heads: proto::PackedHeads::try_from(vec![listed])? };
    let known_state = proto::KnownState { spec: empty_offer(&selection).spec, spans: vec![proto::SpanContent::Heads(inventory)] };
    let response = client.request(server.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::Fetch { selection, known_state }).await?;
    assert!(matches!(response, proto::NodeResponseBody::Fetch(_)), "{response:?}");
    let pushed = pushed_until(&gate, visible).await;
    assert!(!pushed.contains(&hidden), "neither request pushed the hidden pet");
    Ok(())
}

/// The entities of the updates the gate holds, gathered until one carries `until`.
async fn pushed_until(gate: &common::MessageGate, until: proto::EntityId) -> HashSet<proto::EntityId> {
    let mut pushed = HashSet::new();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !pushed.contains(&until) {
            for message in gate.take_held() {
                let proto::NodeMessage::Update(update) = message else { continue };
                let proto::NodeUpdateBody::SubscriptionUpdate { items } = update.body;
                pushed.extend(items.iter().map(|item| item.entity_id));
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the server pushes the visible pet");
    pushed
}

/// Counts received-event validation and allows tests to deny all reads or selected events.
#[derive(Clone)]
struct BridgePolicyAgent {
    validate_calls: Arc<AtomicUsize>,
    deny_read_events: Arc<Mutex<HashSet<EventId>>>,
    deny_reads: Arc<AtomicBool>,
    deny_read_states: Arc<Mutex<HashSet<proto::EntityId>>>,
}

impl BridgePolicyAgent {
    fn new() -> Self {
        Self {
            validate_calls: Arc::new(AtomicUsize::new(0)),
            deny_read_events: Arc::new(Mutex::new(HashSet::new())),
            deny_reads: Arc::new(AtomicBool::new(false)),
            deny_read_states: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn check_read_access(&self) -> Result<(), AccessDenied> {
        if self.deny_reads.load(Ordering::SeqCst) {
            Err(AccessDenied::ByPolicy("all reads denied by test agent"))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl PolicyAgent for BridgePolicyAgent {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        cdata: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &NodeAlias<SE, Self>,
        auth: &A,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &Entity,
        _entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        self.validate_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeAlias<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> {
        None
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::EntityId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C>(&self, _data: &C) -> Result<Predicate<ankql::ast::Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
        self.check_read_access()?;
        Ok(Predicate::True)
    }

    fn check_reads<C>(
        &self,
        _data: &C,
        states: &[(&proto::EntityId, &proto::State)],
    ) -> std::collections::HashMap<proto::EntityId, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        let denied = self.deny_read_states.lock().unwrap();
        states
            .iter()
            .filter(|(id, _)| denied.contains(id))
            .map(|(id, _)| (**id, AccessDenied::ByPolicy("state read denied by test agent")))
            .collect()
    }

    fn check_read_event<C>(&self, _data: &C, event: &Attested<proto::Event>) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        self.check_read_access()?;
        if self.deny_read_events.lock().unwrap().contains(&event.payload.id()) {
            return Err(AccessDenied::ByPolicy("event read denied by test agent"));
        }
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _peer_id: &proto::EntityId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

#[tokio::test]
async fn catalog_reads_bypass_policy_including_event_bridges() -> Result<()> {
    use ankurah::core::schema::{MODEL_COLLECTION_ID, MODEL_PROPERTY_COLLECTION_ID, PROPERTY_COLLECTION_ID};

    let agent = BridgePolicyAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let context = server.context_async(DEFAULT_CONTEXT).await?;
    context.resolve_model_id::<Pet>().await?;
    agent.deny_reads.store(true, Ordering::SeqCst);

    // Advance one stored row of each catalog model with an idempotent membership
    // event, to exercise a real bridge. The raw storage write bypasses the commit
    // path, so it precedes the client's connection, whose catalog subscriptions
    // admit the server's residents for these selections.
    let mut rows = Vec::new();
    for label in [MODEL_COLLECTION_ID, PROPERTY_COLLECTION_ID, MODEL_PROPERTY_COLLECTION_ID] {
        let model = ankurah::core::schema::system_model_id(label).unwrap();
        let storage = &server.storage;
        let states = storage.fetch_states(&Predicate::MemberOf(model).into()).await?;
        assert!(!states.is_empty(), "registration must populate {label}");
        let mut state = states[0].clone();
        let entity_id = state.payload.entity_id;
        let known_head = state.payload.state.head.clone();
        let update = proto::Event::update(
            entity_id,
            known_head.clone(),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]),
        );
        let head = proto::Clock::singleton(&update);
        state.payload.state.head = head.clone();
        let mut transaction = storage.transaction();
        transaction.set_state(&known_head, &state).await?;
        transaction.add_events(&[Attested::opt(update, None)]).await?;
        assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
        let events = storage.dump_entity_events(entity_id).await?;
        assert!(!events.is_empty());
        // Bring the server's resident catalog entity up to the stored row, as a commit would.
        server.fetch_entities_from_local(&Predicate::MemberOf(model).into()).await?;
        rows.push((label, model, entity_id, known_head, head, events));
    }

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    // Updates for the client stay in the gate, so the test reads what each request pushed.
    let (_conn, gate) = GatedConnection::new(&server, &client, |message| matches!(message, proto::NodeMessage::Update(_))).await;
    client.system.wait_system_ready().await.unwrap();
    let credentials = Vec::<&'static DefaultContext>::new();

    for (label, model, entity_id, known_head, head, events) in rows {
        // The client's offer lists the row at its old head, so the server bridges from it.
        let selection: ankql::ast::Selection<ankql::ast::Resolved> = Predicate::MemberOf(model).into();
        let spec = empty_offer(&selection).spec;
        let listed = proto::KeyedEntityHead { key: vec![], entity: proto::EntityHead { entity_id, head: known_head.clone() } };
        let inventory = proto::TreeSpanInventory { bounds: proto::TreeSpan::all(), heads: proto::PackedHeads::try_from(vec![listed])? };
        let offer = proto::KnownState { spec: spec.clone(), spans: vec![proto::SpanContent::Heads(inventory)] };
        let query_id = proto::QueryId::new();
        let requests = [
            proto::NodeRequestBody::Get { ids: vec![entity_id] },
            proto::NodeRequestBody::GetEvents { event_ids: events.iter().map(|event| event.payload.id()).collect() },
            proto::NodeRequestBody::Fetch { selection: selection.clone(), known_state: offer.clone() },
            proto::NodeRequestBody::SubscribeQuery { query_id, selection: selection.clone(), version: 1, known_state: offer },
            proto::NodeRequestBody::CompareSpans {
                selection: selection.clone(),
                spec,
                questions: vec![proto::SpanQuestion::Heads(proto::TreeSpan::all())],
            },
            proto::NodeRequestBody::SyncEntities {
                selection: selection.clone(),
                entities: vec![proto::EntityHead { entity_id, head: known_head.clone() }],
            },
        ];
        for request in requests {
            match client.request(server.id, &credentials, request).await? {
                proto::NodeResponseBody::Get(results) => assert!(matches!(results.as_slice(), [proto::GetResult::Found(_)])),
                proto::NodeResponseBody::GetEvents(received) => assert_eq!(received.len(), events.len()),
                proto::NodeResponseBody::Fetch(target) => {
                    assert!(lists(&target.spans, entity_id, &head), "the account lists the catalog row at its head");
                    assert_eq!(pushed_bridge(&gate, entity_id).await, vec![], "a Fetch's push belongs to no standing query");
                }
                proto::NodeResponseBody::QuerySubscribed { target, .. } => {
                    assert!(lists(&target.spans, entity_id, &head), "the account lists the catalog row at its head");
                    let initial = vec![(query_id, proto::MembershipChange::Initial)];
                    assert_eq!(pushed_bridge(&gate, entity_id).await, initial, "a subscription's push is its initial data");
                }
                proto::NodeResponseBody::SpansCompared { answers } => {
                    let [proto::SpanAnswer::Heads(page)] = answers.as_slice() else { panic!("one page answers one heads question") };
                    let spans = [proto::SpanContent::Heads(page.clone())];
                    assert!(lists(&spans, entity_id, &head), "the page lists the catalog row at its head");
                }
                proto::NodeResponseBody::EntitiesSynced { held, unavailable } => {
                    assert!(matches!(held.as_slice(), [held] if held.entity_id == entity_id && held.head == head), "{held:?}");
                    assert!(unavailable.is_empty());
                    assert_eq!(pushed_bridge(&gate, entity_id).await, vec![], "a repair's push belongs to no standing query");
                }
                response => panic!("catalog read failed for {label}: {response:?}"),
            }
        }
    }

    for model in [
        context.resolve_model_id::<Pet>().await?,
        proto::ModelId::System(proto::SystemModel::System),
        proto::ModelId::EntityId(proto::EntityId::random()),
    ] {
        let selection: ankql::ast::Selection<ankql::ast::Resolved> = Predicate::MemberOf(model).into();
        let spec = empty_offer(&selection).spec;
        for request in [
            proto::NodeRequestBody::Fetch { known_state: empty_offer(&selection), selection: selection.clone() },
            proto::NodeRequestBody::CompareSpans {
                selection: selection.clone(),
                spec,
                questions: vec![proto::SpanQuestion::Digest(proto::TreeSpan::all())],
            },
        ] {
            let response = client.request(server.id, &DEFAULT_CONTEXT, request).await?;
            assert!(matches!(response, proto::NodeResponseBody::Error(message) if message.contains("all reads denied")));
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_catalog_live_query_repairs_lost_rows_without_credentials() -> Result<()> {
    let agent = BridgePolicyAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    // While set, every update to the client is lost.
    let lose = Arc::new(AtomicBool::new(false));
    let (_conn, _gate) = {
        let lose = lose.clone();
        GatedConnection::new(&server, &client, move |message| {
            lose.load(Ordering::SeqCst) && matches!(message, proto::NodeMessage::Update(_))
        })
        .await
    };
    client.wait_ready().await?;
    agent.deny_reads.store(true, Ordering::SeqCst);

    // Registering a model pushes its catalog rows to the client's catalog queries, and they are lost.
    lose.store(true, Ordering::SeqCst);
    let model = server.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<Album>().await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    lose.store(false, Ordering::SeqCst);
    assert_eq!(client.catalog.model_id_for("album")?, None, "the client's catalog missed the registration");

    // The catalog queries carry no credentials, and their repair bypasses the deny-all policy.
    client.check_remote_queries_now().await;
    client.check_remote_queries_now().await;
    let repaired = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while client.catalog.model_id_for("album").ok().flatten().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(repaired.is_ok(), "two disagreeing checks repaired the client's catalog");
    assert_eq!(client.catalog.model_id_for("album")?, Some(model));
    Ok(())
}

/// Whether `spans` list `entity_id` at `head`.
fn lists(spans: &[proto::SpanContent], entity_id: proto::EntityId, head: &proto::Clock) -> bool {
    spans.iter().any(|span| match span {
        proto::SpanContent::Heads(inventory) => {
            inventory.heads.to_vec().iter().any(|listed| listed.entity.entity_id == entity_id && &listed.entity.head == head)
        }
        proto::SpanContent::Digest(_) => false,
    })
}

/// Wait for the gate to hold a push of a one-event bridge to `entity_id`, and
/// return the push's predicate relevance. The client's own catalog
/// subscription pushes the row's state, never a bridge.
async fn pushed_bridge(gate: &common::MessageGate, entity_id: proto::EntityId) -> Vec<(proto::QueryId, proto::MembershipChange)> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            for message in gate.take_held() {
                let proto::NodeMessage::Update(update) = message else { continue };
                let proto::NodeUpdateBody::SubscriptionUpdate { items } = update.body;
                for item in items.into_iter().filter(|item| item.entity_id == entity_id) {
                    if matches!(&item.content, proto::UpdateContent::EventOnly(events) if events.len() == 1) {
                        return item.predicate_relevance;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the server pushes a bridge to the catalog row")
}

/// Receive side: EventBridge events must pass validate_received_event like
/// every other transport path; transport must not decide trust.
#[tokio::test]
async fn test_event_bridge_events_are_policy_validated_on_receive() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client_agent = BridgePolicyAgent::new();
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), client_agent.clone());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await.unwrap();

    let ctx_s = server.context_async(DEFAULT_CONTEXT).await?;
    let ctx_c = client.context_async(DEFAULT_CONTEXT).await?;

    let pet_id = {
        let trx = ctx_s.begin();
        let pet = trx.create(&Pet { name: "bridge-validate".to_string(), age: "1".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // Client learns the entity at its initial head (state snapshot, no events).
    let query = format!("id = '{}'", pet_id);
    let initial = ctx_c.fetch::<PetView>(query.as_str()).await?;
    assert_eq!(initial.len(), 1);
    let validate_calls_before = client_agent.validate_calls.load(Ordering::SeqCst);

    // Server advances two events; the client's re-fetch is served by a bridge.
    for age in ["2", "3"] {
        let trx = ctx_s.begin();
        ctx_s.get::<PetView>(pet_id).await?.edit(&trx)?.age()?.replace(age)?;
        trx.commit().await?;
    }
    let results = ctx_c.fetch::<PetView>(query.as_str()).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].age().unwrap(), "3");

    let validated = client_agent.validate_calls.load(Ordering::SeqCst) - validate_calls_before;
    assert!(validated >= 2, "both bridge events must pass validate_received_event, saw {validated} calls");

    Ok(())
}

/// Send side: events the read policy hides must not leak through a bridge.
/// The producer gives up on the bridge entirely and falls back to a state
/// snapshot. The behind-client then cannot VERIFY the snapshot's lineage
/// (verification would require the hidden event, which GetEvents also
/// filters), so it conservatively refuses to adopt and stays stale. That is
/// the intended posture: staleness is the price of redaction, and the
/// security property pinned here is that the hidden event never reaches the
/// client through any path. (Redaction-tolerant catch-up is a phase 2
/// design question for the snapshot-authority design.)
#[tokio::test]
async fn test_event_bridge_respects_read_policy_on_send() -> Result<()> {
    use ankurah::core::error::{ApplyError, MutationError, RetrievalError};
    use std::time::{Duration, Instant};

    let server_agent = BridgePolicyAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), server_agent.clone());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await.unwrap();

    let ctx_s = server.context_async(DEFAULT_CONTEXT).await?;
    let ctx_c = client.context_async(DEFAULT_CONTEXT).await?;

    let pet_id = {
        let trx = ctx_s.begin();
        let pet = trx.create(&Pet { name: "bridge-redact".to_string(), age: "1".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    let query = format!("id = '{}'", pet_id);
    let initial = ctx_c.fetch::<PetView>(query.as_str()).await?;
    assert_eq!(initial.len(), 1);

    // Two more server events; the first one is read-denied for peers.
    let denied_id = {
        let trx = ctx_s.begin();
        ctx_s.get::<PetView>(pet_id).await?.edit(&trx)?.age()?.replace("2")?;
        trx.commit_and_return_events().await?[0].id()
    };
    server_agent.deny_read_events.lock().unwrap().insert(denied_id.clone());
    let open_id = {
        let trx = ctx_s.begin();
        ctx_s.get::<PetView>(pet_id).await?.edit(&trx)?.age()?.replace("3")?;
        trx.commit_and_return_events().await?[0].id()
    };

    // Re-fetch: the bridge would need the denied event, so it must be
    // suppressed entirely. The snapshot fallback arrives but cannot be
    // lineage-verified without the hidden event, so the client refuses it
    // rather than adopting unverifiable state, and the fetch fails at once
    // with the refusal.
    let started = Instant::now();
    let error =
        ctx_c.fetch::<PetView>(query.as_str()).await.expect_err("client must not silently adopt state whose lineage it cannot verify");
    assert!(started.elapsed() < Duration::from_secs(1), "the refusal fails the fetch before its first idle interval ends");
    let RetrievalError::ApplyError(refusal) = &error else { panic!("the fetch fails with the client's refusal to apply, got {error}") };
    let ApplyError::Items(items) = &**refusal else { panic!("the refusal names the item it refused, got {refusal}") };
    assert!(
        matches!(items.as_slice(), [item] if item.entity_id == pet_id
            && matches!(&item.cause, MutationError::RetrievalError(RetrievalError::EventNotFound(hidden)) if *hidden == denied_id)),
        "the pet's snapshot alone was refused, for want of the hidden event: {refusal}"
    );

    // The security property: the hidden event never reached the client, and
    // neither did the rest of the redacted window (a partial chain would
    // lose operations). The client's view of the entity is stale but honest.
    let ids: HashSet<_> = client.storage.dump_entity_events(pet_id).await?.iter().map(|e| e.payload.id()).collect();
    assert!(!ids.contains(&denied_id), "the read-denied event must not reach the client through any path");
    // The non-denied tip MAY reach the client: its verification attempt
    // fetches readable events through GetEvents, which applies policy per
    // event. What matters is that the hidden event stays hidden and the
    // unverifiable state is not adopted.
    let _ = open_id;
    assert_eq!(initial[0].age().unwrap(), "1", "the resident view remains at its last verified state");

    Ok(())
}

/// A live query's check meets the refusal the fetch above meets: repairing a
/// lost edit would need its read-denied event, so the client is pushed the
/// state and refuses it. The query stays established with the pet stale, and
/// once the event is readable the next check that disagrees repairs the pet.
#[tokio::test]
async fn a_refused_repair_leaves_a_live_query_established_and_the_next_disagreeing_check_repairs() -> Result<()> {
    let server_agent = BridgePolicyAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent.clone());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    // While set, every update to the client is lost.
    let lose = Arc::new(AtomicBool::new(false));
    let (_conn, _gate) = {
        let lose = lose.clone();
        GatedConnection::new(&server, &client, move |message| {
            lose.load(Ordering::SeqCst) && matches!(message, proto::NodeMessage::Update(_))
        })
        .await
    };
    client.wait_ready().await?;
    let (ctx_s, ctx_c) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);

    let pet_id = {
        let trx = ctx_s.begin();
        let pet = trx.create(&Pet { name: "bridge-repair".to_string(), age: "1".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };
    let query = ctx_c.query_wait::<PetView>(nocache(format!("id = '{}'", pet_id).as_str())?).await?;
    let age = || query.peek().first().map(|pet| pet.age().unwrap());

    // The edit's live push is lost, and its event is read-denied before any repair.
    lose.store(true, Ordering::SeqCst);
    let denied_id = {
        let trx = ctx_s.begin();
        ctx_s.get::<PetView>(pet_id).await?.edit(&trx)?.age()?.replace("2")?;
        trx.commit_and_return_events().await?[0].id()
    };
    server_agent.deny_read_events.lock().unwrap().insert(denied_id);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    lose.store(false, Ordering::SeqCst);

    // The first check disagrees; the second confirms it and repairs, and the client refuses the state.
    client.check_remote_queries_now().await;
    client.check_remote_queries_now().await;
    assert!(query.error().peek().is_none(), "the refused repair leaves the query established");
    assert_eq!(age().as_deref(), Some("1"), "with the pet stale");

    server_agent.deny_read_events.lock().unwrap().clear();
    client.check_remote_queries_now().await;
    let repaired = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while age().as_deref() != Some("2") {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(repaired.is_ok(), "the first check to disagree once the event is readable repairs the pet");
    Ok(())
}

/// Event retrieval addresses identities across memberships, with policy checked per event.
#[tokio::test]
async fn get_events_retrieves_identities_across_model_memberships() -> Result<()> {
    let agent = BridgePolicyAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), agent.clone());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await?;

    let ctx = server.context_async(DEFAULT_CONTEXT).await?;
    let pet_event = {
        let trx = ctx.begin();
        trx.create(&Pet { name: "membership-bound".into(), age: "1".into() }).await?;
        trx.commit_and_return_events().await?.into_iter().next().expect("pet genesis event")
    };
    let album_event = {
        let trx = ctx.begin();
        trx.create(&Album { name: "other-model".into(), year: "2026".into() }).await?;
        trx.commit_and_return_events().await?.into_iter().next().expect("album genesis event")
    };
    let response = client
        .request(server.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::GetEvents { event_ids: vec![pet_event.id(), album_event.id()] })
        .await?;

    match response {
        proto::NodeResponseBody::GetEvents(events) => {
            let ids: HashSet<_> = events.iter().map(|event| event.payload.id()).collect();
            assert_eq!(ids, [pet_event.id(), album_event.id()].into());
        }
        other => panic!("expected GetEvents response, got {other}"),
    }

    agent.deny_read_events.lock().unwrap().insert(pet_event.id());
    let response = client
        .request(server.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::GetEvents { event_ids: vec![pet_event.id(), album_event.id()] })
        .await?;
    assert!(
        matches!(response, proto::NodeResponseBody::GetEvents(events)
        if events.len() == 1 && events[0].payload.id() == album_event.id()),
        "event-specific denial still applies"
    );

    let model = *Pet::descriptor().bind_local(&server.catalog, server.system.system_epoch().unwrap())?.as_entity_id().unwrap();
    let catalog_event = server.storage.dump_entity_events(model).await?.into_iter().next().unwrap();
    agent.deny_read_events.lock().unwrap().insert(catalog_event.payload.id());
    agent.deny_reads.store(true, Ordering::SeqCst);
    let response = client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::GetEvents {
                event_ids: vec![pet_event.id(), album_event.id(), catalog_event.payload.id(), EventId::from_bytes([0xfe; 32])],
            },
        )
        .await?;
    assert!(
        matches!(response, proto::NodeResponseBody::GetEvents(events) if events == vec![catalog_event]),
        "entity denial excludes user events but cannot block catalog bootstrap"
    );

    Ok(())
}
