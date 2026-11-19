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

use common::{Album, Model, Pet, PetView};

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
    let response =
        client.request(server.id().unwrap(), &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![id, missing, model] }).await?;
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

/// Records peer requests and allows tests to restrict predicates, states, or events.
#[derive(Clone)]
struct BridgePolicyAgent {
    validate_calls: Arc<AtomicUsize>,
    deny_read_events: Arc<Mutex<HashSet<EventId>>>,
    deny_reads: Arc<AtomicBool>,
    deny_read_states: Arc<Mutex<HashSet<proto::EntityId>>>,
    query_filter: Arc<Mutex<Predicate<ankql::ast::Resolved>>>,
    query_calls: Arc<AtomicUsize>,
    sent_queries: Arc<Mutex<Vec<Vec<u8>>>>,
    sent_repairs: Arc<Mutex<Vec<Vec<proto::RepairMember>>>>,
}

impl BridgePolicyAgent {
    fn new() -> Self {
        Self {
            validate_calls: Arc::new(AtomicUsize::new(0)),
            deny_read_events: Arc::new(Mutex::new(HashSet::new())),
            deny_reads: Arc::new(AtomicBool::new(false)),
            deny_read_states: Arc::new(Mutex::new(HashSet::new())),
            query_filter: Arc::new(Mutex::new(Predicate::True)),
            query_calls: Arc::new(AtomicUsize::new(0)),
            sent_queries: Arc::new(Mutex::new(Vec::new())),
            sent_repairs: Arc::new(Mutex::new(Vec::new())),
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
        request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        if matches!(request.body, proto::NodeRequestBody::Fetch { .. } | proto::NodeRequestBody::SubscribeQuery { .. }) {
            self.sent_queries.lock().unwrap().push(bincode::serialize(&request.body).unwrap());
        }
        if let proto::NodeRequestBody::Compare { request: proto::ComparisonRequest::Repair { members, .. }, .. } = &request.body {
            self.sent_repairs.lock().unwrap().push(members.clone());
        }
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
        _from_node: &proto::NodeId,
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
        _from_node: &proto::NodeId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C>(&self, data: &C) -> Result<Predicate<ankql::ast::Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
        self.query_calls.fetch_add(1, Ordering::SeqCst);
        self.retrieval_predicate(data)
    }

    fn retrieval_predicate<C>(&self, _data: &C) -> Result<Predicate<ankql::ast::Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
        self.check_read_access()?;
        Ok(self.query_filter.lock().unwrap().clone())
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
        _peer_id: &proto::NodeId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

#[tokio::test]
async fn fetch_and_subscription_summaries_exclude_states_denied_after_predicate_matching() -> Result<()> {
    use ankurah::View;

    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let writer = server.context_async(DEFAULT_CONTEXT).await?;
    let trx = writer.begin();
    let visible = trx.create(&Pet { name: "Visible".into(), age: "3".into() }).await?.id();
    let hidden = trx.create(&Pet { name: "Hidden".into(), age: "4".into() }).await?.id();
    trx.create(&Pet { name: "Outside".into(), age: "5".into() }).await?;
    trx.commit().await?;

    let agent = BridgePolicyAgent::new();
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let hold_updates = Arc::new(AtomicBool::new(false));
    let held = hold_updates.clone();
    let (connection, gate) = common::GatedConnection::new(&server, &client, move |message| {
        held.load(Ordering::SeqCst)
            && matches!(
                message,
                proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::SubscriptionUpdate { .. }, .. })
            )
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    assert_eq!(reader.fetch::<PetView>("true").await?.len(), 3);
    let model = reader.resolve_model_id::<Pet>().await?;
    let restriction = client.catalog.resolve_selection(&model, ankql::parser::parse_selection("name != 'Outside'")?)?.predicate;
    *agent.query_filter.lock().unwrap() = restriction.clone();
    agent.deny_read_states.lock().unwrap().insert(hidden);
    agent.sent_queries.lock().unwrap().clear();
    agent.sent_repairs.lock().unwrap().clear();

    let scoped_before = agent.query_calls.load(Ordering::SeqCst);
    assert_eq!(reader.fetch::<PetView>("true").await?.len(), 1);
    assert!(agent.query_calls.load(Ordering::SeqCst) > scoped_before, "fetch checks current summary visibility");
    let query = reader.query_wait::<PetView>("true").await?;
    let (retained_selection, _) = query.selection().value().unwrap();
    assert!(matches!(&retained_selection.predicate, Predicate::And(_, allowed) if **allowed == restriction));
    let requests = std::mem::take(&mut *agent.sent_queries.lock().unwrap());
    let mut summaries = Vec::new();
    for request in requests {
        let request: proto::NodeRequestBody = bincode::deserialize(&request)?;
        let (subscription, selection, known_state) = match request {
            proto::NodeRequestBody::Fetch { selection, known_state, .. } => (false, selection, known_state),
            proto::NodeRequestBody::SubscribeQuery { selection, known_state, .. } => (true, selection, known_state),
            _ => unreachable!(),
        };
        assert_eq!(selection, retained_selection, "the wire account and retained tree use the same exact selection");
        assert_eq!(known_state.digest.count(), 1);
        summaries.push((subscription, known_state));
    }
    assert_eq!(summaries.len(), 2);
    assert!(!summaries[0].0 && summaries[1].0);
    assert_eq!(summaries[0].1, summaries[1].1, "both request paths describe the same allowed members and heads");
    hold_updates.store(true, Ordering::SeqCst);
    for (id, changes_summary) in [(hidden, false), (visible, true)] {
        let view = writer.get::<PetView>(id).await?;
        let trx = writer.begin();
        trx.edit::<Pet>(view.entity())?.name()?.insert(0, "Changed ")?;
        trx.commit().await?;
        // Apply the peer's new state, then prepare another account of local state.
        reader.fetch::<PetView>("true").await?;
        agent.sent_queries.lock().unwrap().clear();
        reader.fetch::<PetView>("true").await?;
        let encoded = agent.sent_queries.lock().unwrap().pop().unwrap();
        let proto::NodeRequestBody::Fetch { known_state, .. } = bincode::deserialize(&encoded)? else { panic!("expected fetch") };
        assert_eq!(known_state.digest.count(), 1);
        assert_eq!(known_state != summaries[0].1, changes_summary, "only an allowed member's changed head changes its digest");
    }
    let repairs = agent.sent_repairs.lock().unwrap();
    let hidden_repairs: Vec<_> = repairs.iter().flatten().filter(|member| member.known.entity_id == hidden).collect();
    assert!(!hidden_repairs.is_empty(), "the changed hidden member exercises repair sanitation");
    assert!(hidden_repairs.iter().all(|member| member.known.head.is_empty()), "a denied local head is never disclosed in a repair request");
    drop(repairs);
    hold_updates.store(false, Ordering::SeqCst);
    gate.release_held(&client).await;

    // A relay retry keeps the installed selection, but policy may have narrowed.
    // Its digest must not disclose a formerly allowed member on reconnect.
    drop(connection);
    for connection in client.connections_of(server.id().unwrap()) {
        client.deregister_peer(connection);
    }
    for connection in server.connections_of(client.id().unwrap()) {
        server.deregister_peer(connection);
    }
    let missing_property = Predicate::Comparison {
        left: Box::new(ankql::ast::Expr::Path(proto::PropertyId::EntityId(proto::EntityId::random()).into())),
        operator: ankql::ast::ComparisonOperator::Equal,
        right: Box::new(ankql::ast::Expr::Literal("required".into())),
    };
    for restriction in
        [client.catalog.resolve_selection(&model, ankql::parser::parse_selection("name = 'Outside'")?)?.predicate, missing_property]
    {
        *agent.query_filter.lock().unwrap() = restriction;
        agent.sent_queries.lock().unwrap().clear();
        let _connection = LocalProcessConnection::new(&client, &server).await?;
        let (selection, known_state) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                for bytes in std::mem::take(&mut *agent.sent_queries.lock().unwrap()) {
                    if let proto::NodeRequestBody::SubscribeQuery { query_id, selection, known_state, .. } =
                        bincode::deserialize(&bytes).unwrap()
                    {
                        if query_id == query.query_id() {
                            return (selection, known_state);
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(selection, retained_selection, "a reconnect does not redefine the installed query");
        assert_eq!(known_state, proto::KnownState::empty());
    }
    Ok(())
}

#[tokio::test]
async fn matching_limited_known_state_skips_pages_for_fetch_and_subscription() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let writer = server.context_async(DEFAULT_CONTEXT).await?;
    let trx = writer.begin();
    let mut ids = Vec::new();
    for name in ["A", "B", "C"] {
        ids.push(trx.create(&Pet { name: name.into(), age: "3".into() }).await?.id());
    }
    trx.commit().await?;
    let agent = BridgePolicyAgent::new();
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    reader.fetch::<PetView>("true").await?;
    agent.sent_queries.lock().unwrap().clear();
    reader.fetch::<PetView>("true ORDER BY name ASC LIMIT 2").await?;
    let request = agent.sent_queries.lock().unwrap().pop().unwrap();
    let proto::NodeRequestBody::Fetch { selection, known_state, key_spec, .. } = bincode::deserialize(&request)? else {
        panic!("expected fetch");
    };
    assert_eq!(known_state.digest.count(), 2);

    for subscription in [false, true] {
        for summary in [known_state.clone(), proto::KnownState::empty()] {
            let equal = summary == known_state;
            let comparison = proto::ComparisonId::new();
            let request = if subscription {
                proto::NodeRequestBody::SubscribeQuery {
                    query_id: proto::QueryId::new(),
                    selection: selection.clone(),
                    version: 1,
                    comparison,
                    key_spec: key_spec.clone(),
                    known_state: summary,
                }
            } else {
                proto::NodeRequestBody::Fetch { selection: selection.clone(), comparison, key_spec: key_spec.clone(), known_state: summary }
            };
            let response = client.request(server.id().unwrap(), &DEFAULT_CONTEXT, request).await?;
            let target = match response {
                proto::NodeResponseBody::Fetch(target) | proto::NodeResponseBody::QuerySubscribed { target, .. } => target,
                response => panic!("unexpected response: {response:?}"),
            };
            assert_eq!(target.known_state, known_state);
            if equal {
                assert!(target.page.is_none(), "equal digests need no pages or entity inventory");
            } else {
                let Some(ankurah_digest_tree::Page::Members(members)) = target.page else { panic!("small unequal target has leaves") };
                assert_eq!(members.iter().map(|member| member.id).collect::<Vec<_>>(), ids[..2]);
            }
            server
                .handle_message(
                    server.connections_of(client.id().unwrap())[0],
                    proto::NodeMessage::ReleaseComparison { from: client.id().unwrap(), comparison },
                )
                .await?;
        }
    }
    Ok(())
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

    let client_agent = BridgePolicyAgent::new();
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent.clone());
    let mut connection = Some(LocalProcessConnection::new(&client, &server).await?);
    client.wait_ready().await?;
    let credentials = Vec::<&'static DefaultContext>::new();

    for label in [MODEL_COLLECTION_ID, PROPERTY_COLLECTION_ID, MODEL_PROPERTY_COLLECTION_ID] {
        let model = ankurah::core::schema::system_model_id(label).unwrap();
        let storage = &server.storage;
        let states = storage.fetch_states(&Predicate::MemberOf(model).into()).await?;
        assert!(!states.is_empty(), "registration must populate {label}");
        let mut state = states[0].clone();
        let entity_id = state.payload.entity_id;
        let known_head = state.payload.state.head.clone();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if client.storage.get_state(entity_id).await.is_ok_and(|state| state.payload.state.head == known_head) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let validated = client_agent.validate_calls.load(Ordering::SeqCst);
        // Advance the stored row with an idempotent membership event to exercise a real bridge.
        let update = proto::Event::update(
            entity_id,
            known_head.clone(),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(
                ankurah::core::schema::system_model_id(label).unwrap(),
            ))]),
        );
        state.payload.state.head = proto::Clock::singleton(&update);
        let update_id = update.id();
        let mut transaction = storage.transaction();
        transaction.set_state(&known_head, &state).await?;
        transaction.add_events(&[Attested::opt(update, None)]).await?;
        assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
        let events = storage.dump_entity_events(entity_id).await?;
        assert!(!events.is_empty());

        for request in [
            proto::NodeRequestBody::Get { ids: vec![entity_id] },
            proto::NodeRequestBody::GetEvents { event_ids: events.iter().map(|event| event.payload.id()).collect() },
        ] {
            match client.request(server.id().unwrap(), &credentials, request).await? {
                proto::NodeResponseBody::Get(results) => assert!(matches!(results.as_slice(), [proto::GetResult::Found(_)])),
                proto::NodeResponseBody::GetEvents(received) => assert_eq!(received.len(), events.len()),
                response => panic!("catalog read failed for {label}: {response:?}"),
            }
        }
        for subscription in [false, true] {
            let comparison = proto::ComparisonId::new();
            let selection = Predicate::MemberOf(model).into();
            let key_spec = ankurah_core_types::key_spec::KeySpec::new(vec![]);
            let request = if subscription {
                proto::NodeRequestBody::SubscribeQuery {
                    query_id: proto::QueryId::new(),
                    selection,
                    version: 1,
                    comparison,
                    key_spec,
                    known_state: proto::KnownState::empty(),
                }
            } else {
                proto::NodeRequestBody::Fetch { selection, comparison, key_spec, known_state: proto::KnownState::empty() }
            };
            let target = match client.request(server.id().unwrap(), &credentials, request).await? {
                proto::NodeResponseBody::Fetch(target) | proto::NodeResponseBody::QuerySubscribed { target, .. } => target,
                response => panic!("catalog capture failed for {label}: {response:?}"),
            };
            assert_eq!(target.known_state.digest.count() as usize, states.len());
            server
                .handle_message(
                    server.connections_of(client.id().unwrap())[0],
                    proto::NodeMessage::ReleaseComparison { from: client.id().unwrap(), comparison },
                )
                .await?;
        }

        // Reconnect the standing catalog queries through the complete comparison
        // path. An earlier in-flight comparison may already have repaired this
        // update; either way the existing head requires validated event repair.
        drop(connection.take());
        connection = Some(LocalProcessConnection::new(&client, &server).await?);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if client.storage.get_state(entity_id).await?.payload.state.head == state.payload.state.head {
                    break Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        assert!(client_agent.validate_calls.load(Ordering::SeqCst) > validated, "catalog bridge events pass receive validation");
        assert!(client.storage.dump_entity_events(entity_id).await?.iter().any(|event| event.payload.id() == update_id));
    }

    for model in [
        context.resolve_model_id::<Pet>().await?,
        proto::ModelId::System(proto::SystemModel::System),
        proto::ModelId::EntityId(proto::EntityId::random()),
    ] {
        let response = client
            .request(
                server.id().unwrap(),
                &DEFAULT_CONTEXT,
                proto::NodeRequestBody::Fetch {
                    selection: Predicate::MemberOf(model).into(),
                    comparison: proto::ComparisonId::new(),
                    key_spec: ankurah_core_types::key_spec::KeySpec::new(vec![]),
                    known_state: proto::KnownState::empty(),
                },
            )
            .await?;
        assert!(matches!(&response, proto::NodeResponseBody::Error(message) if message == "access denied"), "{response:?}");
    }
    Ok(())
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
/// A redacted causal history is unavailable for repair. The client must report
/// incomplete comparison and retain its last verified version.
#[tokio::test]
async fn test_event_bridge_respects_read_policy_on_send() -> Result<()> {
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

    // The bridge would need the denied event, so the whole repair is unavailable.
    let refetch = ctx_c.fetch::<PetView>(query.as_str()).await;
    assert!(refetch.is_err(), "client must not silently adopt state whose lineage it cannot verify, got {refetch:?}");

    // The security property: the hidden event never reached the client, and
    // neither did the rest of the redacted window (a partial chain would
    // lose operations). The client's view of the entity is stale but honest.
    let ids: HashSet<_> = client.storage.dump_entity_events(pet_id).await?.iter().map(|e| e.payload.id()).collect();
    assert!(!ids.contains(&denied_id), "the read-denied event must not reach the client through any path");
    assert!(!ids.contains(&open_id), "a refused repair does not send a partial bridge");
    assert_eq!(initial[0].age().unwrap(), "1", "the resident view remains at its last verified state");

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
        .request(
            server.id().unwrap(),
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::GetEvents { event_ids: vec![pet_event.id(), album_event.id()] },
        )
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
        .request(
            server.id().unwrap(),
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::GetEvents { event_ids: vec![pet_event.id(), album_event.id()] },
        )
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
            server.id().unwrap(),
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
