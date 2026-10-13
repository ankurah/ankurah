//! Initial clock answers and live connecting-history requests use their stream's credentials.
mod common;

use ankurah::{
    core::{
        entity::Entity,
        error::ValidationError,
        node::{ContextData, NodeInner},
        util::Iterable,
    },
    policy::{AccessDenied, PolicyAgent},
    proto::{self, Attested},
    Node,
};
use anyhow::Result;
use common::{GatedConnection, Peek, Record, RecordView, SledStorageEngine, StorageEngine, Subscribe, TestWatcher};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Credential(u8);
impl ContextData for Credential {}

#[derive(Clone, Default)]
struct CredentialAgent {
    subscriptions: Arc<Mutex<Vec<(Credential, proto::QueryId, proto::RequestId)>>>,
    event_requests: Arc<Mutex<Vec<(Vec<proto::EventId>, Vec<Credential>)>>>,
    initial_inventories: Arc<Mutex<Vec<(proto::QueryId, Option<proto::KnownState>)>>>,
    feedback: Arc<Mutex<Vec<(Vec<Credential>, proto::NodeRequest)>>>,
    feedback_arrived: Arc<tokio::sync::Notify>,
    predicate_denied: Arc<Mutex<HashSet<proto::EntityId>>>,
    hook_denied: Arc<Mutex<HashSet<proto::EntityId>>>,
    read_checks: Arc<Mutex<Vec<(Vec<Credential>, proto::EntityId)>>>,
}

fn alpha_only<C: Iterable<Credential>>(credentials: &C) -> bool {
    credentials.iterable().any(|credential| credential.0 == 1) && credentials.iterable().all(|credential| credential.0 == 1)
}

#[async_trait::async_trait]
impl PolicyAgent for CredentialAgent {
    type ContextData = Credential;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        credentials: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Credential>,
    {
        Ok(credentials.iterable().map(|credential| proto::AuthData(vec![credential.0])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &Node<SE, Self>,
        auth: &A,
        request: &proto::NodeRequest,
    ) -> Result<Vec<Credential>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        let credentials = auth.iterable().map(|auth| Credential(auth.0.first().copied().unwrap_or(0))).collect::<Vec<_>>();
        match &request.body {
            proto::NodeRequestBody::SubscribeQuery { query_id, known_state, .. } => {
                self.subscriptions
                    .lock()
                    .unwrap()
                    .extend(credentials.iter().map(|credential| (credential.clone(), *query_id, request.id.clone())));
                self.initial_inventories.lock().unwrap().push((*query_id, known_state.clone()));
            }
            proto::NodeRequestBody::GetEvents { event_ids } => {
                self.event_requests.lock().unwrap().push((event_ids.clone(), credentials.clone()));
            }
            proto::NodeRequestBody::ContinueState { .. } => {
                let request = bincode::deserialize(&bincode::serialize(request).unwrap()).unwrap();
                self.feedback.lock().unwrap().push((credentials.clone(), request));
                self.feedback_arrived.notify_one();
            }
            _ => {}
        }
        Ok(credentials)
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _credential: &Credential,
        _before: &Entity,
        _after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer: &proto::EntityId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &Node<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer: &proto::EntityId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C>(&self, credentials: &C) -> Result<ankql::ast::Predicate<ankql::ast::Resolved>, AccessDenied>
    where C: Iterable<Credential> {
        use ankql::ast::{ComparisonOperator, Expr, Predicate, PropertyPath, Value};
        if credentials.iterable().next().is_none() {
            return Ok(Predicate::False);
        }
        let mut predicate = Predicate::True;
        if alpha_only(credentials) {
            for id in self.predicate_denied.lock().unwrap().iter() {
                predicate = Predicate::And(
                    Box::new(predicate),
                    Box::new(Predicate::Comparison {
                        left: Box::new(Expr::Path(PropertyPath::id())),
                        operator: ComparisonOperator::NotEqual,
                        right: Box::new(Expr::Literal(Value::EntityId(*id))),
                    }),
                );
            }
        }
        Ok(predicate)
    }

    fn check_reads<C>(
        &self,
        credentials: &C,
        states: &[(&proto::EntityId, &proto::State)],
    ) -> std::collections::HashMap<proto::EntityId, AccessDenied>
    where
        C: Iterable<Credential>,
    {
        let captured = credentials.iterable().cloned().collect::<Vec<_>>();
        self.read_checks.lock().unwrap().extend(states.iter().map(|(id, _)| (captured.clone(), **id)));
        let denied = self.hook_denied.lock().unwrap();
        states
            .iter()
            .filter(|(id, _)| alpha_only(credentials) && denied.contains(id))
            .map(|(id, _)| (**id, AccessDenied::ByPolicy("state denied to alpha")))
            .collect()
    }

    fn check_write(&self, _credential: &Credential, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer: &proto::EntityId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

#[tokio::test]
async fn live_connecting_history_uses_the_stream_credential_instead_of_the_peer_union() -> Result<()> {
    let agent = CredentialAgent::default();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let writer = server.context_async(Credential(0)).await?;
    let transaction = writer.begin();
    let id = transaction.create(&Record { title: "initial".into(), artist: "artist".into() }).await?.id();
    transaction.commit().await?;

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let frames = Arc::new(Mutex::new(Vec::<(proto::RequestId, proto::StateFrame)>::new()));
    let arrived = Arc::new(tokio::sync::Notify::new());
    let (_connection, _gate) = GatedConnection::new(&server, &client, {
        let frames = frames.clone();
        let arrived = arrived.clone();
        move |message| {
            if let proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { request_id, frame }, .. }) =
                message
            {
                if let proto::StateFrameBody::Live(items) = &frame.body {
                    if items.iter().any(|item| item.delta.entity_id == id) {
                        frames.lock().unwrap().push((request_id.clone(), frame.clone()));
                        arrived.notify_one();
                        return true;
                    }
                }
            }
            false
        }
    })
    .await;
    client.wait_ready().await?;

    let alpha = client.context_async(Credential(1)).await?;
    let beta = client.context_async(Credential(2)).await?;
    let target = alpha.query_wait::<RecordView>(common::nocache(format!("id = '{id}'"))?).await?;
    target.wait_durable_answered().await?;
    let unrelated = beta.query_wait::<RecordView>(common::nocache("title = 'unrelated'")?).await?;
    unrelated.wait_durable_answered().await?;
    assert!(unrelated.ids().is_empty());
    assert_eq!(target.ids(), vec![id]);
    let subscriptions = agent.subscriptions.lock().unwrap().clone();
    let (_, alpha_query, alpha_request) = subscriptions.iter().find(|(credential, _, _)| *credential == Credential(1)).unwrap();
    let (_, beta_query, beta_request) = subscriptions.iter().find(|(credential, _, _)| *credential == Credential(2)).unwrap();
    assert_ne!(alpha_query, beta_query, "both credentials must have distinct real standing queries");
    assert_ne!(alpha_request, beta_request);

    let watcher = TestWatcher::changeset();
    let _guard = target.subscribe(&watcher);
    let view = writer.get::<RecordView>(id).await?;
    let first = writer.begin();
    view.edit(&first)?.title()?.set(&"connecting".into())?;
    let parent = first.commit_and_return_events().await?.remove(0);
    let second = writer.begin();
    view.edit(&second)?.title()?.set(&"final".into())?;
    let child = second.commit_and_return_events().await?.remove(0);
    assert_eq!(child.parent, proto::Clock::singleton(&parent));
    let target_head = proto::Clock::singleton(&child);
    assert!(client.storage.get_events(vec![parent.id()]).await?.is_empty(), "the connecting parent must be missing locally");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let notified = arrived.notified();
            if !frames.lock().unwrap().is_empty() {
                break;
            }
            notified.await;
        }
    })
    .await?;
    let (request_id, mut frame) = frames.lock().unwrap()[0].clone();
    assert_eq!(&request_id, alpha_request, "the held live frame must belong to the target query's stream");
    let state = server.storage.get_state(id).await?;
    assert_eq!(state.payload.state.head, target_head);
    // Replace a held live frame with an honest target whose carried child
    // needs the omitted parent. The real receiver must fetch that history.
    frame.body = proto::StateFrameBody::Live(vec![proto::SubscriptionUpdateItem {
        delta: proto::EntityDelta {
            entity_id: id,
            content: proto::DeltaContent::StateAndEvents { state: state.into(), events: vec![Attested::opt(child, None).into()] },
        },
        predicate_relevance: vec![],
    }]);
    client
        .handle_message(proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: server.id,
            to: client.id,
            body: proto::NodeUpdateBody::StateFrame { request_id, frame },
        }))
        .await?;
    tokio::time::timeout(Duration::from_secs(5), watcher.take(1)).await??;
    assert_eq!(target.ids(), vec![id]);
    assert_eq!(target.peek()[0].title()?, "final");
    assert_eq!(client.storage.get_state(id).await?.payload.state.head, target_head);
    let history = agent.event_requests.lock().unwrap().iter().filter(|(ids, _)| ids.contains(&parent.id())).cloned().collect::<Vec<_>>();
    assert!(!history.is_empty(), "the live receiver must actually request the omitted connecting event");
    for (_, credentials) in history {
        assert_eq!(credentials, vec![Credential(1)], "GetEvents must use the captured stream credential, not another query's credentials");
    }
    Ok(())
}

#[tokio::test]
async fn initial_clock_questions_use_the_stream_identity_read_permission() -> Result<()> {
    let agent = CredentialAgent::default();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let writer = server.context_async(Credential(0)).await?;
    let trx = writer.begin();
    let mut ids = Vec::new();
    for title in ["captured", "predicate-denied", "hook-denied", "off-query"] {
        ids.push(trx.create(&Record { title: title.into(), artist: "artist".into() }).await?.id());
    }
    trx.commit().await?;
    let [captured, predicate_denied, hook_denied, off_query] = ids.as_slice() else { unreachable!() };
    let (captured, predicate_denied, hook_denied, off_query) = (*captured, *predicate_denied, *hook_denied, *off_query);
    agent.predicate_denied.lock().unwrap().insert(predicate_denied);
    agent.hook_denied.lock().unwrap().insert(hook_denied);

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let metadata = Arc::new(Mutex::new(Vec::<proto::NodeResponse>::new()));
    let metadata_arrived = Arc::new(tokio::sync::Notify::new());
    let (_connection, _gate) = GatedConnection::new(&server, &client, {
        let agent = agent.clone();
        let metadata = metadata.clone();
        let metadata_arrived = metadata_arrived.clone();
        move |message| {
            if let proto::NodeMessage::Response(response) = message {
                if let proto::NodeResponseBody::QuerySubscribed { query_id, .. } = &response.body {
                    if agent.subscriptions.lock().unwrap().iter().any(|(credential, id, _)| credential.0 == 1 && id == query_id) {
                        metadata.lock().unwrap().push(bincode::deserialize(&bincode::serialize(response).unwrap()).unwrap());
                        metadata_arrived.notify_one();
                        return true;
                    }
                }
            }
            false
        }
    })
    .await;
    client.wait_ready().await?;
    let beta = client.context_async(Credential(2)).await?;
    assert_eq!(beta.fetch::<RecordView>("true").await?.len(), 4);
    let broader = beta.query_wait::<RecordView>(common::nocache("true")?).await?;
    broader.wait_durable_answered().await?;
    let mut actual_heads = BTreeMap::new();
    for id in &ids {
        actual_heads.insert(*id, client.storage.get_state(*id).await?.payload.state.head);
    }
    let alpha = client.context_async(Credential(1)).await?;
    for id in [predicate_denied, hook_denied] {
        assert!(matches!(alpha.get_cached::<RecordView>(id).await, Err(ankurah::error::RetrievalError::AccessDenied(_))));
    }
    let query = alpha.query::<RecordView>(common::nocache(format!("id = '{captured}'"))?)?;
    let mut response = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let notified = metadata_arrived.notified();
            if let Some(response) = metadata.lock().unwrap().iter().find(|response| {
                matches!(&response.body, proto::NodeResponseBody::QuerySubscribed { query_id, .. } if *query_id == query.query_id())
            }) {
                break bincode::deserialize::<proto::NodeResponse>(&bincode::serialize(response).unwrap()).unwrap();
            }
            notified.await;
        }
    })
    .await?;
    let inventory = agent.initial_inventories.lock().unwrap().iter().find(|(id, _)| *id == query.query_id()).unwrap().1.clone().unwrap();
    let offered = inventory
        .regions
        .iter()
        .flat_map(|region| match region {
            proto::KnownRegion::Leaves(page) => page.leaves.iter().map(|leaf| leaf.entity.entity_id).collect::<Vec<_>>(),
            _ => panic!("one captured row should offer its actual head inline"),
        })
        .collect::<Vec<_>>();
    assert_eq!(offered, [captured], "the question's other IDs must be outside the captured query");
    let proto::NodeResponseBody::QuerySubscribed { state, .. } = &mut response.body else { unreachable!() };
    let comparison = state.frame.comparison;
    let missing = proto::EntityId::random();
    let proto::StateFrameBody::State { items, .. } = &mut state.frame.body else { unreachable!() };
    items.push(proto::StateItem::NeedHeads(vec![captured, predicate_denied, hook_denied, off_query, missing]));
    // A custom responder can ask this in valid metadata, though the stock
    // producer never asks it. Inspect the real outbound answer before that
    // producer rejects the unsolicited control; do not claim activation.
    let message = proto::NodeMessage::Response(response);
    client.handle_message(bincode::deserialize(&bincode::serialize(&message)?)?).await?;
    let (credentials, known, unknown) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let notified = agent.feedback_arrived.notified();
            let answer = agent.feedback.lock().unwrap().iter().find_map(|(credentials, request)| {
                let proto::NodeRequestBody::ContinueState { comparison: id, answers, .. } = &request.body else { return None };
                if *id != comparison {
                    return None;
                }
                answers.iter().find_map(|answer| match answer {
                    proto::StateAnswer::Heads { known, unknown } => Some((credentials.clone(), known.clone(), unknown.clone())),
                    _ => None,
                })
            });
            if let Some(answer) = answer {
                break answer;
            }
            notified.await;
        }
    })
    .await?;
    assert_eq!(credentials, [Credential(1)], "another standing query must not widen this stream's read authority");
    assert!(agent.subscriptions.lock().unwrap().iter().any(|(credential, id, _)| credential.0 == 2 && *id == broader.query_id()));
    drop(query);
    let known = known.into_iter().map(|entity| (entity.entity_id, entity.head)).collect::<BTreeMap<_, _>>();
    assert_eq!(
        known,
        BTreeMap::from([(captured, actual_heads[&captured].clone()), (off_query, actual_heads[&off_query].clone())]),
        "only captured and authorized off-query clocks may leave the client"
    );
    assert_eq!(unknown.into_iter().collect::<BTreeSet<_>>(), BTreeSet::from([predicate_denied, hook_denied, missing]));
    assert!(agent.read_checks.lock().unwrap().iter().any(|(credentials, id)| *id == off_query && *credentials == [Credential(1)]));
    Ok(())
}

#[tokio::test]
async fn credentialless_identity_read_check_preserves_the_catalog_bypass() -> Result<()> {
    use ankurah::core::{
        policy::ContextPolicy,
        schema::{system_model_id, MODEL_COLLECTION_ID},
    };

    let agent = CredentialAgent::default();
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    node.system.create().await?;
    let writer = node.context_async(Credential(0)).await?;
    let trx = writer.begin();
    let private_id = trx.create(&Record { title: "private".into(), artist: "artist".into() }).await?.id();
    trx.commit().await?;
    let proto::ModelId::EntityId(catalog_id) = writer.resolve_model_id::<Record>().await? else { unreachable!() };
    let catalog = node.storage.get_state(catalog_id).await?;
    let private = node.storage.get_state(private_id).await?;
    assert!(catalog.payload.state.memberships.iter().any(|model| *model == system_model_id(MODEL_COLLECTION_ID).unwrap()));

    let credentials = Vec::<Credential>::new();
    let policy = ContextPolicy::from_credentials(&agent, &credentials);
    assert_eq!(agent.retrieval_predicate(&credentials)?, ankql::ast::Predicate::False, "the agent grants no credentialless private reads");
    assert!(policy.check_read(&private_id, &private.payload.state).is_err());
    assert!(
        policy.check_read(&catalog_id, &catalog.payload.state).is_ok(),
        "the added identity-read check must preserve catalog bootstrap"
    );
    // The NeedHeads catalog-membership guard remains independently required:
    // this proves check_read's credentialless contract, not a wire companion.
    Ok(())
}
