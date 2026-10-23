//! Initial causal repair and live connecting history use their stream's credentials.
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
    Mutable, Node, View,
};
use anyhow::{Context as _, Result};
use common::{GatedConnection, Peek, Record, RecordView, SledStorageEngine, StorageEngine, Subscribe, TestWatcher};
use std::{
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
    feedback: Arc<Mutex<Vec<(Vec<Credential>, proto::NodeRequest)>>>,
    feedback_arrived: Arc<tokio::sync::Notify>,
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
            proto::NodeRequestBody::SubscribeQuery { query_id, .. } => {
                self.subscriptions
                    .lock()
                    .unwrap()
                    .extend(credentials.iter().map(|credential| (credential.clone(), *query_id, request.id.clone())));
            }
            proto::NodeRequestBody::GetEvents { event_ids } => {
                self.event_requests.lock().unwrap().push((event_ids.clone(), credentials.clone()));
            }
            proto::NodeRequestBody::StateFeedback { .. } | proto::NodeRequestBody::FinishStateRound { .. } => {
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
        Ok(if credentials.iterable().next().is_some() { ankql::ast::Predicate::True } else { ankql::ast::Predicate::False })
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
            if let proto::NodeMessage::StateFrame { request_id, frame, .. } = message {
                if let proto::StateFrameBody::Live { items, .. } = &frame.body {
                    if items.iter().any(|item| item.entity_id == id) {
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
    let (revision, mut member) = match &frame.body {
        proto::StateFrameBody::Live { revision, items, .. } => {
            (*revision, items.iter().find(|item| item.entity_id == id).unwrap().member.clone().expect("matching row remains present"))
        }
        _ => unreachable!(),
    };
    member.head = target_head.clone();
    frame.body = proto::StateFrameBody::Live {
        revision,
        items: vec![proto::QueryChange {
            entity_id: id,
            member: Some(member),
            delta: Some(proto::DeltaContent::StateAndEvents { state: state.into(), events: vec![Attested::opt(child, None).into()] }),
        }],
        complete: true,
    };
    client.handle_message(proto::NodeMessage::StateFrame { from: server.id, to: client.id, request_id, frame }).await?;
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
async fn initial_snapshot_repair_and_application_use_the_stream_credential() -> Result<()> {
    let agent = CredentialAgent::default();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let writer = server.context_async(Credential(0)).await?;
    let trx = writer.begin();
    let record = trx.create(&Record { title: "base".into(), artist: "base".into() }).await?.read()?;
    trx.commit().await?;
    let id = record.id();
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let metadata = Arc::new(Mutex::new(Vec::<proto::NodeResponse>::new()));
    let metadata_arrived = Arc::new(tokio::sync::Notify::new());
    let (_connection, _gate) = GatedConnection::new(&server, &client, {
        let agent = agent.clone();
        let metadata = metadata.clone();
        let metadata_arrived = metadata_arrived.clone();
        move |message| {
            match message {
                proto::NodeMessage::Response(response) => {
                    if let proto::NodeResponseBody::QuerySubscribed { query_id, .. } = &response.body {
                        if agent.subscriptions.lock().unwrap().iter().any(|(credential, query, _)| credential.0 == 1 && query == query_id) {
                            metadata.lock().unwrap().push(bincode::deserialize(&bincode::serialize(response).unwrap()).unwrap());
                            metadata_arrived.notify_one();
                            return true;
                        }
                    }
                }
                proto::NodeMessage::StateFrame { request_id, frame, .. } => {
                    // Keep the existing beta query at the base head while alpha
                    // captures a different branch. These are genuine producer
                    // frames; no response or event is forged for the test.
                    if matches!(&frame.body, proto::StateFrameBody::Live { items, .. } if items.iter().any(|item| item.entity_id == id))
                        && agent
                            .subscriptions
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|(credential, _, origin)| credential.0 == 2 && origin == request_id)
                    {
                        return true;
                    }
                }
                _ => {}
            }
            false
        }
    })
    .await;
    client.wait_ready().await?;
    let beta = client.context_async(Credential(2)).await?;
    let broader = beta.query_wait::<RecordView>(common::nocache("true")?).await?;
    broader.wait_durable_answered().await?;
    assert_eq!(broader.ids(), [id]);
    let cached = broader.peek()[0].clone();
    let local = beta.begin();
    cached.edit(&local)?.artist()?.set(&"local-artist".to_owned())?;

    // Repeated large title edits make the actual producer prefer its compact
    // snapshot over the old-to-new event bridge. The prepared local edit still
    // descends from base, and all beta live deliveries are held.
    let mut remote_title = String::new();
    for edit in 0..8 {
        remote_title = format!("{}-{edit}", "x".repeat(8192));
        let trx = writer.begin();
        record.edit(&trx)?.title()?.set(&remote_title)?;
        trx.commit().await?;
    }
    let captured_target = record.entity().head();
    assert_eq!(cached.title()?, "base");
    let alpha = client.context_async(Credential(1)).await?;
    let query = alpha.query::<RecordView>(common::nocache(format!("id = '{id}'"))?)?;
    let response = tokio::time::timeout(Duration::from_secs(5), async {
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
    let proto::NodeResponseBody::QuerySubscribed { state, .. } = &response.body else { unreachable!() };
    let comparison = state.frame.stream;
    let proto::StateFrameBody::Round { items, .. } = &state.frame.body else { unreachable!() };
    assert!(
        items.iter().any(|item| matches!(item, proto::StateItem::Repair { target, delta }
        if delta.entity_id == id && *target == captured_target && matches!(delta.content, proto::DeltaContent::StateSnapshot { .. }))),
        "the stock initial producer must offer the exact captured snapshot"
    );
    local.commit().await?;
    let local_head = cached.entity().head();
    assert!(
        captured_target.entries().iter().all(|event| !local_head.entries().contains(event)),
        "the held sender target must be uncovered"
    );
    assert_eq!(cached.artist()?, "local-artist");
    assert_eq!(cached.title()?, "base");
    assert!(!query.loaded());
    // Deliver only alpha's unchanged genuine metadata. Releasing the whole
    // gate would allow beta's later live frames to conceal missing repair.
    client.handle_message(proto::NodeMessage::Response(response)).await?;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    assert_eq!(query.ids(), [id]);
    assert_eq!(query.peek()[0].title()?, remote_title);
    assert_eq!(query.peek()[0].artist()?, "local-artist");
    let applied = query.peek()[0].entity().head();
    assert!(captured_target.entries().iter().all(|event| applied.entries().contains(event)));
    assert!(local_head.entries().iter().all(|event| applied.entries().contains(event)));
    assert_eq!(client.storage.get_state(id).await?.payload.state.head, applied);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let notified = agent.feedback_arrived.notified();
            if agent.feedback.lock().unwrap().iter().any(|(_, request)| matches!(request.body,
                proto::NodeRequestBody::FinishStateRound { stream: found, outcome: proto::StateRoundOutcome::Applied { .. }, .. } if found == comparison)) {
                break;
            }
            notified.await;
        }
    }).await?;
    let feedback = agent.feedback.lock().unwrap();
    assert!(
        feedback.iter().any(|(_, request)| matches!(&request.body,
        proto::NodeRequestBody::StateFeedback { stream: found, answers, .. } if *found == comparison && answers.iter().any(|answer|
            matches!(answer, proto::StateAnswer::NeedRepair { entity_id, target } if *entity_id == id && *target == captured_target)))),
        "the uncovered initial snapshot must actually request connecting history"
    );
    for (credentials, request) in feedback.iter().filter(|(_, request)| {
        matches!(request.body,
        proto::NodeRequestBody::StateFeedback { stream: found, .. } | proto::NodeRequestBody::FinishStateRound { stream: found, .. } if found == comparison)
    }) {
        assert_eq!(
            *credentials,
            [Credential(1)],
            "initial repair and Applied must retain alpha authority despite the beta standing query: {request:?}"
        );
    }
    assert!(agent.subscriptions.lock().unwrap().iter().any(|(credential, query, _)| credential.0 == 2 && *query == broader.query_id()));
    Ok(())
}

#[tokio::test]
async fn cancelled_fetch_abandons_after_its_context_is_gone_and_preserves_another_query() -> Result<()> {
    let agent = CredentialAgent::default();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let writer = server.context_async(Credential(0)).await?;
    let trx = writer.begin();
    let fetched_id = trx.create(&Record { title: "fetch-only".into(), artist: "initial".into() }).await?.id();
    let standing = trx.create(&Record { title: "standing".into(), artist: "initial".into() }).await?.read()?;
    trx.commit().await?;

    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    let metadata = Arc::new(Mutex::new(None::<proto::NodeResponse>));
    let response_arrived = Arc::new(tokio::sync::Notify::new());
    let (_connection, gate) = GatedConnection::new(&server, &client, {
        let metadata = metadata.clone();
        let response_arrived = response_arrived.clone();
        move |message| {
            if let proto::NodeMessage::Response(response) = message {
                let hold = matches!(&response.body, proto::NodeResponseBody::Fetch(_));
                if hold {
                    *metadata.lock().unwrap() = Some(bincode::deserialize(&bincode::serialize(response).unwrap()).unwrap());
                }
                response_arrived.notify_one();
                return hold;
            }
            false
        }
    })
    .await;
    client.wait_ready().await?;
    let beta = client.context_async(Credential(2)).await?;
    let query = beta.query_wait::<RecordView>(common::nocache("title = 'standing'")?).await?;
    query.wait_durable_answered().await?;
    assert_eq!(query.ids(), [standing.id()]);
    let subscription_requests = agent.subscriptions.lock().unwrap().iter().filter(|(_, id, _)| *id == query.query_id()).count();
    assert_eq!(subscription_requests, 1, "the other credential must own an established standing stream");

    let alpha = client.context_async(Credential(1)).await?;
    let mut fetch = Box::pin(alpha.fetch::<RecordView>("title = 'fetch-only'"));
    let response = tokio::select! {
        result = &mut fetch => panic!("Fetch completed before its held metadata arrived: {result:?}"),
        response = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = response_arrived.notified();
                if let Some(response) = metadata.lock().unwrap().take() {
                    break response;
                }
                notified.await;
            }
        }) => response?,
    };
    let proto::NodeResponseBody::Fetch(state) = &response.body else { unreachable!() };
    let comparison = state.frame.stream;
    drop(fetch);
    drop(alpha);
    assert!(client.storage.get_state(fetched_id).await.is_err(), "the cancelled Fetch must not have installed its held result");

    // The late response reveals the responder-issued handle only after the
    // requesting context is gone. Cleanup needs only this peer and stream.
    gate.release_held(&client).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // A deliberately invalid receipt distinguishes active from retired;
            // abandonment itself is one-way and has no response to await.
            let result = client
                .request(
                    server.id,
                    &vec![Credential(1)],
                    proto::NodeRequestBody::StateFeedback {
                        stream: comparison,
                        round: Some(0),
                        processed_through: u64::MAX,
                        applied_through: None,
                        answers: vec![],
                    },
                )
                .await?;
            if matches!(result, proto::NodeResponseBody::StateStreamUnavailable) {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;

    // A duplicate of the genuine initial metadata must neither resurrect the
    // cancelled Fetch nor abandon a different stream on this connection.
    client.handle_message(proto::NodeMessage::Response(response)).await?;
    ankurah::core::test_helpers::expire_state_stream(&server, client.id, comparison).await?;
    let watcher = TestWatcher::changeset();
    let _guard = query.subscribe(&watcher);
    let trx = writer.begin();
    standing.edit(&trx)?.artist()?.set(&"still live".into())?;
    trx.commit().await?;
    tokio::time::timeout(Duration::from_secs(5), watcher.take(1)).await??;
    assert_eq!(query.ids(), [standing.id()]);
    assert_eq!(query.peek()[0].artist()?, "still live");
    assert_eq!(client.storage.get_state(standing.id()).await?.payload.state.head, standing.entity().head());
    assert!(client.storage.get_state(fetched_id).await.is_err(), "delayed duplicate metadata must not install cancelled Fetch state");
    assert_eq!(
        agent.subscriptions.lock().unwrap().iter().filter(|(_, id, _)| *id == query.query_id()).count(),
        subscription_requests,
        "the unrelated stream must keep flowing without resubscription"
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_catalog_fetch_keeps_credentialless_cleanup() -> Result<()> {
    use ankurah::core::schema::catalog::SysModelRowView;

    let agent = CredentialAgent::default();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    server.context_async(Credential(0)).await?.resolve_model_id::<Record>().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), agent);
    let origin = Arc::new(Mutex::new(None::<proto::RequestId>));
    let abandonment = Arc::new(Mutex::new(None::<proto::StateStreamId>));
    let arrived = Arc::new(tokio::sync::Notify::new());
    let (client_id, server_id) = (client.id, server.id);
    let (_connection, gate) = GatedConnection::new(&client, &server, {
        let origin = origin.clone();
        let abandonment = abandonment.clone();
        let arrived = arrived.clone();
        move |message| {
            if let proto::NodeMessage::AbandonStateStream { from, to, stream } = message {
                assert_eq!((*from, *to), (client_id, server_id));
                *abandonment.lock().unwrap() = Some(*stream);
                arrived.notify_one();
            }
            if let proto::NodeMessage::Request { request, .. } = message {
                match request.body {
                    proto::NodeRequestBody::Fetch { .. } => {
                        *origin.lock().unwrap() = Some(request.id.clone());
                        arrived.notify_one();
                        return true;
                    }
                    _ => {}
                }
            }
            false
        }
    })
    .await;
    client.wait_ready().await?;
    let context = client.context_async(Credential(1)).await?;
    let mut fetch = Box::pin(context.fetch::<SysModelRowView>("true"));
    tokio::select! {
        result = &mut fetch => panic!("catalog Fetch completed while its request was held: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = arrived.notified();
                if origin.lock().unwrap().is_some() { break; }
                notified.await;
            }
        }) => result.context("catalog Fetch request was not observed")?,
    }
    drop(fetch);
    // The response and stream identity arrive only after the caller is gone.
    gate.release_held(&server).await;
    let comparison = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let notified = arrived.notified();
            if let Some(comparison) = abandonment.lock().unwrap().take() {
                break comparison;
            }
            notified.await;
        }
    })
    .await
    .context("catalog abandonment was not observed")?;
    // Retired streams still accept valid duplicate receipts. An impossible
    // receipt instead distinguishes the retired handle from an active stream.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = client
                .request(
                    server.id,
                    &vec![Credential(1)],
                    proto::NodeRequestBody::StateFeedback {
                        stream: comparison,
                        round: Some(0),
                        processed_through: u64::MAX,
                        applied_through: None,
                        answers: vec![],
                    },
                )
                .await?;
            if matches!(response, proto::NodeResponseBody::StateStreamUnavailable) {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("catalog comparison did not retire")??;
    Ok(())
}
