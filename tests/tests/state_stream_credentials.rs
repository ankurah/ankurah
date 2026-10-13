//! A live stream's connecting-history requests use its captured query credentials.
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

    fn query_predicate<C>(&self, _credentials: &C) -> Result<ankql::ast::Predicate<ankql::ast::Resolved>, AccessDenied>
    where C: Iterable<Credential> {
        Ok(ankql::ast::Predicate::True)
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
