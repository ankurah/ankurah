use super::*;
use crate::{
    connector::{PeerSender, SendError},
    node::NodeKey,
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    test_utils::{connect_peer, TestStorage},
};
use std::{
    sync::{mpsc, OnceLock},
    time::Duration,
};

#[derive(Clone)]
struct Sender(mpsc::Sender<proto::NodeMessage>);
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.0.send(message).map_err(|_| SendError::ConnectionClosed)
    }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn fixture() -> anyhow::Result<(Node<TestStorage, PermissiveAgent>, ConnectionId, mpsc::Receiver<proto::NodeMessage>)> {
    fixture_with_key(&NodeKey::generate()).await
}

async fn fixture_with_key(
    key: &NodeKey,
) -> anyhow::Result<(Node<TestStorage, PermissiveAgent>, ConnectionId, mpsc::Receiver<proto::NodeMessage>)> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let (sent, messages) = mpsc::channel();
    let peer = connect_peer(&node, key, false, Sender(sent)).await?;
    Ok((node, peer, messages))
}

#[test]
fn simultaneous_local_admission_obeys_one_capacity_bound() {
    let registry = Arc::new(IncomingComparisons::default());
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let registry = registry.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                registry
                    .admit(ComparisonId::new(), vec![DEFAULT_CONTEXT], ComparisonLimits { max_captures: 3, ..Default::default() })
                    .is_ok()
            })
        })
        .collect();
    assert_eq!(workers.into_iter().map(|worker| worker.join().unwrap() as usize).sum::<usize>(), 3);
    assert_eq!(registry.0.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn delivery_before_reply_is_admitted_once_and_cancelled_requests_leave_no_registration() -> anyhow::Result<()> {
    for cancel in [false, true] {
        let (node, peer, messages) = fixture().await?;
        let id = ComparisonId::new();
        let connection = node.peer_connections.get(&peer).unwrap();
        connection.comparisons.admit(id, vec![DEFAULT_CONTEXT], ComparisonLimits::default())?;
        let selection = ankql::ast::Predicate::False.into();
        let members =
            vec![proto::RepairMember { known: proto::KnownEntity { entity_id: EntityId::random(), head: Default::default() }, key: None }];
        let credentials = vec![DEFAULT_CONTEXT];
        let mut request = Box::pin(node.request_comparison_repair(peer, id, &selection, members, false, &credentials));
        assert!(futures::poll!(&mut request).is_pending());
        let proto::NodeMessage::Request { request: sent, .. } = messages.try_recv()? else { panic!("expected request") };
        let proto::NodeRequestBody::Compare { request: proto::ComparisonRequest::Repair { delivery, .. }, .. } = sent.body else {
            panic!("expected repair")
        };
        let incoming = connection.comparisons.get(&id).unwrap();
        assert_eq!(incoming.deliveries.len(), 1);
        if cancel {
            drop(request);
            assert!(incoming.deliveries.is_empty());
            assert!(connection.pending_requests.is_empty());
            assert!(node.receive_comparison_repair(peer, id, delivery, vec![]).await.is_err());
        } else {
            node.receive_comparison_repair(peer, id, delivery.clone(), vec![]).await?;
            assert!(node.receive_comparison_repair(peer, id, delivery.clone(), vec![]).await.is_err());
            node.handle_message(
                peer,
                proto::NodeMessage::Response(proto::NodeResponse {
                    from: node.peer_node_id(peer)?,
                    to: node.keyed_id(),
                    request_id: sent.id,
                    body: proto::NodeResponseBody::Compared(Ok(proto::ComparisonReply::Repaired { delivery, unavailable: vec![] })),
                }),
            )
            .await?;
            let receipt = request.await?;
            assert_eq!(receipt.events, 0);
            assert_eq!(receipt.bytes, bincode::serialized_size(&Vec::<proto::EntityDelta>::new())? as usize);
            assert!(connection.pending_requests.is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn unrequested_entities_and_deliveries_never_reach_application() -> anyhow::Result<()> {
    let (node, peer, _) = fixture().await?;
    let id = ComparisonId::new();
    let delivery = proto::UpdateId::new();
    assert!(node.receive_comparison_repair(peer, id, delivery.clone(), vec![]).await.is_err());
    let connection = node.peer_connections.get(&peer).unwrap();
    connection.comparisons.admit(id, vec![DEFAULT_CONTEXT], ComparisonLimits::default())?;
    let incoming = connection.comparisons.get(&id).unwrap();
    let (done, received) = oneshot::channel();
    incoming.deliveries.insert(delivery.clone(), Delivery { entities: [EntityId::random()].into_iter().collect(), done });
    let foreign = EntityId::random();
    let delta = proto::EntityDelta {
        entity_id: foreign,
        content: proto::DeltaContent::StateSnapshot {
            state: proto::StateFragment { state: Default::default(), attestations: Default::default() },
        },
    };
    assert!(node.receive_comparison_repair(peer, id, delivery, vec![delta]).await.is_err());
    assert!(received.await?.is_err());
    assert!(matches!(node.storage.get_state(foreign).await, Err(RetrievalError::EntityNotFound(_))));
    Ok(())
}

#[tokio::test]
async fn cancelling_initial_capture_releases_local_admission_before_any_request() -> anyhow::Result<()> {
    let (node, peer, messages) = fixture().await?;
    let connection = node.peer_connections.get(&peer).unwrap();
    let (entered, entering) = oneshot::channel();
    let (_resume, blocked) = oneshot::channel();
    *node.storage.hold_state_snapshot.lock().unwrap() = Some((entered, blocked));
    let owner = node.clone();
    let task =
        tokio::spawn(async move { owner.reconcile_query(peer, ankql::ast::Predicate::False.into(), &vec![DEFAULT_CONTEXT], None).await });
    tokio::time::timeout(Duration::from_secs(2), entering).await??;
    assert_eq!(connection.comparisons.0.lock().unwrap().len(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(connection.comparisons.0.lock().unwrap().is_empty());
    assert!(matches!(messages.try_recv()?, proto::NodeMessage::ReleaseComparison { .. }));
    assert!(messages.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn transient_head_snapshot_conflict_restarts_the_query_and_completes_repair() -> anyhow::Result<()> {
    use ankurah_digest_tree::{Digest, Page, Ristretto255};
    use std::{future::Future, sync::atomic::Ordering, task::Poll};

    let (node, peer, messages) = fixture().await?;
    let connection = node.peer_connections.get(&peer).unwrap();
    let model = proto::ModelId::EntityId(EntityId::random());
    let id = EntityId::random();
    let head = proto::Clock::genesis(proto::EventId::from_bytes([71; 32]));
    node.storage.set_state(proto::Attested::opt(
        proto::EntityState {
            entity_id: id,
            state: proto::State { memberships: [model].into_iter().collect(), head: head.clone(), ..Default::default() },
        },
        None,
    ));
    let credentials = vec![DEFAULT_CONTEXT];
    let mut query = Box::pin(node.reconcile_query(peer, ankql::ast::Predicate::MemberOf(model).into(), &credentials, None));
    assert!(futures::poll!(&mut query).is_pending());
    let proto::NodeMessage::Request { request: first, .. } = messages.try_recv()? else { panic!("expected initial Fetch") };
    let proto::NodeRequestBody::Fetch { comparison: first_id, key_spec, known_state, .. } = first.body else {
        panic!("expected initial Fetch")
    };
    assert_eq!(known_state.digest.count(), 1);
    let empty_target = |comparison| {
        proto::NodeResponseBody::Fetch(proto::ComparisonTarget {
            id: comparison,
            key_spec: key_spec.clone(),
            known_state: proto::KnownState { digest: Digest::<Ristretto255>::empty() },
            page: Some(Page::Members(vec![])),
        })
    };
    node.handle_message(
        peer,
        proto::NodeMessage::Response(proto::NodeResponse {
            from: node.peer_node_id(peer)?,
            to: node.keyed_id(),
            request_id: first.id,
            body: empty_target(first_id),
        }),
    )
    .await?;

    // Inject the conflict after capture and before the differing member's head
    // read. Poll immediately so background maintenance cannot consume the hook.
    let reads = node.storage.state_point_reads.load(Ordering::SeqCst);
    node.storage.reject_state_finish.store(true, Ordering::SeqCst);
    let first_attempt = futures::poll!(&mut query);
    node.storage.reject_state_finish.store(false, Ordering::SeqCst);
    assert!(first_attempt.is_pending(), "a raw head-read WriteConflict must enter the bounded retry: {first_attempt:?}");
    assert_eq!(node.storage.state_point_reads.load(Ordering::SeqCst), reads + 1);
    assert!(matches!(messages.try_recv()?, proto::NodeMessage::ReleaseComparison { comparison, .. } if comparison == first_id));
    assert!(connection.comparisons.get(&first_id).is_none());

    let next = tokio::time::timeout(
        Duration::from_secs(2),
        std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = query.as_mut().poll(cx) {
                panic!("retry ended before its Fetch: {result:?}");
            }
            match messages.try_recv() {
                Ok(message) => Poll::Ready(message),
                Err(mpsc::TryRecvError::Empty) => Poll::Pending,
                Err(error) => panic!("retry transport closed: {error}"),
            }
        }),
    )
    .await?;
    let proto::NodeMessage::Request { request: second, .. } = next else { panic!("expected renewed Fetch") };
    let proto::NodeRequestBody::Fetch { comparison: second_id, known_state: renewed, .. } = second.body else {
        panic!("expected renewed Fetch")
    };
    assert_ne!(first_id, second_id);
    assert_eq!(renewed.digest, known_state.digest);
    node.handle_message(
        peer,
        proto::NodeMessage::Response(proto::NodeResponse {
            from: node.peer_node_id(peer)?,
            to: node.keyed_id(),
            request_id: second.id,
            body: empty_target(second_id),
        }),
    )
    .await?;
    assert!(futures::poll!(&mut query).is_pending());
    let proto::NodeMessage::Request { request: repair, .. } = messages.try_recv()? else { panic!("expected known-ID repair") };
    let proto::NodeRequestBody::Compare {
        comparison, request: proto::ComparisonRequest::Repair { delivery, members, events_only }, ..
    } = repair.body
    else {
        panic!("expected known-ID repair")
    };
    assert_eq!(comparison, second_id);
    assert!(!events_only);
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].known.entity_id, id);
    assert_eq!(members[0].known.head, head);
    assert!(members[0].key.is_none());
    node.receive_comparison_repair(peer, second_id, delivery.clone(), vec![]).await?;
    node.handle_message(
        peer,
        proto::NodeMessage::Response(proto::NodeResponse {
            from: node.peer_node_id(peer)?,
            to: node.keyed_id(),
            request_id: repair.id,
            body: proto::NodeResponseBody::Compared(Ok(proto::ComparisonReply::Repaired { delivery, unavailable: vec![id] })),
        }),
    )
    .await?;
    query.await?;
    assert!(connection.comparisons.0.lock().unwrap().is_empty());
    assert!(connection.pending_requests.is_empty());
    assert_eq!(node.storage.get_state(id).await?.payload.state.head, head, "an absent remote member does not delete local history");
    assert!(matches!(messages.try_recv()?, proto::NodeMessage::ReleaseComparison { comparison, .. } if comparison == second_id));
    assert!(messages.try_recv().is_err(), "one transient conflict needs exactly one renewed capture");
    Ok(())
}

#[test]
fn snapshot_write_conflict_is_a_typed_restart_response() {
    let error = RetrievalError::from(crate::error::MutationError::WriteConflict);
    let response = crate::node::comparison::response_error(anyhow::Error::new(error).context("comparison snapshot"));
    assert!(matches!(response, proto::NodeResponseBody::Compared(Err(ComparisonFailure::Restart))));
}

#[tokio::test]
async fn replacement_connection_rejects_old_deliveries_and_frees_pending_requests() -> anyhow::Result<()> {
    let key = NodeKey::generate();
    let (node, peer, _) = fixture_with_key(&key).await?;
    let old = node.peer_connections.get(&peer).unwrap();
    let id = ComparisonId::new();
    old.comparisons.admit(id, vec![DEFAULT_CONTEXT], ComparisonLimits::default())?;
    let (done, waiting) = oneshot::channel();
    old.pending_requests.insert(proto::RequestId::new(), done);
    node.deregister_peer(peer);
    let (sent, _messages) = mpsc::channel();
    let replacement = connect_peer(&node, &key, false, Sender(sent)).await?;
    assert_ne!(replacement, peer);
    assert!(waiting.await.is_err());
    assert!(old.comparisons.0.lock().unwrap().is_empty());
    assert!(node.receive_comparison_repair(peer, id, proto::UpdateId::new(), vec![]).await.is_err());
    assert!(node.receive_comparison_repair(replacement, id, proto::UpdateId::new(), vec![]).await.is_err());
    Ok(())
}

#[tokio::test]
async fn same_key_connections_cannot_consume_each_others_deliveries() -> anyhow::Result<()> {
    let key = NodeKey::generate();
    let (node, first, first_messages) = fixture_with_key(&key).await?;
    let (sent, second_messages) = mpsc::channel();
    let second = connect_peer(&node, &key, false, Sender(sent)).await?;
    assert_ne!(first, second);
    assert_eq!(node.peer_node_id(first)?, node.peer_node_id(second)?);
    let id = ComparisonId::new();
    for connection in [first, second] {
        node.peer_connections.get(&connection).unwrap().comparisons.admit(id, vec![DEFAULT_CONTEXT], ComparisonLimits::default())?;
    }
    let incoming = node.peer_connections.get(&first).unwrap().comparisons.get(&id).unwrap();
    let delivery = proto::UpdateId::new();
    let (done, received) = oneshot::channel();
    incoming.deliveries.insert(delivery.clone(), Delivery { entities: HashSet::new(), done });
    let update = || {
        proto::NodeMessage::Update(proto::NodeUpdate {
            from: key.public_key().node_id(),
            to: node.keyed_id(),
            id: delivery.clone(),
            body: proto::NodeUpdateBody::Comparison { comparison: id, deltas: vec![] },
        })
    };
    node.handle_message(second, update()).await?;
    assert!(matches!(
        second_messages.try_recv()?,
        proto::NodeMessage::UpdateAck(proto::NodeUpdateAck { body: proto::NodeUpdateAckBody::Error(_), .. })
    ));
    assert_eq!(incoming.deliveries.len(), 1, "the other connection cannot consume the requested delivery");
    node.handle_message(first, update()).await?;
    assert!(matches!(
        first_messages.try_recv()?,
        proto::NodeMessage::UpdateAck(proto::NodeUpdateAck { body: proto::NodeUpdateAckBody::Success, .. })
    ));
    assert_eq!(received.await??.1, 0);
    assert!(incoming.deliveries.is_empty());
    Ok(())
}

#[tokio::test]
async fn same_key_connections_cannot_answer_each_others_requests_and_disconnect_independently() -> anyhow::Result<()> {
    let key = NodeKey::generate();
    let (node, first, first_messages) = fixture_with_key(&key).await?;
    let (sent, second_messages) = mpsc::channel();
    let second = connect_peer(&node, &key, false, Sender(sent)).await?;
    let credentials = vec![DEFAULT_CONTEXT];
    let id = ComparisonId::new();
    let body = || proto::NodeRequestBody::Compare {
        comparison: id,
        selection: ankql::ast::Predicate::False.into(),
        request: proto::ComparisonRequest::Pages { spans: vec![] },
    };
    let mut first_request = Box::pin(node.request_on(first, &credentials, body()));
    assert!(futures::poll!(&mut first_request).is_pending());
    let proto::NodeMessage::Request { request: first_sent, .. } = first_messages.try_recv()? else { panic!("expected request") };
    let reply = |request_id| {
        proto::NodeMessage::Response(proto::NodeResponse {
            from: key.public_key().node_id(),
            to: node.keyed_id(),
            request_id,
            body: proto::NodeResponseBody::Compared(Ok(proto::ComparisonReply::Pages(vec![]))),
        })
    };
    node.handle_message(second, reply(first_sent.id)).await?;
    assert!(futures::poll!(&mut first_request).is_pending(), "the same node id on another connection cannot answer this request");
    assert_eq!(node.peer_connections.get(&first).unwrap().pending_requests.len(), 1);

    let mut second_request = Box::pin(node.request_on(second, &credentials, body()));
    assert!(futures::poll!(&mut second_request).is_pending());
    let proto::NodeMessage::Request { request: second_sent, .. } = second_messages.try_recv()? else { panic!("expected request") };
    node.deregister_peer(first);
    assert!(matches!(first_request.await, Err(RequestError::InternalChannelClosed)));
    assert!(futures::poll!(&mut second_request).is_pending());
    assert_eq!(node.peer_connections.get(&second).unwrap().pending_requests.len(), 1);
    node.handle_message(second, reply(second_sent.id)).await?;
    assert!(
        matches!(second_request.await?, proto::NodeResponseBody::Compared(Ok(proto::ComparisonReply::Pages(pages))) if pages.is_empty())
    );
    assert!(node.peer_connections.get(&second).unwrap().pending_requests.is_empty());
    Ok(())
}

#[derive(Default)]
struct QueryGate {
    query: Mutex<Option<proto::QueryId>>,
    comparison: Mutex<Option<ComparisonId>>,
    held: Mutex<Option<proto::NodeMessage>>,
    holding: tokio::sync::Notify,
    unsubscribed: tokio::sync::Notify,
    subscriptions: std::sync::atomic::AtomicUsize,
}

#[derive(Clone)]
struct Forward {
    node: crate::node::WeakNode<TestStorage, PermissiveAgent>,
    connection: Arc<OnceLock<ConnectionId>>,
    gate: Arc<QueryGate>,
}

impl PeerSender for Forward {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        use std::sync::atomic::Ordering;
        let query = *self.gate.query.lock().unwrap();
        let unsubscribing = matches!(&message, proto::NodeMessage::UnsubscribeQuery { query_id, .. } if Some(*query_id) == query);
        match &message {
            proto::NodeMessage::Request {
                request: proto::NodeRequest { body: proto::NodeRequestBody::SubscribeQuery { query_id, .. }, .. },
                ..
            } if Some(*query_id) == query => {
                self.gate.subscriptions.fetch_add(1, Ordering::SeqCst);
            }
            proto::NodeMessage::Response(proto::NodeResponse {
                body: proto::NodeResponseBody::QuerySubscribed { query_id, target },
                ..
            }) if Some(*query_id) == query => {
                *self.gate.comparison.lock().unwrap() = Some(target.id);
            }
            proto::NodeMessage::Request {
                request: proto::NodeRequest { body: proto::NodeRequestBody::Compare { comparison, .. }, .. },
                ..
            } if Some(*comparison) == *self.gate.comparison.lock().unwrap() => {
                *self.gate.held.lock().unwrap() = Some(message);
                self.gate.holding.notify_one();
                return Ok(());
            }
            _ => {}
        }
        let node = self.node.upgrade().ok_or(SendError::ConnectionClosed)?;
        let connection = *self.connection.get().ok_or(SendError::ConnectionClosed)?;
        let gate = self.gate.clone();
        tokio::spawn(async move {
            node.handle_message(connection, message).await.unwrap();
            if unsubscribing {
                gate.unsubscribed.notify_one();
            }
        });
        Ok(())
    }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

#[tokio::test]
async fn dropping_a_query_during_comparison_never_reinstalls_its_remote_subscription() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;
    let server = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let gate = Arc::new(QueryGate::default());
    let on_client = Arc::new(OnceLock::new());
    let on_server = Arc::new(OnceLock::new());
    let challenge = proto::Challenge::random();
    let server_connection = server
        .register_peer(
            proto::ConnectionSide::Accepting,
            &challenge,
            client.presence(proto::ConnectionSide::Dialing, &challenge).await?,
            Box::new(Forward { node: client.weak(), connection: on_client.clone(), gate: gate.clone() }),
        )
        .await?;
    on_server.set(server_connection).unwrap();
    let challenge = proto::Challenge::random();
    let client_connection = client
        .register_peer(
            proto::ConnectionSide::Dialing,
            &challenge,
            server.presence(proto::ConnectionSide::Accepting, &challenge).await?,
            Box::new(Forward { node: server.weak(), connection: on_server, gate: gate.clone() }),
        )
        .await?;
    on_client.set(client_connection).unwrap();
    client.wait_ready().await?;
    let model = proto::ModelId::EntityId(EntityId::random());
    server.storage.set_state(proto::Attested::opt(
        proto::EntityState {
            entity_id: EntityId::random(),
            state: proto::State { memberships: [model].into_iter().collect(), ..Default::default() },
        },
        None,
    ));
    let query = crate::livequery::EntityLiveQuery::new(
        client.context(DEFAULT_CONTEXT)?.0,
        crate::node::CachePolicy::Durable,
        crate::livequery::QueryResolution::Resolved(ankql::ast::Predicate::MemberOf(model).into()),
    )?;
    let query_id = query.query_id();
    *gate.query.lock().unwrap() = Some(query_id);
    tokio::time::timeout(Duration::from_secs(2), gate.holding.notified()).await?;
    assert_eq!(gate.subscriptions.load(Ordering::SeqCst), 1);
    let old_comparison = gate.comparison.lock().unwrap().unwrap();
    drop(query);
    tokio::time::timeout(Duration::from_secs(2), gate.unsubscribed.notified()).await?;
    // The already-sent RPC can arrive after unsubscribe. Its Expired response
    // must not drive a fresh SubscribeQuery for the dropped local query.
    let held = gate.held.lock().unwrap().take().unwrap();
    *gate.comparison.lock().unwrap() = None;
    server.handle_message(server_connection, held).await?;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.subscriptions.load(Ordering::SeqCst), 1);
    let peer = server.peer_connections.get(&server_connection).unwrap();
    assert!(matches!(
        peer.subscription_handler.subscription().remove_predicate(query_id),
        Err(crate::error::SubscriptionError::PredicateNotFound)
    ));
    assert!(client.peer_connections.get(&client_connection).unwrap().comparisons.get(&old_comparison).is_none());
    Ok(())
}
