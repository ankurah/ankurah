use super::*;
use ankurah_digest_tree::leaf_address;
use std::ops::Bound::*;

mod cancellation;

fn id(n: u8) -> proto::EntityId { proto::EntityId::from_bytes([n; 32]) }
fn address(n: u8) -> Vec<u8> { leaf_address(&[], id(n)) }
#[test]
fn small_compressed_frame_cannot_bypass_the_aggregate_work_budget() {
    let rows = (0..128)
        .map(|n| proto::KeyedEntityHead {
            key: vec![42; 256],
            entity: proto::EntityHead { entity_id: id(n), head: proto::Clock::default() },
        })
        .collect::<Vec<_>>();
    let page = proto::TreeSpanInventory { bounds: super::super::whole(), heads: rows.into() };
    assert!(page.expanded_size() < super::super::PAGE_BYTES as u64);
    let frame = proto::StateFrame {
        stream: proto::StateStreamId::new(),
        sequence: 0,
        body: proto::StateFrameBody::Round {
            round: 0,
            items: vec![proto::StateItem::Heads(page.clone()), proto::StateItem::Heads(page)],
            progress: proto::ComparisonProgress::Continue,
        },
    };
    assert!(bincode::serialized_size(&frame).unwrap() < super::super::FRAME_BYTES as u64);
    let inbox = super::super::Inbox::default();
    assert!(inbox.insert(frame).unwrap_err().to_string().contains("decoded-work limit"));
}

// An empty compiled model keeps this lifecycle fixture independent of derives
// and property backends; its catalog binding is pinned for the test's epochs.
struct StreamRow;
#[derive(Clone)]
struct StreamRowView(crate::entity::Entity);
struct StreamRowMutable(crate::entity::LocalTrxEntity);
impl crate::model::Model for StreamRow {
    type View = StreamRowView;
    type Mutable = StreamRowMutable;
    fn descriptor() -> &'static crate::schema::ModelStructDescriptor {
        static DESCRIPTOR: std::sync::OnceLock<crate::schema::ModelStructDescriptor> = std::sync::OnceLock::new();
        DESCRIPTOR.get_or_init(|| crate::schema::ModelStructDescriptor {
            label: "stream_row",
            name: "StreamRow",
            properties: &[],
            system: None,
            explicit_id: None,
            build_id: [9; 16],
            resolved: crate::schema::PerSystemOnceCell::Pinned(proto::ModelId::EntityId(id(99))),
        })
    }
    fn initialize_new_entity(
        &self,
        _entity: &crate::entity::LocalTrxEntity,
        _model: proto::ModelId,
        _epoch: crate::schema::SystemEpoch,
    ) -> Result<(), crate::property::PropertyError> {
        Ok(())
    }
}
impl crate::model::View for StreamRowView {
    type Model = StreamRow;
    type Mutable = StreamRowMutable;
    fn entity(&self) -> &crate::entity::Entity { &self.0 }
    fn from_entity(entity: crate::entity::Entity) -> Result<Self, RetrievalError> { Ok(Self(entity)) }
    fn to_model(&self) -> Result<StreamRow, crate::property::PropertyError> { Ok(StreamRow) }
}
impl crate::model::Mutable for StreamRowMutable {
    type Model = StreamRow;
    type View = StreamRowView;
    fn entity(&self) -> &crate::entity::LocalTrxEntity { &self.0 }
    fn new(entity: crate::entity::LocalTrxEntity) -> Result<Self, RetrievalError> { Ok(Self(entity)) }
}

type StreamNode = Node<crate::test_utils::TestStorage, crate::policy::PermissiveAgent>;
type RequestGate = Arc<std::sync::Mutex<Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>>;

fn hold_request(gate: &RequestGate) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *gate.lock().unwrap() = Some((entered, release_rx));
    (entered_rx, release)
}

#[derive(Clone)]
struct StreamSender {
    target: crate::node::WeakNode<crate::test_utils::TestStorage, crate::policy::PermissiveAgent>,
    peer: proto::EntityId,
    messages: Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>,
    hold_continuations: Arc<std::sync::atomic::AtomicBool>,
    requests: RequestGate,
}
impl crate::connector::PeerSender for StreamSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), crate::connector::SendError> {
        self.messages.lock().unwrap().push(bincode::deserialize(&bincode::serialize(&message).unwrap()).unwrap());
        if self.hold_continuations.load(std::sync::atomic::Ordering::SeqCst)
            && matches!(message, proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { .. }, .. }))
        {
            return Ok(());
        }
        let pause = if matches!(
            &message,
            proto::NodeMessage::Request {
                request: proto::NodeRequest {
                    body: proto::NodeRequestBody::SubscribeQuery { .. } | proto::NodeRequestBody::Fetch { .. },
                    ..
                },
                ..
            }
        ) {
            self.requests.lock().unwrap().take()
        } else {
            None
        };
        let target = self.target.clone();
        crate::task::spawn(async move {
            if let Some((entered, release)) = pause {
                let _ = entered.send(());
                let _ = release.await;
            }
            if let Some(node) = target.upgrade() {
                let _ = node.handle_message(message).await;
            }
        });
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.peer }
    fn cloned(&self) -> Box<dyn crate::connector::PeerSender> { Box::new(self.clone()) }
}
async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn stream_fixture(
) -> anyhow::Result<(StreamNode, StreamNode, crate::livequery::LiveQuery<StreamRowView>, Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>)> {
    stream_fixture_with_gate(Arc::new(std::sync::atomic::AtomicBool::new(false))).await
}

async fn stream_fixture_with_gate(
    hold_continuations: Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<(StreamNode, StreamNode, crate::livequery::LiveQuery<StreamRowView>, Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>)> {
    stream_fixture_with_gates(hold_continuations, RequestGate::default()).await
}

async fn stream_fixture_with_gates(
    hold_continuations: Arc<std::sync::atomic::AtomicBool>,
    requests: RequestGate,
) -> anyhow::Result<(StreamNode, StreamNode, crate::livequery::LiveQuery<StreamRowView>, Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>)> {
    let server = StreamNode::new_durable(Arc::new(crate::test_utils::TestStorage::default()), crate::policy::PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = StreamNode::new(Arc::new(crate::test_utils::TestStorage::default()), crate::policy::PermissiveAgent::new());
    let messages = Arc::new(std::sync::Mutex::new(Vec::new()));
    server
        .register_peer(
            client.presence().await?,
            Box::new(StreamSender {
                target: client.weak(),
                peer: client.id,
                messages: messages.clone(),
                hold_continuations: hold_continuations.clone(),
                requests: requests.clone(),
            }),
        )
        .await?;
    client
        .register_peer(
            server.presence().await?,
            Box::new(StreamSender { target: server.weak(), peer: server.id, messages: messages.clone(), hold_continuations, requests }),
        )
        .await?;
    client.wait_ready().await?;
    let model = proto::ModelId::EntityId(id(99));
    for n in 0..64 {
        let state = proto::Attested::opt(
            proto::EntityState { entity_id: id(n), state: proto::State { memberships: [model].into(), ..Default::default() } },
            None,
        );
        server.storage.set_state(state.clone());
        client.storage.set_state(state);
    }
    let reader = client.context_async(crate::policy::DEFAULT_CONTEXT).await?;
    let query = reader
        .query_wait::<StreamRowView>(crate::node::MatchArgs {
            selection: ankql::parser::parse_selection("true")?,
            cache_policy: crate::node::CachePolicy::Durable,
        })
        .await?;
    query.wait_durable_answered().await?;
    Ok((server, client, query, messages))
}

#[tokio::test]
async fn selection_replacement_keeps_the_active_responder_tree_while_its_request_waits() -> anyhow::Result<()> {
    let requests = RequestGate::default();
    let (server, client, query, messages) =
        stream_fixture_with_gates(Arc::new(std::sync::atomic::AtomicBool::new(false)), requests.clone()).await?;
    let query_id = query.query_id();
    assert_eq!(query.resultset().len(), 64);
    let peer = server.peer_connections.get(&client.id).unwrap();
    let (version, original_tree) = peer.subscription_handler.query_tree_for_test(query_id).await.unwrap();
    assert_eq!(version, 1);
    let (origin, comparison) = messages
        .lock()
        .unwrap()
        .iter()
        .find_map(|message| match message {
            proto::NodeMessage::Response(proto::NodeResponse {
                request_id,
                body: proto::NodeResponseBody::QuerySubscribed { query_id: found, state },
                ..
            }) if *found == query_id => Some((request_id.clone(), state.frame.stream)),
            _ => None,
        })
        .unwrap();
    let (entered_rx, release) = hold_request(&requests);
    query.update_selection("true")?;
    tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx).await??;
    let client_peer = client.peer_connections.get(&server.id).unwrap();
    until(|| !client_peer.outgoing_state_streams.contains_key(&comparison)).await;
    assert!(!client_peer.incoming_states.is_current(&origin));
    // Queued traffic on the canceled, already-bound origin must not invoke
    // the orphan-metadata cleanup path either.
    client
        .handle_message(proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: server.id,
            to: client.id,
            body: proto::NodeUpdateBody::StateFrame {
                request_id: origin,
                frame: proto::StateFrame {
                    stream: comparison,
                    sequence: 1,
                    body: proto::StateFrameBody::Live { revision: 1, items: vec![], complete: true },
                },
            },
        }))
        .await?;
    tokio::task::yield_now().await;
    let (_, retained) =
        peer.subscription_handler.query_tree_for_test(query_id).await.expect("replacement preparation cannot remove the standing query");
    assert!(Arc::ptr_eq(&original_tree, &retained));
    assert!(!messages.lock().unwrap().iter().any(|message| matches!(message,
        proto::NodeMessage::AbandonStateStream { stream: found, .. } if *found == comparison
    )));
    release.send(()).unwrap();
    query.wait_durable_answered().await?;
    let (version, replacement_tree) = peer.subscription_handler.query_tree_for_test(query_id).await.unwrap();
    assert_eq!(version, 2);
    assert!(Arc::ptr_eq(&original_tree, &replacement_tree));
    assert_eq!(query.resultset().len(), 64);
    drop(query);
    until(|| {
        messages.lock().unwrap().iter().any(|message| {
            matches!(message,
                proto::NodeMessage::UnsubscribeQuery {query_id: found, ..} if *found == query_id
            )
        })
    })
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while peer.subscription_handler.query_tree_for_test(query_id).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn a_current_live_stream_failure_still_abandons_its_responder_query() -> anyhow::Result<()> {
    let requests = RequestGate::default();
    let (server, client, query, messages) =
        stream_fixture_with_gates(Arc::new(std::sync::atomic::AtomicBool::new(false)), requests.clone()).await?;
    let query_id = query.query_id();
    let peer = server.peer_connections.get(&client.id).unwrap();
    let (origin, comparison) = messages
        .lock()
        .unwrap()
        .iter()
        .find_map(|message| match message {
            proto::NodeMessage::Response(proto::NodeResponse {
                request_id,
                body: proto::NodeResponseBody::QuerySubscribed { query_id: found, state },
                ..
            }) if *found == query_id => Some((request_id.clone(), state.frame.stream)),
            _ => None,
        })
        .unwrap();
    assert!(client.peer_connections.get(&server.id).unwrap().incoming_states.is_current(&origin));
    // Hold the immediate retry so the old failure cleanup is observable
    // before a replacement can reinstall the same query.
    let (entered_rx, release) = hold_request(&requests);
    client
        .handle_message(proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: server.id,
            to: client.id,
            body: proto::NodeUpdateBody::StateFrame {
                request_id: origin.clone(),
                frame: proto::StateFrame { stream: comparison, sequence: 1, body: proto::StateFrameBody::RestartRequired },
            },
        }))
        .await?;
    until(|| {
        messages.lock().unwrap().iter().any(|message| {
            matches!(message,
                proto::NodeMessage::AbandonStateStream { stream: found, .. } if *found == comparison
            )
        })
    })
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while peer.subscription_handler.query_tree_for_test(query_id).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let client_peer = client.peer_connections.get(&server.id).unwrap();
    until(|| !client_peer.incoming_states.is_current(&origin) && !client_peer.outgoing_state_streams.contains_key(&comparison)).await;
    assert!(!query.resultset().is_loaded());
    tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx).await??;
    drop(query);
    release.send(()).unwrap();
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn dropping_a_query_after_permanent_inventory_preparation_failure_cleans_its_assigned_peer() -> anyhow::Result<()> {
    let (server, client, query, messages) = stream_fixture().await?;
    let query_id = query.query_id();
    let peer = server.peer_connections.get(&client.id).unwrap();
    let (_, original_tree) = peer.subscription_handler.query_tree_for_test(query_id).await.unwrap();
    // This is a local preparation failure, before a replacement SubscribeQuery
    // can reach the peer. The previous remote generation still needs an owner.
    // An uncertain commit makes a new source capture permanently unavailable.
    drop(client.reactor.begin_commit().await);
    query.update_selection("true")?;
    assert!(tokio::time::timeout(std::time::Duration::from_secs(5), query.wait_initialized()).await?.is_err());
    assert!(
        !messages.lock().unwrap().iter().any(|message| matches!(message,
            proto::NodeMessage::Request {request: proto::NodeRequest {
                body: proto::NodeRequestBody::SubscribeQuery {query_id: found, version: 2, ..}, ..
            }, ..} if *found == query_id
        )),
        "the failure must happen before sending replacement metadata"
    );
    let (_, retained) = peer.subscription_handler.query_tree_for_test(query_id).await.unwrap();
    assert!(Arc::ptr_eq(&original_tree, &retained));
    drop(query);
    until(|| {
        messages.lock().unwrap().iter().any(|message| {
            matches!(message,
                proto::NodeMessage::UnsubscribeQuery {query_id: found, ..} if *found == query_id
            )
        })
    })
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while peer.subscription_handler.query_tree_for_test(query_id).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn replacement_during_initial_stream_keeps_its_responder_tree() -> anyhow::Result<()> {
    use ankurah_signals::Peek;
    let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let requests = RequestGate::default();
    let (server, client, query, messages) = stream_fixture_with_gates(gate.clone(), requests.clone()).await?;
    let query_id = query.query_id();
    let selection = query.selection().peek().unwrap().0;
    let states = client.storage.fetch_states(&selection).await?;
    let tree = super::super::QueryTree::default();
    let mut snapshot = tree.refresh(super::super::QueryTree::spec(&selection, &client.catalog), states).await?;
    // A mixed leaf/summary inventory requires per-region work. Thirty-three
    // positive spans exceed that budget, leaving a continuation on wire.
    let mut start = super::super::whole().start;
    let mut spans = Vec::new();
    for n in 1..=32 {
        let bounds = proto::TreeSpan::new(start, Excluded(address(n)));
        spans.push(if n == 1 {
            proto::SpanContent::Heads(snapshot.page(bounds)?)
        } else {
            proto::SpanContent::Digest(snapshot.span_digest(bounds).await?)
        });
        start = Included(address(n));
    }
    spans.push(proto::SpanContent::Digest(snapshot.span_digest(proto::TreeSpan::new(start, super::super::whole().end)).await?));
    gate.store(true, std::sync::atomic::Ordering::SeqCst);
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let (stream, response) = request_state(
        &client,
        server.id,
        &credentials,
        proto::NodeRequestBody::SubscribeQuery {
            query_id,
            selection,
            version: 1,
            known_state: proto::KnownState { spec: snapshot.spec.clone(), spans },
        },
    )
    .await?;
    let proto::NodeResponseBody::QuerySubscribed { state, .. } = response else { panic!("expected query metadata") };
    assert!(matches!(state.frame.body, proto::StateFrameBody::Round { progress: proto::ComparisonProgress::Continue, .. }));
    let comparison = state.frame.stream;
    let peer = server.peer_connections.get(&client.id).unwrap();
    let (_, original_tree) = peer.subscription_handler.query_tree_for_test(query_id).await.unwrap();
    crate::peer_subscription::RemoteQuerySubscriber::stream_bound(&query.weak(), 1, stream.request.clone(), Arc::new(|| false));
    let receive = receive_state(&client, server.id, &credentials, snapshot, state, false, Some((query_id, 1)), stream);
    tokio::pin!(receive);
    assert!(futures::poll!(&mut receive).is_pending());
    let (entered_rx, release) = hold_request(&requests);
    query.update_selection("true")?;
    tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx).await??;
    assert!(receive.await.is_err(), "replacement must close the unfinished initial receiver");
    let (_, retained) =
        peer.subscription_handler.query_tree_for_test(query_id).await.expect("superseded initial cleanup cannot erase the tree");
    assert!(Arc::ptr_eq(&original_tree, &retained));
    assert!(!messages.lock().unwrap().iter().any(|message| matches!(message,
        proto::NodeMessage::AbandonStateStream { stream: found, .. } if *found == comparison
    )));
    gate.store(false, std::sync::atomic::Ordering::SeqCst);
    release.send(()).unwrap();
    query.wait_durable_answered().await?;
    let (_, replacement_tree) = peer.subscription_handler.query_tree_for_test(query_id).await.unwrap();
    assert!(Arc::ptr_eq(&original_tree, &replacement_tree));
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn a_current_initial_validation_error_still_abandons_its_comparison() -> anyhow::Result<()> {
    use ankurah_signals::Peek;
    let (server, client, query, messages) = stream_fixture().await?;
    let query_id = query.query_id();
    let selection = query.selection().peek().unwrap().0;
    let states = client.storage.fetch_states(&selection).await?;
    let mut snapshot =
        super::super::QueryTree::default().refresh(super::super::QueryTree::spec(&selection, &client.catalog), states).await?;
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let (stream, response) = request_state(
        &client,
        server.id,
        &credentials,
        proto::NodeRequestBody::SubscribeQuery { query_id, selection, version: 1, known_state: snapshot.offer().await? },
    )
    .await?;
    let proto::NodeResponseBody::QuerySubscribed { mut state, .. } = response else { panic!("expected query metadata") };
    let comparison = state.frame.stream;
    state.frame.sequence = 1;
    assert!(receive_state(&client, server.id, &credentials, snapshot, state, false, Some((query_id, 1)), stream).await.is_err());
    assert!(messages.lock().unwrap().iter().any(|message| matches!(message,
        proto::NodeMessage::AbandonStateStream { stream: found, .. } if *found == comparison
    )));
    // One-way cleanup returns after sending, independently of remote teardown.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while server.peer_connections.get(&client.id).unwrap().subscription_handler.query_tree_for_test(query_id).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn fetch_and_subscription_share_each_nodes_resident_tree() -> anyhow::Result<()> {
    use ankurah_signals::Peek;
    let (server, client, query, _messages) = stream_fixture().await?;
    let selection = query.selection().peek().unwrap().0;
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let spec = super::super::QueryTree::spec(&selection, &client.catalog);
    let local = client.resident_trees.acquire(&client, selection.clone(), Some(spec.clone()), credentials.clone()).await?;
    let local_tree = local.tree();
    drop(local);
    let connection = server.peer_connections.get(&client.id).unwrap();
    let (_, serving_tree) = connection.subscription_handler.query_tree_for_test(query.query_id()).await.unwrap();
    // Both roles retain an RAII lease after the initial comparison finishes.
    client.resident_trees.evict_idle();
    server.resident_trees.evict_idle();
    let rows = client.fetch_from_peer(selection.clone(), &credentials).await?;
    assert_eq!(rows.len(), 64);
    let local = client.resident_trees.acquire(&client, selection.clone(), Some(spec.clone()), credentials.clone()).await?;
    let serving = server.resident_trees.acquire(&server, selection, Some(spec), credentials).await?;
    assert!(Arc::ptr_eq(&local_tree, &local.tree()), "making Fetch reuses the tree used to make SubscribeQuery");
    assert!(Arc::ptr_eq(&serving_tree, &serving.tree()), "serving Fetch reuses the standing subscription tree");
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}

#[tokio::test]
async fn disconnected_query_pins_its_tree_until_selection_replacement_or_drop() -> anyhow::Result<()> {
    use ankurah_signals::Peek;
    for replace_selection in [false, true] {
        let (server, client, query, _messages) = stream_fixture().await?;
        let selection = query.selection().peek().unwrap().0;
        let credentials = vec![crate::policy::DEFAULT_CONTEXT];
        let local = client.resident_trees.acquire(&client, selection.clone(), None, credentials.clone()).await?;
        let tree = local.tree();
        drop(local);
        let streams = client.peer_connections.get(&server.id).unwrap().outgoing_state_streams.clone();
        client.deregister_peer(server.id);
        server.deregister_peer(client.id);
        // IncomingStream removes this binding on drop, after Driver releases its pin.
        until(|| streams.is_empty()).await;
        client.resident_trees.evict_idle();
        let resident = client.resident_trees.acquire(&client, selection.clone(), None, credentials.clone()).await?;
        assert!(Arc::ptr_eq(&tree, &resident.tree()), "the live query retains its tree while no stream exists");
        drop(resident);

        if replace_selection {
            query.update_selection("false")?;
        } else {
            drop(query);
        }
        client.resident_trees.evict_idle();
        let replacement = client.resident_trees.acquire(&client, selection, None, credentials).await?;
        assert!(!Arc::ptr_eq(&tree, &replacement.tree()), "ending the query generation releases its tree pin");
    }
    Ok(())
}
