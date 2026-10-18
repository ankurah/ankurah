use super::*;
use std::ops::Bound::*;

mod cancellation;

fn id(n: u8) -> proto::EntityId { proto::EntityId::from_bytes([n; 32]) }
fn address(n: u8) -> Vec<u8> { leaf_address(&[], id(n)) }
fn spec() -> proto::TreeSpec { proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True) }
fn leaf(n: u8) -> proto::KeyedEntityHead {
    proto::KeyedEntityHead { key: vec![], entity: proto::EntityHead { entity_id: id(n), head: proto::Clock::default() } }
}

#[test]
fn authoritative_range_updates_remove_only_the_exact_interval() -> anyhow::Result<()> {
    let mut members: BTreeMap<_, _> = (0..10).map(|n| (address(n), id(n))).collect();
    let page = proto::TreeSpanInventory {
        bounds: proto::TreeSpan::new(Excluded(address(2)), Included(address(7))),
        leaves: vec![leaf(4), leaf(6)].into(),
    };
    replace_members(&mut members, &spec(), page.clone(), super::super::LEAF_PAGE)?;
    replace_members(&mut members, &spec(), page, super::super::LEAF_PAGE)?;
    assert_eq!(members.values().copied().collect::<Vec<_>>(), vec![id(0), id(1), id(2), id(4), id(6), id(8), id(9)]);
    replace_members(
        &mut members,
        &spec(),
        proto::TreeSpanInventory { bounds: proto::TreeSpan::new(Included(address(4)), Excluded(address(8))), leaves: vec![].into() },
        super::super::LEAF_PAGE,
    )?;
    assert_eq!(members.values().copied().collect::<Vec<_>>(), vec![id(0), id(1), id(2), id(8), id(9)]);
    Ok(())
}

#[test]
fn malformed_pages_cannot_partly_replace_membership_or_panic() {
    let original: BTreeMap<_, _> = (0..10).map(|n| (address(n), id(n))).collect();
    let cases = [
        proto::TreeSpanInventory { bounds: proto::TreeSpan::new(Excluded(address(4)), Excluded(address(4))), leaves: vec![].into() },
        proto::TreeSpanInventory { bounds: proto::TreeSpan::new(Included(address(8)), Included(address(2))), leaves: vec![].into() },
        proto::TreeSpanInventory { bounds: super::super::whole(), leaves: vec![leaf(4), leaf(3)].into() },
        proto::TreeSpanInventory { bounds: proto::TreeSpan::new(Included(address(4)), Included(address(6))), leaves: vec![leaf(3)].into() },
    ];
    for page in cases {
        let mut members = original.clone();
        assert!(replace_members(&mut members, &spec(), page, super::super::LEAF_PAGE).is_err());
        assert_eq!(members, original);
    }
}

#[test]
fn snapshot_pages_derive_sender_keys_targets_and_exact_membership() -> anyhow::Result<()> {
    let spec = proto::TreeSpec::new(
        ankurah_core_types::KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(
            proto::PropertyId::Id,
            ankurah_core_types::ValueType::EntityId,
        )]),
        ankql::ast::Predicate::True,
    );
    let states = [2, 4]
        .into_iter()
        .map(|n| proto::Attested {
            payload: proto::EntityState {
                entity_id: id(n),
                state: proto::State { head: proto::Clock::genesis(proto::EventId::from_bytes([n; 32])), ..Default::default() },
            },
            attestations: proto::AttestationSet(vec![proto::Attestation(vec![n])]),
        })
        .collect::<Vec<_>>();
    let page = proto::SnapshotPage { bounds: super::super::whole(), states: states.clone() };
    let leaves = snapshot_leaves(&spec, &page)?;
    assert!(leaves.leaves.to_vec().iter().all(|leaf| !leaf.key.is_empty()));
    for (leaf, captured) in leaves.leaves.to_vec().iter().zip(&states) {
        assert_eq!(leaf.entity.entity_id, captured.payload.entity_id);
        assert_eq!(leaf.entity.head, captured.payload.state.head);
    }
    let mut members = BTreeMap::new();
    replace_members(&mut members, &spec, leaves, super::super::MAX_SNAPSHOT_ROWS)?;
    assert_eq!(members.values().copied().collect::<Vec<_>>(), [id(2), id(4)]);
    let start = members.first_key_value().unwrap().0.clone();
    let end = members.last_key_value().unwrap().0.clone();
    let empty = proto::SnapshotPage { bounds: proto::TreeSpan::new(Included(start), Excluded(end)), states: vec![] };
    replace_members(&mut members, &spec, snapshot_leaves(&spec, &empty)?, super::super::MAX_SNAPSHOT_ROWS)?;
    assert_eq!(members.values().copied().collect::<Vec<_>>(), [id(4)]);
    Ok(())
}

#[test]
fn malformed_snapshot_pages_cannot_partly_change_membership() {
    let state = |n| proto::Attested::opt(proto::EntityState { entity_id: id(n), state: Default::default() }, None);
    let original = [(address(2), id(2)), (address(4), id(4))].into_iter().collect::<BTreeMap<_, _>>();
    let pages = [
        proto::SnapshotPage { bounds: super::super::whole(), states: vec![state(2), state(2)] },
        proto::SnapshotPage { bounds: super::super::whole(), states: vec![state(4), state(2)] },
        proto::SnapshotPage { bounds: proto::TreeSpan::new(Excluded(address(2)), Included(address(4))), states: vec![state(2)] },
        proto::SnapshotPage {
            bounds: super::super::whole(),
            states: (0..=super::super::MAX_SNAPSHOT_ROWS).map(|n| state(n as u8)).collect(),
        },
    ];
    for page in pages {
        let mut members = original.clone();
        let result = snapshot_leaves(&spec(), &page)
            .and_then(|leaves| replace_members(&mut members, &spec(), leaves, super::super::MAX_SNAPSHOT_ROWS));
        assert!(result.is_err());
        assert_eq!(members, original);
    }
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
#[derive(Clone)]
struct StreamSender {
    target: crate::node::WeakNode<crate::test_utils::TestStorage, crate::policy::PermissiveAgent>,
    peer: proto::EntityId,
    messages: Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>,
    hold_continuations: Arc<std::sync::atomic::AtomicBool>,
}
impl crate::connector::PeerSender for StreamSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), crate::connector::SendError> {
        self.messages.lock().unwrap().push(bincode::deserialize(&bincode::serialize(&message).unwrap()).unwrap());
        if self.hold_continuations.load(std::sync::atomic::Ordering::SeqCst)
            && matches!(message, proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { .. }, .. }))
        {
            return Ok(());
        }
        let target = self.target.clone();
        crate::task::spawn(async move {
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
            }),
        )
        .await?;
    client
        .register_peer(
            server.presence().await?,
            Box::new(StreamSender { target: server.weak(), peer: server.id, messages: messages.clone(), hold_continuations }),
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
async fn selection_replacement_keeps_the_active_responder_tree_while_preparing_its_inventory() -> anyhow::Result<()> {
    let (server, client, query, messages) = stream_fixture().await?;
    let model = proto::ModelId::EntityId(id(99));
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
            }) if *found == query_id => Some((request_id.clone(), state.frame.comparison)),
            _ => None,
        })
        .unwrap();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    client.storage.hold_fetch.lock().unwrap().insert(model, (entered, release_rx));
    query.update_selection("true")?;
    entered_rx.await?;
    let client_peer = client.peer_connections.get(&server.id).unwrap();
    until(|| !client_peer.outgoing_comparisons.contains_key(&comparison)).await;
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
                    comparison,
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
        proto::NodeMessage::Request { request: proto::NodeRequest {
            body: proto::NodeRequestBody::CloseComparison { comparison: found, outcome: proto::ComparisonOutcome::Abandoned }, ..
        }, .. } if *found == comparison
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
    let (server, client, query, messages) = stream_fixture().await?;
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
            }) if *found == query_id => Some((request_id.clone(), state.frame.comparison)),
            _ => None,
        })
        .unwrap();
    assert!(client.peer_connections.get(&server.id).unwrap().incoming_states.is_current(&origin));
    // Hold the immediate retry so the old failure cleanup is observable
    // before a replacement can reinstall the same query.
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    client.storage.hold_fetch.lock().unwrap().insert(proto::ModelId::EntityId(id(99)), (entered, release_rx));
    client
        .handle_message(proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: server.id,
            to: client.id,
            body: proto::NodeUpdateBody::StateFrame {
                request_id: origin.clone(),
                frame: proto::StateFrame { comparison, sequence: 1, body: proto::StateFrameBody::RestartRequired },
            },
        }))
        .await?;
    until(|| {
        messages.lock().unwrap().iter().any(|message| {
            matches!(message,
                proto::NodeMessage::Request { request: proto::NodeRequest {
                    body: proto::NodeRequestBody::CloseComparison { comparison: found, outcome: proto::ComparisonOutcome::Abandoned }, ..
                }, .. } if *found == comparison
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
    until(|| !client_peer.incoming_states.is_current(&origin) && !client_peer.outgoing_comparisons.contains_key(&comparison)).await;
    assert!(!query.resultset().is_loaded());
    entered_rx.await?;
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
    let mut corrupt = client.storage.get_state(id(0)).await?;
    corrupt.payload.entity_id = id(80);
    corrupt.payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([42; 32]));
    corrupt.payload.state.state_buffers = proto::StateBuffers([("unknown-test-backend".into(), vec![1])].into());
    client.storage.set_state(corrupt);
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
    let (server, client, query, messages) = stream_fixture_with_gate(gate.clone()).await?;
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
            proto::SpanContent::Digest(snapshot.head(bounds).await?)
        });
        start = Included(address(n));
    }
    spans.push(proto::SpanContent::Digest(snapshot.head(proto::TreeSpan::new(start, super::super::whole().end)).await?));
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
    assert!(matches!(state.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Continue, .. }));
    let comparison = state.frame.comparison;
    let peer = server.peer_connections.get(&client.id).unwrap();
    let (_, original_tree) = peer.subscription_handler.query_tree_for_test(query_id).await.unwrap();
    crate::peer_subscription::RemoteQuerySubscriber::stream_bound(&query.weak(), 1, stream.request.clone(), Arc::new(|| false));
    let receive = receive_state(&client, server.id, &credentials, snapshot, state, false, Some((query_id, 1)), stream);
    tokio::pin!(receive);
    assert!(futures::poll!(&mut receive).is_pending());
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    client.storage.hold_fetch.lock().unwrap().insert(proto::ModelId::EntityId(id(99)), (entered, release_rx));
    query.update_selection("true")?;
    entered_rx.await?;
    assert!(receive.await.is_err(), "replacement must close the unfinished initial receiver");
    let (_, retained) =
        peer.subscription_handler.query_tree_for_test(query_id).await.expect("superseded initial cleanup cannot erase the tree");
    assert!(Arc::ptr_eq(&original_tree, &retained));
    assert!(!messages.lock().unwrap().iter().any(|message| matches!(message,
        proto::NodeMessage::Request { request: proto::NodeRequest {
            body: proto::NodeRequestBody::CloseComparison { comparison: found, outcome: proto::ComparisonOutcome::Abandoned }, ..
        }, .. } if *found == comparison
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
    let comparison = state.frame.comparison;
    state.frame.sequence = 1;
    assert!(receive_state(&client, server.id, &credentials, snapshot, state, false, Some((query_id, 1)), stream).await.is_err());
    assert!(messages.lock().unwrap().iter().any(|message| matches!(message,
        proto::NodeMessage::Request { request: proto::NodeRequest {
            body: proto::NodeRequestBody::CloseComparison { comparison: found, outcome: proto::ComparisonOutcome::Abandoned }, ..
        }, .. } if *found == comparison
    )));
    assert!(server.peer_connections.get(&client.id).unwrap().subscription_handler.query_tree_for_test(query_id).await.is_none());
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    Ok(())
}
