use super::*;
use crate::{
    connector::{PeerSender, SendError},
    error::{MutationError, RetrievalError},
    node::applier::{Delivery, NodeApplier},
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    selection::filter::Filterable,
    storage::StorageTransaction,
    test_utils::{TestStorage, TestStorageTransaction},
};
use ankql::ast::{OrderByItem, OrderDirection, Predicate};
use ankurah_core_types::PropertyPath;

/// The core test engine deliberately rejects ORDER BY/LIMIT. This fixture adds
/// exactly the ordered bounded read exercised here, leaving its real commit and
/// pause seams intact.
#[derive(Default)]
struct OrderedStorage(TestStorage);

#[async_trait::async_trait]
impl StorageEngine for OrderedStorage {
    type Value = Value;
    type Transaction<'a> = TestStorageTransaction<'a>;
    fn transaction(&self) -> Self::Transaction<'_> { self.0.transaction() }
    async fn get_state(&self, id: proto::EntityId) -> Result<Attested<proto::EntityState>, RetrievalError> { self.0.get_state(id).await }
    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<Attested<proto::EntityState>>, RetrievalError> {
        let mut scan = selection.clone();
        scan.order_by = None;
        scan.limit = None;
        let states = self.0.fetch_states(&scan).await?;
        let mut rows: Vec<_> = states
            .into_iter()
            .map(|state| {
                let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
                let value = match entity.value(&property()) {
                    Some(Value::String(value)) => value,
                    _ => String::new(),
                };
                Ok((value, state.payload.entity_id, state))
            })
            .collect::<Result<_, RetrievalError>>()?;
        if let Some(order) = &selection.order_by {
            assert_eq!(order.len(), 1);
            assert_eq!(order[0].path.property_id(), property());
            assert_eq!(order[0].direction, OrderDirection::Asc);
            rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        }
        if let Some(limit) = selection.limit {
            rows.truncate(limit as usize);
        }
        Ok(rows.into_iter().map(|(_, _, state)| state).collect())
    }
    async fn get_events(&self, ids: Vec<proto::EventId>) -> Result<Vec<Attested<proto::Event>>, RetrievalError> {
        self.0.get_events(ids).await
    }
    async fn dump_entity_events(&self, id: proto::EntityId) -> Result<Vec<Attested<proto::Event>>, RetrievalError> {
        self.0.dump_entity_events(id).await
    }
    async fn delete_all(&self) -> Result<bool, MutationError> { self.0.delete_all().await }
    async fn list_materializations(&self) -> Result<Vec<proto::ModelId>, RetrievalError> { self.0.list_materializations().await }
}

type TestNode = Node<OrderedStorage, PermissiveAgent>;
fn model() -> proto::ModelId { proto::ModelId::EntityId(proto::EntityId::from_bytes([23; 32])) }
fn property() -> proto::PropertyId { proto::PropertyId::System(proto::SystemProperty::Label) }
fn operation(value: &str) -> anyhow::Result<proto::Operation> {
    let lww = LWWBackend::new();
    lww.set(property(), Some(Value::String(value.into())));
    Ok(proto::Operation::Backend { backend: "lww".into(), operations: lww.to_operations()?.unwrap() })
}
fn genesis(value: &str) -> anyhow::Result<proto::Event> {
    Ok(proto::Event::genesis(
        None,
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model())), operation(value)?]),
    ))
}
fn update(parent: &proto::Event, value: &str) -> anyhow::Result<proto::Event> {
    Ok(proto::Event::update(
        parent.entity_id,
        proto::Clock::singleton(parent),
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![operation(value)?]),
    ))
}
fn selection(limit: Option<u64>) -> Selection<Resolved> {
    Selection {
        predicate: Predicate::MemberOf(model()),
        order_by: Some(vec![OrderByItem { path: PropertyPath::from(property()), direction: OrderDirection::Asc }]),
        limit,
    }
}
async fn node() -> anyhow::Result<TestNode> {
    let node = Node::new_durable(Arc::new(OrderedStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    Ok(node)
}
async fn apply(node: &TestNode, event: &proto::Event) -> anyhow::Result<()> {
    let events = LocalEventGetter::new(node.storage.clone(), true);
    events.stage_event(event.clone());
    NodeApplier::apply_delta(
        node,
        &proto::EntityId::from_bytes([24; 32]),
        proto::EntityDelta {
            entity_id: event.entity_id,
            content: proto::DeltaContent::EventBridge { events: vec![Attested::from(event.clone()).into()] },
        },
        Delivery::Live,
        &events,
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    Ok(())
}
fn hold_fetch(node: &TestNode) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered, observed) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    node.storage.0.hold_fetch.lock().unwrap().insert(model(), (entered, resume));
    (observed, release)
}

#[tokio::test]
async fn unlimited_capture_folds_an_entrant_and_an_ahead_resident_without_replaying_old_state() -> anyhow::Result<()> {
    let node = node().await?;
    let a = genesis("01")?;
    apply(&node, &a).await?;
    let selection = selection(None);
    let spec = QueryTree::spec(&selection, &node.catalog);
    let tree = Arc::new(QueryTree::default());
    let (entered, release) = hold_fetch(&node);
    let capture = capture_query(&node, selection, spec, vec![DEFAULT_CONTEXT], tree);
    tokio::pin!(capture);
    assert!(futures::poll!(&mut capture).is_pending());
    entered.await?;
    let moved = update(&a, "09")?;
    let b = genesis("02")?;
    apply(&node, &moved).await?;
    apply(&node, &b).await?;
    release.send(()).unwrap();
    let captured = capture.await?;
    assert_eq!(captured.projection.rows.len(), 2);
    assert_eq!(captured.snapshot.states[&a.entity_id].payload.state.head, proto::Clock::singleton(&moved));
    assert!(captured.snapshot.states.contains_key(&b.entity_id));
    assert!(captured.projection.rows[&b.entity_id].key < captured.projection.rows[&a.entity_id].key);
    Ok(())
}

#[tokio::test]
async fn limited_capture_retries_to_discover_an_unchanged_replacement() -> anyhow::Result<()> {
    let node = node().await?;
    let [a, b, c] = ["01", "02", "03"].map(genesis).into_iter().collect::<anyhow::Result<Vec<_>>>()?.try_into().unwrap();
    for event in [&a, &b, &c] {
        apply(&node, event).await?;
    }
    let selection = selection(Some(2));
    let spec = QueryTree::spec(&selection, &node.catalog);
    let (entered, release) = hold_fetch(&node);
    let capture = capture_query(&node, selection, spec, vec![DEFAULT_CONTEXT], Arc::new(QueryTree::default()));
    tokio::pin!(capture);
    assert!(futures::poll!(&mut capture).is_pending());
    entered.await?;
    apply(&node, &update(&a, "09")?).await?;
    release.send(()).unwrap();
    let captured = capture.await?;
    assert_eq!(captured.projection.rows.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([b.entity_id, c.entity_id]));
    assert_eq!(captured.snapshot.states.len(), 2);
    Ok(())
}

#[derive(Clone)]
struct Sender(proto::EntityId);
impl PeerSender for Sender {
    fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn recipient_node_id(&self) -> proto::EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

#[tokio::test]
async fn a_delayed_limit_refill_rejects_stale_membership_and_keeps_the_old_snapshot() -> anyhow::Result<()> {
    let node = node().await?;
    let mut events = Vec::new();
    for value in ["01", "02", "03"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        events.push(event);
    }
    let selection = selection(Some(2));
    let spec = QueryTree::spec(&selection, &node.catalog);
    let tree = Arc::new(QueryTree::default());
    let captured = capture_query(&node, selection, spec, vec![DEFAULT_CONTEXT], tree.clone()).await?;
    let CapturedQuery { revision, snapshot, projection, cursor } = captured;
    let old_members = snapshot.members();
    let peer = proto::EntityId::from_bytes([25; 32]);
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(Sender(peer)),
    )
    .await?;
    let state = crate::node::state_sync::start_comparison(
        &node,
        peer,
        proto::RequestId::new(),
        Vec::new(),
        vec![DEFAULT_CONTEXT],
        Some((proto::QueryId::new(), 1)),
        false,
        revision,
        snapshot,
        None,
    )
    .await?;
    let mut actor = Actor::new(peer, state.frame.comparison, vec![DEFAULT_CONTEXT], projection, cursor, revision);
    let moved_a = update(&events[0], "09")?;
    apply(&node, &moved_a).await?;
    let batch = actor.cursor.next().await?;
    let (entered, release) = hold_fetch(&node);
    let refill = actor.refill(&node, vec![batch]);
    tokio::pin!(refill);
    assert!(futures::poll!(&mut refill).is_pending());
    entered.await?;
    // The in-flight read selected B,C. C moves past A before its rows
    // are captured, so accepting that stale list would miss unchanged A.
    apply(&node, &update(&events[2], "10")?).await?;
    release.send(()).unwrap();
    refill.await?;
    assert_eq!(actor.projection.rows.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([events[0].entity_id, events[1].entity_id]));
    assert_eq!(actor.projection.rows.len(), 2);
    let current = tree.current_snapshot().await?;
    assert_ne!(current.members(), old_members, "the moved source key must be replaced, not left stale");
    node.deregister_peer(peer);
    Ok(())
}
