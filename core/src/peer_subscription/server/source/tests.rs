use super::*;
use crate::{
    connector::{PeerSender, SendError},
    error::{MutationError, RetrievalError},
    node::applier::{Delivery, NodeApplier},
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    selection::filter::Filterable,
    test_utils::{TestStorage, TestStorageTransaction},
};
use ankql::ast::{OrderByItem, OrderDirection, Predicate};
use ankurah_core_types::PropertyPath;

mod capture_retry;

/// The core test engine deliberately rejects ORDER BY/LIMIT. This fixture adds
/// exactly the ordered bounded read exercised here, leaving its real commit and
/// pause seams intact.
#[derive(Default)]
struct OrderedStorage(TestStorage, std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>);

#[async_trait::async_trait]
impl StorageEngine for OrderedStorage {
    type Value = Value;
    type Transaction<'a> = TestStorageTransaction<'a>;
    fn transaction(&self) -> Self::Transaction<'_> { self.0.transaction() }
    async fn get_state(&self, id: proto::EntityId) -> Result<Attested<proto::EntityState>, RetrievalError> { self.0.get_state(id).await }
    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<Attested<proto::EntityState>>, RetrievalError> {
        let pause = self.1.lock().unwrap().take();
        if let Some((entered, resume)) = pause {
            let _ = entered.send(());
            let _ = resume.await;
        }
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
async fn apply<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    node: &Node<OrderedStorage, PA>,
    event: &proto::Event,
) -> anyhow::Result<()> {
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
async fn unlimited_capture_folds_an_entrant_and_movement_after_its_storage_read() -> anyhow::Result<()> {
    let node = node().await?;
    let a = genesis("01")?;
    apply(&node, &a).await?;
    let selection = selection(None);
    let spec = proto::TreeSpec::new(
        ankurah_core_types::KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(property(), crate::value::ValueType::String)]),
        selection.predicate.clone(),
    );
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
async fn limited_projection_keeps_storage_query_order_independently_of_digest_keys() -> anyhow::Result<()> {
    let node = node().await?;
    let a = genesis("01")?;
    let b = genesis("02")?;
    apply(&node, &a).await?;
    apply(&node, &b).await?;
    // Deliberately oppose ID order and requested ascending label order.
    let (first, last) = if a.entity_id > b.entity_id { (&a, &b) } else { (&b, &a) };
    apply(&node, &update(first, "01")?).await?;
    apply(&node, &update(last, "02")?).await?;
    let selection = selection(Some(2));
    let by_id = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), selection.predicate.clone());
    let captured = capture_query(&node, selection, by_id, vec![DEFAULT_CONTEXT], Arc::new(QueryTree::default())).await?;
    assert_eq!(
        captured.projection.rows.keys().copied().collect::<Vec<_>>(),
        vec![first.entity_id, last.entity_id],
        "unchanged refill entrants must retain storage query order, not entity-ID order"
    );
    assert_eq!(
        captured.snapshot.members().values().copied().collect::<Vec<_>>(),
        vec![last.entity_id, first.entity_id],
        "digest address order is independently allowed to be by ID"
    );
    Ok(())
}

#[tokio::test]
async fn limited_capture_retries_to_discover_an_unchanged_replacement() -> anyhow::Result<()> {
    let node = node().await?;
    let [a, b, c]: [proto::Event; 3] = ["01", "02", "03"].map(genesis).into_iter().collect::<anyhow::Result<Vec<_>>>()?.try_into().unwrap();
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
    let known = proto::KnownState { spec: snapshot.spec.clone(), regions: vec![] };
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
        known,
    )
    .await?;
    let mut actor = QuerySource::new(peer, state.frame.comparison, vec![DEFAULT_CONTEXT], projection, cursor, revision);
    let moved_a = update(&events[0], "09")?;
    apply(&node, &moved_a).await?;
    let batch = actor.cursor.next().await?;
    let (entered, release) = hold_fetch(&node);
    {
        let refill = actor.refill(&node, vec![batch]);
        tokio::pin!(refill);
        assert!(futures::poll!(&mut refill).is_pending());
        entered.await?;
        // The in-flight read selected B,C. C moves past A before its rows
        // are captured, so accepting that stale list would miss unchanged A.
        apply(&node, &update(&events[2], "10")?).await?;
        release.send(()).unwrap();
        refill.await?;
    }
    assert_eq!(actor.projection.rows.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([events[0].entity_id, events[1].entity_id]));
    assert_eq!(actor.projection.rows.len(), 2);
    let current = tree.current_snapshot().await?;
    assert_ne!(current.members(), old_members, "the moved source key must be replaced, not left stale");
    node.deregister_peer(peer);
    Ok(())
}

#[derive(Clone, Default)]
struct RestrictedDiscovery {
    denied_event: Option<proto::EventId>,
}
#[async_trait::async_trait]
impl PolicyAgent for RestrictedDiscovery {
    type ContextData = &'static crate::policy::DefaultContext;
    fn sign_request<SE: StorageEngine, C: crate::util::Iterable<Self::ContextData>>(
        &self,
        _: &crate::node::NodeInner<SE, Self>,
        _: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, crate::policy::AccessDenied> {
        Ok(Vec::new())
    }
    async fn check_request<SE: StorageEngine, C: crate::util::Iterable<proto::AuthData> + Send + Sync>(
        &self,
        _: &Node<SE, Self>,
        _: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, crate::error::ValidationError> {
        Ok(vec![DEFAULT_CONTEXT])
    }
    fn query_predicate<C: crate::util::Iterable<Self::ContextData>>(
        &self,
        _: &C,
    ) -> Result<Predicate<Resolved>, crate::policy::AccessDenied> {
        Ok(Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(PropertyPath::from(property()))),
            operator: ankql::ast::ComparisonOperator::LessThan,
            right: Box::new(ankql::ast::Expr::Literal(Value::String("05".into()))),
        })
    }
    fn retrieval_predicate<C: crate::util::Iterable<Self::ContextData>>(
        &self,
        _: &C,
    ) -> Result<Predicate<Resolved>, crate::policy::AccessDenied> {
        Ok(Predicate::True)
    }
    fn check_read_event<C: crate::util::Iterable<Self::ContextData>>(
        &self,
        _: &C,
        event: &Attested<proto::Event>,
    ) -> Result<(), crate::policy::AccessDenied> {
        if self.denied_event.as_ref() == Some(&event.payload.id()) {
            Err(crate::policy::AccessDenied::ByPolicy("fixture refuses this event"))
        } else {
            Ok(())
        }
    }
    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        _: &crate::entity::Entity,
        _: &crate::entity::Entity,
        _: &proto::Event,
    ) -> Result<Option<proto::Attestation>, crate::policy::AccessDenied> {
        Ok(None)
    }
    fn check_write(
        &self,
        _: &Self::ContextData,
        _: &crate::entity::Entity,
        _: Option<&proto::Event>,
    ) -> Result<(), crate::policy::AccessDenied> {
        Ok(())
    }
    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &proto::EntityState) -> Option<proto::Attestation> { None }
    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::EntityId,
        _: &Attested<proto::Event>,
    ) -> Result<(), crate::policy::AccessDenied> {
        Ok(())
    }
    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::EntityId,
        _: &Attested<proto::EntityState>,
    ) -> Result<(), crate::policy::AccessDenied> {
        Ok(())
    }
    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::EntityId,
        _: &proto::CausalAssertion,
    ) -> Result<(), crate::policy::AccessDenied> {
        Ok(())
    }
}

#[tokio::test]
async fn discovery_filter_is_preserved_for_live_changes_when_known_id_reads_are_broader() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(OrderedStorage::default()), RestrictedDiscovery::default());
    node.system.create().await?;
    node.wait_ready().await?;
    let inside = genesis("01")?;
    let outside = genesis("09")?;
    apply(&node, &inside).await?;
    apply(&node, &outside).await?;
    let selection = selection(None);
    let spec = QueryTree::spec(&selection, &node.catalog);
    let mut captured = capture_query(&node, selection, spec, vec![DEFAULT_CONTEXT], Arc::new(QueryTree::default())).await?;
    assert_eq!(captured.projection.rows.len(), 1);
    assert!(captured.projection.rows.contains_key(&inside.entity_id));
    apply(&node, &update(&outside, "08")?).await?;
    let batch = captured.cursor.next().await?;
    assert!(captured.projection.apply(&node, &vec![DEFAULT_CONTEXT], &batch).await?.is_empty());
    assert!(!captured.projection.rows.contains_key(&outside.entity_id));
    apply(&node, &update(&inside, "09")?).await?;
    let batch = captured.cursor.next().await?;
    let changes = captured.projection.apply(&node, &vec![DEFAULT_CONTEXT], &batch).await?;
    assert_eq!(changes.len(), 1);
    assert!(changes[0].member.is_none());
    assert!(changes[0].delta.is_some(), "readable former matches still carry causal data");
    assert!(captured.projection.rows.is_empty());
    Ok(())
}

#[tokio::test]
async fn replacing_a_query_cancels_its_in_flight_refill_before_reusing_the_tree() -> anyhow::Result<()> {
    let node = node().await?;
    let mut initial = Vec::new();
    for value in ["01", "02", "03"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        initial.push(event);
    }
    let peer = proto::EntityId::from_bytes([26; 32]);
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(Sender(peer)),
    )
    .await?;
    let handler = crate::peer_subscription::SubscriptionHandler::new(peer);
    let query = proto::QueryId::new();
    handler
        .subscribe_query(
            &node,
            proto::RequestId::new(),
            Vec::new(),
            query,
            selection(Some(2)),
            Some(&DEFAULT_CONTEXT),
            1,
            proto::KnownState { spec: QueryTree::spec(&selection(Some(2)), &node.catalog), regions: vec![] },
        )
        .await?;
    let (_, original_tree) = handler.query_tree_for_test(query).await.unwrap();
    let (entered, release) = hold_fetch(&node);
    apply(&node, &update(&initial[0], "09")?).await?;
    entered.await?;
    handler
        .subscribe_query(
            &node,
            proto::RequestId::new(),
            Vec::new(),
            query,
            selection(Some(1)),
            Some(&DEFAULT_CONTEXT),
            2,
            proto::KnownState { spec: QueryTree::spec(&selection(Some(1)), &node.catalog), regions: vec![] },
        )
        .await?;
    assert!(release.send(()).is_err(), "the old generation's pending read must be dropped before replacement installs");
    let (version, reused) = handler.query_tree_for_test(query).await.unwrap();
    assert_eq!(version, 2);
    assert!(Arc::ptr_eq(&original_tree, &reused));
    let snapshot = reused.current_snapshot().await?;
    assert_eq!(snapshot.states.keys().copied().collect::<Vec<_>>(), vec![initial[1].entity_id]);
    handler.remove_predicate(&node, query).await?;
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn a_storage_read_ahead_of_admission_cannot_be_replaced_by_an_older_window_record() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let selection = selection(None);
    let spec = QueryTree::spec(&selection, &node.catalog);
    let (entered, observed) = oneshot::channel();
    let (read, resume) = oneshot::channel();
    *node.storage.1.lock().unwrap() = Some((entered, resume));
    let capture = capture_query(&node, selection, spec, vec![DEFAULT_CONTEXT], Arc::new(QueryTree::default()));
    tokio::pin!(capture);
    assert!(futures::poll!(&mut capture).is_pending());
    observed.await?;
    let first = update(&initial, "02")?;
    apply(&node, &first).await?;
    let second = update(&first, "03")?;
    let (committed, stored) = oneshot::channel();
    let (admit, admission) = oneshot::channel();
    *node.storage.0.hold_after_commit.lock().unwrap() = Some((committed, admission));
    let publishing = apply(&node, &second);
    tokio::pin!(publishing);
    assert!(futures::poll!(&mut publishing).is_pending());
    stored.await?;
    read.send(()).unwrap();
    // The raw storage read now sees "03" while the window still contains
    // only "02". Its closing fence must wait for the second admission.
    assert!(futures::poll!(&mut capture).is_pending());
    admit.send(()).unwrap();
    let (published, captured) = tokio::join!(publishing, capture);
    published?;
    let captured = captured?;
    assert_eq!(captured.snapshot.states[&initial.entity_id].payload.state.head, proto::Clock::singleton(&second));
    let row = &captured.snapshot.states[&initial.entity_id].payload;
    assert_eq!(TemporaryEntity::new(row.entity_id, &row.state)?.value(&property()), Some(Value::String("03".into())));
    Ok(())
}

#[tokio::test]
async fn a_readable_live_state_cannot_carry_a_denied_event() -> anyhow::Result<()> {
    let initial = genesis("01")?;
    let denied = update(&initial, "02")?;
    let policy = RestrictedDiscovery { denied_event: Some(denied.id()) };
    let node = Node::new_durable(Arc::new(OrderedStorage::default()), policy);
    node.system.create().await?;
    node.wait_ready().await?;
    apply(&node, &initial).await?;
    let selection = selection(None);
    let spec = QueryTree::spec(&selection, &node.catalog);
    let mut captured = capture_query(&node, selection, spec, vec![DEFAULT_CONTEXT], Arc::new(QueryTree::default())).await?;
    apply(&node, &denied).await?;
    let batch = captured.cursor.next().await?;
    let credentials = vec![DEFAULT_CONTEXT];
    let read_policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    read_policy.check_read(&initial.entity_id, &batch.changes[0].state.state)?;
    assert_eq!(batch.changes[0].events[0].payload.id(), denied.id());
    let error = captured.projection.apply(&node, &credentials, &batch).await.expect_err("denied event must fail the publication");
    assert!(matches!(error.downcast_ref::<crate::policy::AccessDenied>(), Some(crate::policy::AccessDenied::ByPolicy(_))));
    Ok(())
}
