use super::*;
use super::{
    actor::ResidentActor,
    identity::Identity,
    route::{current_state, Route},
};
use crate::{
    connector::{PeerSender, SendError},
    error::{MutationError, RetrievalError},
    node::{
        applier::{Delivery, NodeApplier},
        WeakNode,
    },
    policy::{ContextPolicy, PermissiveAgent, DEFAULT_CONTEXT},
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    selection::filter::Filterable,
    test_utils::{TestStorage, TestStorageTransaction},
};
use crate::{entity::TemporaryEntity, node::state_sync::Snapshot, schema::catalog::CatalogManager, session::ContextData, value::Value};
use ankql::ast::{OrderByItem, OrderDirection, Predicate};
use ankurah_core_types::PropertyPath;
use ankurah_proto::Attested;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Weak,
};

mod bounded;
mod capture_retry;
mod captured;
mod resident;
mod routes;
mod view_isolation;

/// The core test engine deliberately rejects ORDER BY/LIMIT. This fixture adds
/// exactly the ordered bounded read exercised here, leaving its real commit and
/// pause seams intact.
#[derive(Default)]
struct OrderedStorage(
    TestStorage,
    std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    std::sync::atomic::AtomicUsize,
    std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    std::sync::atomic::AtomicUsize, // Rows returned to callers, after the storage LIMIT.
    std::sync::atomic::AtomicUsize, // Individual state reads, including default get_states calls.
);

#[async_trait::async_trait]
impl StorageEngine for OrderedStorage {
    type Value = Value;
    type Transaction<'a> = TestStorageTransaction<'a>;
    fn transaction(&self) -> Self::Transaction<'_> { self.0.transaction() }
    async fn get_state(&self, id: proto::EntityId) -> Result<Attested<proto::EntityState>, RetrievalError> {
        self.5.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.0.get_state(id).await
    }
    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<Attested<proto::EntityState>>, RetrievalError> {
        self.2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            rows.sort_by(|a, b| {
                let values = match order[0].direction {
                    OrderDirection::Asc => a.0.cmp(&b.0),
                    OrderDirection::Desc => b.0.cmp(&a.0),
                };
                values.then_with(|| a.1.cmp(&b.1))
            });
        }
        if let Some(limit) = selection.limit {
            rows.truncate(limit as usize);
        }
        self.4.fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(rows.into_iter().map(|(_, _, state)| state).collect())
    }
    async fn get_events(&self, ids: Vec<proto::EventId>) -> Result<Vec<Attested<proto::Event>>, RetrievalError> {
        let pause = self.3.lock().unwrap().take();
        if let Some((entered, resume)) = pause {
            let _ = entered.send(());
            let _ = resume.await;
        }
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
/// A complete resident admitting `spec`'s whole predicate under `credentials`,
/// bootstrapped as the LRU builds one for an unlimited selection.
async fn complete_resident<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    node: &Node<OrderedStorage, PA>,
    spec: proto::TreeSpec,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<ResidentActor<PA::ContextData>> {
    node.system.require_system_ready()?;
    let selection = Selection::from(spec.predicate().clone());
    ResidentActor::bootstrap(node, Identity::requested(node, selection, spec, credentials)?.admission()).await
}

/// A resident bootstrapped as the LRU builds one for `selection` under the
/// default credentials: limited when `selection` has a limit.
async fn resident<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    node: &Node<OrderedStorage, PA>,
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
) -> anyhow::Result<ResidentActor<PA::ContextData>> {
    ResidentActor::bootstrap(node, Identity::requested(node, selection, spec, vec![DEFAULT_CONTEXT])?.admission()).await
}

/// The view `selection` selects from `actor`'s tree as it stands.
async fn view<CD: ContextData>(
    actor: &ResidentActor<CD>,
    selection: &Selection<Resolved>,
    catalog: &CatalogManager,
) -> anyhow::Result<Snapshot> {
    actor.identity.coverage.select(actor.tree.snapshot(), selection, catalog).await
}

impl<CD: ContextData> ResidentLease<CD> {
    /// Which resident this lease holds, for [`Weak::ptr_eq`]. The weak
    /// reference keeps the resident's allocation, so no later resident
    /// compares equal to it.
    pub(crate) fn identity(&self) -> Weak<dyn std::any::Any + Send + Sync> {
        let resident: Weak<dyn std::any::Any + Send + Sync> = Arc::downgrade(&self.resident) as _;
        resident
    }
}

impl<CD: ContextData> ResidentSubscription<CD> {
    /// Which resident this subscription's lease holds, for [`Weak::ptr_eq`].
    pub(crate) fn identity(&self) -> Weak<dyn std::any::Any + Send + Sync> { self.lease.identity() }
}

impl<CD: ContextData> ResidentTrees<CD> {
    /// How many residents the LRU holds, pinned or idle.
    pub(crate) fn len(&self) -> usize { self.lru.lock().unwrap().slots.len() }

    /// Evict every idle resident at once.
    pub(crate) fn evict_idle(&self) { self.lru.lock().unwrap().slots.retain(|slot| slot.resident.pins.load(Ordering::Acquire) != 0); }
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
    let (entered, release) = hold_fetch(&node);
    let capture = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]);
    tokio::pin!(capture);
    assert!(futures::poll!(&mut capture).is_pending());
    entered.await?;
    let moved = update(&a, "09")?;
    let b = genesis("02")?;
    apply(&node, &moved).await?;
    apply(&node, &b).await?;
    release.send(()).unwrap();
    let actor = capture.await?;
    let captured = view(&actor, &selection, &node.catalog).await?;
    assert_eq!(actor.tree.entries().len(), 2);
    assert_eq!(captured.entries[&a.entity_id].head, proto::Clock::singleton(&moved));
    assert!(captured.entries.contains_key(&b.entity_id));
    assert!(captured.entries[&b.entity_id].key() < captured.entries[&a.entity_id].key());
    Ok(())
}

#[tokio::test]
async fn a_complete_id_keyed_tree_cannot_select_an_unindexed_ordered_limit() -> anyhow::Result<()> {
    let node = node().await?;
    let a = genesis("01")?;
    let b = genesis("02")?;
    apply(&node, &a).await?;
    apply(&node, &b).await?;
    // Deliberately oppose ID order and requested ascending label order.
    let (first, last) = if a.entity_id > b.entity_id { (&a, &b) } else { (&b, &a) };
    apply(&node, &update(first, "01")?).await?;
    apply(&node, &update(last, "02")?).await?;
    let selection = selection(Some(1));
    let by_id = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), selection.predicate.clone());
    let actor = complete_resident(&node, by_id, vec![DEFAULT_CONTEXT]).await?;
    assert!(view(&actor, &selection, &node.catalog).await.is_err(), "a complete tree cannot rank properties absent from its keys");
    assert_eq!(actor.tree.entries().len(), 2, "a failed scope never truncates the resident admission");
    assert_eq!(
        actor.tree.snapshot().member_map().values().copied().collect::<Vec<_>>(),
        vec![last.entity_id, first.entity_id],
        "physical digest address order remains by ID"
    );
    Ok(())
}

#[tokio::test]
async fn full_capture_folds_changes_before_resolving_a_limit_with_unchanged_entrants() -> anyhow::Result<()> {
    let node = node().await?;
    let [a, b, c]: [proto::Event; 3] = ["01", "02", "03"].map(genesis).into_iter().collect::<anyhow::Result<Vec<_>>>()?.try_into().unwrap();
    for event in [&a, &b, &c] {
        apply(&node, event).await?;
    }
    let selection = selection(Some(2));
    let spec = spec_for(&selection, &node.catalog);
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    let (entered, release) = hold_fetch(&node);
    let capture = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]);
    tokio::pin!(capture);
    assert!(futures::poll!(&mut capture).is_pending());
    entered.await?;
    apply(&node, &update(&a, "09")?).await?;
    release.send(()).unwrap();
    let actor = capture.await?;
    assert_eq!(actor.tree.entries().len(), 3, "the unchanged replacement is already resident");
    let captured = view(&actor, &selection, &node.catalog).await?;
    assert_eq!(captured.entries.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([b.entity_id, c.entity_id]));
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed), scans + 1, "a concurrent update does not cause a refill read");
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
async fn limited_scopes_follow_concurrent_movements_without_refetching_or_mutating_old_snapshots() -> anyhow::Result<()> {
    let node = node().await?;
    let mut events = Vec::new();
    for value in ["01", "02", "03"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        events.push(event);
    }
    let selection = selection(Some(2));
    let spec = spec_for(&selection, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let frozen = view(&actor, &selection, &node.catalog).await?;
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    let moved_a = update(&events[0], "09")?;
    apply(&node, &moved_a).await?;
    let intermediate = actor.refresh(&node).await?.select(&selection, &node.catalog).await?;
    assert_eq!(intermediate.entries.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([events[1].entity_id, events[2].entity_id]));
    // C now moves past A. The next scope must re-admit unchanged A from the
    // same resident tree, while both previous comparisons stay frozen.
    apply(&node, &update(&events[2], "10")?).await?;
    let current = actor.refresh(&node).await?.select(&selection, &node.catalog).await?;
    assert_eq!(current.entries.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([events[0].entity_id, events[1].entity_id]));
    assert_eq!(current.entries[&events[0].entity_id].head, proto::Clock::singleton(&moved_a));
    assert_eq!(frozen.entries[&events[0].entity_id].head, proto::Clock::singleton(&events[0]));
    assert_eq!(intermediate.entries[&events[2].entity_id].head, proto::Clock::singleton(&events[2]));
    assert_eq!(actor.tree.entries().len(), 3);
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed), scans, "supported limited scopes never refill from storage");
    Ok(())
}

#[derive(Clone, Default)]
struct RestrictedDiscovery {
    retrieval: Option<Predicate<Resolved>>,
    denied_event: Option<proto::EventId>,
    read_allowed: Option<Arc<std::sync::atomic::AtomicBool>>,
    discover_all: Option<Arc<std::sync::atomic::AtomicBool>>,
    deny_empty: bool,
    state_free_reads: bool,
    state_free_proofs: Option<Arc<std::sync::atomic::AtomicUsize>>,
    state_checks: Option<Arc<std::sync::atomic::AtomicUsize>>,
    retrieval_error: bool,
}
#[async_trait::async_trait]
impl PolicyAgent for RestrictedDiscovery {
    type ContextData = &'static crate::policy::DefaultContext;
    fn allows_unrestricted_state_reads<C: crate::util::Iterable<Self::ContextData>>(&self, data: &C) -> bool {
        if let Some(calls) = &self.state_free_proofs {
            calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.state_free_reads
            && !self.retrieval_error
            && self.retrieval.as_ref().is_none_or(|predicate| *predicate == Predicate::True)
            && !(self.deny_empty && data.iterable().next().is_none())
            && self.read_allowed.as_ref().is_none_or(|allowed| allowed.load(std::sync::atomic::Ordering::SeqCst))
    }
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
        if self.discover_all.as_ref().is_some_and(|allowed| allowed.load(std::sync::atomic::Ordering::SeqCst)) {
            return Ok(Predicate::True);
        }
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
        if self.retrieval_error {
            return Err(crate::policy::AccessDenied::ByPolicy("fixture retrieval denied"));
        }
        Ok(self.retrieval.clone().unwrap_or(Predicate::True))
    }
    fn check_reads<C: crate::util::Iterable<Self::ContextData>>(
        &self,
        data: &C,
        states: &[(&proto::EntityId, &proto::State)],
    ) -> std::collections::HashMap<proto::EntityId, crate::policy::AccessDenied> {
        if let Some(checks) = &self.state_checks {
            checks.fetch_add(states.len(), std::sync::atomic::Ordering::Relaxed);
        }
        if (self.deny_empty && data.iterable().next().is_none())
            || self.read_allowed.as_ref().is_some_and(|allowed| !allowed.load(std::sync::atomic::Ordering::SeqCst))
        {
            states.iter().map(|(id, _)| (**id, crate::policy::AccessDenied::ByPolicy("fixture state denied"))).collect()
        } else {
            Default::default()
        }
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
async fn maintenance_respects_retrieval_permission_when_query_membership_is_unchanged() -> anyhow::Result<()> {
    let retrieval = Predicate::Comparison {
        left: Box::new(ankql::ast::Expr::Path(PropertyPath::from(property()))),
        operator: ankql::ast::ComparisonOperator::LessThan,
        right: Box::new(ankql::ast::Expr::Literal(Value::String("03".into()))),
    };
    let node =
        Node::new_durable(Arc::new(OrderedStorage::default()), RestrictedDiscovery { retrieval: Some(retrieval), ..Default::default() });
    node.system.create().await?;
    node.wait_ready().await?;
    let initial = genesis("01")?;
    let initially_denied = genesis("04")?;
    apply(&node, &initial).await?;
    apply(&node, &initially_denied).await?;
    let selected = selection(None);
    let spec = spec_for(&selected, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let captured = view(&actor, &selected, &node.catalog).await?;
    assert!(captured.entries.contains_key(&initial.entity_id));
    assert!(!captured.entries.contains_key(&initially_denied.entity_id));

    let mut current = initial;
    for (value, readable) in [("04", false), ("02", true), ("04", false)] {
        current = update(&current, value)?;
        apply(&node, &current).await?;
        let batch = actor.cursor.next().await?;
        let change = batch.changes.iter().find(|change| change.entity_id == current.entity_id).unwrap();
        assert!(change.matches, "all values still satisfy the query/discovery predicate");
        assert_eq!(change.entry.is_some(), readable, "maintenance must enforce the separate retrieval predicate");
        actor.apply(batch.changes, false).await?;
        let snapshot = actor.tree.snapshot();
        assert_eq!(snapshot.entries.contains_key(&current.entity_id), readable);
        if readable {
            assert_eq!(snapshot.entries[&current.entity_id].head, proto::Clock::singleton(&current));
        }
        assert!(actor.candidates.contains(&current.entity_id), "denied query candidates remain watched for reentry");
    }
    let newly_readable = update(&initially_denied, "02")?;
    apply(&node, &newly_readable).await?;
    let batch = actor.cursor.next().await?;
    actor.apply(batch.changes, false).await?;
    assert_eq!(
        actor.tree.entries()[&newly_readable.entity_id].head,
        proto::Clock::singleton(&newly_readable),
        "an initially denied candidate enters when it becomes readable"
    );
    Ok(())
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
    let spec = spec_for(&selection, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(actor.tree.entries().len(), 1);
    assert!(actor.tree.entries().contains_key(&inside.entity_id));
    let mut route = routes::route(&node, &actor, selection).await?;
    apply(&node, &update(&outside, "08")?).await?;
    if let Some(batch) = actor.cursor.fence().await?.batch {
        assert!(actor.apply(batch.changes, true).await?.is_empty());
    }
    assert!(!actor.tree.entries().contains_key(&outside.entity_id));
    apply(&node, &update(&inside, "09")?).await?;
    let batch = actor.cursor.next().await?;
    let changes = actor.apply(batch.changes, true).await?;
    assert_eq!(changes.len(), 1);
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    let changes = route.changes(&node, &actor.identity.coverage, &actor.tree, &policy, &changes).await?;
    assert_eq!(changes.len(), 1);
    assert!(changes[0].member.is_none());
    assert!(changes[0].delta.is_some(), "readable former matches still carry causal data");
    assert!(actor.tree.entries().is_empty());
    Ok(())
}

#[tokio::test]
async fn replacing_a_limit_reuses_the_full_resident_tree() -> anyhow::Result<()> {
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
    // Complete coverage can serve several limits without a bounded refill.
    let full = node
        .resident_trees
        .acquire(&node, selection(None), Some(spec_for(&selection(Some(2)), &node.catalog)), vec![DEFAULT_CONTEXT])
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
            proto::KnownState { spec: spec_for(&selection(Some(2)), &node.catalog), spans: vec![] },
        )
        .await?;
    let (_, original) = handler.query_resident_for_test(query).await.unwrap();
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    apply(&node, &update(&initial[0], "09")?).await?;
    handler
        .subscribe_query(
            &node,
            proto::RequestId::new(),
            Vec::new(),
            query,
            selection(Some(1)),
            Some(&DEFAULT_CONTEXT),
            2,
            proto::KnownState { spec: spec_for(&selection(Some(1)), &node.catalog), spans: vec![] },
        )
        .await?;
    let (version, reused) = handler.query_resident_for_test(query).await.unwrap();
    assert_eq!(version, 2);
    assert!(Weak::ptr_eq(&original, &reused), "compatible limits select scopes of the same resident tree");
    assert!(Weak::ptr_eq(&full.identity(), &reused), "both limits reuse the complete resident");
    let snapshot = full.snapshot().await?.snapshot;
    assert_eq!(snapshot.entries.len(), 3, "the resident keeps the full admission");
    let snapshot = snapshot.select(&selection(Some(1)), &node.catalog).await?;
    assert_eq!(snapshot.entries.keys().copied().collect::<Vec<_>>(), vec![initial[1].entity_id]);
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed), scans);
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
    let spec = spec_for(&selection, &node.catalog);
    let (entered, observed) = oneshot::channel();
    let (read, resume) = oneshot::channel();
    *node.storage.1.lock().unwrap() = Some((entered, resume));
    let capture = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]);
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
    let actor = captured?;
    assert_eq!(actor.tree.entries()[&initial.entity_id].head, proto::Clock::singleton(&second));
    Ok(())
}

#[tokio::test]
async fn a_readable_live_state_cannot_carry_a_denied_event() -> anyhow::Result<()> {
    let initial = genesis("01")?;
    let denied = update(&initial, "02")?;
    let policy = RestrictedDiscovery { denied_event: Some(denied.id()), ..Default::default() };
    let node = Node::new_durable(Arc::new(OrderedStorage::default()), policy);
    node.system.create().await?;
    node.wait_ready().await?;
    apply(&node, &initial).await?;
    let selection = selection(None);
    let spec = spec_for(&selection, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let mut route = routes::route(&node, &actor, selection).await?;
    apply(&node, &denied).await?;
    let batch = actor.cursor.next().await?;
    let credentials = vec![DEFAULT_CONTEXT];
    let read_policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    read_policy.check_read(&initial.entity_id, &current_state(&node, initial.entity_id).await?.unwrap().state)?;
    assert_eq!(batch.changes[0].events[0], denied.id());
    let changes = actor.apply(batch.changes, true).await?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let error = route
        .changes(&node, &actor.identity.coverage, &actor.tree, &policy, &changes)
        .await
        .expect_err("denied event must fail delivery without poisoning the tree");
    assert!(matches!(error.downcast_ref::<crate::policy::AccessDenied>(), Some(crate::policy::AccessDenied::ByPolicy(_))));
    Ok(())
}

#[tokio::test]
async fn a_complete_resident_applies_a_published_batch_while_another_commit_is_in_flight() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let mut actor = complete_resident(&node, spec_for(&selection(None), &node.catalog), vec![DEFAULT_CONTEXT]).await?;
    let first = update(&initial, "02")?;
    apply(&node, &first).await?;
    let batch = actor.cursor.next().await?;
    // Hold the next commit after its storage write, while it holds the commit gate.
    let (committed, stored) = oneshot::channel();
    let (admit, admission) = oneshot::channel();
    *node.storage.0.hold_after_commit.lock().unwrap() = Some((committed, admission));
    let second = update(&first, "03")?;
    let publishing = apply(&node, &second);
    tokio::pin!(publishing);
    assert!(futures::poll!(&mut publishing).is_pending());
    stored.await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), actor.publish(&node, batch))
        .await
        .expect("a published batch does not wait for a commit in flight")?;
    assert_eq!(actor.tree.entries()[&initial.entity_id].head, proto::Clock::singleton(&first));
    admit.send(()).unwrap();
    publishing.await?;
    Ok(())
}
