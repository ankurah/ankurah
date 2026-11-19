use super::*;

use std::sync::Arc;

use ankql::ast::Predicate;
use ankurah_core_types::key_spec::KeySpec;
use ankurah_digest_tree::Page;

use crate::{
    connector::{PeerSender, SendError},
    entity::Entity,
    error::ValidationError,
    policy::{AccessDenied, DefaultContext, DEFAULT_CONTEXT},
    storage::StorageTransaction,
    test_utils::TestStorage,
    util::Iterable,
};

#[derive(Default)]
struct Restrictions {
    states: HashSet<EntityId>,
    events: HashSet<EventId>,
    limits: ComparisonLimits,
}

#[derive(Clone, Default)]
struct ReadPolicy(Arc<Mutex<Restrictions>>);

#[async_trait]
impl PolicyAgent for ReadPolicy {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C: Iterable<Self::ContextData>>(
        &self,
        _: &crate::node::NodeInner<SE, Self>,
        _: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied> {
        Ok(vec![])
    }

    async fn check_request<SE: StorageEngine, A: Iterable<proto::AuthData> + Send + Sync>(
        &self,
        _: &Node<SE, Self>,
        _: &A,
        _: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError> {
        Ok(vec![DEFAULT_CONTEXT])
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        _: &Entity,
        _: &Entity,
        _: &Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &Attested<Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied> { Ok(Predicate::True) }

    fn check_reads<C: Iterable<Self::ContextData>>(&self, _: &C, states: &[(&EntityId, &State)]) -> HashMap<EntityId, AccessDenied> {
        let restrictions = self.0.lock().unwrap();
        states
            .iter()
            .filter(|(id, _)| restrictions.states.contains(id))
            .map(|(id, _)| (**id, AccessDenied::ByPolicy("state read revoked")))
            .collect()
    }

    fn check_read_event<C: Iterable<Self::ContextData>>(&self, _: &C, event: &Attested<Event>) -> Result<(), AccessDenied> {
        if self.0.lock().unwrap().events.contains(&event.payload.id()) {
            Err(AccessDenied::ByPolicy("event read revoked"))
        } else {
            Ok(())
        }
    }

    fn comparison_limits<C: Iterable<Self::ContextData>>(&self, _: &C) -> ComparisonLimits { self.0.lock().unwrap().limits }

    fn state_reads_follow_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> bool { self.0.lock().unwrap().states.is_empty() }

    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

#[derive(Clone)]
struct Sender(EntityId);
impl PeerSender for Sender {
    fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn recipient_node_id(&self) -> EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

struct Fixture {
    node: Node<TestStorage, ReadPolicy>,
    peer: EntityId,
    model: proto::ModelId,
    root: Event,
    current: Event,
    selection: Selection<Resolved>,
}

impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        let storage = Arc::new(TestStorage::default());
        let node = Node::new_durable(storage, ReadPolicy::default());
        node.system.create().await?;
        node.wait_ready().await?;
        let peer = EntityId::random();
        node.register_peer(
            proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
            Box::new(Sender(peer)),
        )
        .await?;
        let model = proto::ModelId::EntityId(EntityId::random());
        let root = Event::genesis(
            None,
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]),
        );
        let current = child(&root);
        store(&node, &[root.clone(), current.clone()]).await?;
        let fixture = Self { node, peer, model, root, current, selection: Predicate::MemberOf(model).into() };
        fixture.state(&fixture.current, true);
        Ok(fixture)
    }

    fn state(&self, event: &Event, member: bool) {
        self.node.storage.set_state(Attested::opt(
            proto::EntityState {
                entity_id: event.entity_id,
                state: State {
                    head: Clock::singleton(event),
                    memberships: member.then_some(self.model).into_iter().collect(),
                    ..State::default()
                },
            },
            None,
        ));
    }

    async fn capture(&self) -> anyhow::Result<(proto::ComparisonId, Vec<u8>)> {
        let id = proto::ComparisonId::new();
        let target = self
            .node
            .begin_comparison(
                self.peer,
                id,
                self.selection.clone(),
                KeySpec::new(vec![]),
                proto::KnownState::empty(),
                &vec![DEFAULT_CONTEXT],
            )
            .await?;
        let Some(Page::Members(members)) = target.page else { panic!("small fixture returns members") };
        let key = members.into_iter().find(|member| member.id == self.current.entity_id).unwrap().key;
        Ok((id, key))
    }

    async fn repair(
        &self,
        id: proto::ComparisonId,
        key: Option<Vec<u8>>,
        known: Clock,
        events_only: bool,
    ) -> Result<PreparedRepair<TestStorage, &'static DefaultContext>, RetrievalError> {
        self.node
            .prepare_comparison_repair(
                self.peer,
                id,
                &self.selection,
                vec![proto::RepairMember { known: proto::KnownEntity { entity_id: self.current.entity_id, head: known }, key }],
                events_only,
                &vec![DEFAULT_CONTEXT],
            )
            .await
    }
}

fn child(parent: &Event) -> Event {
    Event::update(parent.entity_id, Clock::singleton(parent), proto::AuthorId::Unknown, proto::OperationSet::default())
}

async fn store(node: &Node<TestStorage, ReadPolicy>, events: &[Event]) -> anyhow::Result<()> {
    let mut trx = node.storage.transaction();
    trx.add_events(&events.iter().cloned().map(Into::into).collect::<Vec<_>>()).await?;
    trx.commit().await?.committed()?;
    Ok(())
}

fn delivered(repair: &PreparedRepair<TestStorage, &'static DefaultContext>) -> Vec<Event> {
    let mut events = Vec::new();
    for delta in &repair.deltas {
        let proto::DeltaContent::EventBridge { events: fragments } = &delta.content else { panic!("expected event repair") };
        events.extend(fragments.iter().cloned().map(|fragment| Attested::<Event>::from((delta.entity_id, fragment)).payload));
    }
    events
}

#[tokio::test]
async fn cold_repair_uses_current_state_and_exact_captured_key() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (id, key) = fixture.capture().await?;
    let newer = child(&fixture.current);
    store(&fixture.node, &[newer.clone()]).await?;
    fixture.state(&newer, true);
    let repair = fixture.repair(id, Some(key), Clock::default(), false).await?;
    repair.revalidate(&fixture.node, &vec![DEFAULT_CONTEXT]).await?;
    assert!(repair.unavailable.is_empty());
    assert!(matches!(&repair.deltas[0].content,
        proto::DeltaContent::StateSnapshot { state } if state.state.head == Clock::singleton(&newer)));
    assert!(fixture.repair(id, Some(vec![99]), Clock::default(), false).await.is_err());
    Ok(())
}

#[tokio::test]
async fn known_id_repair_does_not_require_captured_membership() -> anyhow::Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.selection = Predicate::False.into();
    let id = proto::ComparisonId::new();
    fixture
        .node
        .begin_comparison(
            fixture.peer,
            id,
            fixture.selection.clone(),
            KeySpec::new(vec![]),
            proto::KnownState::empty(),
            &vec![DEFAULT_CONTEXT],
        )
        .await?;
    let newer = child(&fixture.current);
    store(&fixture.node, &[newer.clone()]).await?;
    fixture.state(&newer, true);
    let repair = fixture.repair(id, None, Clock::singleton(&fixture.current), false).await?;
    repair.revalidate(&fixture.node, &vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(delivered(&repair), vec![newer]);
    Ok(())
}

#[tokio::test]
async fn unknown_concurrent_head_gets_complete_parent_first_history() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (id, _) = fixture.capture().await?;
    let remote = child(&fixture.root); // This branch is not stored on the serving node.
    let repair = fixture.repair(id, None, Clock::singleton(&remote), false).await?;
    repair.revalidate(&fixture.node, &vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(delivered(&repair), vec![fixture.root, fixture.current]);
    Ok(())
}

#[tokio::test]
async fn event_repair_covers_both_captured_and_concurrent_current_heads() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (id, key) = fixture.capture().await?;
    let current = child(&fixture.root);
    store(&fixture.node, &[current.clone()]).await?;
    fixture.state(&current, true);
    let repair = fixture.repair(id, Some(key), Clock::singleton(&current), true).await?;
    repair.revalidate(&fixture.node, &vec![DEFAULT_CONTEXT]).await?;
    let events = delivered(&repair);
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], fixture.root);
    assert!(events.contains(&fixture.current));
    assert!(events.contains(&current));
    Ok(())
}

#[tokio::test]
async fn event_denial_never_sends_a_bridge_with_a_hole() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (id, key) = fixture.capture().await?;
    fixture.node.policy_agent.0.lock().unwrap().events.insert(fixture.root.id());
    let repair = fixture.repair(id, Some(key), Clock::default(), true).await?;
    assert!(repair.deltas.is_empty());
    assert_eq!(repair.unavailable, vec![fixture.current.entity_id]);
    Ok(())
}

#[tokio::test]
async fn final_disclosure_rechecks_event_permission() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (id, key) = fixture.capture().await?;
    let repair = fixture.repair(id, Some(key), Clock::default(), true).await?;
    fixture.node.policy_agent.0.lock().unwrap().events.insert(fixture.current.id());
    assert!(repair.revalidate(&fixture.node, &vec![DEFAULT_CONTEXT]).await.is_err());
    Ok(())
}

#[tokio::test]
async fn equal_member_before_changed_member_keeps_authorization_paired_by_identity() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let root = Event::genesis(
        None,
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(fixture.model))]),
    );
    let changed = child(&root);
    store(&fixture.node, &[root.clone(), changed.clone()]).await?;
    fixture.state(&changed, true);
    let (id, _) = fixture.capture().await?;
    let repair = fixture
        .node
        .prepare_comparison_repair(
            fixture.peer,
            id,
            &fixture.selection,
            vec![
                proto::RepairMember {
                    known: proto::KnownEntity { entity_id: fixture.current.entity_id, head: Clock::singleton(&fixture.current) },
                    key: None,
                },
                proto::RepairMember {
                    known: proto::KnownEntity { entity_id: changed.entity_id, head: Clock::singleton(&root) },
                    key: None,
                },
            ],
            false,
            &vec![DEFAULT_CONTEXT],
        )
        .await?;
    assert_eq!(repair.deltas.len(), 1);
    assert_eq!(repair.deltas[0].entity_id, changed.entity_id);
    repair.revalidate(&fixture.node, &vec![DEFAULT_CONTEXT]).await?;
    fixture.node.policy_agent.0.lock().unwrap().events.insert(changed.id());
    assert!(repair.revalidate(&fixture.node, &vec![DEFAULT_CONTEXT]).await.is_err());
    Ok(())
}

#[tokio::test]
async fn unchanged_known_id_still_requires_current_read_authority() -> anyhow::Result<()> {
    let mut fixture = Fixture::new().await?;
    fixture.selection = Predicate::False.into();
    let id = proto::ComparisonId::new();
    fixture
        .node
        .begin_comparison(
            fixture.peer,
            id,
            fixture.selection.clone(),
            KeySpec::new(vec![]),
            proto::KnownState::empty(),
            &vec![DEFAULT_CONTEXT],
        )
        .await?;
    let repair = fixture.repair(id, None, Clock::singleton(&fixture.current), false).await?;
    assert!(repair.deltas.is_empty());
    fixture.node.policy_agent.0.lock().unwrap().states.insert(fixture.current.entity_id);
    assert!(repair.revalidate(&fixture.node, &vec![DEFAULT_CONTEXT]).await.is_err());
    Ok(())
}

#[tokio::test]
async fn foreign_entity_and_false_generations_are_not_accepted_as_a_bridge_floor() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (id, _) = fixture.capture().await?;
    let foreign = Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
    store(&fixture.node, &[foreign.clone()]).await?;
    assert!(fixture.repair(id, None, Clock::singleton(&foreign), false).await.is_err());
    let false_clock = Clock::new(vec![(99, fixture.root.id())])?;
    assert!(matches!(fixture.repair(id, None, false_clock, false).await, Err(RetrievalError::EventStructure(_))));
    Ok(())
}

#[tokio::test]
async fn missing_required_history_is_reported_unavailable() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (id, _) = fixture.capture().await?;
    let missing = child(&fixture.current);
    fixture.state(&missing, true);
    let repair = fixture.repair(id, None, Clock::singleton(&fixture.current), false).await?;
    assert!(repair.deltas.is_empty());
    assert_eq!(repair.unavailable, vec![fixture.current.entity_id]);
    Ok(())
}

#[tokio::test]
async fn event_and_byte_limits_refuse_the_whole_repair() -> anyhow::Result<()> {
    for event_limit in [true, false] {
        let fixture = Fixture::new().await?;
        let (id, key) = fixture.capture().await?;
        if event_limit {
            fixture.node.policy_agent.0.lock().unwrap().limits.max_events = 1;
        } else {
            fixture.node.policy_agent.0.lock().unwrap().limits.max_bytes = 1;
        }
        let error = fixture.repair(id, Some(key), Clock::default(), true).await.err().unwrap();
        assert!(matches!(error, RetrievalError::StorageError(ref error)
            if error.downcast_ref::<ComparisonFailure>() == Some(&ComparisonFailure::LimitExceeded)));
    }
    Ok(())
}

#[tokio::test]
async fn coverage_needs_local_causal_proof_and_accepts_newer_local_versions() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let current = Clock::singleton(&fixture.current);
    let root = Clock::singleton(&fixture.root);
    let id = fixture.current.entity_id;
    assert!(fixture.node.comparison_covers(id, &current, &root, &vec![DEFAULT_CONTEXT], &|_| Ok(())).await?);
    let concurrent = child(&fixture.root);
    assert!(!fixture.node.comparison_covers(id, &current, &Clock::singleton(&concurrent), &vec![DEFAULT_CONTEXT], &|_| Ok(())).await?);
    assert!(
        !fixture.node.comparison_covers(id, &root, &current, &vec![DEFAULT_CONTEXT], &|_| Ok(())).await?,
        "stale local reads must retry"
    );
    Ok(())
}

#[tokio::test]
async fn repeated_coverage_proofs_share_the_callers_work_budget() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let used = Mutex::new(0usize);
    let charge = |steps| {
        let mut used = used.lock().unwrap();
        *used = used.checked_add(steps).filter(|sum| *sum <= 2).ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
        Ok(())
    };
    let head = Clock::singleton(&fixture.current);
    assert!(fixture.node.comparison_covers(fixture.current.entity_id, &head, &head, &vec![DEFAULT_CONTEXT], &charge).await?);
    assert!(fixture.node.comparison_covers(fixture.current.entity_id, &head, &head, &vec![DEFAULT_CONTEXT], &charge).await.is_err());
    Ok(())
}

#[tokio::test]
async fn releasing_a_comparison_cancels_blocked_repair_reads() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let (id, key) = fixture.capture().await?;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (_resume, resume) = tokio::sync::oneshot::channel();
    *fixture.node.storage.hold_state_snapshot.lock().unwrap() = Some((entered, resume));
    let node = fixture.node.clone();
    let peer = fixture.peer;
    let selection = fixture.selection.clone();
    let member =
        proto::RepairMember { known: proto::KnownEntity { entity_id: fixture.current.entity_id, head: Clock::default() }, key: Some(key) };
    let worker =
        tokio::spawn(
            async move { node.prepare_comparison_repair(peer, id, &selection, vec![member], false, &vec![DEFAULT_CONTEXT]).await },
        );
    entering.await?;
    fixture.node.captures.release(peer, id);
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), worker).await??;
    assert!(matches!(result, Err(RetrievalError::StorageError(ref error))
        if error.downcast_ref::<ComparisonFailure>() == Some(&ComparisonFailure::Expired)));
    Ok(())
}
