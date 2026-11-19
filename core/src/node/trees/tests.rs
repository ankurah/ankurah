use super::*;
use ankurah_proto::ModelId;

use crate::{
    entity::Entity,
    error::ValidationError,
    policy::{DefaultContext, DEFAULT_CONTEXT},
    property::backend::{lww::LWWBackend, PropertyBackend},
    test_utils::TestStorage,
    util::Iterable,
};
use ankql::ast::{ComparisonOperator, Expr};
use ankurah_core_types::{PropertyId, Value};
use ankurah_proto::{self as proto, Attested, Clock, EntityState, Event, EventId};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Clone)]
struct Policy {
    allowed: Predicate<Resolved>,
    proof: bool,
    revoked: Arc<AtomicBool>,
    checks: Arc<AtomicUsize>,
    limits: ComparisonLimits,
    revoke_after: Option<usize>,
}

impl Policy {
    fn new(allowed: Predicate<Resolved>, proof: bool) -> Self {
        Self {
            allowed,
            proof,
            revoked: Arc::new(AtomicBool::new(false)),
            checks: Arc::new(AtomicUsize::new(0)),
            limits: ComparisonLimits::default(),
            revoke_after: None,
        }
    }
}

#[async_trait::async_trait]
impl PolicyAgent for Policy {
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
        _: &proto::NodeId,
        _: &Attested<Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &EntityState) -> Option<proto::Attestation> { None }
    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &Attested<EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&Event>) -> Result<(), AccessDenied> { Ok(()) }
    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
    fn query_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied> {
        Ok(self.allowed.clone())
    }
    fn state_reads_follow_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> bool {
        self.proof && !self.revoked.load(Ordering::SeqCst)
    }
    fn comparison_limits<C: Iterable<Self::ContextData>>(&self, _: &C) -> ComparisonLimits { self.limits }
    fn check_reads<C: Iterable<Self::ContextData>>(&self, _: &C, states: &[(&EntityId, &State)]) -> HashMap<EntityId, AccessDenied> {
        let checks = self.checks.fetch_add(states.len(), Ordering::SeqCst) + states.len();
        let denied = self.revoked.load(Ordering::SeqCst);
        if self.revoke_after.is_some_and(|after| checks >= after) {
            self.revoked.store(true, Ordering::SeqCst);
        }
        if denied {
            states.iter().map(|(id, _)| (**id, AccessDenied::ByPolicy("revoked"))).collect()
        } else {
            HashMap::new()
        }
    }
}

fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([70; 32])) }

fn state(number: u32, model: ModelId, version: u8) -> Attested<EntityState> {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&number.to_be_bytes());
    Attested::opt(
        EntityState {
            entity_id: EntityId::from_bytes(bytes),
            state: State {
                memberships: [model].into_iter().collect(),
                head: Clock::genesis(EventId::from_bytes([version; 32])),
                ..State::default()
            },
        },
        None,
    )
}

fn failure(result: Result<(), RetrievalError>, expected: ComparisonFailure) {
    assert!(matches!(result, Err(RetrievalError::StorageError(error)) if error.downcast_ref::<ComparisonFailure>() == Some(&expected)));
}

#[tokio::test]
async fn unchanged_grant_does_not_authorize_a_captured_member_that_leaves_its_tenant() -> anyhow::Result<()> {
    let tenant = PropertyId::EntityId(EntityId::from_bytes([90; 32]));
    let allowed = Predicate::Comparison {
        left: Box::new(Expr::Path(tenant.into())),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String("A".into()))),
    };
    let selection: Selection<Resolved> = Predicate::And(Box::new(Predicate::MemberOf(model())), Box::new(allowed.clone())).into();
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), Policy::new(allowed, true));
    let make = |value: &str, version| -> anyhow::Result<_> {
        let backend = LWWBackend::new();
        backend.set(tenant, Some(Value::String(value.into())));
        crate::test_helpers::commit_backend(&backend, version)?;
        let mut state = state(1, model(), version);
        state.payload.state.state_buffers.0.insert("lww".into(), backend.to_state_buffer()?);
        Ok(state)
    };
    storage.set_state(make("A", 1)?);
    let (_owner, captured, discovery) = node.capture_comparison(selection.clone(), &vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(captured.view().count(..).await?, 1);
    storage.set_state(make("B", 2)?);
    failure(
        node.validate_comparison_authorization(&selection, &captured.view(), &vec![DEFAULT_CONTEXT], &discovery, &|_| Ok(())).await,
        ComparisonFailure::Restart,
    );
    assert_eq!(node.policy_agent.checks.load(Ordering::SeqCst), 0, "the predicate proof needs no per-state policy scan");
    Ok(())
}

#[tokio::test]
async fn unchanged_large_scoped_tree_is_proved_without_enumerating_canonical_states() -> anyhow::Result<()> {
    let selection: Selection<Resolved> = Predicate::MemberOf(model()).into();
    let storage = Arc::new(TestStorage::default());
    let mut policy = Policy::new(selection.predicate.clone(), true);
    policy.limits.max_capture_members = 1;
    let node = Node::new_durable(storage.clone(), policy);
    for id in 1..=1024 {
        storage.set_state(state(id, model(), 1));
    }
    let (_owner, captured, discovery) = node.capture_comparison(selection.clone(), &vec![DEFAULT_CONTEXT]).await?;
    storage.state_point_reads.store(0, Ordering::SeqCst);
    let work = AtomicUsize::new(0);
    node.validate_comparison_authorization(&selection, &captured.view(), &vec![DEFAULT_CONTEXT], &discovery, &|steps| {
        work.fetch_add(steps, Ordering::SeqCst);
        Ok(())
    })
    .await?;
    assert_eq!(storage.state_point_reads.load(Ordering::SeqCst), 0);
    assert_eq!(node.policy_agent.checks.load(Ordering::SeqCst), 0);
    assert!(work.load(Ordering::SeqCst) < 128, "equal subtrees must skip their members");
    failure(
        node.validate_comparison_authorization(&selection, &captured.view(), &vec![DEFAULT_CONTEXT], &discovery, &|_| {
            Err(RetrievalError::storage(ComparisonFailure::LimitExceeded))
        })
        .await,
        ComparisonFailure::LimitExceeded,
    );
    Ok(())
}

#[tokio::test]
async fn scoped_proof_allows_head_changes_and_new_members() -> anyhow::Result<()> {
    let selection: Selection<Resolved> = Predicate::MemberOf(model()).into();
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), Policy::new(selection.predicate.clone(), true));
    storage.set_state(state(1, model(), 1));
    let (_owner, captured, discovery) = node.capture_comparison(selection.clone(), &vec![DEFAULT_CONTEXT]).await?;
    storage.set_state(state(1, model(), 2));
    storage.set_state(state(2, model(), 3));
    node.validate_comparison_authorization(&selection, &captured.view(), &vec![DEFAULT_CONTEXT], &discovery, &|_| Ok(())).await?;
    assert_eq!(captured.view().range(..)?.try_next().await?.unwrap().payload.0, Clock::genesis(EventId::from_bytes([1; 32])));
    Ok(())
}

#[tokio::test]
async fn catalog_bypass_does_not_authorize_members_that_leave_the_catalog() -> anyhow::Result<()> {
    let catalog = ModelId::System(proto::SystemModel::Property);
    let selection: Selection<Resolved> = Predicate::MemberOf(catalog).into();
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), Policy::new(Predicate::False, true));
    storage.set_state(state(1, catalog, 1));
    let (_owner, captured, discovery) = node.capture_comparison(selection.clone(), &vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(discovery, Predicate::True);
    storage.set_state(state(1, model(), 2));
    failure(
        node.validate_comparison_authorization(&selection, &captured.view(), &vec![DEFAULT_CONTEXT], &discovery, &|_| Ok(())).await,
        ComparisonFailure::Restart,
    );
    Ok(())
}

#[tokio::test]
async fn revocation_while_finishing_applies_to_every_previously_read_member() -> anyhow::Result<()> {
    let selection: Selection<Resolved> = Predicate::MemberOf(model()).into();
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), Policy::new(selection.predicate.clone(), false));
    // Cross the batching boundary, so this also covers members loaded in an earlier batch.
    for id in 1..=257 {
        storage.set_state(state(id, model(), 1));
    }
    let (_owner, captured, discovery) = node.capture_comparison(selection.clone(), &vec![DEFAULT_CONTEXT]).await?;
    // Keep background maintenance from consuming the snapshot-finish pause.
    let _publication = node.commit_publication_lock.lock().await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *storage.hold_state_finish.lock().unwrap() = Some((entered, resume));
    let checks = node.policy_agent.checks.load(Ordering::SeqCst);
    let worker = node.clone();
    let task = tokio::spawn(async move {
        worker.validate_comparison_authorization(&selection, &captured.view(), &vec![DEFAULT_CONTEXT], &discovery, &|_| Ok(())).await
    });
    tokio::time::timeout(Duration::from_secs(5), entering).await??;
    assert_eq!(node.policy_agent.checks.load(Ordering::SeqCst), checks, "state checks run only after the snapshot finishes");
    node.policy_agent.revoked.store(true, Ordering::SeqCst);
    release.send(()).unwrap();
    failure(task.await?, ComparisonFailure::AuthorizationChanged);
    Ok(())
}

#[test]
fn predicate_proof_requires_every_restriction_and_does_not_treat_alternatives_as_grants() {
    let first = Predicate::MemberOf(ModelId::EntityId(EntityId::from_bytes([1; 32])));
    let second = Predicate::MemberOf(ModelId::EntityId(EntityId::from_bytes([2; 32])));
    let both = Predicate::And(Box::new(first.clone()), Box::new(second.clone()));
    let either = Predicate::Or(Box::new(first.clone()), Box::new(second.clone()));
    assert!(implies(&both, &first));
    assert!(implies(&both, &Predicate::And(Box::new(second.clone()), Box::new(first.clone()))));
    assert!(!implies(&first, &both));
    assert!(!implies(&either, &first));
    assert!(!implies(&Predicate::True, &first));
}

#[test]
fn authorization_limits_include_member_identity_and_canonical_state_bytes() -> anyhow::Result<()> {
    let state = State::default();
    let one_member_bytes = 32 + bincode::serialized_size(&state)? as usize;
    for limits in [
        ComparisonLimits { max_capture_members: 1, ..Default::default() },
        ComparisonLimits { max_capture_bytes: one_member_bytes, ..Default::default() },
    ] {
        let mut budget = AuthorizationBudget::new(limits);
        budget.inspect(&state)?;
        assert!(matches!(budget.inspect(&state), Err(RetrievalError::StorageError(error))
            if error.downcast_ref::<ComparisonFailure>() == Some(&ComparisonFailure::LimitExceeded)));
    }
    let mut budget = AuthorizationBudget::new(ComparisonLimits { max_capture_bytes: one_member_bytes - 1, ..Default::default() });
    assert!(budget.inspect(&state).is_err());
    Ok(())
}

#[tokio::test]
async fn requester_rechecks_capture_authority_before_disclosing_its_initial_digest() -> anyhow::Result<()> {
    use crate::connector::{PeerSender, SendError};
    #[derive(Clone)]
    struct Closed;
    impl PeerSender for Closed {
        fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Err(SendError::ConnectionClosed) }
        fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    }
    let storage = Arc::new(TestStorage::default());
    storage.set_state(state(1, model(), 1));
    let mut policy = Policy::new(Predicate::True, false);
    policy.revoke_after = Some(1);
    let node = Node::new_durable(storage, policy);
    let peer = crate::test_utils::connect_peer(&node, &crate::node::NodeKey::generate(), false, Closed).await?;
    let result = node.reconcile_query(peer, Predicate::MemberOf(model()).into(), &vec![DEFAULT_CONTEXT], None).await;
    failure(result, ComparisonFailure::AuthorizationChanged);
    assert!(node.policy_agent.checks.load(Ordering::SeqCst) >= 2, "the captured permission must be checked again before sending");
    Ok(())
}
