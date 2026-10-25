use super::*;
use super::{actor::TreeActor, identity::Identity};
use crate::{
    entity::TemporaryEntity, node::state_sync::tree::Snapshot, schema::catalog::CatalogManager, session::ContextData, value::Value,
};
use crate::{
    error::{MutationError, RetrievalError},
    node::applier::NodeApplier,
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    selection::filter::Filterable,
    test_utils::{TestStorage, TestStorageTransaction},
};
use ankql::ast::{OrderByItem, OrderDirection, Predicate};
use ankurah_core_types::PropertyPath;
use ankurah_proto::Attested;
use std::sync::atomic::Ordering;
use std::{collections::BTreeSet, sync::Weak};

mod authorization;
mod bootstrap;
mod builds;
mod digests;
mod lru;
mod retry;
mod sharing;

/// The core test engine deliberately rejects ORDER BY/LIMIT. This fixture adds
/// exactly the ordered bounded read exercised here, leaving its real commit and
/// pause seams intact, and counts the reads it serves.
#[derive(Default)]
struct OrderedStorage {
    inner: TestStorage,
    /// Pauses the next `fetch_states` until released.
    fetch_pause: std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    /// `fetch_states` calls.
    fetches: std::sync::atomic::AtomicUsize,
    /// Pauses the next `get_events` until released.
    events_pause: std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    /// Rows `fetch_states` returned to callers, after the storage LIMIT.
    returned_rows: std::sync::atomic::AtomicUsize,
    /// Individual state reads, including those each `get_states` call makes.
    state_reads: std::sync::atomic::AtomicUsize,
    /// `get_states` calls.
    state_batches: std::sync::atomic::AtomicUsize,
    /// `get_events` calls.
    event_reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl StorageEngine for OrderedStorage {
    type Value = Value;
    type Transaction<'a> = TestStorageTransaction<'a>;
    fn transaction(&self) -> Self::Transaction<'_> { self.inner.transaction() }
    async fn get_state(&self, id: proto::EntityId) -> Result<Attested<proto::EntityState>, RetrievalError> {
        self.state_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.get_state(id).await
    }
    async fn get_states(
        &self,
        ids: Vec<proto::EntityId>,
        predicate: &Predicate<Resolved>,
    ) -> Result<Vec<crate::storage::GetStateResult>, RetrievalError> {
        self.state_batches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut states = Vec::with_capacity(ids.len());
        for id in ids {
            states.push(match self.get_state(id).await {
                Ok(state) => crate::storage::GetStateResult::matching(state, predicate)?,
                Err(RetrievalError::EntityNotFound(_)) => crate::storage::GetStateResult::NotFound(id),
                Err(error) => return Err(error),
            });
        }
        Ok(states)
    }
    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<Attested<proto::EntityState>>, RetrievalError> {
        self.fetches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pause = self.fetch_pause.lock().unwrap().take();
        if let Some((entered, resume)) = pause {
            let _ = entered.send(());
            let _ = resume.await;
        }
        let mut scan = selection.clone();
        scan.order_by = None;
        scan.limit = None;
        let states = self.inner.fetch_states(&scan).await?;
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
        self.returned_rows.fetch_add(rows.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(rows.into_iter().map(|(_, _, state)| state).collect())
    }
    async fn get_events(&self, ids: Vec<proto::EventId>) -> Result<Vec<Attested<proto::Event>>, RetrievalError> {
        self.event_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pause = self.events_pause.lock().unwrap().take();
        if let Some((entered, resume)) = pause {
            let _ = entered.send(());
            let _ = resume.await;
        }
        self.inner.get_events(ids).await
    }
    async fn dump_entity_events(&self, id: proto::EntityId) -> Result<Vec<Attested<proto::Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
    }
    async fn delete_all(&self) -> Result<bool, MutationError> { self.inner.delete_all().await }
    async fn list_materializations(&self) -> Result<Vec<proto::ModelId>, RetrievalError> { self.inner.list_materializations().await }
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
    NodeApplier::apply_deltas(
        node,
        &proto::EntityId::from_bytes([24; 32]),
        vec![proto::EntityDelta {
            entity_id: event.entity_id,
            content: proto::DeltaContent::EventBridge { events: vec![Attested::from(event.clone()).into()] },
        }],
        &events,
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    Ok(())
}
/// A complete resident tree admitting `spec`'s whole predicate under `credentials`,
/// bootstrapped as the LRU builds one for an unlimited selection.
async fn complete_tree<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    node: &Node<OrderedStorage, PA>,
    spec: proto::TreeSpec,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<TreeActor<PA::ContextData>> {
    node.system.require_system_ready()?;
    let selection = Selection::from(spec.predicate().clone());
    TreeActor::bootstrap(node, Identity::requested(node, selection, spec, credentials)?.admission()).await
}

/// A resident tree bootstrapped as the LRU builds one for `selection` under the
/// default credentials: limited when `selection` has a limit.
async fn bootstrap_tree<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    node: &Node<OrderedStorage, PA>,
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
) -> anyhow::Result<TreeActor<PA::ContextData>> {
    TreeActor::bootstrap(node, Identity::requested(node, selection, spec, vec![DEFAULT_CONTEXT])?.admission()).await
}

/// The view `selection` selects from `actor`'s tree as it stands.
fn view<CD: ContextData>(actor: &TreeActor<CD>, selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<Snapshot> {
    actor.identity.coverage.select(actor.tree.snapshot(), selection, catalog)
}

impl<CD: ContextData> TreeLease<CD> {
    /// Which resident tree this lease holds, for [`Weak::ptr_eq`]. The weak
    /// reference keeps the tree's allocation, so no later tree
    /// compares equal to it.
    pub(crate) fn identity(&self) -> Weak<dyn std::any::Any + Send + Sync> {
        let tree: Weak<dyn std::any::Any + Send + Sync> = Arc::downgrade(&self.tree) as _;
        tree
    }
}

impl<CD: ContextData> ResidentTrees<CD> {
    /// How many resident trees the LRU holds, pinned or idle.
    pub(crate) fn len(&self) -> usize { self.lru.lock().unwrap().slots.len() }

    /// Evict every idle tree at once.
    pub(crate) fn evict_idle(&self) { self.lru.lock().unwrap().slots.retain(|slot| slot.tree.pins.load(Ordering::Acquire) != 0); }
}

fn hold_fetch(node: &TestNode) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered, observed) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    node.storage.inner.hold_fetch.lock().unwrap().insert(model(), (entered, resume));
    (observed, release)
}

#[derive(Clone, Default)]
struct RestrictedDiscovery {
    retrieval: Option<Predicate<Resolved>>,
    read_allowed: Option<Arc<std::sync::atomic::AtomicBool>>,
    discover_all: Option<Arc<std::sync::atomic::AtomicBool>>,
    deny_empty: bool,
    state_free_reads: bool,
    state_free_proofs: Option<Arc<std::sync::atomic::AtomicUsize>>,
    state_checks: Option<Arc<std::sync::atomic::AtomicUsize>>,
    retrieval_error: bool,
    /// When set, only credential lists no longer than this hold the proof.
    proof_up_to: Option<Arc<std::sync::atomic::AtomicUsize>>,
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
            && self.proof_up_to.as_ref().is_none_or(|longest| data.iterable().count() <= longest.load(std::sync::atomic::Ordering::SeqCst))
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

async fn acquire<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    cache: &ResidentTrees<PA::ContextData>,
    node: &Node<OrderedStorage, PA>,
    selection: Selection<Resolved>,
) -> anyhow::Result<crate::node::state_sync::resident_trees::TreeLease<PA::ContextData>> {
    cache.acquire(node, selection, None, vec![DEFAULT_CONTEXT]).await
}

fn above(value: &str, limit: Option<u64>) -> Selection<Resolved> {
    use ankql::ast::{ComparisonOperator, Expr};
    let mut selected = selection(limit);
    selected.predicate = Predicate::And(
        Box::new(selected.predicate),
        Box::new(Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::from(property()))),
            operator: ComparisonOperator::GreaterThan,
            right: Box::new(Expr::Literal(Value::String(value.into()))),
        }),
    );
    selected
}
