use super::*;

use crate::node::comparison::wire::page_to_wire;
use ankurah_core_types::{
    key_spec::{IndexKeyPart, KeySpec},
    PropertyId, Value, ValueType,
};
use ankurah_digest_tree::{Member, MemoryStore};
use ankurah_proto::{EventId, KnownState};
use std::{collections::HashMap, convert::Infallible, time::Duration};

type Tree =
    DigestTree<TreeProperty, EntityMemberId, EntityHead, Ristretto255, MemoryStore<RowKey, Row<TreeProperty, EntityHead, Ristretto255>>>;

#[derive(Clone)]
struct Item {
    id: EntityId,
    key: i64,
    head: Clock,
}
impl Member<TreeProperty> for Item {
    type Id = EntityMemberId;
    type Payload = EntityHead;
    type KeyError = Infallible;
    fn id(&self) -> Self::Id { EntityMemberId(self.id) }
    fn value(&self, _: &IndexKeyPart<TreeProperty>) -> Result<Option<Value>, Infallible> { Ok(Some(Value::I64(self.key))) }
    fn payload(&self) -> Self::Payload { EntityHead(self.head.clone()) }
}
fn item(n: u64, key: i64, version: u8) -> Item {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    Item { id: EntityId::from_bytes(bytes), key, head: Clock::genesis(EventId::from_bytes([version; 32])) }
}
async fn tree(items: &[Item]) -> Tree {
    DigestTree::create(
        MemoryStore::new(),
        KeySpec::new(vec![IndexKeyPart::asc(TreeProperty(PropertyId::EntityId(EntityId::from_bytes([1; 32]))), ValueType::I64)]),
        items,
    )
    .await
    .unwrap()
}
async fn target(tree: &Tree, prefetch: bool) -> ComparisonTarget {
    ComparisonTarget {
        id: ComparisonId::new(),
        key_spec: key_spec_to_wire(tree.key_spec()),
        known_state: KnownState { digest: tree.view().span_digest(..).await.unwrap().digest },
        page: if prefetch { tree.view().pages(vec![Span::all()]).await.unwrap().pop().map(page_to_wire) } else { None },
    }
}

#[derive(Default)]
struct Calls {
    spans: Vec<usize>,
    repairs: Vec<(bool, Vec<RepairMember>)>,
    reads: Vec<usize>,
}
struct Mock {
    tree: Tree,
    current: Mutex<HashMap<EntityId, Clock>>,
    remote: HashMap<EntityId, Clock>,
    calls: Mutex<Calls>,
    unavailable: HashSet<EntityId>,
    need_events: bool,
    refuse_coverage: bool,
    malformed: bool,
    pending: bool,
    pending_after_repair: bool,
    reply_bytes: usize,
}
impl Mock {
    async fn new(ours: &[Item], theirs: &[Item]) -> Self {
        Self {
            tree: tree(theirs).await,
            current: Mutex::new(ours.iter().map(|item| (item.id, item.head.clone())).collect()),
            remote: theirs.iter().map(|item| (item.id, item.head.clone())).collect(),
            calls: Mutex::new(Calls::default()),
            unavailable: HashSet::new(),
            need_events: false,
            refuse_coverage: false,
            malformed: false,
            pending: false,
            pending_after_repair: false,
            reply_bytes: 1,
        }
    }
}
#[async_trait]
impl Exchange for Mock {
    async fn pages(&self, spans: Vec<Span>) -> Result<Vec<ComparisonPage>, RetrievalError> {
        self.calls.lock().unwrap().spans.push(spans.len());
        if self.pending || (self.pending_after_repair && !self.calls.lock().unwrap().repairs.is_empty()) {
            return futures::future::pending().await;
        }
        if self.malformed {
            return Ok(vec![]);
        }
        Ok(self.tree.view().pages(spans).await.map_err(view_error)?.into_iter().map(page_to_wire).collect())
    }
    async fn heads(&self, ids: &[EntityId]) -> Result<Vec<KnownEntity>, RetrievalError> {
        self.calls.lock().unwrap().reads.push(ids.len());
        let current = self.current.lock().unwrap();
        Ok(ids.iter().map(|&entity_id| KnownEntity { entity_id, head: current.get(&entity_id).cloned().unwrap_or_default() }).collect())
    }
    async fn repair(&self, members: Vec<RepairMember>, events_only: bool) -> Result<RepairReceipt, RetrievalError> {
        self.calls.lock().unwrap().repairs.push((events_only, members.clone()));
        let mut unavailable = Vec::new();
        for member in members {
            let id = member.known.entity_id;
            match self.remote.get(&id).filter(|_| !self.unavailable.contains(&id)) {
                Some(head) if !self.need_events || events_only => {
                    self.current.lock().unwrap().insert(id, head.clone());
                }
                Some(_) => {}
                None => unavailable.push(id),
            }
        }
        Ok(RepairReceipt { unavailable, bytes: self.reply_bytes, events: usize::from(events_only) })
    }
    async fn covers(&self, _: EntityId, local: &Clock, remote: &Clock) -> Result<bool, RetrievalError> {
        Ok(!self.refuse_coverage && local == remote)
    }
}
fn budget() -> ComparisonBudget { ComparisonBudget::new(ComparisonLimits::default()) }
fn assert_failure(error: RetrievalError, expected: ComparisonFailure) {
    let RetrievalError::StorageError(error) = error else { panic!("unexpected error: {error:?}") };
    assert_eq!(error.downcast_ref::<ComparisonFailure>(), Some(&expected));
}

#[tokio::test]
async fn equal_root_and_wrong_key_spec_do_no_network_work() {
    let items = [item(1, 1, 1)];
    let ours = tree(&items).await;
    let remote = Mock::new(&items, &items).await;
    compare(&ours, target(&remote.tree, false).await, &remote, &budget()).await.unwrap();
    assert!(remote.calls.lock().unwrap().spans.is_empty());
    assert!(remote.calls.lock().unwrap().reads.is_empty());
    let mut wrong = target(&remote.tree, false).await;
    wrong.key_spec.keyparts.clear();
    assert_failure(compare(&ours, wrong, &remote, &budget()).await.unwrap_err(), ComparisonFailure::KeySpecMismatch);
    assert!(remote.calls.lock().unwrap().repairs.is_empty());
}

#[tokio::test]
async fn incremental_repairs_batch_pages_and_re_read_current_heads() {
    let items: Vec<_> = (1..=180).map(|n| item(n, n as i64, 2)).collect();
    let ours = tree(&[]).await;
    let remote = Mock::new(&[], &items).await;
    let limits = ComparisonLimits { max_spans: 8, max_members: 8, ..ComparisonLimits::default() };
    compare(&ours, target(&remote.tree, true).await, &remote, &ComparisonBudget::new(limits)).await.unwrap();
    let calls = remote.calls.lock().unwrap();
    assert!(calls.spans.iter().all(|&n| n <= 8));
    assert!(calls.spans.iter().any(|&n| n > 1), "descent should request siblings together");
    assert!(calls.repairs.iter().all(|(_, members)| members.len() <= 8));
    assert!(calls.reads.iter().all(|&n| n <= 8));
    assert_eq!(remote.current.lock().unwrap().len(), items.len());
}

#[tokio::test]
async fn moved_member_is_reconciled_idempotently_across_batches() {
    let old = item(1, 1, 1);
    let moved = item(1, 100, 2);
    let ours = tree(&[old.clone()]).await;
    let remote = Mock::new(&[old], &[moved.clone()]).await;
    let limits = ComparisonLimits { max_spans: 1, max_members: 1, ..ComparisonLimits::default() };
    compare(&ours, target(&remote.tree, false).await, &remote, &ComparisonBudget::new(limits)).await.unwrap();
    assert_eq!(remote.current.lock().unwrap().get(&moved.id), Some(&moved.head));
    assert!(remote.calls.lock().unwrap().repairs.iter().all(|(_, members)| members.len() == 1));
}

#[tokio::test]
async fn empty_remote_enumeration_is_bounded_and_charged() {
    let items: Vec<_> = (1..=100).map(|n| item(n, n as i64, 1)).collect();
    let ours = tree(&items).await;
    let remote = Mock::new(&items, &[]).await;
    let limits = ComparisonLimits { max_spans: 4, max_members: 4, max_steps: 19, ..ComparisonLimits::default() };
    let error = compare(&ours, target(&remote.tree, true).await, &remote, &ComparisonBudget::new(limits)).await.unwrap_err();
    assert_failure(error, ComparisonFailure::LimitExceeded);
    let calls = remote.calls.lock().unwrap();
    assert!(calls.spans.is_empty(), "initial empty page needs no more network pages");
    assert!(calls.reads.iter().all(|&n| n <= 4));
    assert!(calls.repairs.len() < 25, "work limit must stop local-only traversal");
    assert_eq!(remote.current.lock().unwrap().len(), 100, "missing remote members never delete local history");
}

#[tokio::test]
async fn coverage_requires_events_when_state_delivery_does_not_suffice() {
    let ours = tree(&[item(1, 1, 1)]).await;
    let mut remote = Mock::new(&[item(1, 1, 1)], &[item(1, 1, 2)]).await;
    remote.need_events = true;
    compare(&ours, target(&remote.tree, false).await, &remote, &budget()).await.unwrap();
    assert_eq!(remote.calls.lock().unwrap().repairs.iter().map(|(events, _)| *events).collect::<Vec<_>>(), vec![false, true]);
    remote.refuse_coverage = true;
    let error = compare(&ours, target(&remote.tree, false).await, &remote, &budget()).await.unwrap_err();
    let RetrievalError::StorageError(error) = error else { panic!("{error:?}") };
    assert!(matches!(error.downcast_ref::<ComparisonFailure>(), Some(ComparisonFailure::InvalidRequest(_))));
}

#[tokio::test]
async fn unavailable_required_member_is_distinct_from_expiry_and_malformed_pages_never_repair() {
    let ours = tree(&[]).await;
    let added = item(1, 1, 1);
    let mut remote = Mock::new(&[], &[added.clone()]).await;
    remote.unavailable.insert(added.id);
    assert_failure(
        compare(&ours, target(&remote.tree, false).await, &remote, &budget()).await.unwrap_err(),
        ComparisonFailure::MemberUnavailable,
    );
    remote.calls.lock().unwrap().repairs.clear();
    remote.malformed = true;
    let error = compare(&ours, target(&remote.tree, false).await, &remote, &budget()).await.unwrap_err();
    let RetrievalError::StorageError(error) = error else { panic!("{error:?}") };
    assert!(matches!(error.downcast_ref::<ComparisonError<RetrievalError>>(), Some(ComparisonError::MalformedPage(_))));
    assert!(remote.calls.lock().unwrap().repairs.is_empty());
}

#[tokio::test]
async fn deadline_cancels_pending_pages_and_delivery_bytes_are_enforced() {
    let ours = tree(&[]).await;
    let mut remote = Mock::new(&[], &[item(1, 1, 1)]).await;
    remote.pending = true;
    let limits = ComparisonLimits { lifetime: Duration::from_millis(25), ..ComparisonLimits::default() };
    assert_failure(
        compare(&ours, target(&remote.tree, false).await, &remote, &ComparisonBudget::new(limits)).await.unwrap_err(),
        ComparisonFailure::Expired,
    );
    assert_eq!(remote.calls.lock().unwrap().spans.len(), 1);
    assert!(remote.calls.lock().unwrap().repairs.is_empty());
    remote.pending = false;
    remote.reply_bytes = ComparisonLimits::default().max_bytes + 1;
    assert_failure(
        compare(&ours, target(&remote.tree, false).await, &remote, &budget()).await.unwrap_err(),
        ComparisonFailure::LimitExceeded,
    );
}

#[tokio::test]
async fn renewed_capture_does_not_renew_work_allowance() {
    let ours = tree(&[]).await;
    let remote = Mock::new(&[], &[item(1, 1, 1)]).await;
    let budget = ComparisonBudget::new(ComparisonLimits { max_steps: 1, ..ComparisonLimits::default() });
    let first = compare(&ours, target(&remote.tree, false).await, &remote, &budget).await.unwrap_err();
    assert_failure(first, ComparisonFailure::LimitExceeded);
    let requests = remote.calls.lock().unwrap().spans.len();
    assert_failure(
        compare(&ours, target(&remote.tree, false).await, &remote, &budget).await.unwrap_err(),
        ComparisonFailure::LimitExceeded,
    );
    assert_eq!(remote.calls.lock().unwrap().spans.len(), requests);
}

#[test]
fn unavailable_local_capture_restarts_but_permanent_storage_failure_does_not() {
    assert_failure(
        view_error(ankurah_digest_tree::Error::Store(ankurah_digest_tree::StoreError::retryable("capture history expired"))),
        ComparisonFailure::Restart,
    );
    let error = view_error(ankurah_digest_tree::Error::Store(ankurah_digest_tree::StoreError::permanent("damaged tree row")));
    let RetrievalError::StorageError(error) = error else { panic!("{error:?}") };
    assert!(error.downcast_ref::<ComparisonFailure>().is_none());
}

#[tokio::test]
async fn cancellation_retains_completed_repairs_and_retry_rechecks_them() {
    let items: Vec<_> = (1..=180).map(|n| item(n, n as i64, 2)).collect();
    let ours = tree(&[]).await;
    let mut remote = Mock::new(&[], &items).await;
    remote.pending_after_repair = true;
    let target = target(&remote.tree, false).await;
    let limits = ComparisonLimits { max_spans: 4, max_members: 4, lifetime: Duration::from_millis(50), ..ComparisonLimits::default() };
    assert_failure(compare(&ours, target.clone(), &remote, &ComparisonBudget::new(limits)).await.unwrap_err(), ComparisonFailure::Expired);
    let completed = remote.current.lock().unwrap().len();
    assert!(completed > 0 && completed < items.len());
    remote.pending_after_repair = false;
    compare(&ours, target, &remote, &budget()).await.unwrap();
    assert_eq!(remote.current.lock().unwrap().len(), items.len());
    let repairs = remote.calls.lock().unwrap();
    let sent: Vec<_> = repairs.repairs.iter().flat_map(|(_, members)| members.iter().map(|member| member.known.entity_id)).collect();
    assert_eq!(sent.len(), sent.iter().copied().collect::<HashSet<_>>().len(), "completed repairs must be recognized on retry");
}

#[tokio::test]
async fn reply_bytes_consume_cumulative_work_even_under_per_reply_ceiling() {
    let items: Vec<_> = (1..=10).map(|n| item(n, n as i64, 2)).collect();
    let ours = tree(&[]).await;
    let mut remote = Mock::new(&[], &items).await;
    remote.reply_bytes = 32 * 1024;
    let limits = ComparisonLimits { max_members: 1, max_spans: 1, max_steps: 60, ..ComparisonLimits::default() };
    assert_failure(
        compare(&ours, target(&remote.tree, true).await, &remote, &ComparisonBudget::new(limits)).await.unwrap_err(),
        ComparisonFailure::LimitExceeded,
    );
    assert!(remote.current.lock().unwrap().len() < items.len());
}

#[tokio::test]
async fn claims_are_chunked_by_identity_for_single_member_repair_policy() {
    let local = item(1, 1, 1);
    let remote_item = item(2, 2, 1);
    let remote = Mock::new(&[local.clone()], &[remote_item.clone()]).await;
    let leaf = |item: &Item| ankurah_digest_tree::Leaf { key: vec![], id: EntityMemberId(item.id), payload: EntityHead(item.head.clone()) };
    let differences = vec![Difference { ours: Some(leaf(&local)), theirs: Some(leaf(&remote_item)) }];
    let limits = ComparisonLimits { max_members: 1, ..ComparisonLimits::default() };
    reconcile(differences, &remote, &ComparisonBudget::new(limits)).await.unwrap();
    let calls = remote.calls.lock().unwrap();
    assert!(calls.reads.iter().all(|&count| count == 1));
    assert!(calls.repairs.iter().all(|(_, members)| members.len() == 1));
    assert_eq!(remote.current.lock().unwrap().len(), 2);
}

#[test]
fn repair_disclosure_uses_fresh_authorization_and_never_leaks_denied_heads() {
    let old = item(1, 1, 1);
    let current = item(1, 1, 2);
    let private_remote = item(2, 2, 3);
    let private_local = item(3, 3, 4);
    let missing = item(4, 4, 5);
    let member = |item: &Item, remote: bool| RepairMember {
        known: KnownEntity { entity_id: item.id, head: item.head.clone() },
        key: remote.then(|| vec![1]),
    };
    let state = |item: &Item| {
        Some(ankurah_proto::Attested::opt(
            ankurah_proto::EntityState {
                entity_id: item.id,
                state: ankurah_proto::State { head: item.head.clone(), ..Default::default() },
            },
            None,
        ))
    };
    let members = vec![member(&old, true), member(&private_remote, true), member(&private_local, false), member(&missing, true)];
    let states = vec![state(&current), state(&private_remote), state(&private_local), None];
    // Authorization is evaluated on the refreshed state, after all snapshot
    // awaits, not on the older known head assembled by the comparison loop.
    let (disclosed, withheld) = disclosable_members(members, states, |id, state| *id == current.id && state.head == current.head).unwrap();
    assert_eq!(withheld, vec![private_local.id]);
    assert_eq!(disclosed.len(), 3);
    assert_eq!(disclosed[0].known.head, current.head);
    assert_eq!(disclosed[1].known.entity_id, private_remote.id);
    assert!(disclosed[1].known.head.is_empty());
    assert!(disclosed[2].known.head.is_empty());
    assert!(disclosed.iter().all(|member| member.known.entity_id != private_local.id));
}

#[test]
fn repair_disclosure_rejects_mismatched_canonical_state_answers() {
    let known = item(1, 1, 1);
    let other = item(2, 2, 2);
    let member = RepairMember { known: KnownEntity { entity_id: known.id, head: known.head }, key: Some(vec![1]) };
    let state = Some(ankurah_proto::Attested::opt(
        ankurah_proto::EntityState { entity_id: other.id, state: ankurah_proto::State { head: other.head, ..Default::default() } },
        None,
    ));
    assert!(disclosable_members(vec![member.clone()], vec![state], |_, _| true).is_err());
    assert!(disclosable_members(vec![member], vec![], |_, _| true).is_err());
}
