use super::*;

use std::{ops::Bound, time::Duration};

use ankurah_core_types::{key_spec::IndexKeyPart, ValueType};
use ankurah_digest_tree::{Digest, Ristretto255};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, State};

use crate::{
    connector::{PeerSender, SendError},
    policy::{DefaultContext, PermissiveAgent, DEFAULT_CONTEXT},
    test_utils::TestStorage,
};

#[derive(Clone)]
struct Sender;

impl PeerSender for Sender {
    fn send_message(&self, _: ankurah_proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn fixture() -> anyhow::Result<(Node<TestStorage, PermissiveAgent>, ConnectionId, Selection<Resolved>)> {
    fixture_with_key(&crate::node::NodeKey::generate()).await
}

async fn fixture_with_key(
    key: &crate::node::NodeKey,
) -> anyhow::Result<(Node<TestStorage, PermissiveAgent>, ConnectionId, Selection<Resolved>)> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let peer = crate::test_utils::connect_peer(&node, key, false, Sender).await?;
    let model = ModelId::EntityId(EntityId::from_bytes([70; 32]));
    storage.set_state(Attested::opt(
        EntityState {
            entity_id: EntityId::from_bytes([71; 32]),
            state: State {
                memberships: [model].into_iter().collect(),
                head: Clock::genesis(EventId::from_bytes([72; 32])),
                ..State::default()
            },
        },
        None,
    ));
    Ok((node, peer, Predicate::MemberOf(model).into()))
}

fn known() -> KnownState { KnownState { digest: Digest::<Ristretto255>::empty() } }

fn refusal<T>(result: Result<T, RetrievalError>, expected: ComparisonFailure) {
    assert!(matches!(result, Err(RetrievalError::StorageError(error)) if error.downcast_ref::<ComparisonFailure>() == Some(&expected)));
}

#[tokio::test]
async fn reservations_bound_in_flight_work_and_cancel_without_consuming_capacity() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let connection = node.peer_connections.get(&peer).unwrap();
    let captures = Captures::<TestStorage, &'static DefaultContext>::default();
    let limits = ComparisonLimits { max_captures: 1, ..Default::default() };
    let id = ComparisonId::new();
    let first = captures.reserve((peer, id), &connection, selection.clone(), vec![DEFAULT_CONTEXT], limits)?;
    refusal(
        captures.reserve((peer, ComparisonId::new()), &connection, selection.clone(), vec![DEFAULT_CONTEXT], limits),
        ComparisonFailure::LimitExceeded,
    );
    assert!(captures.reserve((peer, id), &connection, selection.clone(), vec![DEFAULT_CONTEXT], limits).is_err());
    drop(first);
    assert!(captures.inner.lock().unwrap().is_empty());
    let second = captures.reserve((peer, id), &connection, selection, vec![DEFAULT_CONTEXT], limits)?;
    drop(second);
    assert!(captures.inner.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn expiry_releases_the_captured_tree_without_another_request() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let captures = Captures::<TestStorage, &'static DefaultContext>::default();
    let limits = ComparisonLimits { lifetime: Duration::from_millis(20), ..Default::default() };
    let connection = node.peer_connections.get(&peer).unwrap();
    let mut reservation = captures.reserve((peer, ComparisonId::new()), &connection, selection.clone(), vec![DEFAULT_CONTEXT], limits)?;
    let (owner, captured, discovery) = node.capture_comparison(selection.clone(), &vec![DEFAULT_CONTEXT]).await?;
    let weak_tree = captured.downgrade();
    reservation.slot.retained.set(Retained { _owner: owner, view: captured.view(), discovery, selection }).ok().unwrap();
    drop(captured);
    reservation.installed = true;
    drop(reservation);
    assert!(weak_tree.upgrade().is_some());
    let weak_node = node.weak();
    drop(node);
    assert!(weak_node.upgrade().is_none(), "expiry tasks and retained captures do not own the Node");
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak_tree.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert!(captures.inner.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn cancelled_begin_releases_its_reserved_slot() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (_release, resume) = tokio::sync::oneshot::channel();
    *node.storage.hold_state_snapshot.lock().unwrap() = Some((entered, resume));
    let worker = node.clone();
    let id = ComparisonId::new();
    let task =
        tokio::spawn(
            async move { worker.begin_comparison(peer, id, selection, KeySpec::new(vec![]), known(), &vec![DEFAULT_CONTEXT]).await },
        );
    entering.await?;
    assert_eq!(node.captures.inner.lock().unwrap().len(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(node.captures.inner.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn releasing_a_pending_begin_cancels_capture_without_waiting_for_expiry() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (_release, resume) = tokio::sync::oneshot::channel();
    *node.storage.hold_state_snapshot.lock().unwrap() = Some((entered, resume));
    let worker = node.clone();
    let id = ComparisonId::new();
    let task =
        tokio::spawn(
            async move { worker.begin_comparison(peer, id, selection, KeySpec::new(vec![]), known(), &vec![DEFAULT_CONTEXT]).await },
        );
    entering.await?;
    node.release_comparison(peer, id);
    refusal(tokio::time::timeout(Duration::from_secs(1), task).await??, ComparisonFailure::Expired);
    assert!(node.captures.inner.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn pages_bind_credentials_selection_and_the_peer_connection() -> anyhow::Result<()> {
    for changed in ["credentials", "selection", "connection"] {
        let (node, peer, selection) = fixture().await?;
        let id = ComparisonId::new();
        let target = node.begin_comparison(peer, id, selection.clone(), KeySpec::new(vec![]), known(), &vec![DEFAULT_CONTEXT]).await?;
        assert_eq!(target.known_state.digest.count(), 1);
        assert!(matches!(target.page, Some(Page::Members(ref members)) if members.len() == 1));
        let pages = node.comparison_pages(peer, id, &selection, vec![Span::all()], &vec![DEFAULT_CONTEXT]).await?;
        assert!(matches!(&pages[0], Page::Members(members) if members.len() == 1));
        let mut credentials = vec![DEFAULT_CONTEXT];
        let mut requested = selection;
        match changed {
            "credentials" => credentials.clear(),
            "selection" => requested.limit = Some(1),
            "connection" => node.deregister_peer(peer),
            _ => unreachable!(),
        }
        assert!(node.comparison_pages(peer, id, &requested, vec![Span::all()], &credentials).await.is_err());
        assert!(node.captures.inner.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn same_key_connections_keep_independent_captures_through_release_and_disconnect() -> anyhow::Result<()> {
    let key = crate::node::NodeKey::generate();
    let (node, first, selection) = fixture_with_key(&key).await?;
    let second = crate::test_utils::connect_peer(&node, &key, false, Sender).await?;
    assert_ne!(first, second);
    assert_eq!(node.peer_node_id(first)?, node.peer_node_id(second)?);
    let id = ComparisonId::new();
    let credentials = vec![DEFAULT_CONTEXT];
    let target = node.begin_comparison(first, id, selection.clone(), KeySpec::new(vec![]), known(), &credentials).await?;
    let expected = vec![target.page.expect("the capture contains one member")];
    refusal(node.comparison_pages(second, id, &selection, vec![Span::all()], &credentials).await, ComparisonFailure::Expired);
    node.handle_message(second, ankurah_proto::NodeMessage::ReleaseComparison { from: key.public_key().node_id(), comparison: id }).await?;
    assert_eq!(node.comparison_pages(first, id, &selection, vec![Span::all()], &credentials).await?, expected);

    // A repeated comparison id on another physical connection names its own capture.
    node.begin_comparison(second, id, selection.clone(), KeySpec::new(vec![]), known(), &credentials).await?;
    assert_eq!(node.captures.inner.lock().unwrap().len(), 2);
    node.deregister_peer(first);
    assert_eq!(node.captures.inner.lock().unwrap().len(), 1);
    refusal(node.comparison_pages(first, id, &selection, vec![Span::all()], &credentials).await, ComparisonFailure::Expired);
    assert_eq!(node.comparison_pages(second, id, &selection, vec![Span::all()], &credentials).await?, expected);
    node.handle_message(second, ankurah_proto::NodeMessage::ReleaseComparison { from: key.public_key().node_id(), comparison: id }).await?;
    assert!(node.captures.inner.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn mismatched_key_specs_release_admission_and_equal_digests_omit_the_page() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    refusal(
        node.begin_comparison(
            peer,
            ComparisonId::new(),
            selection.clone(),
            KeySpec::new(vec![IndexKeyPart::asc(PropertyId::Id, ValueType::EntityId)]),
            known(),
            &vec![DEFAULT_CONTEXT],
        )
        .await,
        ComparisonFailure::KeySpecMismatch,
    );
    assert!(node.captures.inner.lock().unwrap().is_empty());
    let id = ComparisonId::new();
    let first = node.begin_comparison(peer, id, selection.clone(), KeySpec::new(vec![]), known(), &vec![DEFAULT_CONTEXT]).await?;
    node.captures.release(peer, id);
    let equal = node
        .begin_comparison(peer, ComparisonId::new(), selection, KeySpec::new(vec![]), first.known_state, &vec![DEFAULT_CONTEXT])
        .await?;
    assert!(equal.page.is_none());
    Ok(())
}

#[tokio::test]
async fn concurrent_requests_share_one_work_budget() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let id = ComparisonId::new();
    node.begin_comparison(peer, id, selection, KeySpec::new(vec![]), known(), &vec![DEFAULT_CONTEXT]).await?;
    let limits = ComparisonLimits { max_steps: 2, ..Default::default() };
    let first = node.captures.get(peer, id, limits)?;
    first.slot.steps.store(0, Ordering::Release);
    let second = node.captures.get(peer, id, limits)?;
    first.charge(1, 0)?;
    second.charge(1, 0)?;
    refusal(first.charge(0, 1), ComparisonFailure::LimitExceeded);
    refusal(second.check_active(), ComparisonFailure::Expired);
    assert!(node.captures.inner.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn malformed_and_oversized_span_batches_are_refused() {
    let limits = ComparisonLimits { max_spans: 1, max_bytes: 128, ..Default::default() };
    assert!(validate_spans(&[Span::all()], limits).is_ok());
    assert!(validate_spans(&[Span::all(), Span::all()], limits).is_err());
    for span in [
        Span::new(Bound::Included(vec![2]), Bound::Included(vec![1])),
        Span::new(Bound::Excluded(vec![1]), Bound::Included(vec![1])),
        Span::new(Bound::Included(vec![1; 256]), Bound::Unbounded),
    ] {
        assert!(validate_spans(&[span], limits).is_err());
    }
    assert!(validate_spans(&[Span::new(Bound::Included(vec![1]), Bound::Included(vec![1]))], limits).is_ok());
}

#[tokio::test]
async fn the_original_selection_binds_a_separately_scoped_capture() -> anyhow::Result<()> {
    let (node, peer, scoped) = fixture().await?;
    let requested: Selection<Resolved> = Predicate::True.into();
    let id = ComparisonId::new();
    node.begin_scoped_comparison(peer, id, requested.clone(), scoped.clone(), KeySpec::new(vec![]), known(), &vec![DEFAULT_CONTEXT])
        .await?;
    let pages = node.comparison_pages(peer, id, &requested, vec![Span::all()], &vec![DEFAULT_CONTEXT]).await?;
    assert!(matches!(&pages[0], Page::Members(members) if members.len() == 1));
    refusal(
        node.comparison_pages(peer, id, &scoped, vec![Span::all()], &vec![DEFAULT_CONTEXT]).await,
        ComparisonFailure::AuthorizationChanged,
    );
    Ok(())
}

#[tokio::test]
async fn pages_keep_the_captured_head_across_flushes_and_restart_when_history_expires() -> anyhow::Result<()> {
    use crate::storage::StorageEngine;
    let (node, peer, selection) = fixture().await?;
    let id = ComparisonId::new();
    let target = node.begin_comparison(peer, id, selection.clone(), KeySpec::new(vec![]), known(), &vec![DEFAULT_CONTEXT]).await?;
    let original = target.page.unwrap();
    let mut state = node.storage.get_state(EntityId::from_bytes([71; 32])).await?;
    for generation in 0..66u8 {
        state.payload.state.head = Clock::genesis(EventId::from_bytes([generation; 32]));
        node.storage.set_state(state.clone());
        node.invalidate_trees();
        node.flush_trees().await?;
        if generation == 0 {
            let pages = node.comparison_pages(peer, id, &selection, vec![Span::all()], &vec![DEFAULT_CONTEXT]).await?;
            assert_eq!(pages, vec![original.clone()]);
        }
    }
    refusal(node.comparison_pages(peer, id, &selection, vec![Span::all()], &vec![DEFAULT_CONTEXT]).await, ComparisonFailure::Restart);
    Ok(())
}
