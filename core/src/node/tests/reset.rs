use super::{register_keyed_peer, RecordingSender};
use crate::connector::PeerProvenance;
use crate::error::{MutationError, NodeHaltReason, RetrievalError};
use crate::node::handler::commit_transaction;
use crate::node::{Node, NodeKey, NodeState};
use crate::policy::{PermissiveAgent, DEFAULT_CONTEXT};
use crate::retrieval::{CachedEventGetter, GetEvents};
use crate::schema::catalog::SysModelRow;
use crate::storage::StorageEngine;
use crate::test_utils::TestStorage;
use ankurah_proto as proto;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tokio::sync::oneshot;

type TestNode = Node<TestStorage, PermissiveAgent>;

/// Return once every other task on this runtime is blocked: under a paused clock, a timer fires
/// only when the runtime has nothing else to run.
async fn settle() { tokio::time::sleep(Duration::from_secs(1)).await }

async fn ready_durable_node(storage: &Arc<TestStorage>) -> TestNode {
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.wait_ready().await.unwrap();
    node
}

/// Commit one entity through the node's local commit path.
async fn commit_row(node: &TestNode, label: &str) -> Result<proto::EntityId, MutationError> {
    let transaction = node.privileged_context().begin();
    let id = transaction.create(&SysModelRow { label: label.into(), name: label.into() }).await?.id();
    transaction.commit().await?;
    Ok(id)
}

#[tokio::test(start_paused = true)]
async fn a_reset_polled_once_wipes_the_store_though_its_caller_stops_waiting() {
    let storage = Arc::new(TestStorage::default());
    let node = ready_durable_node(&storage).await;
    let id = commit_row(&node, "wiped").await.unwrap();

    let mut reset = Box::pin(node.reset());
    assert!(futures::poll!(&mut reset).is_pending());
    drop(reset);
    settle().await;
    assert_eq!(node.state().value(), NodeState::Halted(NodeHaltReason::Reset));
    assert_eq!(storage.delete_all_calls(), 1);
    assert!(matches!(storage.get_state(id).await, Err(RetrievalError::EntityNotFound(_))));
}

#[tokio::test(start_paused = true)]
async fn a_reset_waits_for_a_storage_write_in_flight_and_refuses_one_begun_after_its_halt() {
    let storage = Arc::new(TestStorage::default());
    let node = ready_durable_node(&storage).await;
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    *storage.hold_commit.lock().unwrap() = Some((entered, release_rx));
    let commit = tokio::spawn({
        let node = node.clone();
        async move { commit_row(&node, "in flight").await }
    });
    entered_rx.await.unwrap();

    let reset = tokio::spawn({
        let node = node.clone();
        async move { node.reset().await }
    });
    settle().await;
    assert_eq!(storage.delete_all_calls(), 0, "the wipe waits for the write in flight");
    assert!(!reset.is_finished());
    assert!(matches!(node.lifecycle.begin_storage_write(), Err(NodeHaltReason::Reset)), "no write begins after the halt");

    release.send(()).unwrap();
    let id = commit.await.unwrap().unwrap();
    reset.await.unwrap().unwrap();
    assert_eq!(storage.delete_all_calls(), 1);
    assert!(matches!(storage.get_state(id).await, Err(RetrievalError::EntityNotFound(_))), "the write landed before the wipe");
}

/// A local or remote commit that passed its first halt check, and reaches storage only after the
/// reset's wipe, writes nothing.
#[tokio::test(start_paused = true)]
async fn a_commit_reaching_storage_after_a_reset_is_refused() {
    for remote in [false, true] {
        let storage = Arc::new(TestStorage::default());
        let node = ready_durable_node(&storage).await;
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        *storage.hold_set_state.lock().unwrap() = Some((entered, release_rx));
        let model = proto::ModelId::EntityId(proto::EntityId::from_bytes([7; 32]));
        let genesis =
            proto::Event::genesis(node.identity(), proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]));
        let commit = tokio::spawn({
            let node = node.clone();
            async move {
                let refusal = if remote {
                    let committed = commit_transaction(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![genesis.into()]).await;
                    committed.unwrap_err().downcast::<MutationError>().unwrap()
                } else {
                    commit_row(&node, "late").await.unwrap_err()
                };
                matches!(refusal, MutationError::NodeHalted(NodeHaltReason::Reset))
            }
        });
        entered_rx.await.unwrap();
        node.reset().await.unwrap();
        release.send(()).unwrap();
        assert!(commit.await.unwrap(), "the commit is refused for the reset");
        assert!(storage.fetch_states(&ankql::ast::Predicate::True.into()).await.unwrap().is_empty(), "nothing was written after the wipe");
    }
}

/// A peer's answer that reaches a fetch after the node reset is not written to the wiped store.
#[tokio::test(start_paused = true)]
async fn an_event_fetched_from_a_peer_after_a_reset_is_not_stored() {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new(storage.clone(), PermissiveAgent::new());
    let key = NodeKey::generate();
    let peer = key.node_id();
    let (sent, messages) = mpsc::channel();
    let registration =
        register_keyed_peer(&node, &key, true, PeerProvenance::Configured, Box::new(RecordingSender(peer, sent))).await.unwrap();
    let model = proto::ModelId::EntityId(proto::EntityId::from_bytes([7; 32]));
    let event = proto::Event::genesis(
        key.node_id().identity(),
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]),
    );

    let getter = CachedEventGetter::new(&node, &DEFAULT_CONTEXT);
    let id = event.id();
    let fetch = getter.get_event(&id);
    tokio::pin!(fetch);
    assert!(futures::poll!(&mut fetch).is_pending());
    let request_id = messages
        .try_iter()
        .find_map(|message| match message {
            proto::NodeMessage::Request { request, .. }
                if matches!(request.body.decode(), Ok(proto::NodeRequestBody::GetEvents { .. })) =>
            {
                Some(request.id)
            }
            _ => None,
        })
        .expect("the fetch asked the peer");
    let body = proto::NodeResponseBody::GetEvents(vec![event.clone().into()]);
    let response = proto::NodeResponse { request_id, from: peer, to: node.id, body: proto::CarriedBody::encode(&body) };
    node.handle_message(registration, proto::NodeMessage::Response(response)).await.unwrap();

    node.reset().await.unwrap();
    assert!(matches!(fetch.await, Err(RetrievalError::NodeHalted(NodeHaltReason::Reset))));
    assert!(storage.get_events(vec![event.id()]).await.unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_reset_during_or_after_another_waits_for_its_wipe_and_does_not_wipe_again() {
    let storage = Arc::new(TestStorage::default());
    let node = ready_durable_node(&storage).await;
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    *storage.hold_delete_all.lock().unwrap() = Some((entered, release_rx));
    let reset = || {
        let node = node.clone();
        tokio::spawn(async move { node.reset().await })
    };

    let first = reset();
    entered_rx.await.unwrap();
    let second = reset();
    settle().await;
    assert!(!second.is_finished(), "the second reset waits for the first one's wipe");
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    node.reset().await.unwrap();
    assert_eq!(storage.delete_all_calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_failed_wipe_is_reported_and_the_next_reset_retries_it() {
    let storage = Arc::new(TestStorage::default());
    let node = ready_durable_node(&storage).await;
    let id = commit_row(&node, "kept by the failed wipe").await.unwrap();

    storage.fail_next_delete_all();
    assert!(matches!(node.reset().await, Err(MutationError::WipeFailed(_))));
    assert_eq!(node.state().value(), NodeState::Halted(NodeHaltReason::Reset));
    assert!(storage.get_state(id).await.is_ok());

    node.reset().await.unwrap();
    assert_eq!(storage.delete_all_calls(), 2);
    assert!(matches!(storage.get_state(id).await, Err(RetrievalError::EntityNotFound(_))));
}

/// Counts the error events this crate's lifecycle module emits while it is the default subscriber.
struct LifecycleErrors(Arc<AtomicUsize>);

impl tracing::Subscriber for LifecycleErrors {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool { true }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id { tracing::span::Id::from_u64(1) }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let metadata = event.metadata();
        if *metadata.level() == tracing::Level::ERROR && metadata.target() == "ankurah_core::node::lifecycle" {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(start_paused = true)]
async fn a_failed_wipe_nobody_waits_for_is_logged() {
    let errors = Arc::new(AtomicUsize::new(0));
    let _subscriber = tracing::subscriber::set_default(LifecycleErrors(errors.clone()));
    let storage = Arc::new(TestStorage::default());
    let node = ready_durable_node(&storage).await;

    storage.fail_next_delete_all();
    let mut reset = Box::pin(node.reset());
    assert!(futures::poll!(&mut reset).is_pending());
    drop(reset);
    settle().await;
    assert_eq!(storage.delete_all_calls(), 1);
    assert_eq!(errors.load(Ordering::SeqCst), 1);
}
