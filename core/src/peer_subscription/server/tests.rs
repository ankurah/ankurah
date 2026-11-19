use super::*;

use std::sync::Arc;

use ankql::ast::Predicate;
use ankurah_core_types::{
    key_spec::{IndexKeyPart, KeySpec},
    ValueType,
};

use crate::{
    connector::{PeerSender, SendError},
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    test_utils::TestStorage,
};

#[derive(Clone)]
struct Sender;

impl PeerSender for Sender {
    fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

async fn fixture() -> anyhow::Result<(Node<TestStorage, PermissiveAgent>, ConnectionId, ankql::ast::Selection<Resolved>)> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let peer = crate::test_utils::connect_peer(&node, &crate::node::NodeKey::generate(), false, Sender).await?;
    Ok((node, peer, Predicate::MemberOf(proto::ModelId::EntityId(proto::EntityId::random())).into()))
}

#[tokio::test]
async fn a_failed_capture_removes_a_fresh_subscription_and_releases_its_identity() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let handler = SubscriptionHandler::new(peer, &node);
    let query = proto::QueryId::new();
    let comparison = proto::ComparisonId::new();
    assert!(handler
        .subscribe_query(
            &node,
            query,
            selection.clone(),
            Some(&DEFAULT_CONTEXT),
            1,
            comparison,
            KeySpec::new(vec![IndexKeyPart::asc(proto::PropertyId::Id, ValueType::EntityId)]),
            proto::KnownState::empty()
        )
        .await
        .is_err());
    assert!(handler.queries.lock().await.is_empty());
    assert!(matches!(handler.subscription.remove_predicate(query), Err(SubscriptionError::PredicateNotFound)));
    node.begin_scoped_comparison(
        peer,
        comparison,
        selection.clone(),
        selection,
        KeySpec::new(vec![]),
        proto::KnownState::empty(),
        &vec![DEFAULT_CONTEXT],
    )
    .await?;
    node.release_comparison(peer, comparison);
    Ok(())
}

#[tokio::test]
async fn cancelling_initial_capture_removes_the_already_installed_query() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let handler = Arc::new(SubscriptionHandler::new(peer, &node));
    let query = proto::QueryId::new();
    let comparison = proto::ComparisonId::new();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (_release, resume) = tokio::sync::oneshot::channel();
    *node.storage.hold_state_snapshot.lock().unwrap() = Some((entered, resume));
    let worker = node.clone();
    let installing = handler.clone();
    let request_selection = selection.clone();
    let task = tokio::spawn(async move {
        installing
            .subscribe_query(
                &worker,
                query,
                request_selection,
                Some(&DEFAULT_CONTEXT),
                1,
                comparison,
                KeySpec::new(vec![]),
                proto::KnownState::empty(),
            )
            .await
    });
    entering.await?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(handler.queries.lock().await.is_empty());
    assert!(matches!(handler.subscription.remove_predicate(query), Err(SubscriptionError::PredicateNotFound)));
    node.begin_scoped_comparison(
        peer,
        comparison,
        selection.clone(),
        selection,
        KeySpec::new(vec![]),
        proto::KnownState::empty(),
        &vec![DEFAULT_CONTEXT],
    )
    .await?;
    node.release_comparison(peer, comparison);
    Ok(())
}

#[tokio::test]
async fn stale_versions_preserve_the_capture_and_unsubscribe_releases_it() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let handler = SubscriptionHandler::new(peer, &node);
    let query = proto::QueryId::new();
    let comparison = proto::ComparisonId::new();
    let response = handler
        .subscribe_query(
            &node,
            query,
            selection.clone(),
            Some(&DEFAULT_CONTEXT),
            2,
            comparison,
            KeySpec::new(vec![]),
            proto::KnownState::empty(),
        )
        .await?;
    assert!(
        matches!(response, proto::NodeResponseBody::QuerySubscribed { query_id, target } if query_id == query && target.id == comparison)
    );
    assert!(
        handler
            .subscribe_query(
                &node,
                query,
                selection.clone(),
                Some(&DEFAULT_CONTEXT),
                2,
                comparison,
                KeySpec::new(vec![]),
                proto::KnownState::empty()
            )
            .await
            .is_err(),
        "duplicate initialization cannot replace a frozen comparison"
    );
    assert!(
        handler
            .subscribe_query(
                &node,
                proto::QueryId::new(),
                selection.clone(),
                Some(&DEFAULT_CONTEXT),
                1,
                comparison,
                KeySpec::new(vec![]),
                proto::KnownState::empty()
            )
            .await
            .is_err(),
        "another query cannot take the capture identity"
    );
    assert!(handler
        .subscribe_query(
            &node,
            query,
            selection.clone(),
            Some(&DEFAULT_CONTEXT),
            1,
            proto::ComparisonId::new(),
            KeySpec::new(vec![]),
            proto::KnownState::empty()
        )
        .await
        .is_err());
    assert_eq!(handler.queries.lock().await[&query].comparison, Some(comparison));
    assert!(
        node.begin_scoped_comparison(
            peer,
            comparison,
            selection.clone(),
            selection.clone(),
            KeySpec::new(vec![]),
            proto::KnownState::empty(),
            &vec![DEFAULT_CONTEXT]
        )
        .await
        .is_err(),
        "the current capture remains reserved"
    );
    handler.remove_predicate(query).await?;
    node.begin_scoped_comparison(
        peer,
        comparison,
        selection.clone(),
        selection,
        KeySpec::new(vec![]),
        proto::KnownState::empty(),
        &vec![DEFAULT_CONTEXT],
    )
    .await?;
    node.release_comparison(peer, comparison);
    Ok(())
}

#[tokio::test]
async fn dropping_the_subscription_handler_releases_its_capture() -> anyhow::Result<()> {
    let (node, peer, selection) = fixture().await?;
    let handler = SubscriptionHandler::new(peer, &node);
    let comparison = proto::ComparisonId::new();
    handler
        .subscribe_query(
            &node,
            proto::QueryId::new(),
            selection.clone(),
            Some(&DEFAULT_CONTEXT),
            1,
            comparison,
            KeySpec::new(vec![]),
            proto::KnownState::empty(),
        )
        .await?;
    drop(handler);
    node.begin_scoped_comparison(
        peer,
        comparison,
        selection.clone(),
        selection,
        KeySpec::new(vec![]),
        proto::KnownState::empty(),
        &vec![DEFAULT_CONTEXT],
    )
    .await?;
    node.release_comparison(peer, comparison);
    Ok(())
}
