//! The subscription server: what one peer's queries wait on while another is answered.

use super::*;
use crate::{policy::PermissiveAgent, test_utils::TestStorage};
use ankql::ast::{Predicate, Selection};
use ankurah_core_types::KeySpec;
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

type TestNode = Node<TestStorage, PermissiveAgent>;

fn model() -> proto::ModelId { proto::ModelId::EntityId(proto::EntityId::from_bytes([7; 32])) }

/// A durable node with a subscription handler for a peer, and the model's members with an empty offer of them.
async fn serving(
) -> anyhow::Result<(TestNode, SubscriptionHandler<<PermissiveAgent as PolicyAgent>::ContextData>, Selection<Resolved>, proto::KnownState)>
{
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let handler = SubscriptionHandler::new(proto::EntityId::random(), &node);
    let selection: Selection<Resolved> = Predicate::MemberOf(model()).into();
    let offer = proto::KnownState { spec: proto::TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone()), spans: vec![] };
    Ok((node, handler, selection, offer))
}

#[tokio::test]
async fn a_subscription_waiting_on_its_snapshot_holds_up_no_other_query_of_its_peer() -> anyhow::Result<()> {
    let (node, handler, selection, offer) = serving().await?;

    // A Fetch's snapshot admits the selection's tree and stalls reading storage.
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    node.storage.hold_fetch.lock().unwrap().insert(model(), (entered, release_rx));
    let fetch = tokio::spawn({
        let (node, peer, selection, offer) = (node.clone(), handler.peer_id, selection.clone(), offer.clone());
        async move {
            crate::node::state_sync::answer_offer(&node, peer, selection, offer, vec![], crate::node::state_sync::OfferedIn::Fetch).await
        }
    });
    entered_rx.await?;

    // A subscription of the same selection installs, then waits on that admission.
    let subscribing = handler.subscribe_query(&node, proto::QueryId::new(), selection, None, 1, offer);
    tokio::pin!(subscribing);
    assert!(tokio::time::timeout(Duration::from_millis(200), subscribing.as_mut()).await.is_err(), "the subscription waits");

    let removal = tokio::time::timeout(Duration::from_secs(1), handler.remove_predicate(proto::QueryId::new())).await;
    assert!(removal.is_ok(), "another query of the peer's proceeds meanwhile");
    release.send(()).unwrap();
    assert!(matches!(subscribing.await?, proto::NodeResponseBody::QuerySubscribed { .. }));
    fetch.await??;
    Ok(())
}

#[tokio::test]
async fn a_failed_answer_removes_its_query_only_while_no_later_version_replaced_it() -> anyhow::Result<()> {
    let (node, handler, selection, offer) = serving().await?;
    let query = proto::QueryId::new();
    handler.subscribe_query(&node, query, selection.clone(), None, 1, offer.clone()).await?;
    handler.subscribe_query(&node, query, selection, None, 2, offer).await?;

    handler.remove_unless_replaced(query, 1).await;
    assert!(handler.queries.lock().await.contains_key(&query), "version 2 replaced the query version 1 created");
    handler.remove_unless_replaced(query, 2).await;
    assert!(!handler.queries.lock().await.contains_key(&query));
    Ok(())
}

#[tokio::test]
async fn replacing_or_removing_a_query_cancels_its_push() -> anyhow::Result<()> {
    let (node, handler, selection, offer) = serving().await?;
    let query = proto::QueryId::new();
    handler.subscribe_query(&node, query, selection.clone(), None, 1, offer.clone()).await?;
    let first = handler.queries.lock().await[&query].push.clone();
    handler.subscribe_query(&node, query, selection, None, 2, offer).await?;
    let second = handler.queries.lock().await[&query].push.clone();
    assert!(first.canceled() && !second.canceled(), "version 2's push replaces version 1's");

    handler.remove_predicate(query).await?;
    assert!(second.canceled(), "removing the query cancels its push");
    Ok(())
}
