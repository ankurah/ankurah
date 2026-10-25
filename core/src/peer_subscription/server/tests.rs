//! The subscription server: what one peer's queries wait on while another is answered.

use super::*;
use crate::{policy::PermissiveAgent, test_utils::TestStorage};
use ankql::ast::{Predicate, Selection};
use ankurah_core_types::KeySpec;
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

#[tokio::test]
async fn a_subscription_waiting_on_its_snapshot_holds_up_no_other_query_of_its_peer() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let peer = proto::EntityId::random();
    let model = proto::ModelId::EntityId(proto::EntityId::from_bytes([7; 32]));
    let selection: Selection<Resolved> = Predicate::MemberOf(model.clone()).into();
    let offer = proto::KnownState { spec: proto::TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone()), spans: vec![] };

    // A Fetch's snapshot admits the selection's tree and stalls reading storage.
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    storage.hold_fetch.lock().unwrap().insert(model, (entered, release_rx));
    let fetch = tokio::spawn({
        let (node, selection, offer) = (node.clone(), selection.clone(), offer.clone());
        async move { crate::node::state_sync::answer_offer(&node, peer, selection, offer, vec![], vec![]).await }
    });
    entered_rx.await?;

    // A subscription of the same selection installs, then waits on that admission.
    let handler = SubscriptionHandler::new(peer, &node);
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
