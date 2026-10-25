//! Pushes: how they fill their updates, and that each ends with the connection
//! it began on or once canceled.

use super::*;
use crate::{node::state_sync::fixtures::*, policy::PermissiveAgent};

/// Add `state`'s entity to `push` as its state.
fn add_state(node: &TestNode, push: &mut Push<<PermissiveAgent as PolicyAgent>::ContextData>, state: proto::Attested<proto::EntityState>) {
    push.add(node, state.payload.entity_id, proto::UpdateContent::StateAndEvent(state.into(), vec![]));
}

#[tokio::test]
async fn pushes_fill_updates_by_count_and_by_size() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, mut messages) = recording_peer(&node).await?;

    let mut push = Push::new(&node, peer, vec![], CancelFlag::default());
    for state in states(MAX_PAGE_HEADS + 1, 10) {
        add_state(&node, &mut push, state);
    }
    push.send(&node);
    let counts: Vec<_> = pushed(&mut messages, MAX_PAGE_HEADS + 1).await.iter().map(Vec::len).collect();
    assert_eq!(counts, [MAX_PAGE_HEADS, 1], "at most a page of items per update");

    // Each item's data lies half in its buffers or diffs and half in its attestation.
    let attested = || proto::AttestationSet(vec![proto::Attestation(vec![0; 200 * 1024])]);
    let mut push = Push::new(&node, peer, vec![], CancelFlag::default());
    for mut state in states(3, 200 * 1024) {
        state.attestations = attested();
        add_state(&node, &mut push, state);
    }
    push.send(&node);
    let counts: Vec<_> = pushed(&mut messages, 3).await.iter().map(Vec::len).collect();
    assert_eq!(counts, [2, 1], "about a mebibyte of item data per update");

    let mut push = Push::new(&node, peer, vec![], CancelFlag::default());
    for _ in 0..3 {
        let event = labeled(None, &"x".repeat(200 * 1024))?;
        let events = vec![proto::Attested { payload: event.clone(), attestations: attested() }.into()];
        push.add(&node, event.entity_id, proto::UpdateContent::EventOnly(events));
    }
    push.send(&node);
    let counts: Vec<_> = pushed(&mut messages, 3).await.iter().map(Vec::len).collect();
    assert_eq!(counts, [2, 1], "a bridge counts its events' data the same way");

    let mut push = Push::new(&node, peer, vec![], CancelFlag::default());
    add_state(&node, &mut push, states(1, 2 * 1024 * 1024).pop().unwrap());
    push.send(&node);
    assert_eq!(pushed(&mut messages, 1).await.len(), 1, "an item larger than the bound still goes, alone");
    Ok(())
}

#[tokio::test]
async fn a_push_sends_nothing_to_a_connection_that_replaced_the_one_it_began_on() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, mut first) = recording_peer(&node).await?;
    let mut push = Push::new(&node, peer, vec![], CancelFlag::default());
    add_state(&node, &mut push, states(1, 10).pop().unwrap());
    push.send(&node);
    assert_eq!(pushed(&mut first, 1).await.len(), 1, "the push sends on the connection it began on");

    // The peer reconnects while the push still has items to send.
    add_state(&node, &mut push, states(1, 10).pop().unwrap());
    node.deregister_peer(peer);
    let mut second = recording_connection(&node, peer).await?;
    push.send(&node);
    assert!(second.try_recv().is_err(), "the new connection receives nothing of the old push");
    Ok(())
}

#[tokio::test]
async fn a_push_reads_no_further_page_once_its_connection_is_gone() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, _first) = recording_peer(&node).await?;
    let lacking = create(&[&node], "lacking").await?;
    let push = Push::new(&node, peer, vec![], CancelFlag::default());
    node.deregister_peer(peer);
    let mut second = recording_connection(&node, peer).await?;
    // Reading the lacking entity's state would fail the push.
    *node.storage.reject_state_read.lock().unwrap() = Some(lacking.entity_id);

    push_lacking(node.weak(), vec![(lacking.entity_id, None)], vec![], false, push).await?;
    assert!(second.try_recv().is_err(), "the new connection receives nothing of the old push");
    Ok(())
}

#[tokio::test]
async fn a_canceled_push_sends_and_reads_nothing_further() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, mut messages) = recording_peer(&node).await?;
    let cancel = CancelFlag::default();
    let mut push = Push::new(&node, peer, vec![], cancel.clone());
    add_state(&node, &mut push, states(1, 10).pop().unwrap());
    cancel.cancel();
    push.send(&node);
    assert!(messages.try_recv().is_err(), "a canceled push sends nothing it gathered");

    let lacking = create(&[&node], "lacking").await?;
    // Reading the lacking entity's state would fail the push.
    *node.storage.reject_state_read.lock().unwrap() = Some(lacking.entity_id);
    push_lacking(node.weak(), vec![(lacking.entity_id, None)], vec![], false, push).await?;
    assert!(messages.try_recv().is_err(), "nor any later page");
    Ok(())
}
