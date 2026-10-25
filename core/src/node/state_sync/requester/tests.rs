//! Readiness of a copy against a responder's account, and the requester's
//! descent and repair against a real responder.

use super::{target::Target, *};
use crate::node::state_sync::{fixtures::*, tree::entity_id_spec};
use ankurah_proto::{EntityHead, KeyedEntityHead, PackedHeads, SpanContent, TreeSpanInventory};
use std::collections::BTreeSet;

/// The account of [`members`] that lists `heads`.
fn account(heads: &[(EntityId, proto::Clock)]) -> anyhow::Result<KnownState> {
    let mut listed: Vec<_> =
        heads.iter().map(|(id, head)| KeyedEntityHead { key: vec![], entity: EntityHead { entity_id: *id, head: head.clone() } }).collect();
    listed.sort_by_key(|head| head.entity.entity_id);
    let inventory = TreeSpanInventory { bounds: TreeSpan::all(), heads: PackedHeads::try_from(listed)? };
    Ok(KnownState { spec: entity_id_spec(&members()), spans: vec![SpanContent::Heads(inventory)] })
}

/// What `node`'s copy of [`members`] does not yet match or exceed of `target`.
async fn outstanding(node: &TestNode, target: &mut Target) -> anyhow::Result<target::Outstanding> {
    let mut snapshot = node.resident_trees.snapshot(node, members(), Some(target.spec.clone()), vec![]).await?.snapshot;
    target.outstanding(node, &mut snapshot).await
}

fn head(event: &proto::Event) -> proto::Clock { proto::Clock::singleton(event) }

#[tokio::test]
async fn a_target_is_outstanding_until_the_copy_covers_every_head() -> anyhow::Result<()> {
    let node = durable().await?;
    let held = create(&[&node], "held").await?;
    let lacking = labeled(None, "lacking")?;
    let mut target = Target::new(account(&[(held.entity_id, head(&held)), (lacking.entity_id, head(&lacking))])?)?;

    let found = outstanding(&node, &mut target).await?;
    assert_eq!(found.heads.iter().map(|(id, _, local)| (*id, local.clone())).collect::<Vec<_>>(), vec![(lacking.entity_id, None)]);
    apply(&node, &lacking).await?;
    assert!(outstanding(&node, &mut target).await?.is_empty(), "the copy covers every head once the lacking entity applies");
    Ok(())
}

#[tokio::test]
async fn a_copy_at_a_newer_head_covers_the_responders_older_one_and_never_the_reverse() -> anyhow::Result<()> {
    let node = durable().await?;
    let entity = create(&[&node], "first").await?;
    let edit = labeled(Some(&entity), "edited locally")?;
    apply(&node, &edit).await?;
    let mut older = Target::new(account(&[(entity.entity_id, head(&entity))])?)?;
    assert!(outstanding(&node, &mut older).await?.is_empty(), "a local edit exceeds the responder's older head");

    // A head the responder holds beyond the copy's, whose event the copy never saw.
    let newer = labeled(Some(&edit), "edited remotely")?;
    let mut ahead = Target::new(account(&[(entity.entity_id, head(&newer))])?)?;
    let found = outstanding(&node, &mut ahead).await?;
    assert_eq!(found.heads.len(), 1, "a head beyond the copy's is not covered");
    assert_eq!(found.heads[0].2, Some(head(&edit)), "and the copy's own head goes with it, for the repair to bridge from");
    Ok(())
}

#[tokio::test]
async fn a_local_entity_the_account_does_not_list_holds_readiness_until_the_responder_settles_it() -> anyhow::Result<()> {
    let node = durable().await?;
    let listed = create(&[&node], "listed").await?;
    let held = create(&[&node], "left the responder's result").await?;
    let gone = create(&[&node], "unreadable on the responder").await?;
    let mut target = Target::new(account(&[(listed.entity_id, head(&listed))])?)?;

    let extras: BTreeSet<_> = outstanding(&node, &mut target).await?.extras.into_iter().map(|(id, _)| id).collect();
    assert_eq!(extras, BTreeSet::from([held.entity_id, gone.entity_id]));
    let named = HashSet::from([held.entity_id, gone.entity_id]);
    let held_head = proto::KnownEntity { entity_id: held.entity_id, head: head(&held) };
    target.synced(&named, vec![held_head], vec![gone.entity_id])?;
    assert!(outstanding(&node, &mut target).await?.is_empty(), "covering the held head and an unavailable entity both settle");

    let stray = proto::KnownEntity { entity_id: EntityId::random(), head: proto::Clock::default() };
    assert!(target.synced(&named, vec![stray], vec![]).is_err(), "an answer may name only what its request named");
    Ok(())
}

#[tokio::test]
async fn a_digest_settles_its_span_when_the_copys_digest_over_the_same_bounds_equals_it() -> anyhow::Result<()> {
    let node = durable().await?;
    let elsewhere = durable().await?;
    for label in ["a", "b", "c"] {
        create(&[&node, &elsewhere], label).await?;
    }
    let digest_of = |node: TestNode| async move {
        let mut snapshot = node.resident_trees.snapshot(&node, members(), None, vec![]).await?.snapshot;
        anyhow::Ok(SpanContent::Digest(snapshot.span_digest(TreeSpan::all()).await?))
    };
    let mut equal = Target::new(KnownState { spec: entity_id_spec(&members()), spans: vec![digest_of(elsewhere.clone()).await?] })?;
    assert!(outstanding(&node, &mut equal).await?.is_empty());

    create(&[&elsewhere], "d").await?;
    let mut differing = Target::new(KnownState { spec: entity_id_spec(&members()), spans: vec![digest_of(elsewhere).await?] })?;
    assert_eq!(outstanding(&node, &mut differing).await?.spans.len(), 1, "a differing digest leaves its span to descend into");
    Ok(())
}

#[tokio::test]
async fn a_large_copy_descends_into_differing_spans_and_names_what_it_lacks() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    // More members than one offer's heads, so both accounts are digest frontiers.
    let shared: Vec<_> = states(proto::state_sync::limits::MAX_OFFER_HEADS + 64, 8)
        .into_iter()
        .map(|mut state| {
            state.payload.state.memberships = BTreeSet::from([model()]);
            state
        })
        .collect();
    for state in &shared {
        responder.storage.set_state(state.clone());
        requester.storage.set_state(state.clone());
    }
    let requests = connect(&responder, &requester).await?;
    let lacking = [create(&[&responder], "first new").await?, create(&[&responder], "second new").await?];

    fetch(&requester, responder.id, members(), vec![]).await?;

    for event in &lacking {
        assert_eq!(requester.storage.get_state(event.entity_id).await?.payload.state.head, head(event), "the copy holds what it lacked");
    }
    let requests = requests.lock().unwrap().clone();
    let asked = |kind: &str| requests.iter().filter(|request| request.starts_with(kind)).count();
    assert!(asked("CompareSpans") > 0, "the copy compared spans below the frontier: {requests:?}");
    assert!(asked("SyncEntities") > 0, "and named what it lacked: {requests:?}");
    Ok(())
}
