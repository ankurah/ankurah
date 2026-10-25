//! Readiness of a copy against a responder's account, the end of a sync's
//! wait when a push it waits on fails to apply, and the requester's descent
//! and repair against a real responder.

use super::{target::Target, *};
use crate::{
    error::{ApplyErrorItem, MutationError},
    node::state_sync::{
        fixtures::*,
        tree::{entity_id_spec, fixtures::snapshot_of},
    },
};
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
    assert_eq!(found.heads.iter().map(|head| (head.entity_id, head.local.clone())).collect::<Vec<_>>(), vec![(lacking.entity_id, None)]);
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
    assert_eq!(found.heads[0].local, Some(head(&edit)), "and the copy's own head goes with it, for the repair to bridge from");
    Ok(())
}

#[tokio::test]
async fn a_local_entity_the_account_does_not_list_holds_readiness_until_the_responder_settles_it() -> anyhow::Result<()> {
    let node = durable().await?;
    let listed = create(&[&node], "listed").await?;
    let held = create(&[&node], "left the responder's result").await?;
    let gone = create(&[&node], "unreadable on the responder").await?;
    let mut target = Target::new(account(&[(listed.entity_id, head(&listed))])?)?;

    let unlisted: BTreeSet<_> = outstanding(&node, &mut target).await?.unlisted.into_iter().map(|(id, _)| id).collect();
    assert_eq!(unlisted, BTreeSet::from([held.entity_id, gone.entity_id]));
    let named = HashSet::from([held.entity_id, gone.entity_id]);
    let held_head = EntityHead { entity_id: held.entity_id, head: head(&held) };
    target.synced(&named, vec![held_head], vec![gone.entity_id])?;
    assert!(outstanding(&node, &mut target).await?.is_empty(), "covering the held head and an unavailable entity both settle");

    let stray = EntityHead { entity_id: EntityId::random(), head: proto::Clock::default() };
    assert!(target.synced(&named, vec![stray], vec![]).is_err(), "an answer may name only what its request named");
    Ok(())
}

/// An update's failure to apply its item for `entity`.
fn failed_item(entity: EntityId) -> ApplyError {
    ApplyError::Items(vec![ApplyErrorItem { entity_id: entity, cause: MutationError::InvalidEvent }])
}

#[tokio::test]
async fn a_push_that_fails_to_apply_for_an_entity_the_sync_waits_on_ends_its_wait_at_once() -> anyhow::Result<()> {
    let node = durable().await?;
    let lacking = labeled(None, "lacking")?;
    let unsettled = create(&[&node], "outside the responder's account").await?;
    let peer = EntityId::random();
    // The sync waits on the head it lacks and on the local member the account does not list.
    for waited_on in [lacking.entity_id, unsettled.entity_id] {
        let mut sync = QuerySync::new(&node, peer, members(), vec![]);
        let push = OpeningPush::of(&sync.offer().await?);
        let target = Target::new(account(&[(lacking.entity_id, head(&lacking))])?)?;
        let started = Instant::now();
        let (result, ()) = tokio::join!(sync.until_ready(target, push), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            node.apply_failures.publish(peer, failed_item(waited_on));
        });
        let failed = |error: &ApplyError| matches!(error, ApplyError::Items(items) if items.len() == 1 && items[0].entity_id == waited_on);
        assert!(
            matches!(&result, Ok(Err(failure)) if failure.entity_id == waited_on && failed(&failure.error)),
            "ends with the item's failure: {result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(1), "before the first idle interval ends");
    }
    Ok(())
}

#[tokio::test]
async fn failures_for_other_entities_or_from_other_peers_change_nothing() -> anyhow::Result<()> {
    let node = durable().await?;
    let lacking = labeled(None, "lacking")?;
    let peer = EntityId::random();
    let mut sync = QuerySync::new(&node, peer, members(), vec![]);
    let push = OpeningPush::of(&sync.offer().await?);
    let target = Target::new(account(&[(lacking.entity_id, head(&lacking))])?)?;
    let (ready, applied) = tokio::join!(sync.until_ready(target, push), async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        node.apply_failures.publish(peer, failed_item(EntityId::random()));
        node.apply_failures.publish(EntityId::random(), failed_item(lacking.entity_id));
        tokio::time::sleep(Duration::from_millis(50)).await;
        apply(&node, &lacking).await
    });
    applied?;
    ready?.map_err(RetrievalError::from)?;
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

/// Whether `message` is an update that pushes `entity`.
fn pushes(message: &proto::NodeMessage, entity: EntityId) -> bool {
    match message {
        proto::NodeMessage::Update(update) => {
            let proto::NodeUpdateBody::SubscriptionUpdate { items } = &update.body;
            items.iter().any(|item| item.entity_id == entity)
        }
        _ => false,
    }
}

#[tokio::test]
async fn a_copy_that_keeps_changing_without_coming_closer_is_repaired_at_the_idle_deadline() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    let pushed = create(&[&responder], "pushed").await?;
    let lost = create(&[&responder], "lost").await?;
    let losing = std::sync::atomic::AtomicBool::new(true);
    connect_losing(&responder, &requester, move |message| {
        pushes(message, lost.entity_id) && losing.swap(false, std::sync::atomic::Ordering::SeqCst)
    })
    .await?;
    // While the sync waits for the lost push, the copy's result gains a member
    // the responder never held every 300 ms. Each changes the copy and none
    // brings it closer to the target, so the repair still runs once the
    // outstanding set has not shrunk for an idle second.
    let local_writes = async {
        for n in 0.. {
            tokio::time::sleep(Duration::from_millis(300)).await;
            create(&[&requester], &format!("local {n}")).await?;
        }
        anyhow::Ok(())
    };
    tokio::select! {
        fetched = tokio::time::timeout(Duration::from_secs(5), fetch(&requester, responder.id, members(), vec![])) => {
            fetched.map_err(|_| anyhow::anyhow!("the fetch is ready while its copy keeps changing"))??;
        }
        written = local_writes => written?,
    }
    for event in [&pushed, &lost] {
        assert_eq!(requester.storage.get_state(event.entity_id).await?.payload.state.head, head(event), "the copy holds every member");
    }
    Ok(())
}

#[tokio::test]
async fn a_member_the_account_does_not_list_that_the_copy_gains_while_waiting_is_named_at_once() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    connect(&responder, &requester).await?;
    let pushed = labeled(None, "pushed")?;
    let mut sync = QuerySync::new(&requester, responder.id, members(), vec![]);
    let push = OpeningPush::of(&sync.offer().await?);
    let target = Target::new(account(&[(pushed.entity_id, head(&pushed))])?)?;
    let started = Instant::now();
    let (ready, landed) = tokio::join!(sync.until_ready(target, push), async {
        // The copy gains a member the responder never held, then the push the
        // sync waits for lands, both within the first idle second.
        tokio::time::sleep(Duration::from_millis(100)).await;
        create(&[&requester], "gained").await?;
        tokio::time::sleep(Duration::from_millis(400)).await;
        apply(&requester, &pushed).await
    });
    landed?;
    ready?.map_err(RetrievalError::from)?;
    assert!(started.elapsed() < Duration::from_secs(1), "ready once the push landed, without waiting out an idle second");
    Ok(())
}

#[tokio::test]
async fn a_large_result_whose_members_keep_changing_still_becomes_ready() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    // More members than one offer's heads, so the responder's account is a
    // digest frontier, which the empty copy matches span by span as the
    // opening push lands.
    for mut state in states(proto::state_sync::limits::MAX_OFFER_HEADS, 8) {
        state.payload.state.memberships = BTreeSet::from([model()]);
        responder.storage.set_state(state);
    }
    let mut edited = Vec::new();
    for n in 0..8 {
        edited.push(create(&[&responder], &format!("edited {n}")).await?);
    }
    connect(&responder, &requester).await?;
    // Throughout the sync the responder edits a member every 100 ms, and the
    // copy takes each edit. The copy's digest over an edited member's span
    // then never equals the account's, so only a descent settles the span.
    let edits = async {
        for n in 0.. {
            let at = n % edited.len();
            let edit = labeled(Some(&edited[at]), &format!("edit {n}"))?;
            apply(&responder, &edit).await?;
            edited[at] = edit;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::Ok(())
    };
    let subscribing = subscribe(&requester, responder.id, proto::QueryId::new(), members(), vec![], 1);
    tokio::select! {
        synced = tokio::time::timeout(Duration::from_secs(20), subscribing) => {
            synced.map_err(|_| anyhow::anyhow!("the subscribe is ready while its members keep changing"))??;
        }
        written = edits => written?,
    }
    Ok(())
}

#[tokio::test]
async fn a_member_the_account_lists_at_another_address_is_an_uncovered_head_and_not_unlisted() -> anyhow::Result<()> {
    let node = durable().await?;
    // The member's key moved: the responder holds it at position 2, at a head
    // the copy lacks, and the copy holds it at position 1.
    let responder_view = snapshot_of(position_spec(), vec![positioned_state(1, 2, 2)?]).await?;
    let mut copy = snapshot_of(position_spec(), vec![positioned_state(1, 1, 1)?]).await?;
    let (id, moved_to) = responder_view.entries.iter().map(|(id, entry)| (*id, entry.address().to_vec())).next().unwrap();
    // The page below the new address lists nothing, though it holds the copy's entry.
    let below = TreeSpan::new(TreeSpan::all().start, std::ops::Bound::Excluded(moved_to.clone()));
    let from = TreeSpan::new(std::ops::Bound::Included(moved_to), TreeSpan::all().end);
    let pages = vec![SpanContent::Heads(responder_view.page(below)?), SpanContent::Heads(responder_view.page(from)?)];
    let mut target = Target::new(KnownState { spec: position_spec(), spans: pages })?;

    let found = target.outstanding(&node, &mut copy).await?;
    assert_eq!(found.heads.iter().map(|head| head.entity_id).collect::<Vec<_>>(), [id], "the copy lacks the listed head");
    assert!(found.unlisted.is_empty(), "the account lists the member, if elsewhere: {:?}", found.unlisted);
    Ok(())
}

#[tokio::test]
async fn a_sync_entities_names_each_entity_once() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    connect(&responder, &requester).await?;
    let held = create(&[&responder], "held").await?;
    let mut sync = QuerySync::new(&requester, responder.id, members(), vec![]);
    let mut target = Target::new(account(&[(held.entity_id, head(&held))])?)?;
    // The responder refuses a request that names an entity twice.
    sync.sync_entities(&mut target, vec![(held.entity_id, None), (held.entity_id, None)]).await?;
    Ok(())
}
