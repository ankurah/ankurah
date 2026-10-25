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
    let named = [(held.entity_id, None), (gone.entity_id, None)];
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
        let target = Target::new(account(&[(lacking.entity_id, head(&lacking))])?)?;
        let push = OpeningPush::of(&sync.offer().await?, &target);
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
    let target = Target::new(account(&[(lacking.entity_id, head(&lacking))])?)?;
    let push = OpeningPush::of(&sync.offer().await?, &target);
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
async fn a_digest_settles_its_span_only_when_the_copy_holds_as_many_entities_there_and_its_digest_equals_it() -> anyhow::Result<()> {
    let node = durable().await?;
    let elsewhere = durable().await?;
    let mut created = Vec::new();
    for label in ["a", "b", "c"] {
        created.push(create(&[&node, &elsewhere], label).await?);
    }
    let digest_of = |node: TestNode| async move {
        let mut snapshot = node.resident_trees.snapshot(&node, members(), None, vec![]).await?.snapshot;
        anyhow::Ok(SpanContent::Digest(snapshot.span_digest(TreeSpan::all()).await?))
    };
    let mut equal = Target::new(KnownState { spec: entity_id_spec(&members()), spans: vec![digest_of(elsewhere.clone()).await?] })?;
    assert!(outstanding(&node, &mut equal).await?.is_empty());

    apply(&elsewhere, &labeled(Some(&created[0]), "edited elsewhere")?).await?;
    let mut edited = Target::new(KnownState { spec: entity_id_spec(&members()), spans: vec![digest_of(elsewhere.clone()).await?] })?;
    assert_eq!(
        outstanding(&node, &mut edited).await?.spans.len(),
        1,
        "as many entities, one at another head, leave the span to descend into"
    );

    create(&[&elsewhere], "d").await?;
    let mut more = Target::new(KnownState { spec: entity_id_spec(&members()), spans: vec![digest_of(elsewhere).await?] })?;
    assert_eq!(outstanding(&node, &mut more).await?.spans.len(), 1, "and so does an entity the copy lacks");
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
    let target = Target::new(account(&[(pushed.entity_id, head(&pushed))])?)?;
    let push = OpeningPush::of(&sync.offer().await?, &target);
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

/// What of `outstanding` the push that answered `offer` with `target` leaves
/// to repair: the heads, the bounds of the spans, and the unlisted members.
fn unpushed(offer: &KnownState, target: &Target, outstanding: &Outstanding) -> (Vec<EntityId>, Vec<TreeSpan>, Vec<EntityId>) {
    let unpushed = OpeningPush::of(offer, target).unpushed(outstanding);
    (
        unpushed.heads.iter().map(|head| head.entity_id).collect(),
        unpushed.spans.iter().map(|region| region.bounds().clone()).collect(),
        unpushed.unlisted.iter().map(|(id, _)| *id).collect(),
    )
}

/// Outstanding: a head the copy lacks and a span not learned yet, both inside
/// the whole tree, and unlisted members `listed_leaver` and `other`.
fn outstanding_of(lacking: EntityId, listed_leaver: EntityId, other: EntityId) -> Outstanding {
    Outstanding {
        heads: vec![target::UncoveredHead { entity_id: lacking, address: vec![0; 40], local: None }],
        spans: vec![target::Region::Unknown(TreeSpan::all())],
        unlisted: vec![(listed_leaver, proto::Clock::default()), (other, proto::Clock::default())],
    }
}

#[tokio::test]
async fn an_opening_push_brings_what_the_offer_leaves_out_and_the_leavers_it_lists() -> anyhow::Result<()> {
    let held = states(2, 8);
    let (lacking, leaver, other) = (EntityId::random(), held[0].payload.entity_id, EntityId::random());
    let mut copy = snapshot(held).await?;
    let target = Target::new(KnownState { spec: spec(), spans: vec![] })?;

    let listing = KnownState { spec: spec(), spans: vec![SpanContent::Heads(copy.page(TreeSpan::all())?)] };
    let pushed = unpushed(&listing, &target, &outstanding_of(lacking, leaver, other));
    assert_eq!(pushed, (vec![], vec![], vec![other]), "outside any digest everything is pushed, and so is each listed leaver");

    let describing = KnownState { spec: spec(), spans: vec![SpanContent::Digest(copy.span_digest(TreeSpan::all()).await?)] };
    let pushed = unpushed(&describing, &target, &outstanding_of(lacking, leaver, other));
    assert_eq!(pushed, (vec![lacking], vec![TreeSpan::all()], vec![leaver, other]), "within a nonempty digest nothing is pushed");
    Ok(())
}

#[tokio::test]
async fn under_the_entity_id_fallback_an_opening_push_brings_every_member_and_no_leaver() -> anyhow::Result<()> {
    let held = states(2, 8);
    let (lacking, leaver, other) = (EntityId::random(), held[0].payload.entity_id, EntityId::random());
    let mut copy = snapshot(held).await?;
    // The responder serves no tree under the offer's spec, so it answers under the entity-id spec.
    let fallback = Target::new(KnownState { spec: entity_id_spec(&members()), spans: vec![] })?;

    for offer in [
        KnownState { spec: spec(), spans: vec![SpanContent::Heads(copy.page(TreeSpan::all())?)] },
        KnownState { spec: spec(), spans: vec![SpanContent::Digest(copy.span_digest(TreeSpan::all()).await?)] },
    ] {
        let pushed = unpushed(&offer, &fallback, &outstanding_of(lacking, leaver, other));
        assert_eq!(pushed, (vec![], vec![], vec![leaver, other]), "every member is pushed and no leaver is");
    }
    Ok(())
}

#[tokio::test]
async fn a_member_whose_copy_diverged_settles_at_the_copys_own_head_after_a_re_request() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    connect(&responder, &requester).await?;
    let genesis = create(&[&responder, &requester], "shared").await?;
    // Each side edits the member without the other, so the responder's state
    // is concurrent with the copy's, which the applier does not adopt: no
    // push brings the copy level.
    let here = labeled(Some(&genesis), "edited here")?;
    apply(&requester, &here).await?;
    apply(&responder, &labeled(Some(&genesis), "edited there")?).await?;

    // Its push leaves the copy unchanged at the first idle deadline, which
    // asks for it, and at the second, which asks again; the third settles it,
    // where the sync without the rule would compare once more and stall.
    tokio::time::timeout(Duration::from_secs(12), fetch(&requester, responder.id, members(), vec![])).await??;
    assert_eq!(requester.storage.get_state(genesis.entity_id).await?.payload.state.head, head(&here), "the copy keeps its own head");
    Ok(())
}

/// A member both nodes hold at `genesis`, which the responder has since
/// edited to the returned head without the requester.
async fn behind_on_the_requester(responder: &TestNode, requester: &TestNode) -> anyhow::Result<(proto::Event, proto::Event)> {
    let genesis = create(&[responder, requester], "behind").await?;
    let newer = labeled(Some(&genesis), "newer")?;
    apply(responder, &newer).await?;
    Ok((genesis, newer))
}

/// Wait until `target` is ready, then read `member`'s head in the copy at that
/// moment.
async fn ready_then_head(
    sync: &mut QuerySync<'_, crate::test_utils::TestStorage, crate::policy::PermissiveAgent>,
    target: Target,
    push: OpeningPush,
    member: EntityId,
) -> anyhow::Result<proto::Clock> {
    let ready: Result<(), ApplyFailure> = sync.until_ready(target, push).await?;
    ready.map_err(RetrievalError::from)?;
    Ok(sync.node.storage.get_state(member).await?.payload.state.head)
}

#[tokio::test]
async fn a_member_whose_push_is_held_past_an_idle_interval_while_the_sync_progresses_is_brought_level() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    let (_, newer) = behind_on_the_requester(&responder, &requester).await?;
    let member = newer.entity_id;
    // The connection loses the member's pushes: its push landing is simulated below.
    connect_losing(&responder, &requester, move |message| pushes(message, member)).await?;
    // The account lists members whose pushes land one by one, and not the
    // member, which the sync names at once and the responder answers for.
    let pushed = (0..5).map(|n| labeled(None, &format!("pushed {n}"))).collect::<anyhow::Result<Vec<_>>>()?;
    let target = Target::new(account(&pushed.iter().map(|event| (event.entity_id, head(event))).collect::<Vec<_>>())?)?;
    let push = OpeningPush::of(&KnownState { spec: target.spec.clone(), spans: vec![] }, &target);
    let mut sync = QuerySync::new(&requester, responder.id, members(), vec![]);
    let (ready, landed) = tokio::join!(ready_then_head(&mut sync, target, push, member), async {
        for event in &pushed {
            tokio::time::sleep(Duration::from_millis(300)).await;
            apply(&requester, event).await?;
        }
        // Over an idle interval after the member's answer, its push lands.
        tokio::time::sleep(Duration::from_millis(500)).await;
        apply(&requester, &newer).await
    });
    landed?;
    assert_eq!(ready?, head(&newer), "ready only once the member's push landed, never settled as diverged");
    Ok(())
}

#[tokio::test]
async fn a_member_whose_first_repair_push_is_lost_is_asked_for_again_and_brought_level() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    let (genesis, newer) = behind_on_the_requester(&responder, &requester).await?;
    let member = newer.entity_id;
    let losing = std::sync::atomic::AtomicBool::new(true);
    connect_losing(&responder, &requester, move |message| {
        pushes(message, member) && losing.swap(false, std::sync::atomic::Ordering::SeqCst)
    })
    .await?;
    // The offer listed the member at the copy's head, so the opening push
    // would bring the responder's newer one; nothing arrives, so the first
    // idle deadline asks for it, and that push is lost.
    let target = Target::new(account(&[(member, head(&newer))])?)?;
    let push = OpeningPush::of(&account(&[(member, head(&genesis))])?, &target);
    let mut sync = QuerySync::new(&requester, responder.id, members(), vec![]);
    let head_at_ready = tokio::time::timeout(Duration::from_secs(6), ready_then_head(&mut sync, target, push, member)).await??;
    assert_eq!(head_at_ready, head(&newer), "the next idle deadline asked again, and that push brought the copy level");
    Ok(())
}

#[tokio::test]
async fn a_member_settles_as_diverged_only_once_a_re_request_finds_it_unchanged_before_the_sync_went_quiet() -> anyhow::Result<()> {
    let node = durable().await?;
    let member = create(&[&node], "left the responder's result").await?;
    // The responder holds a head the copy does not cover, as a concurrent one.
    let theirs = labeled(Some(&member), "edited there")?;
    let named = [(member.entity_id, Some(head(&member)))];
    let answer = || vec![EntityHead { entity_id: member.entity_id, head: head(&theirs) }];
    let mut target = Target::new(account(&[])?)?;
    target.synced(&named, answer(), vec![])?;
    let found = outstanding(&node, &mut target).await?;
    assert_eq!(found.unlisted.len(), 1);
    assert!(!target.settle_diverged(&found, Instant::now()), "one answer: its push may have been lost");

    target.synced(&named, answer(), vec![])?;
    let answered = Instant::now();
    assert!(!target.settle_diverged(&found, answered - Duration::from_millis(1)), "the sync progressed since the re-request");
    assert!(target.settle_diverged(&found, answered), "a re-request found the copy unchanged before the sync went quiet");
    assert!(outstanding(&node, &mut target).await?.is_empty());

    // A re-request answered with another head is not a repeat.
    let mut target = Target::new(account(&[])?)?;
    target.synced(&named, answer(), vec![])?;
    let newer = labeled(Some(&theirs), "edited there again")?;
    target.synced(&named, vec![EntityHead { entity_id: member.entity_id, head: head(&newer) }], vec![])?;
    let found = outstanding(&node, &mut target).await?;
    assert!(!target.settle_diverged(&found, Instant::now()));
    Ok(())
}

#[tokio::test]
async fn a_fetch_registers_on_the_connection_its_request_goes_out_on() -> anyhow::Result<()> {
    let responder = durable().await?;
    let requester = node().await?;
    let held = create(&[&responder], "held").await?;
    // The opening push is lost, so the Fetch waits for its idle repair.
    let losing = std::sync::atomic::AtomicBool::new(true);
    connect_losing(&responder, &requester, move |message| {
        pushes(message, held.entity_id) && losing.swap(false, std::sync::atomic::Ordering::SeqCst)
    })
    .await?;
    // The peer reconnects while the Fetch reads its offer, so only the new
    // connection carries its request.
    let (entered, reading) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    requester.storage.hold_fetch.lock().unwrap().insert(model(), (entered, resume));
    let fetching = fetch(&requester, responder.id, members(), vec![]);
    tokio::pin!(fetching);
    tokio::select! {
        _ = &mut fetching => anyhow::bail!("the fetch finished while its offer's read was held"),
        entered = reading => entered?,
    }
    reconnect(&responder, &requester).await?;
    let _ = release.send(());
    tokio::select! {
        _ = &mut fetching => anyhow::bail!("the fetch finished before its repair"),
        () = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    let connection = requester.peer_connections.get(&responder.id).expect("the peer reconnected");
    assert!(connection.fetches.credentials().is_some(), "the connection the request went out on admits the Fetch's pushes");
    drop(connection);
    tokio::time::timeout(Duration::from_secs(5), fetching).await??;
    assert_eq!(requester.storage.get_state(held.entity_id).await?.payload.state.head, head(&held));
    Ok(())
}

/// Store `events` in `node`'s storage without applying them to any entity.
async fn store_events(node: &TestNode, events: &[&proto::Event]) -> anyhow::Result<()> {
    use crate::storage::{StorageEngine, StorageTransaction};
    let mut transaction = node.storage.transaction();
    transaction.add_events(&events.iter().map(|event| proto::Attested::from((*event).clone())).collect::<Vec<_>>()).await?;
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn a_copy_found_short_of_a_head_is_not_judged_again_until_it_moves() -> anyhow::Result<()> {
    let node = durable().await?;
    let older = create(&[&node], "older").await?;
    // The newer head's event is stored but unapplied, so a walk of history
    // settles that the copy is older.
    let newer = labeled(Some(&older), "newer")?;
    store_events(&node, &[&newer]).await?;
    let mut target = Target::new(account(&[(older.entity_id, head(&newer))])?)?;

    assert_eq!(outstanding(&node, &mut target).await?.heads.len(), 1);
    let reads = node.storage.event_reads.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(outstanding(&node, &mut target).await?.heads.len(), 1);
    assert_eq!(node.storage.event_reads.load(std::sync::atomic::Ordering::Relaxed), reads, "the unmoved copy is not walked again");
    apply(&node, &newer).await?;
    assert!(outstanding(&node, &mut target).await?.is_empty(), "once the copy moves, it is judged again");
    Ok(())
}

#[tokio::test]
async fn a_copy_whose_history_is_missing_is_judged_again_once_it_arrives() -> anyhow::Result<()> {
    // Elsewhere the member reached a newer head; this node takes that state
    // before the event behind it.
    let elsewhere = durable().await?;
    let genesis = create(&[&elsewhere], "first").await?;
    let newer = labeled(Some(&genesis), "newer")?;
    apply(&elsewhere, &newer).await?;
    let node = durable().await?;
    node.storage.set_state(elsewhere.storage.get_state(genesis.entity_id).await?);
    store_events(&node, &[&genesis]).await?;
    let mut target = Target::new(account(&[(genesis.entity_id, head(&genesis))])?)?;

    assert_eq!(outstanding(&node, &mut target).await?.heads.len(), 1, "missing history proves no coverage yet");
    store_events(&node, &[&newer]).await?;
    assert!(outstanding(&node, &mut target).await?.is_empty(), "the copy at the same head covers once its history arrives");
    Ok(())
}
