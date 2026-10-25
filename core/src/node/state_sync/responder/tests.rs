//! The responder's entry point and handlers: what each answers, and what it
//! pushes on the update channel.

use super::*;
use crate::node::state_sync::fixtures::*;
use ankurah_core_types::{IndexKeyPart, KeySpec, ValueType};
use ankurah_proto::{
    state_sync::limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS, MAX_PAGE_HEADS},
    Attested, EntityHead, EntityState, KeyedEntityHead, PackedHeads, SpanContent, TreeSpan, TreeSpanDigest, TreeSpanInventory,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

/// The entity-id offer of [`members`] that lists `heads`.
fn offer(heads: &[(&proto::Event, proto::Clock)]) -> anyhow::Result<KnownState> {
    let mut listed: Vec<_> = heads
        .iter()
        .map(|(event, head)| KeyedEntityHead { key: vec![], entity: EntityHead { entity_id: event.entity_id, head: head.clone() } })
        .collect();
    listed.sort_by_key(|head| head.entity.entity_id);
    let inventory = TreeSpanInventory { bounds: TreeSpan::all(), heads: PackedHeads::try_from(listed)? };
    Ok(KnownState { spec: entity_id_spec(&members()), spans: vec![SpanContent::Heads(inventory)] })
}

/// Each entity an account lists, with its head.
fn listed(account: &KnownState) -> BTreeMap<proto::EntityId, proto::Clock> {
    let mut listed = BTreeMap::new();
    for span in &account.spans {
        let SpanContent::Heads(inventory) = span else { panic!("a small result is described by its heads") };
        listed.extend(inventory.heads.to_vec().into_iter().map(|head| (head.entity.entity_id, head.entity.head)));
    }
    listed
}

fn ids(updates: &[Vec<proto::SubscriptionUpdateItem>]) -> BTreeSet<proto::EntityId> {
    updates.iter().flatten().map(|item| item.entity_id).collect()
}

#[tokio::test]
async fn an_offer_is_answered_with_the_snapshots_account_and_what_it_lacks_is_pushed() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, mut messages) = recording_peer(&node).await?;
    let current = create(&[&node], "current").await?;
    let behind = create(&[&node], "behind").await?;
    let edit = labeled(Some(&behind), "edited")?;
    apply(&node, &edit).await?;
    let unlisted = create(&[&node], "unlisted").await?;
    // An entity of another model that changed since the requester listed it: it has left the result.
    let outside = genesis_in(proto::ModelId::EntityId(proto::EntityId::from_bytes([9; 32])), "outside")?;
    apply(&node, &outside).await?;
    apply(&node, &labeled(Some(&outside), "moved on")?).await?;
    let unknown = labeled(None, "never committed")?;

    let known = offer(&[
        (&current, proto::Clock::singleton(&current)),
        (&behind, proto::Clock::singleton(&behind)),
        (&outside, proto::Clock::singleton(&outside)),
        (&unknown, proto::Clock::singleton(&unknown)),
    ])?;
    let query = proto::QueryId::new();
    let target =
        answer_offer(&node, peer, members(), known, vec![], OfferedIn::SubscribeQuery { query_id: query, cancel: CancelFlag::default() })
            .await?;

    let members_now = BTreeMap::from([
        (current.entity_id, proto::Clock::singleton(&current)),
        (behind.entity_id, proto::Clock::singleton(&edit)),
        (unlisted.entity_id, proto::Clock::singleton(&unlisted)),
    ]);
    assert_eq!(listed(&target), members_now, "the target is the snapshot's own account");
    let updates = pushed(&mut messages, 3).await;
    assert_eq!(
        ids(&updates),
        BTreeSet::from([behind.entity_id, unlisted.entity_id, outside.entity_id]),
        "what the offer holds older or lacks, and what left the result; nothing it holds current, nothing never committed"
    );
    let initial = vec![(query, proto::MembershipChange::Initial)];
    assert!(updates.iter().flatten().all(|item| item.predicate_relevance == initial), "every item is the subscription's initial data");
    Ok(())
}

#[tokio::test]
async fn an_offer_under_a_spec_no_tree_serves_is_answered_under_entity_id_keys_and_pushes_every_member() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, mut messages) = recording_peer(&node).await?;
    let first = create(&[&node], "first").await?;
    let second = create(&[&node], "second").await?;
    // A key part the responder's catalog does not type, under which the offer
    // lists the first member at its current head.
    let untyped = proto::PropertyId::EntityId(proto::EntityId::from_bytes([99; 32]));
    let spec = proto::TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(untyped, ValueType::I64)]), members().predicate);
    let mut known = offer(&[(&first, proto::Clock::singleton(&first))])?;
    known.spec = spec;

    let target = answer_offer(&node, peer, members(), known, vec![], OfferedIn::Fetch).await?;

    assert_eq!(target.spec, entity_id_spec(&members()));
    assert_eq!(listed(&target).len(), 2);
    assert_eq!(
        ids(&pushed(&mut messages, 2).await),
        BTreeSet::from([first.entity_id, second.entity_id]),
        "every member, the listed one included: the offer's heads are not read"
    );
    Ok(())
}

#[tokio::test]
async fn a_malformed_offer_is_refused_before_any_tree_is_admitted_for_it() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, _messages) = recording_peer(&node).await?;
    let entity = create(&[&node], "listed").await?;
    let mut overlapping = offer(&[(&entity, proto::Clock::singleton(&entity))])?;
    overlapping.spans.push(overlapping.spans[0].clone());

    assert!(answer_offer(&node, peer, members(), overlapping, vec![], OfferedIn::Fetch).await.is_err());
    assert_eq!(node.resident_trees.len(), 0, "no tree was admitted to compare the offer with");
    Ok(())
}

/// The offer of a copy holding `source`: a frontier of span digests, several
/// of them nonempty.
async fn frontier(source: &[Attested<EntityState>]) -> anyhow::Result<KnownState> {
    let known = snapshot(source.to_vec()).await?.offer().await?;
    assert!(known.spans.iter().all(|span| matches!(span, SpanContent::Digest(_))), "a frontier of digests");
    assert!(known.spans.iter().filter(|span| span.withholds_push()).count() > 2, "several of them nonempty");
    Ok(known)
}

/// The digests `known` gives, in address order.
fn offered_digests(known: &KnownState) -> Vec<TreeSpanDigest> {
    let mut digests: Vec<_> = known
        .spans
        .iter()
        .map(|span| match span {
            SpanContent::Digest(digest) => digest.clone(),
            SpanContent::Heads(_) => panic!("a frontier gives digests"),
        })
        .collect();
    digests.sort_by(|a, b| a.bounds.cmp_start(&b.bounds));
    digests
}

/// `account`'s spans, in address order.
fn in_address_order(account: &KnownState) -> Vec<SpanContent> {
    let mut spans = account.spans.clone();
    spans.sort_by(|a, b| a.bounds().cmp_start(b.bounds()));
    spans
}

/// `source` with every member at `head`.
fn at_head(mut source: Vec<Attested<EntityState>>, head: &proto::Clock) -> Vec<Attested<EntityState>> {
    for state in &mut source {
        state.payload.state.head = head.clone();
    }
    source
}

/// A head no member of [`states`] holds.
fn another_head() -> proto::Clock { proto::Clock::genesis(proto::EventId::from_bytes([9; 32])) }

/// A head of `events` concurrent events, which no member of [`states`] holds.
fn head_of(events: u32) -> anyhow::Result<proto::Clock> {
    let tips = (0..events).map(|n| {
        let mut bytes = [9; 32];
        bytes[..4].copy_from_slice(&n.to_be_bytes());
        (1, proto::EventId::from_bytes(bytes))
    });
    Ok(proto::Clock::new(tips.collect::<Vec<_>>())?)
}

#[tokio::test]
async fn an_offered_span_whose_digest_differs_is_answered_with_its_heads_and_matching_spans_keep_their_digests() -> anyhow::Result<()> {
    let source = states(4096, 8);
    let known = frontier(&source).await?;
    // The responder holds one member at another head.
    let mut current = source;
    current[7].payload.state.head = another_head();
    let changed = current[7].payload.entity_id;
    let mut responder = snapshot(current).await?;

    let answered = target_for(&mut responder, &known).await?;

    answered.validate()?;
    let address = responder.entries[&changed].address().to_vec();
    let offered = offered_digests(&known);
    let spans = in_address_order(&answered);
    assert_eq!(spans.len(), offered.len(), "the target follows the offer's spans");
    for (offered, answered) in offered.iter().zip(spans) {
        match answered {
            SpanContent::Heads(inventory) => {
                assert!(offered.bounds.contains(&address), "only the span whose digest differs lists heads");
                assert_eq!(inventory.bounds, offered.bounds, "over exactly the offered span");
                let held: Vec<_> = responder.entries_in(&offered.bounds).map(|(id, entry)| (id, entry.head.clone())).collect();
                let listed: Vec<_> = inventory.heads.to_vec().into_iter().map(|head| (head.entity.entity_id, head.entity.head)).collect();
                assert_eq!(listed, held, "every head the responder holds within it");
            }
            SpanContent::Digest(digest) => {
                assert!(!offered.bounds.contains(&address), "the span whose digest differs lists its heads");
                assert_eq!(&digest, offered, "a matching digest is kept as offered");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn differing_spans_are_listed_fewest_members_first_until_the_head_limit_and_the_rest_get_the_responders_digests() -> anyhow::Result<()>
{
    let source = states(4096, 8);
    let known = frontier(&source).await?;
    // Every member is at another head, so every nonempty offered span
    // differs, and together they hold more heads than one offer may.
    let mut responder = snapshot(at_head(source, &another_head())).await?;

    let answered = target_for(&mut responder, &known).await?;

    answered.validate()?;
    let (mut listed, mut digests) = (Vec::new(), Vec::new());
    for (offered, answered) in offered_digests(&known).into_iter().zip(in_address_order(&answered)) {
        match answered {
            // An empty span matches: the responder holds no member there either.
            SpanContent::Digest(digest) if offered.digest.count() == 0 => assert_eq!(digest, offered),
            SpanContent::Heads(inventory) => listed.push(inventory.heads.len()),
            SpanContent::Digest(digest) => {
                let own = responder.span_digest(offered.bounds.clone()).await?;
                assert_eq!(digest, TreeSpanDigest { digest: own.digest, ..offered }, "the responder's digest under the offered prefix");
                digests.push(own.digest.count() as usize);
            }
        }
    }
    let (most_listed, fewest_digested) = (*listed.iter().max().unwrap(), *digests.iter().min().unwrap());
    assert!(most_listed <= fewest_digested, "no digest span holds fewer members than a listed one");
    let heads: usize = listed.iter().sum();
    assert!(heads <= MAX_OFFER_HEADS && heads + fewest_digested > MAX_OFFER_HEADS, "listing stopped at the head limit: {heads}");
    Ok(())
}

#[tokio::test]
async fn differing_spans_are_listed_only_where_their_heads_fit_an_offers_work() -> anyhow::Result<()> {
    let source = states(4096, 8);
    let known = frontier(&source).await?;
    // Every member is at another head of forty events, so that the heads of
    // the differing spans outweigh an offer's work long before they outnumber
    // its heads.
    let mut responder = snapshot(at_head(source, &head_of(40)?)).await?;

    let answered = target_for(&mut responder, &known).await?;

    answered.validate()?;
    let listed = |span: &SpanContent| match span {
        SpanContent::Heads(inventory) => inventory.heads.len(),
        SpanContent::Digest(_) => 0,
    };
    let heads: usize = answered.spans.iter().map(listed).sum();
    assert!(heads > 0 && heads < MAX_OFFER_HEADS, "the work limit, short of the head limit, bounds the listing: {heads}");
    Ok(())
}

#[tokio::test]
async fn a_differing_span_whose_heads_fit_is_listed_even_after_a_smaller_one_whose_heads_do_not() -> anyhow::Result<()> {
    let source = states(4096, 8);
    let known = frontier(&source).await?;
    // The responder holds every member at another head, and the members of the
    // offered span with the fewest of them at heads too heavy for that span to
    // fit an offer. Listing reaches that span first; larger spans of light
    // heads still fit after it.
    let smallest =
        offered_digests(&known).into_iter().filter(|digest| digest.digest.count() > 0).min_by_key(|digest| digest.digest.count());
    let smallest = smallest.expect("a nonempty offered span");
    let members = smallest.digest.count() as usize;
    let heavy = head_of((MAX_OFFER_BYTES / (33 * members) + 1) as u32)?;
    let within: std::collections::HashSet<_> = snapshot(source.clone()).await?.entries_in(&smallest.bounds).map(|(id, _)| id).collect();
    let mut current = at_head(source, &another_head());
    for state in &mut current {
        if within.contains(&state.payload.entity_id) {
            state.payload.state.head = heavy.clone();
        }
    }
    let mut responder = snapshot(current).await?;

    let answered = target_for(&mut responder, &known).await?;

    answered.validate()?;
    let spans = in_address_order(&answered);
    let heavy_span = spans.iter().find(|span| *span.bounds() == smallest.bounds).expect("the offered span is answered");
    assert!(matches!(heavy_span, SpanContent::Digest(_)), "the span whose heads do not fit gives a digest");
    let listed_larger = spans.iter().any(|span| matches!(span, SpanContent::Heads(inventory) if inventory.heads.len() > members));
    assert!(listed_larger, "larger spans whose heads fit are still listed after it");
    Ok(())
}

#[tokio::test]
async fn an_offer_whose_spans_and_the_ranges_it_leaves_out_pass_the_span_limit_gets_the_snapshots_own_account() -> anyhow::Result<()> {
    let source = states(4096, 8);
    let mut requester = snapshot(source.clone()).await?;
    // A digest of each of the first hundred members alone, leaving out the
    // ranges around each: more spans than one offer may carry in all.
    let addresses: Vec<_> = requester.entries_in(&TreeSpan::all()).take(100).map(|(_, entry)| entry.address().to_vec()).collect();
    let mut spans = Vec::new();
    for address in addresses {
        let alone = TreeSpan::new(Bound::Included(address.clone()), Bound::Included(address));
        spans.push(SpanContent::Digest(requester.span_digest(alone).await?));
    }
    let known = KnownState { spec: spec(), spans };
    known.validate()?;
    let mut responder = snapshot(source).await?;

    let answered = target_for(&mut responder, &known).await?;

    assert_eq!(bincode::serialize(&answered)?, bincode::serialize(&responder.offer().await?)?, "the snapshot's own account");
    Ok(())
}

#[tokio::test]
async fn span_questions_are_answered_from_a_fresh_snapshot_each_time() -> anyhow::Result<()> {
    let node = durable().await?;
    let _first = create(&[&node], "first").await?;
    let questions = || vec![SpanQuestion::Digest(TreeSpan::all()), SpanQuestion::Heads(TreeSpan::all())];
    let answered = |response| match response {
        proto::NodeResponseBody::SpansCompared { answers } => match answers.as_slice() {
            [SpanAnswer::Digest(digest), SpanAnswer::Heads(page)] => (digest.digest.count(), page.heads.len()),
            other => panic!("one answer per question, in order: {other:?}"),
        },
        other => panic!("{other:?}"),
    };

    let spec = entity_id_spec(&members());
    assert_eq!(answered(compare_spans(&node, members(), spec.clone(), questions(), vec![]).await?), (1, 1));
    let _second = create(&[&node], "second").await?;
    assert_eq!(
        answered(compare_spans(&node, members(), spec.clone(), questions(), vec![]).await?),
        (2, 2),
        "a later request sees later commits"
    );

    let too_many = vec![SpanQuestion::Heads(TreeSpan::all()); proto::state_sync::limits::MAX_HEAD_QUESTIONS + 1];
    assert!(compare_spans(&node, members(), spec, too_many, vec![]).await.is_err(), "questions are validated");
    Ok(())
}

#[tokio::test]
async fn entity_repairs_push_what_is_behind_and_report_held_heads_and_unavailable_entities() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, mut messages) = recording_peer(&node).await?;
    let current = create(&[&node], "current").await?;
    let behind = create(&[&node], "behind").await?;
    let edit = labeled(Some(&behind), "edited")?;
    apply(&node, &edit).await?;
    let unknown = labeled(None, "never committed")?;
    let known = |event: &proto::Event, head: proto::Clock| proto::EntityHead { entity_id: event.entity_id, head };

    let response = sync_entities(
        &node,
        peer,
        members(),
        vec![
            known(&current, proto::Clock::singleton(&current)),
            known(&behind, proto::Clock::singleton(&behind)),
            known(&unknown, proto::Clock::default()),
        ],
        vec![],
    )
    .await?;

    let proto::NodeResponseBody::EntitiesSynced { held, unavailable } = response else { panic!("{response:?}") };
    let held: BTreeMap<_, _> = held.into_iter().map(|entity| (entity.entity_id, entity.head)).collect();
    assert_eq!(
        held,
        BTreeMap::from([(current.entity_id, proto::Clock::singleton(&current)), (behind.entity_id, proto::Clock::singleton(&edit))]),
        "the head held of each entity it may send"
    );
    assert_eq!(unavailable, vec![unknown.entity_id]);
    let updates = pushed(&mut messages, 1).await;
    assert_eq!(ids(&updates), BTreeSet::from([behind.entity_id]), "only what the requester holds older is pushed");
    assert!(updates.iter().flatten().all(|item| item.predicate_relevance.is_empty()), "a repair belongs to no standing query");
    Ok(())
}

#[tokio::test]
async fn an_entity_repair_names_at_most_a_page_of_entities_each_once() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, _messages) = recording_peer(&node).await?;
    let entity = proto::EntityHead { entity_id: proto::EntityId::random(), head: proto::Clock::default() };
    let twice = vec![entity.clone(), entity];
    assert!(sync_entities(&node, peer, members(), twice, vec![]).await.is_err(), "an entity named twice");
    let many = (0..=MAX_PAGE_HEADS).map(|_| proto::EntityHead { entity_id: proto::EntityId::random(), head: proto::Clock::default() });
    assert!(sync_entities(&node, peer, members(), many.collect(), vec![]).await.is_err(), "more than a page");
    Ok(())
}
