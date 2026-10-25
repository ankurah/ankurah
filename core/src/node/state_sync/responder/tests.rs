//! The responder's entry point and handlers: what each answers, and what it
//! pushes on the update channel.

use super::*;
use crate::node::state_sync::fixtures::*;
use ankurah_core_types::{IndexKeyPart, KeySpec, ValueType};
use ankurah_proto::{EntityHead, KeyedEntityHead, PackedHeads, SpanContent, TreeSpan, TreeSpanInventory};
use std::collections::{BTreeMap, BTreeSet};

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
    let initial = vec![(query, proto::MembershipChange::Initial)];
    let target = answer_offer(&node, peer, members(), known, vec![], initial.clone()).await?;

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
    assert!(updates.iter().flatten().all(|item| item.predicate_relevance == initial), "every item carries the relevance asked for");
    Ok(())
}

#[tokio::test]
async fn an_offer_under_a_spec_no_tree_serves_is_answered_under_entity_id_keys_and_pushes_every_member() -> anyhow::Result<()> {
    let node = durable().await?;
    let (peer, mut messages) = recording_peer(&node).await?;
    let first = create(&[&node], "first").await?;
    let second = create(&[&node], "second").await?;
    // A key part the responder's catalog does not type.
    let untyped = proto::PropertyId::EntityId(proto::EntityId::from_bytes([99; 32]));
    let spec = proto::TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(untyped, ValueType::I64)]), members().predicate);
    let known = KnownState { spec, spans: Vec::new() };

    let target = answer_offer(&node, peer, members(), known, vec![], vec![]).await?;

    assert_eq!(target.spec, entity_id_spec(&members()));
    assert_eq!(listed(&target).len(), 2);
    assert_eq!(ids(&pushed(&mut messages, 2).await), BTreeSet::from([first.entity_id, second.entity_id]));
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
