//! Comparing offers with snapshots: equal and differing digests, inventories,
//! unknown ranges and moved members.

use super::*;
use crate::node::state_sync::{fixtures::*, tree::fixtures::snapshot_of};
use ankurah_digest_tree::{Digest, NodePrefix};
use ankurah_proto::{self as proto, state_sync::limits::MAX_PAGE_HEADS, Attested, EntityState, TreeSpanDigest};
use std::collections::BTreeSet;

/// 4096 entities whose ids share a first byte, so that their offer splits into several
/// digests, one of them empty.
fn spread_states() -> Vec<Attested<EntityState>> {
    let mut source = states(4096, 8);
    for (index, state) in source.iter_mut().enumerate() {
        let mut id = [0; proto::EntityId::BYTE_LEN];
        id[0] = 0x20;
        id[1..5].copy_from_slice(&(index as u32).to_be_bytes());
        state.payload.entity_id = proto::EntityId::from_bytes(id);
    }
    source
}

/// The offer of a requester that knows nothing.
fn unknown() -> KnownState { KnownState { spec: spec(), spans: vec![] } }

fn ids(states: &[Attested<EntityState>]) -> BTreeSet<EntityId> { states.iter().map(|state| state.payload.entity_id).collect() }

fn lacking_ids(comparison: &OfferComparison) -> BTreeSet<EntityId> { comparison.lacking.iter().map(|(id, _)| *id).collect() }

/// The position of the first of `known`'s digest spans whose count `select` accepts.
fn digest_span(known: &KnownState, select: impl Fn(i64) -> bool) -> usize {
    known.spans.iter().position(|span| matches!(span, SpanContent::Digest(digest) if select(digest.digest.count()))).unwrap()
}

#[tokio::test]
async fn an_equal_digest_frontier_lacks_nothing() -> anyhow::Result<()> {
    let source = spread_states();
    let known = snapshot(source.clone()).await?.offer().await?;
    assert!(known.spans.len() > 1 && known.spans.iter().all(|span| matches!(span, SpanContent::Digest(_))));
    let comparison = OfferComparison::new(&snapshot(source).await?, known)?;
    assert!(comparison.lacking.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_withheld_span_lacks_every_member_within_it() -> anyhow::Result<()> {
    let source = spread_states();
    let mut known = snapshot(source.clone()).await?.offer().await?;
    let SpanContent::Digest(withheld) = known.spans.remove(digest_span(&known, |count| count > 0)) else { unreachable!() };
    let responder = snapshot(source).await?;
    let within: BTreeSet<_> = responder.entries_in(&withheld.bounds).map(|(id, _)| id).collect();
    let comparison = OfferComparison::new(&responder, known)?;
    assert_eq!(lacking_ids(&comparison), within);
    assert!(comparison.lacking.iter().all(|(_, head)| head.is_none()), "the offer listed none of them");
    Ok(())
}

#[tokio::test]
async fn an_equal_inventory_in_place_of_a_digest_lacks_nothing() -> anyhow::Result<()> {
    let source = spread_states();
    let mut requester = snapshot(source.clone()).await?;
    let mut known = requester.offer().await?;
    let small = digest_span(&known, |count| count > 0 && count <= MAX_PAGE_HEADS as i64);
    let SpanContent::Digest(digest) = &known.spans[small] else { unreachable!() };
    let page = requester.page(digest.bounds.clone())?;
    assert_eq!(page.bounds, digest.bounds, "one page holds the whole span");
    known.spans[small] = SpanContent::Heads(page);
    let comparison = OfferComparison::new(&snapshot(source).await?, known)?;
    assert!(comparison.lacking.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_changed_member_within_a_digest_span_is_left_to_the_requesters_descent() -> anyhow::Result<()> {
    let source = spread_states();
    let known = snapshot(source.clone()).await?.offer().await?;
    let mut current = source;
    current[7].payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([9; 32]));
    let comparison = OfferComparison::new(&snapshot(current).await?, known)?;
    assert!(comparison.lacking.is_empty(), "a digest span names no entities");
    Ok(())
}

#[tokio::test]
async fn a_requester_that_knows_nothing_lacks_every_member() -> anyhow::Result<()> {
    let source = states(5, 8);
    let comparison = OfferComparison::new(&snapshot(source.clone()).await?, unknown())?;
    assert_eq!(lacking_ids(&comparison), ids(&source));
    assert!(comparison.lacking.iter().all(|(_, head)| head.is_none()));
    Ok(())
}

#[tokio::test]
async fn an_empty_digest_over_members_lacks_them() -> anyhow::Result<()> {
    let source = states(3, 8);
    let empty = TreeSpanDigest { prefix: NodePrefix::root(), bounds: TreeSpan::all(), digest: Digest::empty() };
    let known = KnownState { spec: spec(), spans: vec![SpanContent::Digest(empty)] };
    let comparison = OfferComparison::new(&snapshot(source.clone()).await?, known)?;
    assert_eq!(lacking_ids(&comparison), ids(&source));
    Ok(())
}

#[tokio::test]
async fn an_inventory_lacks_entrants_changed_heads_and_departures() -> anyhow::Result<()> {
    let source = states(4, 8);
    let known = snapshot(source.clone()).await?.offer().await?;
    assert!(matches!(known.spans.as_slice(), [SpanContent::Heads(_)]), "four heads fit one inventory");
    // The responder holds the first and last unchanged, the second at a newer
    // head, an entrant, and not the third.
    let mut current = vec![source[0].clone(), source[1].clone(), source[3].clone()];
    current[1].payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([9; 32]));
    let entrant = states(1, 8).pop().unwrap();
    current.push(entrant.clone());
    let comparison = OfferComparison::new(&snapshot(current).await?, known)?;
    let lacking: BTreeMap<_, _> = comparison.lacking.into_iter().collect();
    let listed = |index: usize| (source[index].payload.entity_id, Some(source[index].payload.state.head.clone()));
    assert_eq!(lacking, BTreeMap::from([listed(1), listed(2), (entrant.payload.entity_id, None)]));
    Ok(())
}

#[tokio::test]
async fn a_member_that_moved_within_an_inventory_is_lacked_once_from_its_listed_head() -> anyhow::Result<()> {
    let (before, after, other) = (positioned_state(1, 10, 2)?, positioned_state(1, 20, 3)?, positioned_state(2, 15, 2)?);
    let known = snapshot_of(position_spec(), vec![before.clone(), other.clone()]).await?.offer().await?;
    let responder = snapshot_of(position_spec(), vec![after.clone(), other]).await?;
    let comparison = OfferComparison::new(&responder, known)?;
    assert_eq!(comparison.lacking, vec![(after.payload.entity_id, Some(before.payload.state.head))]);
    Ok(())
}

#[tokio::test]
async fn a_member_that_moved_into_an_unknown_range_keeps_the_head_the_offer_listed() -> anyhow::Result<()> {
    let (moved, kept) = (positioned_state(1, 10, 2)?, positioned_state(2, 30, 2)?);
    let requester = snapshot_of(position_spec(), vec![moved.clone(), kept.clone()]).await?;
    // The offer lists the heads below the kept member and says nothing from it on.
    let split = requester.entries[&kept.payload.entity_id].address().to_vec();
    let below = requester.page(TreeSpan::new(TreeSpan::all().start, Bound::Excluded(split)))?;
    let known = KnownState { spec: position_spec(), spans: vec![SpanContent::Heads(below)] };
    let responder = snapshot_of(position_spec(), vec![positioned_state(1, 40, 3)?, kept.clone()]).await?;
    let comparison = OfferComparison::new(&responder, known)?;
    let lacking: BTreeMap<_, _> = comparison.lacking.into_iter().collect();
    assert_eq!(lacking, BTreeMap::from([(moved.payload.entity_id, Some(moved.payload.state.head)), (kept.payload.entity_id, None)]));
    Ok(())
}

#[tokio::test]
async fn an_invalid_offer_or_another_spec_is_refused() -> anyhow::Result<()> {
    let source = spread_states();
    let mut overlapping = snapshot(source.clone()).await?.offer().await?;
    overlapping.spans.push(overlapping.spans[0].clone());
    assert!(OfferComparison::new(&snapshot(source.clone()).await?, overlapping).is_err());
    let mut other_spec = snapshot(source.clone()).await?.offer().await?;
    other_spec.spec = position_spec();
    assert!(OfferComparison::new(&snapshot(source).await?, other_spec).is_err());
    Ok(())
}
