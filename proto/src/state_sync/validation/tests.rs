use std::ops::Bound::{Excluded, Included, Unbounded};

use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, Value, ValueType};
use ankurah_digest_tree::{leaf_address, CanonicalCodec};

use super::*;
use crate::{Clock, EntityHead, EntityId, EventId, KeyedEntityHead, PackedHeads};

fn spec() -> TreeSpec { TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True) }
fn id(n: u16) -> EntityId {
    let mut bytes = [0; 32];
    bytes[..2].copy_from_slice(&n.to_be_bytes());
    EntityId::from_bytes(bytes)
}
fn head(n: u8) -> Clock { Clock::genesis(EventId::from_bytes([n; 32])) }
fn inventory(bounds: TreeSpan, ids: impl Iterator<Item = u16>) -> TreeSpanInventory {
    let heads = ids.map(|n| KeyedEntityHead { key: vec![], entity: EntityHead { entity_id: id(n), head: head(1) } }).collect::<Vec<_>>();
    TreeSpanInventory { bounds, heads: PackedHeads::try_from(heads).unwrap() }
}
fn offer(spans: Vec<SpanContent>) -> KnownState { KnownState { spec: spec(), spans } }

#[test]
fn an_offer_holds_disjoint_spans_within_the_tree_and_each_entity_once() {
    let tree = TreeSpan::under(&NodePrefix::root());
    let split = leaf_address(&[], id(100));
    let below = TreeSpan::new(tree.start.clone(), Excluded(split.clone()));
    let above = TreeSpan::new(Included(split), Unbounded);
    assert!(offer(vec![SpanContent::Heads(inventory(above.clone(), 100..102)), SpanContent::Heads(inventory(below.clone(), 0..2))])
        .validate()
        .is_ok());
    let overlapping = [SpanContent::Heads(inventory(below.clone(), 0..2)), SpanContent::Heads(inventory(tree.clone(), 0..0))];
    assert!(offer(overlapping.to_vec()).validate().is_err(), "overlapping spans");
    let before_the_tree = TreeSpan::new(Unbounded, Unbounded);
    assert!(offer(vec![SpanContent::Heads(inventory(before_the_tree, 0..0))]).validate().is_err(), "a span starting before the tree");
    let spans = (0..=MAX_OFFER_SPANS as u16)
        .map(|n| {
            let start = leaf_address(&[], id(n));
            SpanContent::Heads(inventory(TreeSpan::new(Included(start.clone()), Included(start)), 0..0))
        })
        .collect();
    assert!(offer(spans).validate().is_err(), "too many spans");
}

#[test]
fn an_offer_lists_each_entity_once_across_inventories() {
    // Under a keyed spec one entity can sit at two addresses, one in each inventory.
    let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(PropertyId::Id, ValueType::I64)]), ankql::ast::Predicate::True);
    let key = |n| CanonicalCodec::encode_nullable(spec.key_spec(), &[Some(Value::I64(n))]).unwrap();
    let split = leaf_address(&key(2), id(0));
    let keyed = |bounds, key| {
        let heads = vec![KeyedEntityHead { key, entity: EntityHead { entity_id: id(7), head: head(1) } }];
        SpanContent::Heads(TreeSpanInventory { bounds, heads: PackedHeads::try_from(heads).unwrap() })
    };
    let tree = TreeSpan::under(&NodePrefix::root());
    let spans = vec![
        keyed(TreeSpan::new(tree.start.clone(), Excluded(split.clone())), key(1)),
        keyed(TreeSpan::new(Included(split), Unbounded), key(3)),
    ];
    let error = KnownState { spec, spans }.validate().unwrap_err();
    assert!(error.to_string().contains("twice"), "{error}");
}

#[test]
fn an_offer_counts_its_heads_across_inventories() {
    let tree = TreeSpan::under(&NodePrefix::root());
    let split = leaf_address(&[], id(MAX_OFFER_HEADS as u16 / 2 + 1));
    let below = TreeSpan::new(tree.start.clone(), Excluded(split.clone()));
    let above = TreeSpan::new(Included(split), Unbounded);
    let half = MAX_OFFER_HEADS as u16 / 2;
    let full = |extra| {
        offer(vec![
            SpanContent::Heads(inventory(below.clone(), 0..half + 1)),
            SpanContent::Heads(inventory(above.clone(), half + 1..MAX_OFFER_HEADS as u16 + extra)),
        ])
    };
    assert!(full(0).validate().is_ok());
    assert!(full(1).validate().is_err());
}

#[test]
fn a_page_holds_at_most_a_page_of_ordered_heads() {
    let page = |ids: Vec<u16>| inventory(TreeSpan::all(), ids.into_iter());
    assert!(page(vec![1, 2]).validate_page(&spec()).is_ok());
    assert!(page(vec![2, 1]).validate_page(&spec()).is_err(), "unordered heads");
    assert!(page((0..MAX_PAGE_HEADS as u16).collect()).validate_page(&spec()).is_ok());
    assert!(page((0..=MAX_PAGE_HEADS as u16).collect()).validate_page(&spec()).is_err(), "more heads than a page holds");
}
