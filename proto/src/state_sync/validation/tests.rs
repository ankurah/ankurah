use std::ops::Bound::{Excluded, Included, Unbounded};

use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, Value, ValueType};
use ankurah_digest_tree::{leaf_address, CanonicalCodec, NodePrefix};

use super::*;
use crate::{Clock, EntityHead, EntityId, EventId, KeyedEntityHead, PackedHeads, TreeSpanDigest};

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
    let tree = TreeSpan::all();
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
    let tree = TreeSpan::all();
    let spans = vec![
        keyed(TreeSpan::new(tree.start.clone(), Excluded(split.clone())), key(1)),
        keyed(TreeSpan::new(Included(split), Unbounded), key(3)),
    ];
    let error = KnownState { spec, spans }.validate().unwrap_err();
    assert!(error.to_string().contains("twice"), "{error}");
}

#[test]
fn an_offer_counts_its_heads_across_inventories() {
    let tree = TreeSpan::all();
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
fn an_offer_refuses_inventories_whose_work_together_passes_its_limit() {
    // Four tips make each head 166 bytes of work, so a thousand heads fit one inventory
    // under the codec's cap, while two such inventories pass the offer's work limit
    // with fewer heads in all than its head limit.
    let clock = Clock::new((1..=4).map(|n| (1, EventId::from_bytes([n; 32]))).collect::<Vec<_>>()).unwrap();
    let inventory = |bounds, ids: std::ops::Range<u16>| {
        let heads =
            ids.map(|n| KeyedEntityHead { key: vec![], entity: EntityHead { entity_id: id(n), head: clock.clone() } }).collect::<Vec<_>>();
        SpanContent::Heads(TreeSpanInventory { bounds, heads: PackedHeads::try_from(heads).unwrap() })
    };
    let split = leaf_address(&[], id(1000));
    let below = inventory(TreeSpan::new(TreeSpan::all().start, Excluded(split.clone())), 0..1000);
    let above = inventory(TreeSpan::new(Included(split), Unbounded), 1000..2000);
    assert!(offer(vec![below.clone()]).validate().is_ok());
    let error = offer(vec![below, above]).validate().unwrap_err();
    assert!(error.to_string().contains("work limit"), "{error}");
}

#[test]
fn a_page_refuses_work_past_its_limit_that_prefix_sharing_kept_off_the_wire() {
    // A page of heads under one long key: sharing it keeps the wire bytes small, while
    // the work counts every key whole.
    let spec = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(PropertyId::Id, ValueType::String)]), ankql::ast::Predicate::True);
    let page = |key_len: usize| {
        let key = CanonicalCodec::encode_nullable(spec.key_spec(), &[Some(Value::String("k".repeat(key_len)))]).unwrap();
        let heads = (0..MAX_PAGE_HEADS as u16)
            .map(|n| KeyedEntityHead { key: key.clone(), entity: EntityHead { entity_id: id(n), head: head(1) } })
            .collect::<Vec<_>>();
        TreeSpanInventory { bounds: TreeSpan::all(), heads: PackedHeads::try_from(heads).unwrap() }
    };
    assert!(page(256).validate_page(&spec).is_ok());
    let long = page(1024);
    assert!(bincode::serialized_size(&long).unwrap() < MAX_PAGE_BYTES as u64 / 4);
    let error = long.validate_page(&spec).unwrap_err();
    assert!(error.to_string().contains("work limit"), "{error}");
}

#[test]
fn a_page_holds_at_most_a_page_of_ordered_heads() {
    let page = |ids: Vec<u16>| inventory(TreeSpan::all(), ids.into_iter());
    assert!(page(vec![1, 2]).validate_page(&spec()).is_ok());
    assert!(page(vec![2, 1]).validate_page(&spec()).is_err(), "unordered heads");
    assert!(page((0..MAX_PAGE_HEADS as u16).collect()).validate_page(&spec()).is_ok());
    assert!(page((0..=MAX_PAGE_HEADS as u16).collect()).validate_page(&spec()).is_err(), "more heads than a page holds");
}

fn digest_of(bounds: TreeSpan) -> SpanAnswer {
    SpanAnswer::Digest(TreeSpanDigest { prefix: NodePrefix::root(), bounds, digest: ankurah_digest_tree::Digest::empty() })
}

#[test]
fn span_questions_stay_within_their_counts_and_the_tree() {
    let heads = |count: usize| vec![SpanQuestion::Heads(TreeSpan::all()); count];
    assert!(SpanQuestion::validate_all(&heads(MAX_HEAD_QUESTIONS)).is_ok());
    assert!(SpanQuestion::validate_all(&heads(MAX_HEAD_QUESTIONS + 1)).is_err(), "too many heads questions");
    assert!(SpanQuestion::validate_all(&vec![SpanQuestion::Digest(TreeSpan::all()); MAX_OFFER_SPANS]).is_ok());
    assert!(SpanQuestion::validate_all(&vec![SpanQuestion::Digest(TreeSpan::all()); MAX_OFFER_SPANS + 1]).is_err(), "too many questions");
    let point = leaf_address(&[], id(3));
    let empty = TreeSpan::new(Excluded(point.clone()), Included(point));
    assert!(SpanQuestion::validate_all(&[SpanQuestion::Digest(empty)]).is_err(), "an empty span");
    let before_the_tree = TreeSpan::new(Unbounded, Unbounded);
    assert!(SpanQuestion::validate_all(&[SpanQuestion::Heads(before_the_tree)]).is_err(), "a span starting before the tree");
}

#[test]
fn a_digest_answer_covers_exactly_its_question() {
    let question = SpanQuestion::Digest(TreeSpan::all());
    assert!(digest_of(TreeSpan::all()).validate(&question, &spec()).is_ok());
    let below = TreeSpan::new(TreeSpan::all().start, Excluded(leaf_address(&[], id(9))));
    assert!(digest_of(below).validate(&question, &spec()).is_err(), "a digest of another span");
    let heads = SpanAnswer::Heads(inventory(TreeSpan::all(), 0..1));
    assert!(heads.validate(&question, &spec()).is_err(), "heads answering a digest question");
}

#[test]
fn a_heads_answer_starts_at_its_question_and_stops_short_only_at_its_last_head() {
    let start = Included(leaf_address(&[], id(1)));
    let question = SpanQuestion::Heads(TreeSpan::new(start.clone(), Unbounded));
    let page = |start, end, ids| SpanAnswer::Heads(inventory(TreeSpan::new(start, end), ids));
    assert!(page(start.clone(), Unbounded, 1..4).validate(&question, &spec()).is_ok(), "the whole span");
    assert!(page(start.clone(), Included(leaf_address(&[], id(3))), 1..4).validate(&question, &spec()).is_ok(), "ends at its last head");
    let short = page(start.clone(), Excluded(leaf_address(&[], id(5))), 1..4);
    assert!(short.validate(&question, &spec()).is_err(), "stops short past its last head");
    let empty = page(start.clone(), Included(leaf_address(&[], id(3))), 1..1);
    assert!(empty.validate(&question, &spec()).is_err(), "stops short with no head");
    let later = page(Included(leaf_address(&[], id(2))), Unbounded, 2..4);
    assert!(later.validate(&question, &spec()).is_err(), "starts after its question");
    let wider = SpanQuestion::Heads(TreeSpan::new(start.clone(), Excluded(leaf_address(&[], id(2)))));
    assert!(page(start, Unbounded, 1..2).validate(&wider, &spec()).is_err(), "extends past its question");
    assert!(digest_of(TreeSpan::all()).validate(&SpanQuestion::Heads(TreeSpan::all()), &spec()).is_err(), "a digest answering heads");
}

#[test]
fn span_answers_pair_one_to_one_with_their_questions() {
    let questions = [SpanQuestion::Digest(TreeSpan::all()), SpanQuestion::Heads(TreeSpan::all())];
    let answers = [digest_of(TreeSpan::all()), SpanAnswer::Heads(inventory(TreeSpan::all(), 0..2))];
    assert!(SpanAnswer::validate_all(&answers, &questions, &spec()).is_ok());
    assert!(SpanAnswer::validate_all(&answers[..1], &questions, &spec()).is_err(), "an answer missing");
    let swapped = [answers[1].clone(), answers[0].clone()];
    assert!(SpanAnswer::validate_all(&swapped, &questions, &spec()).is_err(), "answers out of their questions' order");
}

#[test]
fn a_sync_request_names_at_most_a_page_of_entities_each_once() {
    let entities = |ids: std::ops::Range<u16>| ids.map(|n| EntityHead { entity_id: id(n), head: head(1) }).collect::<Vec<_>>();
    assert!(EntityHead::validate_sync_request(&entities(0..MAX_PAGE_HEADS as u16)).is_ok());
    assert!(EntityHead::validate_sync_request(&entities(0..MAX_PAGE_HEADS as u16 + 1)).is_err(), "more than a page");
    let twice = [entities(0..2), entities(1..2)].concat();
    assert!(EntityHead::validate_sync_request(&twice).is_err(), "an entity named twice");
}
