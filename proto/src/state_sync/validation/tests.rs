use std::ops::Bound::{Excluded, Included, Unbounded};

use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, Value, ValueType};
use ankurah_digest_tree::{leaf_address, CanonicalCodec};

use super::*;
use crate::{
    AttestationSet, CausalAssertionFragment, CausalRelation, Clock, DeltaContent, EntityDelta, EntityDeltaChunk, EntityHead, EntityId,
    EventId, KeyedEntityHead, PackedHeads, RepairRequest, State, StateFragment, StateFrame, StateRound, StateStreamId,
};

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
fn snapshot(head: Clock) -> DeltaContent {
    DeltaContent::StateSnapshot {
        state: StateFragment { state: State { head, ..Default::default() }, attestations: AttestationSet::default() },
    }
}

#[test]
fn an_initial_reply_opens_round_zero_with_frame_zero() {
    let reply = |number, sequence| StateReply {
        round: StateRound { number, revision: 0 },
        frame: StateFrame { stream: StateStreamId::new(), sequence, body: StateFrameBody::RestartRequired },
    };
    assert!(reply(0, 0).validate().is_ok());
    assert!(reply(1, 0).validate().is_err());
    assert!(reply(0, 1).validate().is_err());
}

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
    let key = |n| CanonicalCodec.encode_nullable(spec.key_spec(), &[Some(Value::I64(n))]).unwrap();
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
fn frames_carry_bounded_items_that_reach_their_targets() {
    let chunk = |last| EntityDeltaChunk { entity_id: id(1), offset: 0, bytes: vec![1].into(), last };
    let repair = |target, content| RepairItem::Delta { target, delta: EntityDelta { entity_id: id(1), content } };
    let valid = [
        StateFrameBody::LiveChunk { revision: 1, member: None, chunk: chunk(true), complete: true },
        StateFrameBody::LiveRepair { items: vec![repair(head(1), snapshot(head(1)))] },
        StateFrameBody::Live {
            items: vec![QueryChange { entity_id: id(1), member: None, delta: Some(snapshot(head(1))) }],
            complete: true,
            revision: 1,
        },
    ];
    for body in valid {
        assert!(body.validate(&spec()).is_ok(), "{body:?}");
    }
    let bridge = DeltaContent::EventBridge { events: vec![] };
    let relation = DeltaContent::StateAndRelation {
        state: StateFragment { state: State::default(), attestations: AttestationSet::default() },
        relation: CausalAssertionFragment { relation: CausalRelation::Equal, attestations: AttestationSet::default() },
    };
    let keyed = TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(PropertyId::Id, ValueType::EntityId)]), ankql::ast::Predicate::True);
    let member = Some(QueryMember { key: vec![], head: head(1) });
    let invalid = [
        StateFrameBody::LiveChunk { revision: 1, member: None, chunk: chunk(false), complete: true },
        StateFrameBody::LiveChunk {
            revision: 1,
            member: None,
            chunk: EntityDeltaChunk { bytes: Default::default(), ..chunk(true) },
            complete: true,
        },
        StateFrameBody::LiveRepair { items: vec![repair(head(2), snapshot(head(1)))] },
        StateFrameBody::LiveRepair { items: vec![repair(head(1), bridge.clone())] },
        StateFrameBody::Live {
            items: vec![QueryChange { entity_id: id(1), member: None, delta: Some(bridge) }],
            complete: true,
            revision: 1,
        },
        StateFrameBody::Live {
            items: vec![QueryChange { entity_id: id(1), member: member.clone(), delta: Some(relation) }],
            complete: true,
            revision: 1,
        },
        StateFrameBody::Round {
            round: 0,
            items: vec![StateItem::NeedHeads(TreeSpan::all()); MAX_FRAME_ITEMS + 1],
            progress: crate::ComparisonProgress::Continue,
        },
    ];
    for body in invalid {
        assert!(body.validate(&spec()).is_err(), "{body:?}");
    }
    let live = StateFrameBody::Live { items: vec![QueryChange { entity_id: id(1), member, delta: None }], complete: true, revision: 1 };
    assert!(live.validate(&keyed).is_err(), "an empty key is not canonical under an entity-ID key part");
}

#[test]
fn feedback_carries_at_most_one_feedback_of_answers() {
    let request = |n| RepairRequest { entity_id: id(n), target: head(1) };
    let requests = |count: u16| FeedbackAnswers::LiveRepair { requests: (0..count).map(request).collect() };
    assert!(requests(MAX_FEEDBACK_ANSWERS as u16).validate(&spec()).is_ok());
    assert!(requests(MAX_FEEDBACK_ANSWERS as u16 + 1).validate(&spec()).is_err());
    let page =
        |ids: Vec<u16>| FeedbackAnswers::Round { round: 0, answers: vec![StateAnswer::Heads(inventory(TreeSpan::all(), ids.into_iter()))] };
    assert!(page(vec![1, 2]).validate(&spec()).is_ok());
    assert!(page(vec![2, 1]).validate(&spec()).is_err(), "a malformed page answer");
}
