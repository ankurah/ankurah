use std::ops::Bound::{Excluded, Included, Unbounded};

use ankql::ast::{Predicate, Selection};
use ankurah_core_types::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder, PropertyId, ValueType};
use ankurah_digest_tree::{Digest, HeadHash, Leaf, LtHash16, NodePrefix};
use ankurah_proto::*;
use serde::{de::DeserializeOwned, Serialize};

// Exercise both the binary transport and the JSON tooling representation.
fn wire<T: Serialize + DeserializeOwned>(value: &T) -> T {
    let bytes = bincode::serialize(value).unwrap();
    let decoded: T = bincode::deserialize(&bytes).unwrap();
    assert_eq!(bincode::serialize(&decoded).unwrap(), bytes);
    serde_json::from_value(serde_json::to_value(&decoded).unwrap()).unwrap()
}

fn selection() -> Selection<ankql::ast::Resolved> { Selection::from(Predicate::True) }
fn spec() -> TreeSpec { TreeSpec::new(KeySpec::new(vec![]), Predicate::True) }
fn known(n: u8) -> KnownEntity {
    KnownEntity { entity_id: EntityId::from_bytes([n; 32]), head: Clock::new(vec![(3, EventId::from_bytes([n; 32]))]).unwrap() }
}
fn head(prefix: NodePrefix, bounds: AddressRange, entity: &KnownEntity) -> TreeHead {
    let hash = HeadHash::of(entity.head.ids().map(|id| id.clone().to_bytes()));
    TreeHead { prefix, bounds, digest: Leaf::new(&[], entity.entity_id, hash).element().into() }
}
fn request(body: NodeRequestBody) -> NodeRequest {
    NodeRequest { id: RequestId::new(), from: known(1).entity_id, to: known(2).entity_id, body }
}

#[test]
fn fetch_and_subscribe_carry_the_same_known_state() {
    let summary = head(NodePrefix::root(), AddressRange::under(&NodePrefix::root()), &known(1));
    for body in [
        NodeRequestBody::Fetch { selection: selection(), known_state: Some(KnownState { spec: spec(), heads: vec![summary.clone()] }) },
        NodeRequestBody::SubscribeQuery {
            query_id: QueryId::new(),
            selection: selection(),
            version: 7,
            known_state: Some(KnownState { spec: spec(), heads: vec![summary.clone()] }),
        },
    ] {
        let message = Message::PeerMessage(NodeMessage::Request { auth: vec![], request: request(body) });
        let Message::PeerMessage(NodeMessage::Request { request, .. }) = wire(&message) else { panic!("request") };
        let state = match request.body {
            NodeRequestBody::Fetch { known_state, .. } => known_state.unwrap(),
            NodeRequestBody::SubscribeQuery { version, known_state, .. } => {
                assert_eq!(version, 7);
                known_state.unwrap()
            }
            _ => panic!("query request"),
        };
        assert_eq!(state.spec, spec());
        assert_eq!(state.heads, [summary.clone()]);
    }
}

#[test]
fn no_summary_is_distinct_from_a_known_empty_region() {
    let absent = request(NodeRequestBody::Fetch { selection: selection(), known_state: None });
    let empty = request(NodeRequestBody::Fetch {
        selection: selection(),
        known_state: Some(KnownState {
            spec: spec(),
            heads: vec![TreeHead { prefix: NodePrefix::root(), bounds: AddressRange::under(&NodePrefix::root()), digest: Digest::empty() }],
        }),
    });
    assert!(matches!(wire(&absent).body, NodeRequestBody::Fetch { known_state: None, .. }));
    let NodeRequestBody::Fetch { known_state: Some(state), .. } = wire(&empty).body else { panic!("known empty") };
    assert_eq!(state.heads[0].digest.count(), 0);
    assert_eq!(state.heads[0].bounds, AddressRange::under(&NodePrefix::root()));
}

#[test]
fn followup_batches_mixed_depths_leaf_pages_and_selective_clocks() {
    let comparison = ComparisonId::new();
    let first = head(NodePrefix::root(), AddressRange::under(&NodePrefix::root()), &known(1));
    let prefix = NodePrefix::of(&[0], 3);
    let second = head(prefix.clone(), AddressRange::under(&prefix), &known(2));
    let bounds = AddressRange::new(Included(vec![0]), Excluded(vec![3]));
    let req = request(NodeRequestBody::CompareTree {
        comparison,
        answers: vec![
            StateAnswer::Summary(first.clone()),
            StateAnswer::Summary(second.clone()),
            StateAnswer::Leaves(LeafPage { bounds: bounds.clone(), leaves: vec![KnownLeaf { key: vec![], entity: known(2) }] }),
            StateAnswer::Heads { known: vec![known(20)], unknown: vec![known(21).entity_id] },
        ],
    });
    let NodeRequestBody::CompareTree { comparison: received, answers } = wire(&req).body else { panic!("comparison") };
    assert_eq!(received, comparison);
    assert!(matches!(&answers[0], StateAnswer::Summary(head) if head == &first));
    assert!(matches!(&answers[1], StateAnswer::Summary(head) if head == &second));
    let StateAnswer::Leaves(page) = &answers[2] else { panic!("leaves") };
    assert_eq!(page.bounds, bounds);
    assert_eq!(page.leaves[0].entity, known(2));
    let StateAnswer::Heads { known: clocks, unknown } = &answers[3] else { panic!("clocks") };
    assert_eq!(clocks, &[known(20)]); // Entity need not occur in the offered tree page.
    assert_eq!(unknown, &[known(21).entity_id]);
    let resume = request(NodeRequestBody::CompareTree { comparison, answers: vec![] });
    assert!(matches!(wire(&resume).body, NodeRequestBody::CompareTree { answers, .. } if answers.is_empty()));
}

#[test]
fn initial_replies_describe_the_scope_and_continue_with_mixed_work() {
    let comparison = ComparisonId::new();
    let bounds = AddressRange::new(Excluded(vec![0]), Included(vec![9]));
    let state = StateReply {
        spec: spec(),
        scope: vec![bounds.clone()],
        page: ComparisonReply {
            comparison,
            items: vec![
                StateItem::Equal(AddressRange::new(Excluded(vec![0]), Excluded(vec![1]))),
                StateItem::Summary(head(NodePrefix::root(), bounds.clone(), &known(2))),
                StateItem::Leaves(LeafPage { bounds: AddressRange::new(Included(vec![3]), Included(vec![9])), leaves: vec![] }),
                StateItem::NeedLeaves(bounds.clone()),
                StateItem::NeedHeads(vec![known(2).entity_id]),
            ],
            progress: ComparisonProgress::Continue,
        },
    };
    for body in [NodeResponseBody::Fetch(state.clone()), NodeResponseBody::QuerySubscribed { query_id: QueryId::new(), state }] {
        let response = NodeResponse { request_id: RequestId::new(), from: known(2).entity_id, to: known(1).entity_id, body };
        let message = Message::PeerMessage(NodeMessage::Response(response));
        let Message::PeerMessage(NodeMessage::Response(response)) = wire(&message) else { panic!("response") };
        let state = match response.body {
            NodeResponseBody::Fetch(state) | NodeResponseBody::QuerySubscribed { state, .. } => state,
            _ => panic!("initial state"),
        };
        assert_eq!(state.spec, spec());
        assert_eq!(state.scope, [bounds.clone()]);
        assert_eq!(state.page.comparison, comparison);
        assert_eq!(state.page.progress, ComparisonProgress::Continue);
        assert_eq!(state.page.items.len(), 5);
        assert!(matches!(&state.page.items[2], StateItem::Leaves(page) if page.leaves.is_empty()));
    }
}

#[test]
fn event_bridge_repairs_preserve_the_captured_target_clock() {
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
    let update = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet::default());
    let target = Clock::singleton(&update);
    let response = NodeResponseBody::TreeCompared(ComparisonReply {
        comparison: ComparisonId::new(),
        items: vec![StateItem::Repair {
            target: target.clone(),
            delta: EntityDelta {
                entity_id: genesis.entity_id,
                content: DeltaContent::EventBridge { events: vec![Attested::<Event>::from(update.clone()).into()] },
            },
        }],
        progress: ComparisonProgress::Complete,
    });
    let NodeResponseBody::TreeCompared(page) = wire(&response) else { panic!("comparison reply") };
    let StateItem::Repair { target: received, delta } = &page.items[0] else { panic!("repair") };
    let DeltaContent::EventBridge { events } = &delta.content else { panic!("bridge") };
    assert_eq!(received, &target);
    assert_eq!(delta.entity_id, genesis.entity_id);
    assert_eq!(events[0].parent, Clock::singleton(&genesis));
    let restored: Attested<Event> = (delta.entity_id, events[0].clone()).into();
    assert_eq!(restored.payload.id(), update.id());
}

#[test]
fn oversized_repairs_can_span_replies_without_losing_target_or_content() {
    let entity = known(3);
    let state = State { head: entity.head.clone(), ..Default::default() };
    let content = DeltaContent::StateSnapshot { state: StateFragment { state: state.clone(), attestations: Default::default() } };
    let encoded = bincode::serialize(&content).unwrap();
    let comparison = ComparisonId::new();
    let mut assembled = Vec::new();
    for (i, part) in encoded.chunks(13).enumerate() {
        let last = (i + 1) * 13 >= encoded.len();
        let response = NodeResponseBody::TreeCompared(ComparisonReply {
            comparison,
            items: vec![StateItem::RepairChunk(RepairChunk {
                entity_id: entity.entity_id,
                target: entity.head.clone(),
                offset: (i * 13) as u64,
                bytes: part.to_vec().into(),
                last,
            })],
            progress: if last { ComparisonProgress::Complete } else { ComparisonProgress::Continue },
        });
        let NodeResponseBody::TreeCompared(page) = wire(&response) else { panic!("comparison reply") };
        let StateItem::RepairChunk(chunk) = &page.items[0] else { panic!("chunk") };
        assert_eq!(page.comparison, comparison);
        assert_eq!(chunk.entity_id, entity.entity_id);
        assert_eq!(chunk.target, entity.head);
        assert_eq!(chunk.offset as usize, assembled.len());
        assert_eq!(chunk.last, page.progress == ComparisonProgress::Complete);
        assembled.extend_from_slice(&chunk.bytes);
    }
    let DeltaContent::StateSnapshot { state: fragment } = bincode::deserialize(&assembled).unwrap() else { panic!("snapshot") };
    assert_eq!(fragment.state, state);
}

#[test]
fn completion_application_abandonment_and_snapshot_loss_are_distinct() {
    let comparison = ComparisonId::new();
    for outcome in [ComparisonOutcome::Applied, ComparisonOutcome::Abandoned] {
        let req = request(NodeRequestBody::CloseComparison { comparison, outcome });
        let NodeRequestBody::CloseComparison { comparison: received, outcome: received_outcome } = wire(&req).body else {
            panic!("close comparison")
        };
        assert_eq!(received, comparison);
        assert_eq!(received_outcome, outcome);
    }
    let expired = NodeResponseBody::ComparisonUnavailable { comparison };
    assert!(matches!(wire(&expired), NodeResponseBody::ComparisonUnavailable { comparison: id } if id == comparison));
    let complete = NodeResponseBody::TreeCompared(ComparisonReply { comparison, items: vec![], progress: ComparisonProgress::Complete });
    assert!(matches!(wire(&complete), NodeResponseBody::TreeCompared(page) if page.progress == ComparisonProgress::Complete));
}

#[test]
fn head_deserialization_preserves_the_tree_crates_validation() {
    let root = NodePrefix::root();
    let bounds = AddressRange::under(&NodePrefix::root());
    let mut negative = Digest::<ankurah_digest_tree::Ristretto255>::empty().to_row_bytes();
    let count_offset = negative.len() - 8;
    negative[count_offset..].copy_from_slice(&(-1i64).to_le_bytes());
    let encode = |version, prefix: Vec<u8>, bounds: &AddressRange, digest| {
        bincode::serialize(&(version, prefix, &bounds.start, &bounds.end, digest)).unwrap()
    };
    let empty_digest = Digest::<ankurah_digest_tree::Ristretto255>::empty().to_row_bytes();
    let valid = encode(1u8, root.to_bytes(), &bounds, empty_digest.clone());
    let mut trailing = valid.clone();
    trailing.push(0);
    for bytes in [
        encode(2u8, root.to_bytes(), &bounds, empty_digest.clone()),
        encode(1u8, vec![255], &bounds, empty_digest.clone()),
        encode(1u8, root.to_bytes(), &bounds, negative),
        encode(1u8, root.to_bytes(), &bounds, Digest::<LtHash16>::empty().to_row_bytes()),
        encode(1u8, root.to_bytes(), &AddressRange::new(Included(vec![2]), Excluded(vec![1])), empty_digest.clone()),
        encode(1u8, NodePrefix::of(&[0], 1).to_bytes(), &bounds, empty_digest),
        trailing,
    ] {
        let framed = bincode::serialize(&bytes).unwrap();
        assert!(bincode::deserialize::<TreeHead>(&framed).is_err());
    }
    let decoded: TreeHead = bincode::deserialize(&bincode::serialize(&valid).unwrap()).unwrap();
    assert_eq!(decoded.digest.count(), 0);
    assert_eq!(decoded.bounds, bounds);
}

#[test]
fn deserializing_a_spec_keeps_its_canonical_ascending_key_layout() {
    #[derive(Serialize)]
    struct UnnormalizedSpec {
        key_spec: KeySpec<PropertyId>,
        predicate: Predicate<ankql::ast::Resolved>,
    }
    let mut part = IndexKeyPart::desc(PropertyId::EntityId(known(1).entity_id), ValueType::I64);
    part.nulls = Some(NullsOrder::Last);
    let input = UnnormalizedSpec { key_spec: KeySpec::new(vec![part]), predicate: Predicate::True };
    let decoded: TreeSpec = bincode::deserialize(&bincode::serialize(&input).unwrap()).unwrap();
    assert_eq!(decoded.key_spec().keyparts[0].direction, IndexDirection::Asc);
    assert_eq!(decoded.key_spec().keyparts[0].nulls, None);
    assert_eq!(wire(&decoded), decoded);
    let bounds = AddressRange::new(Unbounded, Excluded(vec![8]));
    assert_eq!(wire(&bounds), bounds);
}
