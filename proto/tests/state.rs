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
    let summary = head(NodePrefix::root(), AddressRange::new(Included(vec![]), Excluded(vec![1])), &known(1));
    let leaves = LeafPage {
        bounds: AddressRange::new(Included(vec![1]), Excluded(vec![2])),
        leaves: vec![KnownLeaf { key: vec![1], entity: known(2) }],
    };
    let inventory = KnownState { spec: spec(), regions: vec![KnownRegion::Summary(summary.clone()), KnownRegion::Leaves(leaves.clone())] };
    for body in [
        NodeRequestBody::Fetch { selection: selection(), known_state: Some(inventory.clone()) },
        NodeRequestBody::SubscribeQuery {
            query_id: QueryId::new(),
            selection: selection(),
            version: 7,
            known_state: Some(inventory.clone()),
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
        assert_eq!(state.regions.len(), 2);
        assert!(matches!(&state.regions[0], KnownRegion::Summary(received) if received == &summary));
        let KnownRegion::Leaves(received) = &state.regions[1] else { panic!("inline leaves") };
        assert_eq!(received.bounds, leaves.bounds);
        assert_eq!(received.leaves[0].key, [1]);
        assert_eq!(received.leaves[0].entity, known(2));
    }
}

#[test]
fn unknown_inventory_is_distinct_from_explicit_empty_leaves_or_summary() {
    let absent = request(NodeRequestBody::Fetch { selection: selection(), known_state: None });
    let empty = request(NodeRequestBody::Fetch {
        selection: selection(),
        known_state: Some(KnownState {
            spec: spec(),
            regions: vec![KnownRegion::Leaves(LeafPage { bounds: AddressRange::under(&NodePrefix::root()), leaves: vec![] })],
        }),
    });
    assert!(matches!(wire(&absent).body, NodeRequestBody::Fetch { known_state: None, .. }));
    let NodeRequestBody::Fetch { known_state: Some(state), .. } = wire(&empty).body else { panic!("known empty") };
    let KnownRegion::Leaves(page) = &state.regions[0] else { panic!("empty leaves") };
    assert!(page.leaves.is_empty());
    assert_eq!(page.bounds, AddressRange::under(&NodePrefix::root()));
    let unknown = KnownState { spec: spec(), regions: vec![] };
    assert!(wire(&unknown).regions.is_empty());
    let empty_summary = KnownRegion::Summary(TreeHead { prefix: NodePrefix::root(), bounds: page.bounds.clone(), digest: Digest::empty() });
    assert!(matches!(wire(&empty_summary), KnownRegion::Summary(summary) if summary.digest.count() == 0));
    assert_ne!(bincode::serialize(&absent).unwrap(), bincode::serialize(&empty).unwrap());
}

#[test]
fn followup_batches_mixed_depths_and_leaf_pages() {
    let comparison = ComparisonId::new();
    let first = head(NodePrefix::root(), AddressRange::under(&NodePrefix::root()), &known(1));
    let prefix = NodePrefix::of(&[0], 3);
    let second = head(prefix.clone(), AddressRange::under(&prefix), &known(2));
    let bounds = AddressRange::new(Included(vec![0]), Excluded(vec![3]));
    let req = request(NodeRequestBody::ContinueState {
        comparison,
        round: Some(2),
        processed_through: 17,
        applied_through: Some(41),
        answers: vec![
            StateAnswer::Summary(first.clone()),
            StateAnswer::Summary(second.clone()),
            StateAnswer::Leaves(LeafPage { bounds: bounds.clone(), leaves: vec![KnownLeaf { key: vec![], entity: known(2) }] }),
        ],
    });
    let NodeRequestBody::ContinueState { comparison: received, round, processed_through, applied_through, answers } = wire(&req).body
    else {
        panic!("comparison")
    };
    assert_eq!(received, comparison);
    assert_eq!(processed_through, 17);
    assert_eq!(round, Some(2));
    assert_eq!(applied_through, Some(41));
    assert!(matches!(&answers[0], StateAnswer::Summary(head) if head == &first));
    assert!(matches!(&answers[1], StateAnswer::Summary(head) if head == &second));
    let StateAnswer::Leaves(page) = &answers[2] else { panic!("leaves") };
    assert_eq!(page.bounds, bounds);
    assert_eq!(page.leaves[0].entity, known(2));
    let receipt =
        request(NodeRequestBody::ContinueState { comparison, round: None, processed_through: 18, applied_through: None, answers: vec![] });
    assert!(matches!(wire(&receipt).body, NodeRequestBody::ContinueState { processed_through: 18, answers, .. } if answers.is_empty()));
}

#[test]
fn initial_replies_continue_with_mixed_work() {
    let comparison = ComparisonId::new();
    let bounds = AddressRange::new(Excluded(vec![0]), Included(vec![9]));
    let state = StateReply {
        spec: spec(),
        round: StateRound { number: 0, revision: 40 },
        frame: StateFrame {
            comparison,
            sequence: 0,
            body: StateFrameBody::State {
                round: 0,
                items: vec![
                    StateItem::Summary(head(NodePrefix::root(), bounds.clone(), &known(2))),
                    StateItem::Leaves(LeafPage { bounds: AddressRange::new(Included(vec![3]), Included(vec![9])), leaves: vec![] }),
                    StateItem::NeedLeaves(bounds.clone()),
                ],
                progress: ComparisonProgress::Continue,
            },
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
        assert_eq!(state.round, StateRound { number: 0, revision: 40 });
        assert_eq!(state.frame.comparison, comparison);
        assert_eq!(state.frame.sequence, 0);
        let StateFrameBody::State { items, progress, round } = state.frame.body else { panic!("initial state") };
        assert_eq!(progress, ComparisonProgress::Continue);
        assert_eq!(round, 0);
        assert_eq!(items.len(), 3);
        assert!(matches!(&items[1], StateItem::Leaves(page) if page.leaves.is_empty()));
    }
}

#[test]
fn event_bridge_repairs_preserve_the_captured_target_clock() {
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
    let update = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet::default());
    let target = Clock::singleton(&update);
    let origin = RequestId::new();
    let comparison = ComparisonId::new();
    let response = NodeUpdateBody::StateFrame {
        request_id: origin.clone(),
        frame: StateFrame {
            comparison,
            sequence: 1,
            body: StateFrameBody::State {
                round: 0,
                items: vec![StateItem::Repair {
                    target: target.clone(),
                    delta: EntityDelta {
                        entity_id: genesis.entity_id,
                        content: DeltaContent::EventBridge { events: vec![Attested::<Event>::from(update.clone()).into()] },
                    },
                }],
                progress: ComparisonProgress::Complete,
            },
        },
    };
    let NodeUpdateBody::StateFrame { request_id, frame } = wire(&response) else { panic!("pushed frame") };
    assert_eq!(request_id, origin);
    assert_eq!(frame.comparison, comparison);
    assert_eq!(frame.sequence, 1);
    let StateFrameBody::State { items, progress, round } = frame.body else { panic!("state") };
    assert_eq!(progress, ComparisonProgress::Complete);
    assert_eq!(round, 0);
    let StateItem::Repair { target: received, delta } = &items[0] else { panic!("repair") };
    let DeltaContent::EventBridge { events } = &delta.content else { panic!("bridge") };
    assert_eq!(received, &target);
    assert_eq!(delta.entity_id, genesis.entity_id);
    assert_eq!(events[0].parent, Clock::singleton(&genesis));
    let restored: Attested<Event> = (delta.entity_id, events[0].clone()).into();
    assert_eq!(restored.payload.id(), update.id());
}

#[test]
fn repair_and_live_chunks_share_content_without_losing_typed_metadata() {
    let entity = known(3);
    let state = State { head: entity.head.clone(), ..Default::default() };
    let content = DeltaContent::StateSnapshot { state: StateFragment { state: state.clone(), attestations: Default::default() } };
    let encoded = bincode::serialize(&content).unwrap();
    let comparison = ComparisonId::new();
    let origin = RequestId::new();
    let member = QueryMember { key: vec![1, 2, 3], head: entity.head.clone() };
    let mut assembled = Vec::new();
    for (i, part) in encoded.chunks(13).enumerate() {
        let last = (i + 1) * 13 >= encoded.len();
        let chunk = EntityDeltaChunk { entity_id: entity.entity_id, offset: (i * 13) as u64, bytes: part.to_vec().into(), last };
        let response = NodeUpdateBody::StateFrame {
            request_id: origin.clone(),
            frame: StateFrame {
                comparison,
                sequence: i as u64 + 1,
                body: StateFrameBody::State {
                    round: 0,
                    items: vec![StateItem::RepairChunk { target: entity.head.clone(), chunk: chunk.clone() }],
                    progress: if last { ComparisonProgress::Complete } else { ComparisonProgress::Continue },
                },
            },
        };
        let NodeUpdateBody::StateFrame { request_id, frame } = wire(&response) else { panic!("pushed repair") };
        assert_eq!(request_id, origin);
        assert_eq!(frame.comparison, comparison);
        assert_eq!(frame.sequence, i as u64 + 1);
        let StateFrameBody::State { items, progress, round } = frame.body else { panic!("state") };
        let StateItem::RepairChunk { target, chunk: repair } = &items[0] else { panic!("repair chunk") };
        assert_eq!(target, &entity.head);
        assert_eq!(repair.entity_id, entity.entity_id);
        assert_eq!(repair.offset as usize, assembled.len());
        assert_eq!(repair.last, progress == ComparisonProgress::Complete);
        let live = StateFrameBody::LiveChunk { revision: 41, chunk, member: Some(member.clone()), complete: last };
        let StateFrameBody::LiveChunk { revision, chunk: live, member: received_member, complete } = wire(&live) else {
            panic!("live chunk")
        };
        assert_eq!(revision, 41);
        assert_eq!(received_member, Some(member.clone()));
        assert_eq!(complete, last);
        assert_eq!(round, 0);
        assert_eq!(bincode::serialize(&live).unwrap(), bincode::serialize(repair).unwrap());
        assembled.extend_from_slice(&repair.bytes);
    }
    let DeltaContent::StateSnapshot { state: fragment } = bincode::deserialize(&assembled).unwrap() else { panic!("snapshot") };
    assert_eq!(fragment.state, state);
}

#[test]
fn completion_application_abandonment_and_snapshot_loss_are_distinct() {
    let comparison = ComparisonId::new();
    for outcome in [
        ComparisonOutcome::Applied { round: 2, processed_through: 19 },
        ComparisonOutcome::Cancelled { round: 2 },
        ComparisonOutcome::Abandoned,
    ] {
        let req = request(NodeRequestBody::CloseComparison { comparison, outcome });
        let NodeRequestBody::CloseComparison { comparison: received, outcome: received_outcome } = wire(&req).body else {
            panic!("close comparison")
        };
        assert_eq!(received, comparison);
        assert_eq!(received_outcome, outcome);
    }
    let expired = NodeResponseBody::ComparisonUnavailable { comparison };
    assert!(matches!(wire(&expired), NodeResponseBody::ComparisonUnavailable { comparison: id } if id == comparison));
    let complete = StateFrame {
        comparison,
        sequence: 18,
        body: StateFrameBody::State { round: 2, items: vec![], progress: ComparisonProgress::Complete },
    };
    assert!(matches!(wire(&complete).body, StateFrameBody::State { progress: ComparisonProgress::Complete, .. }));
    let restart = NodeUpdateBody::StateFrame {
        request_id: RequestId::new(),
        frame: StateFrame { comparison, sequence: 20, body: StateFrameBody::RestartRequired },
    };
    assert!(
        matches!(wire(&restart), NodeUpdateBody::StateFrame { frame: StateFrame { comparison: id, sequence: 20, body: StateFrameBody::RestartRequired }, .. } if id == comparison)
    );
}

#[test]
fn refresh_keeps_the_stream_and_freezes_a_numbered_offer() {
    let comparison = ComparisonId::new();
    let bounds = AddressRange::under(&NodePrefix::root());
    let offer = KnownState {
        spec: spec(),
        regions: vec![KnownRegion::Leaves(LeafPage { bounds: bounds.clone(), leaves: vec![KnownLeaf { key: vec![7], entity: known(7) }] })],
    };
    let refresh = request(NodeRequestBody::RefreshState { comparison, round: 3, known_state: offer });
    let NodeRequestBody::RefreshState { comparison: received, round, known_state } = wire(&refresh).body else { panic!("refresh") };
    assert_eq!(received, comparison);
    assert_eq!(round, 3);
    assert_eq!(known_state.spec, spec());
    let KnownRegion::Leaves(page) = &known_state.regions[0] else { panic!("frozen offer") };
    assert_eq!(page.bounds, bounds);
    assert_eq!(page.leaves[0].key, [7]);
    assert_eq!(page.leaves[0].entity, known(7));

    let begin = StateFrame { comparison, sequence: 25, body: StateFrameBody::BeginRound(StateRound { number: 3, revision: 90 }) };
    let StateFrame { comparison: received, sequence, body: StateFrameBody::BeginRound(round) } = wire(&begin) else {
        panic!("begin round")
    };
    assert_eq!(received, comparison);
    assert_eq!(sequence, 25);
    assert_eq!(round, StateRound { number: 3, revision: 90 });
}

#[test]
fn absolute_membership_preserves_absence_and_targets_without_entity_data() {
    let present = QueryMember { key: vec![0, 255, 7], head: known(2).head };
    let frame = StateFrameBody::Live {
        revision: 91,
        items: vec![
            QueryChange { entity_id: known(2).entity_id, member: Some(present.clone()), delta: None },
            QueryChange { entity_id: known(3).entity_id, member: None, delta: None },
        ],
        complete: true,
    };
    let StateFrameBody::Live { revision, items, complete } = wire(&frame) else { panic!("live publication") };
    assert_eq!(revision, 91);
    assert!(complete);
    assert_eq!(items[0].entity_id, known(2).entity_id);
    assert_eq!(items[0].member, Some(present));
    assert!(items[0].delta.is_none());
    assert_eq!(items[1].entity_id, known(3).entity_id);
    assert!(items[1].member.is_none());
    assert!(items[1].delta.is_none());
}

#[test]
fn split_publication_and_interleaved_comparison_keep_independent_boundaries() {
    let comparison = ComparisonId::new();
    let frames = vec![
        StateFrame {
            comparison,
            sequence: 10,
            body: StateFrameBody::Live {
                revision: 91,
                items: vec![QueryChange { entity_id: known(3).entity_id, member: None, delta: None }],
                complete: false,
            },
        },
        StateFrame {
            comparison,
            sequence: 11,
            body: StateFrameBody::State { round: 3, items: vec![], progress: ComparisonProgress::Complete },
        },
        StateFrame {
            comparison,
            sequence: 12,
            body: StateFrameBody::Live {
                revision: 91,
                items: vec![QueryChange {
                    entity_id: known(4).entity_id,
                    member: Some(QueryMember { key: vec![4], head: known(4).head }),
                    delta: None,
                }],
                complete: true,
            },
        },
    ];
    let received = wire(&frames);
    assert!(received.iter().all(|frame| frame.comparison == comparison));
    assert_eq!(received.iter().map(|frame| frame.sequence).collect::<Vec<_>>(), [10, 11, 12]);
    assert!(matches!(received[0].body, StateFrameBody::Live { revision: 91, complete: false, .. }));
    assert!(matches!(received[1].body, StateFrameBody::State { round: 3, progress: ComparisonProgress::Complete, .. }));
    assert!(matches!(received[2].body, StateFrameBody::Live { revision: 91, complete: true, .. }));
}

#[test]
fn frame_credit_live_coverage_and_round_application_have_separate_wire_markers() {
    let comparison = ComparisonId::new();
    for (round, applied_through) in [(Some(3), None), (None, Some(91)), (None, None)] {
        let feedback =
            request(NodeRequestBody::ContinueState { comparison, round, processed_through: 25, applied_through, answers: vec![] });
        let NodeRequestBody::ContinueState {
            comparison: received,
            round: received_round,
            processed_through,
            applied_through: received_applied,
            answers,
        } = wire(&feedback).body
        else {
            panic!("credit and coverage")
        };
        assert_eq!(received, comparison);
        assert_eq!(received_round, round);
        assert_eq!(processed_through, 25);
        assert_eq!(received_applied, applied_through);
        assert!(answers.is_empty());
    }
    let repair = StateFrameBody::LiveRepair {
        items: vec![StateItem::RepairUnavailable { entity_id: known(3).entity_id, target: known(3).head.clone() }],
    };
    let StateFrameBody::LiveRepair { items } = wire(&repair) else { panic!("live repair") };
    assert!(matches!(&items[0], StateItem::RepairUnavailable { entity_id, target }
        if *entity_id == known(3).entity_id && *target == known(3).head));
    // Wire round trips preserve these independent markers; scheduling and
    // rejection of invalid marker order belong to the node's behavioral tests.
}

#[test]
fn late_repair_feedback_and_failure_have_distinct_wire_fixtures() {
    // Independent tooling fixtures pin the public shape, including the fact that
    // a processed receipt can still report an uncovered causal target.
    let feedback = serde_json::json!({ "ContinueState": {
        "comparison": "00000000000000000000000001",
        "round": 2,
        "processed_through": 7,
        "applied_through": 40,
        "answers": [{ "NeedRepair": {
            "entity_id": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "target": [[3, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"]]
        }}]
    }});
    let request: NodeRequestBody = serde_json::from_value(feedback.clone()).unwrap();
    assert_eq!(serde_json::to_value(wire(&request)).unwrap(), feedback);
    let NodeRequestBody::ContinueState { comparison, round, processed_through, applied_through, answers } = request else {
        panic!("feedback")
    };
    assert_eq!(processed_through, 7);
    assert_eq!(round, Some(2));
    assert_eq!(applied_through, Some(40));
    let StateAnswer::NeedRepair { entity_id, target } = &answers[0] else { panic!("repair request") };
    assert_eq!(*entity_id, EntityId::from_bytes([0; 32]));
    assert_eq!(*target, Clock::new(vec![(3, EventId::from_bytes([0; 32]))]).unwrap());

    let failure = serde_json::json!({ "StateFrame": {
        "request_id": "00000000000000000000000002",
        "frame": {
            "comparison": "00000000000000000000000001", "sequence": 8,
            "body": { "State": { "round": 2, "items": [{ "RepairUnavailable": {
                "entity_id": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "target": [[3, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"]]
            }}], "progress": "Continue" }}
        }
    }});
    let update: NodeUpdateBody = serde_json::from_value(failure.clone()).unwrap();
    assert_eq!(serde_json::to_value(wire(&update)).unwrap(), failure);
    let NodeUpdateBody::StateFrame { request_id, frame } = update else { panic!("pushed failure") };
    assert_eq!(request_id, serde_json::from_str::<RequestId>("\"00000000000000000000000002\"").unwrap());
    assert_eq!(frame.comparison, comparison);
    assert_eq!(frame.sequence, 8);
    let StateFrameBody::State { items, .. } = frame.body else { panic!("repair failure") };
    assert!(matches!(&items[0], StateItem::RepairUnavailable { entity_id: id, target: clock } if id == entity_id && clock == target));
    let applied: NodeRequestBody = serde_json::from_value(serde_json::json!({ "CloseComparison": {
        "comparison": "00000000000000000000000001", "outcome": { "Applied": { "round": 2, "processed_through": 9 }}
    }}))
    .unwrap();
    assert!(matches!(
        wire(&applied),
        NodeRequestBody::CloseComparison { outcome: ComparisonOutcome::Applied { round: 2, processed_through: 9 }, .. }
    ));
    assert!(matches!(wire(&NodeResponseBody::Success), NodeResponseBody::Success));
}

#[test]
fn shared_state_and_events_preserve_history_and_attestations_in_repair_and_live_delivery() {
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
    let update = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet::default());
    let target = Clock::singleton(&update);
    let state = StateFragment {
        state: State { head: target.clone(), state_buffers: StateBuffers([("lww".into(), vec![0, 255, 7])].into()), ..Default::default() },
        attestations: AttestationSet(vec![Attestation(vec![11, 12])]),
    };
    let events = vec![Attested::opt(update.clone(), Some(Attestation(vec![21, 22]))).into()];
    let delta = EntityDelta {
        entity_id: genesis.entity_id,
        content: DeltaContent::StateAndEvents { state: state.clone(), events: events.clone() },
    };
    let member = QueryMember { key: vec![3, 4], head: target.clone() };
    let origin = RequestId::new();
    let comparison = ComparisonId::new();
    let repair = StateItem::Repair { target: target.clone(), delta: delta.clone() };
    let live = Message::PeerMessage(NodeMessage::Update(NodeUpdate {
        id: UpdateId::new(),
        from: known(1).entity_id,
        to: known(2).entity_id,
        body: NodeUpdateBody::StateFrame {
            request_id: origin.clone(),
            frame: StateFrame {
                comparison,
                sequence: 4,
                body: StateFrameBody::Live {
                    revision: 41,
                    items: vec![QueryChange { entity_id: delta.entity_id, member: Some(member.clone()), delta: Some(delta.content) }],
                    complete: true,
                },
            },
        },
    }));
    let StateItem::Repair { target: received_target, delta: repair_delta } = wire(&repair) else { panic!("repair") };
    assert_eq!(received_target, target);
    let Message::PeerMessage(NodeMessage::Update(update_message)) = wire(&live) else { panic!("live update") };
    let NodeUpdateBody::StateFrame { request_id, frame } = update_message.body else { panic!("live frame") };
    assert_eq!(request_id, origin);
    assert_eq!(frame.comparison, comparison);
    assert_eq!(frame.sequence, 4);
    let StateFrameBody::Live { revision, items, complete } = frame.body else { panic!("live items") };
    assert_eq!(revision, 41);
    assert!(complete);
    assert_eq!(items[0].entity_id, genesis.entity_id);
    assert_eq!(items[0].member, Some(member));
    assert_eq!(bincode::serialize(items[0].delta.as_ref().unwrap()).unwrap(), bincode::serialize(&repair_delta.content).unwrap());
    let DeltaContent::StateAndEvents { state: received_state, events: received_events } = items[0].delta.as_ref().unwrap() else {
        panic!("shared content")
    };
    assert_eq!(received_state, &state);
    assert_eq!(received_events, &events);
    let restored: Attested<Event> = (items[0].entity_id, received_events[0].clone()).into();
    assert_eq!(restored.payload, update);
    assert_eq!(restored.attestations, AttestationSet(vec![Attestation(vec![21, 22])]));
    let snapshot: Attested<EntityState> = (items[0].entity_id, received_state.clone()).into();
    assert_eq!(snapshot.payload.entity_id, genesis.entity_id);
    assert_eq!(snapshot.payload.state, state.state);
    assert_eq!(snapshot.attestations, state.attestations);
}

#[test]
fn snapshot_pages_preserve_attestations_and_omit_duplicate_leaf_and_target_data() {
    let entity = known(3);
    let state = Attested {
        payload: EntityState { entity_id: entity.entity_id, state: State { head: entity.head.clone(), ..Default::default() } },
        attestations: AttestationSet(vec![Attestation(vec![11, 12])]),
    };
    let bounds = AddressRange::under(&NodePrefix::root());
    let page = StateItem::Snapshots(SnapshotPage { bounds: bounds.clone(), states: vec![state.clone()] });
    let StateItem::Snapshots(received) = wire(&page) else { panic!("snapshot page") };
    assert_eq!(received.bounds, bounds);
    assert_eq!(received.states[0].payload, state.payload);
    assert_eq!(received.states[0].attestations, state.attestations);
    let separate = vec![
        StateItem::Leaves(LeafPage { bounds, leaves: vec![KnownLeaf { key: vec![], entity: entity.clone() }] }),
        StateItem::Repair {
            target: entity.head,
            delta: EntityDelta { entity_id: entity.entity_id, content: DeltaContent::StateSnapshot { state: state.into() } },
        },
    ];
    assert!(bincode::serialized_size(&vec![page]).unwrap() < bincode::serialized_size(&separate).unwrap());
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
