//! Regression guard for the trace's semantic message digest (C1).
//!
//! The determinism audit compares trace hashes, and the trace records message
//! deliveries by a semantic digest. If that digest keyed only on message
//! cardinality (e.g. "eventonly:2"), two runs of one seed that delivered
//! different batches of the same size at the same position would hash equal, and
//! the audit would false-pass a real divergence. These tests pin that the digest
//! is content-faithful for the live state-frame payloads.

use ankurah::proto::{self, Attested};
use ankurah_tests::sim::model;
use ankurah_tests::sim::transport::message_digest;

fn event_only_message(entity: proto::EntityId, ev: Attested<proto::Event>) -> proto::NodeMessage {
    let frag: proto::EventFragment = ev.into();
    proto::NodeMessage::StateFrame {
        from: entity,
        to: entity,
        request_id: proto::RequestId::new(),
        frame: proto::StateFrame {
            stream: proto::StateStreamId::new(),
            sequence: 1,
            body: proto::StateFrameBody::Live {
                revision: 1,
                items: vec![ankurah_tests::state_stream_fixture::change(proto::EntityDelta {
                    entity_id: entity,
                    content: proto::DeltaContent::EventBridge { events: vec![frag] },
                })],
                complete: true,
            },
        },
    }
}

#[test]
fn digest_distinguishes_different_eventonly_batches_same_count() {
    // Two different single-event EventOnly updates: same shape and
    // cardinality, different content.
    let one = model::genesis_event(1, model::Field::Title, "one");
    let two = model::genesis_event(1, model::Field::Title, "two");
    let d1 = message_digest(&event_only_message(one.entity_id, model::attest(one.clone())));
    let d2 = message_digest(&event_only_message(two.entity_id, model::attest(two)));
    assert_ne!(d1, d2, "different EventOnly batches for one entity must have different digests");
}

#[test]
fn digest_is_stable_for_identical_content() {
    // The other half of the property: equal content must hash equal.
    let same = model::genesis_event(2, model::Field::Title, "same");
    let d1 = message_digest(&event_only_message(same.entity_id, model::attest(same.clone())));
    let d2 = message_digest(&event_only_message(same.entity_id, model::attest(same)));
    assert_eq!(d1, d2, "identical content must produce identical digests");
}

#[test]
fn comparison_digest_tracks_repair_content_but_not_session_ids() {
    let reply = |event: proto::Event| proto::NodeMessage::StateFrame {
        from: event.entity_id,
        to: event.entity_id,
        request_id: proto::RequestId::new(),
        frame: proto::StateFrame {
            stream: proto::StateStreamId::new(),
            sequence: 1,
            body: proto::StateFrameBody::Round {
                round: 0,
                items: vec![proto::StateItem::Repair {
                    target: proto::Clock::singleton(&event),
                    delta: proto::EntityDelta {
                        entity_id: event.entity_id,
                        content: proto::DeltaContent::EventBridge { events: vec![model::attest(event).into()] },
                    },
                }],
                progress: proto::ComparisonProgress::Complete,
            },
        },
    };
    let one = model::genesis_event(1, model::Field::Title, "one");
    let two = model::genesis_event(1, model::Field::Title, "two");
    let first = message_digest(&reply(one.clone()));
    assert_eq!(first, message_digest(&reply(one)), "fresh correlation ids must not change the trace");
    assert_ne!(first, message_digest(&reply(two)), "same-size replies with different repairs must differ");
}

fn frame_message(sequence: u64, body: proto::StateFrameBody) -> proto::NodeMessage {
    proto::NodeMessage::StateFrame {
        from: proto::EntityId::from_bytes([1; 32]),
        to: proto::EntityId::from_bytes([2; 32]),
        request_id: proto::RequestId::new(),
        frame: proto::StateFrame { stream: proto::StateStreamId::new(), sequence, body },
    }
}

#[test]
fn live_frame_digest_tracks_sequence_and_membership_without_random_route_ids() {
    let event = model::genesis_event(3, model::Field::Title, "live");
    let frame = |sequence, change: bool| {
        frame_message(
            sequence,
            proto::StateFrameBody::Live {
                revision: 7,
                items: vec![proto::QueryChange {
                    entity_id: event.entity_id,
                    member: change.then(|| proto::QueryMember { key: vec![], head: proto::Clock::singleton(&event) }),
                    delta: None,
                }],
                complete: true,
            },
        )
    };
    let first = message_digest(&frame(2, true));
    assert_eq!(first, message_digest(&frame(2, true)), "fresh stream ids must not perturb the trace");
    assert_ne!(first, message_digest(&frame(3, true)), "frame ordering is meaningful");
    assert_ne!(first, message_digest(&frame(2, false)), "membership effects are meaningful");
}

#[test]
fn live_chunk_digest_tracks_fragment_bytes_and_offsets() {
    let frame = |offset, bytes: Vec<u8>| {
        frame_message(
            5,
            proto::StateFrameBody::LiveChunk {
                chunk: proto::EntityDeltaChunk {
                    entity_id: proto::EntityId::from_bytes([3; 32]),
                    offset,
                    bytes: bytes.into(),
                    last: false,
                },
                revision: 8,
                member: Some(proto::QueryMember { key: vec![], head: proto::Clock::genesis(proto::EventId::from_bytes([3; 32])) }),
                complete: false,
            },
        )
    };
    let first = message_digest(&frame(0, vec![1, 2]));
    assert_eq!(first, message_digest(&frame(0, vec![1, 2])));
    assert_ne!(first, message_digest(&frame(0, vec![1, 3])), "same-size fragments with different bytes must differ");
    assert_ne!(first, message_digest(&frame(2, vec![1, 2])), "fragment position is meaningful");
}
