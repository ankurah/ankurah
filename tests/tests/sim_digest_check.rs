//! Regression guard for the trace's semantic message digest (C1).
//!
//! The determinism audit compares trace hashes, and the trace records message
//! deliveries by a semantic digest. If that digest keyed only on message
//! cardinality (e.g. "eventonly:2"), two runs of one seed that delivered
//! different batches of the same size at the same position would hash equal, and
//! the audit would false-pass a real divergence. These tests pin that the digest
//! is content-faithful for the subscription-update payloads.

use ankurah::proto::{self, Attested};
use ankurah_tests::sim::model;
use ankurah_tests::sim::transport::message_digest;

fn event_only_message(entity: proto::EntityId, ev: Attested<proto::Event>) -> proto::NodeMessage {
    let frag: proto::EventFragment = ev.into();
    proto::NodeMessage::Update(proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: entity,
        to: entity,
        body: proto::NodeUpdateBody::SubscriptionUpdate {
            items: vec![proto::SubscriptionUpdateItem {
                entity_id: entity,
                content: proto::UpdateContent::EventOnly(vec![frag]),
                predicate_relevance: vec![],
            }],
        },
    })
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
    let reply = |event: proto::Event| {
        proto::NodeMessage::Response(proto::NodeResponse {
            request_id: proto::RequestId::new(),
            from: event.entity_id,
            to: event.entity_id,
            body: proto::NodeResponseBody::TreeCompared(proto::ComparisonReply {
                comparison: proto::ComparisonId::new(),
                items: vec![proto::StateItem::Repair {
                    target: proto::Clock::singleton(&event),
                    delta: proto::EntityDelta {
                        entity_id: event.entity_id,
                        content: proto::DeltaContent::EventBridge { events: vec![model::attest(event).into()] },
                    },
                }],
                progress: proto::ComparisonProgress::Complete,
            }),
        })
    };
    let one = model::genesis_event(1, model::Field::Title, "one");
    let two = model::genesis_event(1, model::Field::Title, "two");
    let first = message_digest(&reply(one.clone()));
    assert_eq!(first, message_digest(&reply(one)), "fresh correlation ids must not change the trace");
    assert_ne!(first, message_digest(&reply(two)), "same-size replies with different repairs must differ");
}
