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
                model: model::sim_model_id(),
                content: proto::UpdateContent::EventOnly(vec![frag]),
                predicate_relevance: vec![],
            }],
        },
        schema: vec![],
    })
}

#[test]
fn digest_distinguishes_different_eventonly_batches_same_count() {
    let e = model::entity_id(1);
    // Two different single-event EventOnly updates for the same entity: same
    // shape and cardinality, different content.
    let d1 = message_digest(&event_only_message(e, model::attest(model::genesis_event(e, model::Field::Title, "one"))));
    let d2 = message_digest(&event_only_message(e, model::attest(model::genesis_event(e, model::Field::Title, "two"))));
    assert_ne!(d1, d2, "different EventOnly batches for one entity must have different digests");
}

#[test]
fn digest_is_stable_for_identical_content() {
    // The other half of the property: equal content must hash equal.
    let e = model::entity_id(2);
    let d1 = message_digest(&event_only_message(e, model::attest(model::genesis_event(e, model::Field::Title, "same"))));
    let d2 = message_digest(&event_only_message(e, model::attest(model::genesis_event(e, model::Field::Title, "same"))));
    assert_eq!(d1, d2, "identical content must produce identical digests");
}
