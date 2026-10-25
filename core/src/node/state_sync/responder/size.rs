//! A delta's size, by which a repair chooses between a bridge and a state and
//! a push bounds its updates: the bytes of the delta's own buffers, which are
//! its state's buffers, its events' operation diffs, and every attestation it
//! carries. These grow with an entity while the rest of a delta stays small,
//! so they measure it without encoding it.

use ankurah_proto as proto;

/// The bytes of `content`'s own buffers.
pub(super) fn data_bytes(content: &proto::DeltaContent) -> usize {
    match content {
        proto::DeltaContent::StateSnapshot { state } => state_bytes(state),
        proto::DeltaContent::EventBridge { events } => events.iter().map(event_bytes).sum(),
        proto::DeltaContent::StateAndRelation { state, relation } => state_bytes(state) + attestation_bytes(&relation.attestations),
    }
}

fn state_bytes(state: &proto::StateFragment) -> usize {
    state.state.state_buffers.values().map(Vec::len).sum::<usize>() + attestation_bytes(&state.attestations)
}

fn event_bytes(event: &proto::EventFragment) -> usize {
    let (proto::EventBody::Genesis { operations, .. } | proto::EventBody::Update { operations, .. }) = &event.body;
    let diffs = operations.0.iter().map(|operation| match operation {
        proto::Operation::Backend { operations: diffs, .. } => diffs.iter().map(|diff| diff.diff.len()).sum(),
        proto::Operation::Membership(_) => 0,
    });
    diffs.sum::<usize>() + attestation_bytes(&event.attestations)
}

fn attestation_bytes(attestations: &proto::AttestationSet) -> usize { attestations.iter().map(|attestation| attestation.0.len()).sum() }
