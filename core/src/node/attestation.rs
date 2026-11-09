//! The attestations a node signs, and the check every received one passes.
//!
//! A node signs a committed attestation on every event it commits, an admitted
//! attestation on an event its policy agent asks it to vouch for, and, when it
//! is durable, a state attestation on every state it serves. Before any policy
//! hook sees a received event or state, the attestations that do not verify
//! under their attester or do not name that payload are stripped.

use crate::internal::prelude::*;
use crate::policy::Admission;
use proto::{Attestation, AttestationBody, Attested, AttesterRole, EntityState, Event};
use tracing::warn;

/// This node's committed attestation on `event`, with empty claims.
pub(crate) fn committed<SE, PA: PolicyAgent>(node: &Node<SE, PA>, event: &Event) -> Attestation {
    node.key.attest(AttestationBody::event(event, AttesterRole::Committed, Vec::new()))
}

/// This node's admitted attestation on `event`, when its policy agent asked for one.
pub(crate) fn admitted<SE, PA: PolicyAgent>(node: &Node<SE, PA>, event: &Event, admission: Admission) -> Option<Attestation> {
    match admission {
        Admission::Attest { claims } => Some(node.key.attest(AttestationBody::event(event, AttesterRole::Admitted, claims))),
        Admission::Allow => None,
    }
}

/// `state` as this node serves it: a durable node attaches its state attestation, carrying the
/// claims its policy agent returns; an ephemeral node attaches none.
pub(crate) fn served_state<SE: StorageEngine, PA: PolicyAgent>(node: &Node<SE, PA>, state: EntityState) -> Attested<EntityState> {
    if !node.durable {
        return state.into();
    }
    let claims = node.policy_agent.attest_state(node, &state).unwrap_or_default();
    let attestation = node.key.attest(AttestationBody::state(&state, claims));
    Attested::opt(state, Some(attestation))
}

/// Strip the attestations on a received event that fail their signature or name another event.
pub(crate) fn verify_event(event: &mut Attested<Event>) {
    let stripped = strip(&mut event.attestations.0, |attestation| attestation.covers_event(&event.payload));
    if stripped > 0 {
        warn!(stripped, event = %event.payload.id(), "stripped attestations that do not verify for this event");
    }
}

/// Strip the attestations on a received state that fail their signature or name another state.
pub(crate) fn verify_state(state: &mut Attested<EntityState>) {
    let stripped = strip(&mut state.attestations.0, |attestation| attestation.covers_state(&state.payload));
    if stripped > 0 {
        warn!(stripped, entity = %state.payload.entity_id, "stripped attestations that do not verify for this state");
    }
}

/// Keep the attestations whose signature verifies and that `covers` the payload; return how many went.
fn strip(attestations: &mut Vec<Attestation>, covers: impl Fn(&Attestation) -> bool) -> usize {
    let before = attestations.len();
    attestations.retain(|attestation| attestation.verify() && covers(attestation));
    before - attestations.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{node::NodeKey, policy::PermissiveAgent, test_utils::TestStorage};
    use proto::{Clock, Identity, OperationSet, State};
    use std::sync::Arc;

    fn genesis(minter: u8) -> Event { Event::genesis(Identity::Ed25519([minter; 32]), OperationSet::default()) }

    #[test]
    fn a_tampered_and_a_transplanted_attestation_are_stripped_and_counted() {
        let key = NodeKey::from_seed([3; 32]);
        let (event, other) = (genesis(1), genesis(2));
        let genuine = key.attest(AttestationBody::event(&event, AttesterRole::Committed, vec![]));
        let mut tampered = genuine.clone();
        tampered.body = AttestationBody::event(&event, AttesterRole::Admitted, vec![1]);
        let transplanted = key.attest(AttestationBody::event(&other, AttesterRole::Committed, vec![]));

        let mut attestations = vec![tampered, genuine.clone(), transplanted];
        assert_eq!(strip(&mut attestations, |attestation| attestation.covers_event(&event)), 2);
        assert_eq!(attestations, vec![genuine]);
    }

    #[tokio::test]
    async fn a_durable_node_signs_the_states_it_serves_and_an_ephemeral_node_does_not() {
        let event = genesis(1);
        let state = EntityState { entity_id: event.entity_id, state: State { head: Clock::singleton(&event), ..State::default() } };

        let durable = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let served = served_state(&durable, state.clone());
        let [attestation] = served.attestations.0.as_slice() else { panic!("one state attestation, got {:?}", served.attestations) };
        assert_eq!(attestation.attester, durable.identity());
        assert!(attestation.verify() && attestation.covers_state(&state));

        let ephemeral = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        assert!(served_state(&ephemeral, state).attestations.is_empty());
    }
}
