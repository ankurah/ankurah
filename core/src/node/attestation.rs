//! The attestations a node signs.
//!
//! A node signs a committed attestation on every event it commits, an admitted
//! attestation on an event its policy agent asks it to vouch for, and, when it
//! is durable, a state attestation on a state it serves when its policy agent
//! returns claims for that state. A node carries the attestations it receives as
//! they came and checks none of them.

use crate::internal::prelude::*;
use crate::policy::Admission;
use proto::{Attestation, AttestationBody, Attested, AttesterRole, EntityState, Event};

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

/// `state` as this node serves it: a durable node attaches a state attestation carrying the claims
/// its policy agent returns, and signs nothing when the agent returns none; an ephemeral node
/// attaches none.
pub(crate) fn served_state<SE: StorageEngine, PA: PolicyAgent>(node: &Node<SE, PA>, state: EntityState) -> Attested<EntityState> {
    let claims = if node.durable { node.policy_agent.attest_state(node, &state) } else { None };
    match claims {
        Some(claims) => {
            let attestation = node.key.attest(AttestationBody::state(&state, claims));
            Attested::opt(state, Some(attestation))
        }
        None => state.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{policy::PermissiveAgent, test_utils::TestStorage};
    use proto::{Clock, Identity, OperationSet, State};
    use std::sync::Arc;

    fn genesis(minter: u8) -> Event { Event::genesis(Identity::Ed25519([minter; 32]), OperationSet::default()) }

    /// `PermissiveAgent` returns no state claims, so neither a durable nor an ephemeral node signs the states it serves.
    #[tokio::test]
    async fn a_state_served_under_an_agent_that_returns_no_claims_carries_no_attestation() {
        let event = genesis(1);
        let state = EntityState { entity_id: event.entity_id, state: State { head: Clock::singleton(&event), ..State::default() } };

        let durable = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        assert!(served_state(&durable, state.clone()).attestations.is_empty());
        let ephemeral = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        assert!(served_state(&ephemeral, state).attestations.is_empty());
    }
}
