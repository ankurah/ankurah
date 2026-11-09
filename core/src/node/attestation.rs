//! The attestations a node makes: only those its policy agent requests, each a statement
//! about an event the node checked or a state it serves, sealed with the node's key. A node
//! carries the attestations it receives as they came and checks none of them.

use crate::internal::prelude::*;
use crate::policy::AttestationRequest;
use proto::{Attested, EntityState, Event, SealedAttestation, Subject};

/// This node's attestation on `event`, when its policy agent's check requested one.
pub(crate) fn on_event<SE, PA: PolicyAgent>(
    node: &Node<SE, PA>,
    event: &Event,
    request: Option<AttestationRequest>,
) -> Option<SealedAttestation> {
    request.map(|request| node.key.attest(Subject::event(event), request.kind, request.payload))
}

/// `state` as this node serves it: a durable node attaches the attestation its policy agent
/// requests for the state, and an ephemeral node attaches none.
pub(crate) fn served_state<SE: StorageEngine, PA: PolicyAgent>(node: &Node<SE, PA>, state: EntityState) -> Attested<EntityState> {
    let request = if node.durable { node.policy_agent.attest_state(node, &state) } else { None };
    let attestation = request.map(|request| node.key.attest(Subject::state(&state), request.kind, request.payload));
    Attested::opt(state, attestation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{policy::PermissiveAgent, test_utils::TestStorage};
    use proto::{Clock, Identity, OperationSet, State};
    use std::sync::Arc;

    fn genesis(minter: u8) -> Event { Event::genesis(Identity::Ed25519([minter; 32]), OperationSet::default()) }

    /// `PermissiveAgent` requests no attestation, so neither a durable nor an ephemeral node signs the states it serves.
    #[tokio::test]
    async fn a_state_served_under_an_agent_that_requests_nothing_carries_no_attestation() {
        let event = genesis(1);
        let state = EntityState { entity_id: event.entity_id, state: State { head: Clock::singleton(&event), ..State::default() } };

        let durable = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        assert!(served_state(&durable, state.clone()).attestations.is_empty());
        let ephemeral = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        assert!(served_state(&ephemeral, state).attestations.is_empty());
    }
}
