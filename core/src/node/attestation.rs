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
    let request = if node.durable { node.policy_agent.request_state_attestation(node, &state) } else { None };
    let attestation = request.map(|request| node.key.attest(Subject::state(&state), request.kind, request.payload));
    Attested::opt(state, attestation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ValidationError;
    use crate::policy::{AccessDenied, DefaultContext, PermissiveAgent};
    use crate::test_utils::TestStorage;
    use crate::util::Iterable;
    use ankql::ast::{Predicate, Resolved};
    use proto::{Clock, EntityId, Identity, OperationSet, State};
    use std::sync::Arc;

    fn genesis(minter: u8) -> Event { Event::genesis(Identity::Ed25519([minter; 32]), OperationSet::default()) }

    /// The statement kind and payload `StateAttester` asks for.
    fn kind() -> EntityId { EntityId::from_bytes([9; 32]) }
    const PAYLOAD: &[u8] = b"served";

    /// An agent that asks for a statement about every state its node serves, and allows everything else.
    #[derive(Clone)]
    struct StateAttester;

    #[async_trait::async_trait]
    impl PolicyAgent for StateAttester {
        type ContextData = &'static DefaultContext;

        fn check_write_event<SE: StorageEngine>(
            &self,
            _: &Node<SE, Self>,
            _: &Self::ContextData,
            _: &Entity,
            _: &Entity,
            _: &Event,
        ) -> Result<Option<AttestationRequest>, AccessDenied> {
            Ok(None)
        }

        fn sign_request<SE: StorageEngine, C: Iterable<Self::ContextData>>(
            &self,
            _: &crate::node::NodeInner<SE, Self>,
            _: &C,
            _: &proto::NodeRequest,
        ) -> Result<Vec<proto::AuthData>, AccessDenied> {
            Ok(vec![])
        }

        async fn check_request<SE: StorageEngine, A: Iterable<proto::AuthData> + Send + Sync>(
            &self,
            _: &Node<SE, Self>,
            _: &A,
            _: &proto::NodeRequest,
        ) -> Result<Vec<Self::ContextData>, ValidationError> {
            Ok(vec![])
        }

        fn query_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied> {
            Ok(Predicate::True)
        }

        fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&Event>) -> Result<(), AccessDenied> { Ok(()) }

        fn request_state_attestation<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &EntityState) -> Option<AttestationRequest> {
            Some(AttestationRequest { kind: kind(), payload: PAYLOAD.to_vec() })
        }

        fn validate_received_event<SE: StorageEngine>(
            &self,
            _: &Node<SE, Self>,
            _: &proto::NodeId,
            _: &Attested<Event>,
        ) -> Result<(), AccessDenied> {
            Ok(())
        }

        fn validate_received_state<SE: StorageEngine>(
            &self,
            _: &Node<SE, Self>,
            _: &proto::NodeId,
            _: &Attested<EntityState>,
        ) -> Result<(), AccessDenied> {
            Ok(())
        }

        fn validate_causal_assertion<SE: StorageEngine>(
            &self,
            _: &Node<SE, Self>,
            _: &proto::NodeId,
            _: &proto::CausalAssertion,
        ) -> Result<(), AccessDenied> {
            Ok(())
        }
    }

    /// A durable node seals the statement its agent asks for about the state it serves, under its
    /// own key; an ephemeral node serves the state with none.
    #[tokio::test]
    async fn a_durable_node_serves_a_state_with_the_statement_its_agent_requests_and_an_ephemeral_node_with_none() {
        let event = genesis(2);
        let state = EntityState { entity_id: event.entity_id, state: State { head: Clock::singleton(&event), ..State::default() } };

        let durable = Node::new_durable(Arc::new(TestStorage::default()), StateAttester);
        let served = served_state(&durable, state.clone());
        let [sealed] = served.attestations.0.as_slice() else { panic!("one attestation: {:?}", served.attestations) };
        let statement = sealed.unseal().unwrap();
        assert_eq!(statement.attester, durable.identity());
        assert_eq!(statement.subject, Subject::state(&state));
        assert_eq!((statement.kind, statement.payload.as_slice()), (kind(), PAYLOAD));

        let ephemeral = Node::new(Arc::new(TestStorage::default()), StateAttester);
        assert!(served_state(&ephemeral, state).attestations.is_empty());
    }

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
