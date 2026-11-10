//! The attestations a node makes: a durable node attaches its policy agent's attestation to a state it serves.

use crate::internal::prelude::*;
use proto::{Attested, EntityState};

/// `state` as this node serves it: a durable node attaches the attestation its policy agent
/// requests for the state, and an ephemeral node attaches none.
pub(crate) fn served_state<SE: StorageEngine, PA: PolicyAgent>(node: &Node<SE, PA>, state: EntityState) -> Attested<EntityState> {
    let attestation = if node.durable { node.policy_agent.attest_state(node, &state) } else { None };
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
    use proto::{Clock, Event, Identity, OperationSet, State};
    use std::sync::Arc;

    fn genesis(minter: u8) -> Event { Event::genesis(Identity::Ed25519([minter; 32]), OperationSet::default()) }

    /// The payload of the attestation `StateAttester` makes.
    const PAYLOAD: &[u8] = b"served";

    /// An agent that attests every state its node serves, and allows everything else.
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
        ) -> Result<Option<proto::Attestation>, AccessDenied> {
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

        fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &EntityState) -> Option<proto::Attestation> {
            Some(proto::Attestation(PAYLOAD.to_vec()))
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

    /// A durable node serves a state with the attestation its agent makes for it; an ephemeral node
    /// serves the state with none.
    #[tokio::test]
    async fn a_durable_node_serves_a_state_with_the_statement_its_agent_requests_and_an_ephemeral_node_with_none() {
        let event = genesis(2);
        let state = EntityState { entity_id: event.entity_id, state: State { head: Clock::singleton(&event), ..State::default() } };

        let durable = Node::new_durable(Arc::new(TestStorage::default()), StateAttester);
        assert_eq!(*served_state(&durable, state.clone()).attestations, [proto::Attestation(PAYLOAD.to_vec())]);

        let ephemeral = Node::new(Arc::new(TestStorage::default()), StateAttester);
        assert!(served_state(&ephemeral, state).attestations.is_empty());
    }

    /// `PermissiveAgent` makes no attestation, so neither a durable nor an ephemeral node attaches one to the states it serves.
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
