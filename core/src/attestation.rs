//! Core-owned attestation production and mechanical verification.

use ankurah_proto as proto;
use ed25519_dalek::Signer;
use std::sync::atomic::Ordering;

use crate::{
    policy::{AccessDenied, Admission, PolicyAgent},
    storage::StorageEngine,
    Node,
};

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn sign_attestation(&self, body: proto::AttestationBody) -> proto::Attestation {
        let signature = self.signing_key.sign(&proto::Attestation::signable_bytes(&body)).into();
        proto::Attestation { attester: self.id, body, signature }
    }

    /// Envelope-only variant of [`Self::attest_event`] for callers that keep
    /// the event elsewhere and must not clone it just to wrap it.
    pub(crate) fn mint_event_attestation(&self, event: &proto::Event, admission: Admission) -> Option<proto::Attestation> {
        match admission {
            Admission::Allow => None,
            Admission::Attest { claims } => Some(self.sign_attestation(proto::AttestationBody::EventAdmitted {
                system_root: self.system.root_id().expect("attestation requires a pinned system root"),
                event: event.id(),
                model: event.model,
                claims,
            })),
        }
    }

    pub(crate) fn attest_event(&self, event: proto::Event, admission: Admission) -> proto::Attested<proto::Event> {
        let attestation = self.mint_event_attestation(&event, admission);
        proto::Attested::opt(event, attestation)
    }

    pub(crate) fn attest_state(&self, state: proto::EntityState, admission: Admission) -> proto::Attested<proto::EntityState> {
        let attestation = match admission {
            Admission::Allow => None,
            Admission::Attest { claims } => Some(self.sign_attestation(proto::AttestationBody::StateAttested {
                system_root: self.system.root_id().expect("attestation requires a pinned system root"),
                entity: state.entity_id,
                model: state.model,
                head: state.state.head.clone(),
                state_digest: proto::Attestation::state_digest(&state),
                claims,
            })),
        };
        proto::Attested::opt(state, attestation)
    }

    fn attester_is_recognized(&self, attester: proto::NodeId) -> bool { self.system.founder() == Some(attester) }

    fn attestation_is_system_local(&self, attestation: &proto::Attestation) -> bool {
        self.system.root_id().is_some_and(|system_root| attestation.matches_system_root(system_root))
    }

    fn record_stripped_attestations(&self, dropped: usize, kind: &str) {
        if dropped == 0 {
            return;
        }
        self.invalid_attestations.fetch_add(dropped as u64, Ordering::Relaxed);
        tracing::warn!(node = %self.id, dropped, payload_kind = kind, "stripped invalid attestation envelopes");
    }

    /// Validate the structural and system-local facts that do not depend on
    /// deployment policy. Ordinary genesis events must name this node's
    /// pinned system. The one `system: None` exception is the pinned system
    /// root's own genesis.
    pub(crate) fn validate_event_scope(&self, event: &proto::Event) -> Result<(), AccessDenied> {
        event.validate_structure()?;
        let proto::EventBody::Genesis { system, .. } = &event.body else {
            return Ok(());
        };

        let expected = self.system.root_id();
        let matches = match (expected, system) {
            (Some(expected), Some(actual)) => *actual == expected,
            (Some(expected), None) => {
                event.entity_id == expected
                    && crate::schema::well_known_collection(&event.model)
                        .is_some_and(|collection| collection.as_str() == crate::system::SYSTEM_COLLECTION_ID)
            }
            (None, _) => false,
        };
        if matches {
            Ok(())
        } else {
            Err(AccessDenied::CrossSystemGenesis { expected, actual: *system })
        }
    }

    /// Strip every envelope that is the wrong kind, names a different
    /// payload or system, has an invalid signature, or comes from an
    /// unrecognized durable. The PolicyAgent then sees verified envelopes
    /// only.
    pub(crate) fn verify_event_attestations(&self, event: &mut proto::Attested<proto::Event>) {
        let payload = &event.payload;
        let dropped = event.attestations.retain(|attestation| {
            attestation.matches_event(payload)
                && attestation.verify_signature()
                && self.attester_is_recognized(attestation.attester)
                && self.attestation_is_system_local(attestation)
        });
        self.record_stripped_attestations(dropped, "event");
    }

    /// State counterpart of [`Self::verify_event_attestations`].
    pub(crate) fn verify_state_attestations(&self, state: &mut proto::Attested<proto::EntityState>) {
        let payload = &state.payload;
        let dropped = state.attestations.retain(|attestation| {
            attestation.matches_state(payload)
                && attestation.verify_signature()
                && self.attester_is_recognized(attestation.attester)
                && self.attestation_is_system_local(attestation)
        });
        self.record_stripped_attestations(dropped, "state");
    }

    /// Apply the agent-independent envelope checks before handing an event
    /// to the deployment-specific sufficiency policy.
    pub(crate) fn validate_incoming_event(
        &self,
        from: &proto::NodeId,
        mut event: proto::Attested<proto::Event>,
    ) -> Result<proto::Attested<proto::Event>, AccessDenied> {
        self.validate_event_scope(&event.payload)?;
        self.verify_event_attestations(&mut event);
        self.policy_agent.validate_received_event(self, from, &event)?;
        Ok(event)
    }

    /// State counterpart of [`Self::validate_incoming_event`].
    pub(crate) fn validate_incoming_state(
        &self,
        from: &proto::NodeId,
        mut state: proto::Attested<proto::EntityState>,
    ) -> Result<proto::Attested<proto::EntityState>, AccessDenied> {
        self.verify_state_attestations(&mut state);
        self.policy_agent.validate_received_state(self, from, &state)?;
        Ok(state)
    }
}
