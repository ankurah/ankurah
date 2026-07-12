use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Clock, EntityId, EntityState, Event, EventId, NodeId, Signature};

/// Domain tag for attestation envelope signatures (RFC
/// specs/identity-attestation/spec.md III.1): the signed bytes are
/// `ATTESTATION_TAG || bincode(body)`.
pub const ATTESTATION_TAG: &[u8] = b"ankurah.attestation.v0";

/// Domain tag for the digest carried by state attestations.
pub const STATE_DIGEST_TAG: &[u8] = b"ankurah.state.v0";

/// Raw context data that can be transmitted between nodes - this may be a bearer token
/// or some other arbitrary data at the discretion of the Policy Agent
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthData(pub Vec<u8>);

/// A signed, structured admission attestation (RFC
/// specs/identity-attestation/spec.md III.1). Verification is a pure
/// function of (envelope, expected attester set): signature valid under
/// `attester`, and `attester` recognized against the system-root founder
/// record. No connection context is involved, which is what makes the
/// artifact portable (multi-durable acceptance, peer-served reads, storage
/// and replay).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attestation {
    pub attester: NodeId,
    pub body: AttestationBody,
    /// Over [`ATTESTATION_TAG`] `|| bincode(body)`, by `attester`'s key.
    pub signature: Signature,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AttestationBody {
    /// Naming an [`EventId`] pins the event's identity-bearing content.
    /// `model` separately pins the deliberately non-identity model envelope,
    /// so an admitted event cannot be relabeled under another model.
    /// `claims` is agent-defined and opaque to core (ruling R5); the
    /// envelope alone ("recognized durable D admitted event E") is the
    /// load-bearing fact.
    EventAdmitted { event: EventId, model: EntityId, claims: Vec<u8> },
    StateAttested {
        entity: EntityId,
        model: EntityId,
        head: Clock,
        /// SHA-256 over [`STATE_DIGEST_TAG`] `|| bincode(EntityState)`.
        state_digest: [u8; 32],
        claims: Vec<u8>,
    },
}

impl Attestation {
    /// The exact bytes an attestation signature covers.
    pub fn signable_bytes(body: &AttestationBody) -> Vec<u8> {
        let mut bytes = ATTESTATION_TAG.to_vec();
        bytes.extend(bincode::serialize(body).expect("attestation body serializes"));
        bytes
    }

    /// Whether `signature` is a valid signature by `attester` over the body.
    /// Attester RECOGNITION (against the system-root founder record) is the
    /// caller's separate check; this verifies the cryptography only.
    pub fn verify_signature(&self) -> bool { self.attester.verify(&Self::signable_bytes(&self.body), &self.signature) }

    /// Digest a state payload exactly as a state attestation does.
    pub fn state_digest(state: &EntityState) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(STATE_DIGEST_TAG);
        hasher.update(bincode::serialize(state).expect("entity state serializes"));
        hasher.finalize().into()
    }

    /// Whether this is the right attestation kind and names the exact event
    /// payload it is attached to. Signature and attester recognition are
    /// separate checks.
    pub fn matches_event(&self, event: &Event) -> bool {
        matches!(
            &self.body,
            AttestationBody::EventAdmitted { event: event_id, model, .. }
                if *event_id == event.id() && *model == event.model
        )
    }

    /// Whether this is the right attestation kind and binds the exact state
    /// payload it is attached to. Signature and attester recognition are
    /// separate checks.
    pub fn matches_state(&self, state: &EntityState) -> bool {
        matches!(
            &self.body,
            AttestationBody::StateAttested { entity, model, head, state_digest, .. }
                if *entity == state.entity_id
                    && *model == state.model
                    && *head == state.state.head
                    && *state_digest == Self::state_digest(state)
        )
    }
}

impl std::fmt::Display for Attestation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.body {
            AttestationBody::EventAdmitted { event, .. } => {
                write!(f, "EventAdmitted({:#} by {:#})", event, self.attester)
            }
            AttestationBody::StateAttested { entity, head, .. } => {
                write!(f, "StateAttested({:#} @ {:#} by {:#})", entity, head, self.attester)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct Attested<T> {
    pub payload: T,
    pub attestations: AttestationSet,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
pub struct AttestationSet(pub Vec<Attestation>);

impl std::ops::Deref for AttestationSet {
    type Target = [Attestation];

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl AttestationSet {
    pub fn push(&mut self, attestation: Attestation) { self.0.push(attestation); }

    /// Keep only the attestations `keep` accepts, returning how many were
    /// dropped. Used by core-side envelope verification to strip invalid
    /// envelopes before any agent hook sees the payload.
    pub fn retain(&mut self, keep: impl FnMut(&Attestation) -> bool) -> usize {
        let before = self.0.len();
        self.0.retain(keep);
        before - self.0.len()
    }
}

impl<T> Attested<T> {
    pub fn opt(payload: T, attestation: Option<Attestation>) -> Self {
        Self { payload, attestations: AttestationSet(attestation.into_iter().collect()) }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    // TODO
}

impl<T: std::fmt::Display> std::fmt::Display for Attested<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Attested({})", self.payload) }
}

impl<T: Clone> Clone for Attested<T> {
    fn clone(&self) -> Self { Self { payload: self.payload.clone(), attestations: AttestationSet(self.attestations.0.clone()) } }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;
    use std::collections::BTreeMap;

    #[test]
    fn attestation_signature_round_trip() {
        let key = ed25519_dalek::SigningKey::from_bytes(&[11u8; 32]);
        let attester = NodeId::from(key.verifying_key());
        let body = AttestationBody::EventAdmitted {
            event: EventId::from_bytes([1u8; 32]),
            model: EntityId::from_bytes([9u8; 32]),
            claims: vec![1, 2, 3],
        };
        let signature: Signature = key.sign(&Attestation::signable_bytes(&body)).into();
        let attestation = Attestation { attester, body, signature };
        assert!(attestation.verify_signature());

        // Tampering with the body invalidates the signature.
        let mut forged = attestation.clone();
        forged.body = AttestationBody::EventAdmitted {
            event: EventId::from_bytes([2u8; 32]),
            model: EntityId::from_bytes([9u8; 32]),
            claims: vec![1, 2, 3],
        };
        assert!(!forged.verify_signature());

        // A different attester cannot claim the same signature.
        let other = ed25519_dalek::SigningKey::from_bytes(&[12u8; 32]);
        let mut forged = attestation.clone();
        forged.attester = NodeId::from(other.verifying_key());
        assert!(!forged.verify_signature());
    }

    #[test]
    fn subject_matching_rejects_transplants_and_state_tampering() {
        let model = EntityId::from_bytes([7u8; 32]);
        let event = Event::genesis(model, None, crate::OperationSet(BTreeMap::new()));
        let event_attestation = Attestation {
            attester: NodeId::from_bytes([3u8; 32]),
            body: AttestationBody::EventAdmitted { event: event.id(), model, claims: vec![] },
            signature: Signature::from_bytes([0u8; 64]),
        };
        assert!(event_attestation.matches_event(&event));

        let mut relabeled = event.clone();
        relabeled.model = EntityId::from_bytes([8u8; 32]);
        assert_eq!(relabeled.id(), event.id(), "model is deliberately outside event identity");
        assert!(!event_attestation.matches_event(&relabeled), "the attestation separately binds the model envelope");

        let state = EntityState {
            entity_id: event.entity_id,
            model,
            state: crate::State {
                state_buffers: crate::StateBuffers(BTreeMap::from([("lww".to_string(), vec![1, 2, 3])])),
                head: crate::Clock::from(vec![event.id()]),
            },
        };
        let state_attestation = Attestation {
            attester: NodeId::from_bytes([3u8; 32]),
            body: AttestationBody::StateAttested {
                entity: state.entity_id,
                model: state.model,
                head: state.state.head.clone(),
                state_digest: Attestation::state_digest(&state),
                claims: vec![],
            },
            signature: Signature::from_bytes([0u8; 64]),
        };
        assert!(state_attestation.matches_state(&state));
        assert!(!state_attestation.matches_event(&event), "attestation kinds are not interchangeable");

        let mut tampered = state.clone();
        tampered.state.state_buffers.0.get_mut("lww").unwrap().push(4);
        assert!(!state_attestation.matches_state(&tampered));
    }
}
