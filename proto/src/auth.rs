use serde::{Deserialize, Serialize};

use crate::{Clock, EntityId, EventId, NodeId, Signature};

/// Domain tag for attestation envelope signatures (RFC
/// specs/identity-attestation/spec.md III.1): the signed bytes are
/// `ATTESTATION_TAG || bincode(body)`.
pub const ATTESTATION_TAG: &[u8] = b"ankurah.attestation.v0";

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
    /// Naming an [`EventId`] transitively pins the exact event bytes
    /// (content addressing), so the attestation covers the event without
    /// signing the event; events themselves stay unsigned (ruling R1).
    /// `claims` is agent-defined and opaque to core (ruling R5); the
    /// envelope alone ("recognized durable D admitted event E") is the
    /// load-bearing fact.
    EventAdmitted {
        event: EventId,
        claims: Vec<u8>,
    },
    StateAttested {
        entity: EntityId,
        head: Clock,
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

    #[test]
    fn attestation_signature_round_trip() {
        let key = ed25519_dalek::SigningKey::from_bytes(&[11u8; 32]);
        let attester = NodeId::from(key.verifying_key());
        let body = AttestationBody::EventAdmitted { event: EventId::from_bytes([1u8; 32]), claims: vec![1, 2, 3] };
        let signature: Signature = key.sign(&Attestation::signable_bytes(&body)).into();
        let attestation = Attestation { attester, body, signature };
        assert!(attestation.verify_signature());

        // Tampering with the body invalidates the signature.
        let mut forged = attestation.clone();
        forged.body = AttestationBody::EventAdmitted { event: EventId::from_bytes([2u8; 32]), claims: vec![1, 2, 3] };
        assert!(!forged.verify_signature());

        // A different attester cannot claim the same signature.
        let other = ed25519_dalek::SigningKey::from_bytes(&[12u8; 32]);
        let mut forged = attestation.clone();
        forged.attester = NodeId::from(other.verifying_key());
        assert!(!forged.verify_signature());
    }
}
