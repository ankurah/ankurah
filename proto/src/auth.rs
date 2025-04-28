use serde::{Deserialize, Serialize};

/// Raw context data that can be transmitted between nodes - this may be a bearer token
/// or some other arbitrary data at the discretion of the Policy Agent
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthData(pub Vec<u8>);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Attestation(pub Vec<u8>);

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Attested<T> {
    pub payload: T,
    pub attestations: AttestationSet,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct AttestationSet(pub Vec<Attestation>);

impl std::ops::Deref for AttestationSet {
    type Target = [Attestation];

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl AttestationSet {
    pub fn push(&mut self, attestation: Attestation) { self.0.push(attestation); }
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
