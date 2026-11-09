use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    clock::Clock,
    data::{EntityState, Event, EventId},
    domain_tag::DomainTag,
    id::EntityId,
    identity::{Identity, Signature},
};

/// Raw context data that can be transmitted between nodes - this may be a bearer token
/// or some other arbitrary data at the discretion of the Policy Agent
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthData(pub Vec<u8>);

/// A signed statement by an identity about one event or one state, which any
/// node that later receives the payload can check without the connection that
/// carried it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attestation {
    pub attester: Identity,
    pub body: AttestationBody,
    /// `attester`'s signature over [`AttestationBody::signable_bytes`].
    pub signature: Signature,
}

/// What an attestation says, and about which payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AttestationBody {
    /// The attester committed or admitted the event with this id.
    Event { event: EventId, role: AttesterRole, claims: Vec<u8> },
    /// The attester held this state of the entity: its head, and a digest of the whole state.
    State { entity: EntityId, head: Clock, state_digest: [u8; 32], claims: Vec<u8> },
}

/// The attester's part in an event.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttesterRole {
    /// The attester wrote the event and committed it.
    Committed,
    /// The attester accepted another node's event under its policy.
    Admitted,
}

impl AttestationBody {
    /// A body naming `event`.
    pub fn event(event: &Event, role: AttesterRole, claims: Vec<u8>) -> Self { Self::Event { event: event.id(), role, claims } }

    /// A body naming `state` by its entity, head and digest.
    pub fn state(state: &EntityState, claims: Vec<u8>) -> Self {
        Self::State { entity: state.entity_id, head: state.state.head.clone(), state_digest: state_digest(state), claims }
    }

    /// The bytes an attestation signature covers: [`DomainTag::Attestation`], then the bincode body.
    pub fn signable_bytes(&self) -> Vec<u8> {
        let mut bytes = DomainTag::Attestation.encoded();
        // Cannot fail: every field is a value bincode can encode.
        bytes.extend(bincode::serialize(self).expect("attestation bodies serialize"));
        bytes
    }
}

/// SHA-256 over [`DomainTag::State`], then the bincode of `state`.
pub fn state_digest(state: &EntityState) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(DomainTag::State.encoded());
    // Cannot fail: bincode encodes every field of an entity state.
    hasher.update(bincode::serialize(state).expect("entity states serialize"));
    hasher.finalize().into()
}

impl Attestation {
    /// Whether the signature is the attester's over this body.
    pub fn verify(&self) -> bool { self.attester.verify(&self.body.signable_bytes(), &self.signature) }

    /// Whether this attestation names `event`, by the id its content derives.
    pub fn covers_event(&self, event: &Event) -> bool {
        matches!(&self.body, AttestationBody::Event { event: id, .. } if *id == event.id())
    }

    /// Whether this attestation names `state`: its entity, its head, and its digest.
    pub fn covers_state(&self, state: &EntityState) -> bool {
        matches!(&self.body, AttestationBody::State { entity, head, state_digest: digest, .. }
            if *entity == state.entity_id && *head == state.state.head && *digest == state_digest(state))
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
}

impl<T> AsRef<T> for Attested<T> {
    fn as_ref(&self) -> &T { &self.payload }
}

impl<T> Attested<T> {
    pub fn opt(payload: T, attestation: Option<Attestation>) -> Self {
        Self { payload, attestations: AttestationSet(attestation.into_iter().collect()) }
    }
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
    use crate::{ModelId, OperationSet, State, StateBuffers, SystemModel};
    use ed25519_dalek::Signer;
    use std::collections::{BTreeMap, BTreeSet};

    fn signed(key: &ed25519_dalek::SigningKey, body: AttestationBody) -> Attestation {
        let signature = Signature::Ed25519(key.sign(&body.signable_bytes()).to_bytes());
        Attestation { attester: Identity::Ed25519(key.verifying_key().to_bytes()), body, signature }
    }

    fn key() -> ed25519_dalek::SigningKey { ed25519_dalek::SigningKey::from_bytes(&[11; 32]) }

    fn state(genesis: &Event) -> EntityState {
        EntityState { entity_id: genesis.entity_id, state: State { head: Clock::singleton(genesis), ..State::default() } }
    }

    /// The state digest vector of specs/protocol/bytes.md section 5.
    #[test]
    fn the_state_digest_matches_the_byte_specification_vector() {
        let state = EntityState {
            entity_id: EntityId::from_bytes([0x55; 32]),
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".to_owned(), vec![1, 2, 3])])),
                memberships: BTreeSet::from([ModelId::System(SystemModel::Model)]),
                head: Clock::new(vec![(1, EventId::from_bytes([0x66; 32]))]).unwrap(),
            },
        };
        assert_eq!(hex::encode(state_digest(&state)), "05e2003ec33f79b0e5b1aec10579c2c75fde70fd91971765028855fb8509221e");
    }

    /// The attestation vectors of specs/protocol/bytes.md section 5: the bytes each body's signature covers.
    #[test]
    fn the_signed_bytes_match_the_byte_specification_vectors() {
        let event = AttestationBody::Event { event: EventId::from_bytes([0xaa; 32]), role: AttesterRole::Admitted, claims: vec![7] };
        assert_eq!(
            hex::encode(event.signable_bytes()),
            "16616e6b757261682e6174746573746174696f6e2e763100000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01000000010000000000000007"
        );
        let state = AttestationBody::State {
            entity: EntityId::from_bytes([0xbb; 32]),
            head: Clock::new(vec![(2, EventId::from_bytes([0xcc; 32]))]).unwrap(),
            state_digest: [0xdd; 32],
            claims: vec![],
        };
        assert_eq!(
            hex::encode(state.signable_bytes()),
            "16616e6b757261682e6174746573746174696f6e2e763101000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb010000000000000002000000ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd0000000000000000"
        );
    }

    /// Bodies and roles are on the wire and in storage, so their variant order is pinned.
    #[test]
    fn body_and_role_variant_order_is_pinned() {
        let genesis = Event::genesis(Identity::Ed25519([1; 32]), OperationSet::default());
        let event = bincode::serialize(&AttestationBody::event(&genesis, AttesterRole::Admitted, vec![])).unwrap();
        assert_eq!(&event[..4], [0, 0, 0, 0], "AttestationBody::Event encodes as variant 0");
        assert_eq!(&event[36..40], [1, 0, 0, 0], "AttesterRole::Admitted encodes as variant 1 after the event id");
        let state = bincode::serialize(&AttestationBody::state(&state(&genesis), vec![])).unwrap();
        assert_eq!(&state[..4], [1, 0, 0, 0], "AttestationBody::State encodes as variant 1");
        assert_eq!(bincode::serialize(&AttesterRole::Committed).unwrap(), 0u32.to_le_bytes());
    }

    #[test]
    fn an_event_attestation_verifies_and_covers_only_its_event() {
        let genesis = Event::genesis(Identity::Ed25519([1; 32]), OperationSet::default());
        let other = Event::genesis(Identity::Ed25519([2; 32]), OperationSet::default());
        let attestation = signed(&key(), AttestationBody::event(&genesis, AttesterRole::Committed, vec![]));
        assert!(attestation.verify());
        assert!(attestation.covers_event(&genesis));
        assert!(!attestation.covers_event(&other), "a transplanted attestation names another event");
        assert!(!attestation.covers_state(&state(&genesis)), "an event attestation covers no state");

        let mut tampered = attestation.clone();
        tampered.body = AttestationBody::event(&genesis, AttesterRole::Admitted, vec![]);
        assert!(!tampered.verify(), "a body changed after signing");
        let mut impersonated = attestation;
        impersonated.attester = Identity::Ed25519(ed25519_dalek::SigningKey::from_bytes(&[12; 32]).verifying_key().to_bytes());
        assert!(!impersonated.verify(), "another identity's name on this signature");
    }

    #[test]
    fn a_state_attestation_covers_only_that_entity_head_and_content() {
        let genesis = Event::genesis(Identity::Ed25519([1; 32]), OperationSet::default());
        let state = state(&genesis);
        let attestation = signed(&key(), AttestationBody::state(&state, vec![]));
        assert!(attestation.verify());
        assert!(attestation.covers_state(&state));

        let mut moved = state.clone();
        moved.state.head = Clock::default();
        assert!(!attestation.covers_state(&moved), "another head");
        let mut rewritten = state.clone();
        rewritten.state.state_buffers.0.insert("lww".into(), vec![1]);
        assert!(!attestation.covers_state(&rewritten), "the same head over other content");
        let mut renamed = state;
        renamed.entity_id = EntityId::from_bytes([3; 32]);
        assert!(!attestation.covers_state(&renamed), "another entity");
    }
}
