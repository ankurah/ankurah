use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    clock::Clock,
    domain_tag::DomainTag,
    encoding::{self, UndecodableBytes},
    event::{Event, EventFragment},
    event_id::EventId,
    id::EntityId,
    identity::{Identity, Signature},
    state::EntityState,
};

/// One attestation as it travels and is stored: the bytes of a [`Statement`] and its attester's
/// signature over them. A node keeps and relays these bytes whether or not it can read them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealedAttestation(pub Vec<u8>);

/// The layout of a sealed attestation's bytes.
#[derive(Serialize, Deserialize)]
struct Sealed {
    /// The statement's bytes, exactly as its attester signed them.
    statement: Vec<u8>,
    /// The attester's signature over [`signed_bytes`] of `statement`.
    signature: Signature,
}

/// What an attester says about one event or one entity state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statement {
    /// The identity whose signature seals the statement.
    pub attester: Identity,
    /// The event or state the statement is about.
    pub subject: Subject,
    /// The entity id of the definition that says what the statement means and how to read `payload`.
    pub kind: EntityId,
    /// The statement's content, read as `kind` says.
    pub payload: Vec<u8>,
}

/// The event or the entity state a statement is about. Its variant index is inside every signed
/// statement, so variants append and never reorder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Subject {
    /// The event with this id.
    Event(EventId),
    /// This state of the entity: its head, and a digest of the whole state.
    State { entity: EntityId, head: Clock, state_digest: [u8; 32] },
}

/// Why a sealed attestation does not unseal.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttestationError {
    /// The bytes are not a sealed statement this build can decode: a malformed encoding, bytes
    /// left over, or an identity, subject or signature variant a later build added.
    #[error("an attestation this build cannot decode: {0}")]
    Undecodable(String),
    /// The signature is not the attester's over the statement's bytes.
    #[error("an attestation whose signature is not its attester's")]
    InvalidSignature,
}

impl Statement {
    /// This statement sealed: its bytes, and `sign`'s signature over them. It unseals only if `sign`
    /// signs as the statement's attester.
    pub fn seal(&self, sign: impl FnOnce(&[u8]) -> Signature) -> SealedAttestation {
        let statement = encoding::encode(self);
        let signature = sign(&signed_bytes(&statement));
        SealedAttestation(encoding::encode(&Sealed { statement, signature }))
    }
}

impl SealedAttestation {
    /// The statement these bytes seal, if they decode and the signature is the statement's
    /// attester's over the statement's bytes as carried.
    pub fn unseal(&self) -> Result<Statement, AttestationError> {
        let sealed: Sealed = decode(&self.0)?;
        let statement: Statement = decode(&sealed.statement)?;
        if !statement.attester.verify(&signed_bytes(&sealed.statement), &sealed.signature) {
            return Err(AttestationError::InvalidSignature);
        }
        Ok(statement)
    }
}

/// The bytes an attestation signature covers: [`DomainTag::Attestation`], then the statement's bytes.
fn signed_bytes(statement: &[u8]) -> Vec<u8> {
    let mut bytes = DomainTag::Attestation.encoded().to_vec();
    bytes.extend_from_slice(statement);
    bytes
}

/// The value `bytes` hold; bytes left over make them undecodable.
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, AttestationError> {
    encoding::decode_exact(bytes).map_err(|error| AttestationError::Undecodable(error.to_string()))
}

impl Subject {
    /// `event`, by the id its content derives.
    pub fn event(event: &Event) -> Self { Self::Event(event.id()) }

    /// `state`, by its entity, its head and its digest.
    pub fn state(state: &EntityState) -> Self {
        Self::State { entity: state.entity_id, head: state.state.head.clone(), state_digest: state_digest(state) }
    }

    /// Whether this subject is `event`, by the id its content derives.
    pub fn names_event(&self, event: &Event) -> bool { *self == Self::event(event) }

    /// Whether this subject is `state`: its entity, its head and its digest.
    pub fn names_state(&self, state: &EntityState) -> bool { *self == Self::state(state) }
}

/// SHA-256 over [`DomainTag::State`], then the encoding of `state`.
pub fn state_digest(state: &EntityState) -> [u8; 32] {
    let mut preimage = DomainTag::State.encoded().to_vec();
    encoding::encode_into(&mut preimage, state);
    Sha256::digest(&preimage).into()
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct Attested<T> {
    pub payload: T,
    pub attestations: AttestationSet,
}

/// The attestations an event or a state carries, each as its sealed bytes.
#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
pub struct AttestationSet(pub Vec<SealedAttestation>);

impl std::ops::Deref for AttestationSet {
    type Target = [SealedAttestation];

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl AttestationSet {
    pub fn push(&mut self, attestation: SealedAttestation) { self.0.push(attestation); }

    /// The bytes a store keeps for this set: each attestation's bytes, unchanged.
    pub fn encode(&self) -> Vec<u8> { encoding::encode(self) }

    /// The set `bytes` hold; bytes left over after it make them undecodable.
    pub fn decode(bytes: &[u8]) -> Result<Self, UndecodableBytes> { encoding::decode_exact(bytes).map_err(UndecodableBytes::of) }
}

impl<T> AsRef<T> for Attested<T> {
    fn as_ref(&self) -> &T { &self.payload }
}

impl<T> Attested<T> {
    pub fn opt(payload: T, attestation: Option<SealedAttestation>) -> Self {
        Self { payload, attestations: AttestationSet(attestation.into_iter().collect()) }
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Attested<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Attested({})", self.payload) }
}

impl<T: Clone> Clone for Attested<T> {
    fn clone(&self) -> Self { Self { payload: self.payload.clone(), attestations: AttestationSet(self.attestations.0.clone()) } }
}

impl Attested<Event> {
    pub fn from_parts(entity_id: EntityId, frag: EventFragment) -> Self {
        Self { payload: Event { entity_id, body: frag.body, parent: frag.parent }, attestations: frag.attestations }
    }

    /// The bytes a store keeps for this event and its attestations.
    pub fn encode(&self) -> Vec<u8> { encoding::encode(self) }

    /// The event and attestations `bytes` hold; bytes left over after them make them undecodable.
    pub fn decode(bytes: &[u8]) -> Result<Self, UndecodableBytes> { encoding::decode_exact(bytes).map_err(UndecodableBytes::of) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::encode;
    use crate::{ClockError, ModelId, OperationSet, State, StateBuffers, SystemModel};
    use ed25519_dalek::Signer;
    use std::collections::{BTreeMap, BTreeSet};

    fn state(genesis: &Event) -> EntityState {
        EntityState { entity_id: genesis.entity_id, state: State { head: Clock::singleton(genesis), ..State::default() } }
    }

    fn attester_key() -> ed25519_dalek::SigningKey { ed25519_dalek::SigningKey::from_bytes(&[13; 32]) }

    fn sign_as(key: &ed25519_dalek::SigningKey) -> impl FnOnce(&[u8]) -> Signature + '_ {
        |bytes| Signature::Ed25519(key.sign(bytes).to_bytes())
    }

    /// A statement by `key` about `subject`, of a kind no build defines.
    fn statement_by(key: &ed25519_dalek::SigningKey, subject: Subject) -> Statement {
        Statement {
            attester: Identity::Ed25519(key.verifying_key().to_bytes()),
            subject,
            kind: EntityId::from_bytes([21; 32]),
            payload: b"payload".to_vec(),
        }
    }

    /// `sealed` with its statement's bytes replaced and its signature kept.
    fn with_statement_bytes(sealed: &SealedAttestation, statement: Vec<u8>) -> SealedAttestation {
        let Sealed { signature, .. } = decode(&sealed.0).unwrap();
        SealedAttestation(encode(&Sealed { statement, signature }))
    }

    /// The attestation vectors of specs/protocol/bytes.md section 5: the bytes each statement's
    /// signature covers, and the layout of a sealed attestation.
    #[test]
    fn signed_and_sealed_bytes_match_the_byte_specification_vectors() {
        let tag = "16616e6b757261682e6174746573746174696f6e2e7631";
        let event_statement = concat!(
            "00000000",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "00000000",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "0100000000000000",
            "07",
        );
        let state_statement = concat!(
            "00000000",
            "1111111111111111111111111111111111111111111111111111111111111111",
            "01000000",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "0100000000000000",
            "02000000",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "0000000000000000",
        );
        let statement = |subject, payload| Statement {
            attester: Identity::Ed25519([0x11; 32]),
            subject,
            kind: EntityId::from_bytes([0xee; 32]),
            payload,
        };
        let state = Subject::State {
            entity: EntityId::from_bytes([0xbb; 32]),
            head: Clock::new(vec![(2, EventId::from_bytes([0xcc; 32]))]).unwrap(),
            state_digest: [0xdd; 32],
        };
        let signature = Signature::Ed25519([0x99; 64]);
        for (statement, expected) in [
            (statement(Subject::Event(EventId::from_bytes([0xaa; 32])), vec![7]), event_statement),
            (statement(state, vec![]), state_statement),
        ] {
            let mut signed = Vec::new();
            let sealed = statement.seal(|bytes| {
                signed = bytes.to_vec();
                signature
            });
            assert_eq!(hex::encode(&signed), format!("{tag}{expected}"));
            let length = hex::encode((expected.len() as u64 / 2).to_le_bytes());
            assert_eq!(hex::encode(&sealed.0), format!("{length}{expected}00000000{}", "99".repeat(64)));
        }
    }

    #[test]
    fn a_sealed_statement_unseals_to_itself_under_its_attesters_signature() {
        let key = attester_key();
        let statement = statement_by(&key, Subject::Event(EventId::from_bytes([1; 32])));
        assert_eq!(statement.seal(sign_as(&key)).unseal(), Ok(statement.clone()));
        assert_eq!(
            statement.seal(sign_as(&ed25519_dalek::SigningKey::from_bytes(&[14; 32]))).unseal(),
            Err(AttestationError::InvalidSignature),
            "signed by a key other than the attester's"
        );
    }

    /// The attester, the subject, the kind and the payload are all inside the signed bytes.
    #[test]
    fn a_changed_attester_subject_kind_or_payload_fails_the_signature() {
        let key = attester_key();
        let statement = statement_by(&key, Subject::Event(EventId::from_bytes([1; 32])));
        let sealed = statement.seal(sign_as(&key));
        let other = ed25519_dalek::SigningKey::from_bytes(&[14; 32]);
        for changed in [
            Statement { attester: Identity::Ed25519(other.verifying_key().to_bytes()), ..statement.clone() },
            Statement { subject: Subject::Event(EventId::from_bytes([2; 32])), ..statement.clone() },
            Statement { kind: EntityId::from_bytes([22; 32]), ..statement.clone() },
            Statement { payload: b"other payload".to_vec(), ..statement.clone() },
        ] {
            let tampered = with_statement_bytes(&sealed, encode(&changed));
            assert_eq!(tampered.unseal(), Err(AttestationError::InvalidSignature), "{changed:?}");
        }
    }

    /// A head's bytes hold its entries in one order only, so a statement signed over a head whose
    /// entries were swapped does not unseal, though its signature covers the bytes it carries.
    #[test]
    fn a_statement_whose_head_is_out_of_order_does_not_unseal() {
        let key = attester_key();
        let (low, high) = (EventId::from_bytes([1; 32]), EventId::from_bytes([2; 32]));
        let statement = statement_by(
            &key,
            Subject::State {
                entity: EntityId::from_bytes([3; 32]),
                head: Clock::new(vec![(1, low), (1, high)]).unwrap(),
                state_digest: [4; 32],
            },
        );
        let canonical = encode(&statement);
        // The head as `count || (generation, id) ..`: swap its two entries, which follow the identity,
        // the subject's variant index, its entity and the head's count.
        let head = 36 + 4 + 32 + 8;
        let entry = 4 + 32;
        let mut carried = canonical.clone();
        carried[head..head + 2 * entry]
            .copy_from_slice(&[&canonical[head + entry..head + 2 * entry], &canonical[head..head + entry]].concat());
        assert_ne!(carried, canonical);

        let sealed = SealedAttestation(encode(&Sealed { statement: carried.clone(), signature: sign_as(&key)(&signed_bytes(&carried)) }));
        assert_eq!(sealed.unseal(), Err(AttestationError::Undecodable(ClockError::OutOfOrder.to_string())));
        let sealed =
            SealedAttestation(encode(&Sealed { statement: canonical.clone(), signature: sign_as(&key)(&signed_bytes(&canonical)) }));
        assert_eq!(sealed.unseal(), Ok(statement), "the same statement with its head in order");
    }

    #[test]
    fn bytes_that_do_not_parse_are_undecodable() {
        let key = attester_key();
        let statement = statement_by(&key, Subject::Event(EventId::from_bytes([1; 32])));
        let sealed = statement.seal(sign_as(&key));
        let mut trailing = sealed.0.clone();
        trailing.push(0);
        let mut statement_bytes = encode(&statement);
        statement_bytes.push(0);
        let mut unknown_attester = encode(&statement);
        // Identity variant index 1 and Subject variant index 2, which no build defines yet.
        unknown_attester[0] = 1;
        let mut unknown_subject = encode(&statement);
        unknown_subject[36] = 2;
        // Signature variant index 1, which no build defines yet: it follows the statement's length and bytes.
        let mut unknown_signature = sealed.0.clone();
        let signature_index = 8 + encode(&statement).len();
        unknown_signature[signature_index..signature_index + 4].copy_from_slice(&1u32.to_le_bytes());
        for undecodable in [
            SealedAttestation(vec![]),
            SealedAttestation(b"not an attestation".to_vec()),
            SealedAttestation(trailing),
            SealedAttestation(unknown_signature),
            with_statement_bytes(&sealed, statement_bytes),
            with_statement_bytes(&sealed, unknown_attester),
            with_statement_bytes(&sealed, unknown_subject),
        ] {
            assert!(matches!(undecodable.unseal(), Err(AttestationError::Undecodable(_))), "{undecodable:?}");
        }
    }

    #[test]
    fn an_event_subject_names_only_its_event() {
        let genesis = Event::genesis(Identity::Ed25519([1; 32]), OperationSet::default());
        let moved = Event::genesis(Identity::Ed25519([2; 32]), OperationSet::default());
        let subject = Subject::event(&genesis);
        assert!(subject.names_event(&genesis));
        assert!(!subject.names_event(&moved), "a statement moved onto another event");
        assert!(!subject.names_state(&state(&genesis)), "an event subject names no state");
    }

    #[test]
    fn a_state_subject_names_only_that_entity_head_and_content() {
        let genesis = Event::genesis(Identity::Ed25519([1; 32]), OperationSet::default());
        let state = state(&genesis);
        let subject = Subject::state(&state);
        assert!(subject.names_state(&state));
        assert!(!subject.names_event(&genesis), "a state subject names no event");

        let mut advanced = state.clone();
        advanced.state.head = Clock::new(vec![(2, EventId::from_bytes([9; 32]))]).unwrap();
        assert!(!subject.names_state(&advanced), "another head of the same entity");
        let mut rewritten = state.clone();
        rewritten.state.state_buffers.0.insert("lww".into(), vec![1]);
        assert!(!subject.names_state(&rewritten), "the same head over other content");
        let mut renamed = state;
        renamed.entity_id = EntityId::from_bytes([3; 32]);
        assert!(!subject.names_state(&renamed), "another entity");
    }

    /// Subjects are inside every signed statement, so their variant order is pinned.
    #[test]
    fn subject_variant_order_is_pinned() {
        let genesis = Event::genesis(Identity::Ed25519([1; 32]), OperationSet::default());
        assert_eq!(encode(&Subject::event(&genesis))[..4], [0, 0, 0, 0], "Subject::Event");
        assert_eq!(encode(&Subject::state(&state(&genesis)))[..4], [1, 0, 0, 0], "Subject::State");
    }

    /// The state digest vector of specs/protocol/bytes.md section 5. Two buffers and two
    /// memberships, so the digest pins the order of each: buffer names by their bytes, and an
    /// entity id model before a system model.
    #[test]
    fn the_state_digest_matches_the_byte_specification_vector() {
        let state = EntityState {
            entity_id: EntityId::from_bytes([0x55; 32]),
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".to_owned(), vec![1, 2, 3]), ("yrs".to_owned(), vec![4, 5])])),
                memberships: BTreeSet::from([ModelId::System(SystemModel::Model), ModelId::EntityId(EntityId::from_bytes([0x77; 32]))]),
                head: Clock::new(vec![(1, EventId::from_bytes([0x66; 32]))]).unwrap(),
            },
        };
        assert_eq!(hex::encode(state_digest(&state)), "ff38c3da54c1ee5405e4ef560f987e2eda6b611009a1875e3d136b0ab1fbf3da");
    }
}
