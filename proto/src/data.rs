use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    auth::Attested,
    clock::Clock,
    domain_tag::DomainTag,
    id::EntityId,
    identity::Identity,
    operation::{CarriedOperations, OperationSet},
    AttestationSet, DecodeError,
};
use ankurah_core_types::ModelId;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventId([u8; 32]);

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EventId({})", self.to_base64()) }
}

impl EventId {
    /// The id of a genesis event: SHA-256 over [`DomainTag::Genesis`], then the
    /// minter, nonce and timestamp, the operation count and each operation's
    /// digest. The minter and the memberships carried in the operations are
    /// part of the entity's identity.
    pub fn from_genesis_parts(minter: &Identity, nonce: &[u8; 32], timestamp: u64, operations: &CarriedOperations) -> Self {
        Self::from_preimage(DomainTag::Genesis, &(minter, nonce, timestamp, operations.digests()))
    }

    /// The id of an update event: SHA-256 over [`DomainTag::Event`], then the
    /// entity id, nonce and timestamp, the operation count and each operation's
    /// digest, and the parent clock. Model projections stay outside the
    /// canonical event and its identity. The nonce is what makes an event id
    /// unguessable to anyone who was not sent the event.
    pub fn from_update_parts(
        entity_id: &EntityId,
        nonce: &[u8; 32],
        timestamp: u64,
        operations: &CarriedOperations,
        parent: &Clock,
    ) -> Self {
        Self::from_preimage(DomainTag::Event, &(entity_id, nonce, timestamp, operations.digests(), parent))
    }

    /// SHA-256 over `tag`, then the bincode of `fields`.
    fn from_preimage(tag: DomainTag, fields: &impl Serialize) -> Self {
        let mut preimage = tag.encoded();
        // Cannot fail: bincode encodes every id field, and writing to a vector cannot fail.
        bincode::serialize_into(&mut preimage, fields).expect("event id fields encode");
        Self(Sha256::digest(&preimage).into())
    }
    pub fn to_base64(&self) -> String {
        use base64::{engine::general_purpose, Engine as _};
        general_purpose::URL_SAFE_NO_PAD.encode(self.0)
    }
    /// The first six characters of the base64 form. For compact diagnostics,
    /// never durable identity.
    pub fn to_base64_short(&self) -> String {
        let value = self.to_base64();
        value[..6].to_string()
    }
    pub fn from_base64<T: AsRef<[u8]>>(input: T) -> Result<Self, DecodeError> {
        use base64::{engine::general_purpose, Engine as _};
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(input)?;
        let v: [u8; 32] = decoded.try_into().map_err(|_| DecodeError::InvalidLength)?;

        Ok(Self(v))
    }
    pub fn to_bytes(self) -> [u8; 32] { self.0 }
    pub fn from_bytes(bytes: [u8; 32]) -> Self { Self(bytes) }
    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "{}", self.to_base64_short())
        } else {
            write!(f, "{}", self.to_base64())
        }
    }
}

impl TryFrom<String> for EventId {
    type Error = DecodeError;

    fn try_from(s: String) -> Result<Self, Self::Error> { Self::from_base64(&s) }
}

impl From<[u8; 32]> for EventId {
    fn from(bytes: [u8; 32]) -> Self { Self(bytes) }
}

/// An entity id IS the id of its genesis event. This conversion is the whole
/// of that equality; nothing else turns one into the other.
impl From<EventId> for EntityId {
    fn from(id: EventId) -> Self { EntityId::from_bytes(id.0) }
}
impl TryFrom<Vec<u8>> for EventId {
    type Error = DecodeError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let v: [u8; 32] = bytes.try_into().map_err(|_| DecodeError::InvalidLength)?;
        Ok(Self(v))
    }
}

impl Serialize for EventId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        if serializer.is_human_readable() {
            // Use base64 for human-readable formats like JSON
            serializer.serialize_str(&self.to_base64())
        } else {
            // Use raw bytes as a fixed-size array for binary formats like bincode
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        if deserializer.is_human_readable() {
            // Deserialize from base64 string for human-readable formats
            let s = String::deserialize(deserializer)?;
            EventId::from_base64(s).map_err(serde::de::Error::custom)
        } else {
            // Deserialize from raw bytes as a fixed-size array for binary formats
            let bytes = <[u8; 32]>::deserialize(deserializer)?;
            Ok(EventId::from_bytes(bytes))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Event {
    /// For a genesis this EQUALS the id derived from the body (checked by
    /// [`Event::validate_structure`]); for an update it names the entity the
    /// event extends.
    pub entity_id: EntityId,
    /// The set of concurrent events (usually only one) which is the precursor
    /// of this event. Empty if and only if the body is a genesis.
    pub parent: Clock,
    pub body: EventBody,
}

impl AsRef<Event> for Event {
    fn as_ref(&self) -> &Event { self }
}

/// The two event shapes. A genesis is an entity's single creation event and
/// carries its initial operations, frozen when `create()` returns; every
/// later event is an update.
///
/// Both shapes carry a nonce and a timestamp, and both are inside the
/// event's id hash. They sit in the variants rather than on [`Event`] so one
/// blob carries a whole body through storage and the wire.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum EventBody {
    Genesis {
        /// The identity that minted the entity, a node's key by default. It
        /// is inside the entity's id, so no relay can drop or re-attribute it.
        minter: Identity,
        /// Creator-random, drawn once when the event is minted. Two
        /// `create()` calls with identical payloads draw different nonces and
        /// are therefore different entities; a retry that re-sends the
        /// already-minted event keeps its nonce and is idempotent.
        nonce: [u8; 32],
        /// Unix ms as the creator reported it. Advisory: the same trust level
        /// the timestamp inside a ULID carried. It adds entropy and keeps a
        /// creation-time signal for storage locality.
        timestamp: u64,
        /// The entity's initial property values and model memberships.
        operations: CarriedOperations,
    },
    Update {
        /// Creator-random, drawn once when the event is minted. An event id
        /// is therefore not computable by anyone who was not sent the event.
        nonce: [u8; 32],
        /// Unix ms as the creator reported it. Advisory; see the genesis
        /// field of the same name.
        timestamp: u64,
        operations: CarriedOperations,
    },
}

/// Invalid event structure or a generation claim contradicted by known history.
#[derive(Debug, Clone, thiserror::Error, PartialEq)]
pub enum EventStructureError {
    #[error("genesis event carries a non-empty parent clock")]
    GenesisWithParent,
    #[error("update event carries an empty parent clock")]
    UpdateWithoutParent,
    #[error("genesis id does not match the claimed entity id (event {event}, claimed {claimed})")]
    GenesisIdMismatch { event: EventId, claimed: EntityId },
    #[error("generation mismatch for event {event}: claimed {claimed}, known {known}")]
    GenerationMismatch { event: EventId, claimed: u32, known: u32 },
}

impl Event {
    /// Mint an entity under `minter`: freeze `operations` into a genesis,
    /// draw its nonce and timestamp, and derive the entity id from the whole
    /// of it. The id is the return value's `entity_id`.
    pub fn genesis(minter: Identity, operations: OperationSet) -> Self {
        let operations = CarriedOperations::encode(&operations);
        let nonce = draw_nonce();
        let timestamp = crate::time::unix_ms_now();
        let entity_id: EntityId = EventId::from_genesis_parts(&minter, &nonce, timestamp, &operations).into();
        Event { entity_id, parent: Clock::default(), body: EventBody::Genesis { minter, nonce, timestamp, operations } }
    }

    /// Mint an update extending `parent`, drawing its nonce and timestamp.
    pub fn update(entity_id: EntityId, parent: Clock, operations: OperationSet) -> Self {
        let operations = CarriedOperations::encode(&operations);
        Event { entity_id, parent, body: EventBody::Update { nonce: draw_nonce(), timestamp: crate::time::unix_ms_now(), operations } }
    }

    pub fn is_entity_create(&self) -> bool { matches!(self.body, EventBody::Genesis { .. }) }

    /// The operations the event carries, whichever shape carries them.
    pub fn operations(&self) -> &CarriedOperations {
        match &self.body {
            EventBody::Genesis { operations, .. } | EventBody::Update { operations, .. } => operations,
        }
    }

    /// The creator-random bytes this event was minted with.
    pub fn nonce(&self) -> &[u8; 32] {
        match &self.body {
            EventBody::Genesis { nonce, .. } | EventBody::Update { nonce, .. } => nonce,
        }
    }

    /// The creator-supplied unix-ms timestamp. Advisory.
    pub fn timestamp(&self) -> u64 {
        match &self.body {
            EventBody::Genesis { timestamp, .. } | EventBody::Update { timestamp, .. } => *timestamp,
        }
    }

    /// The identity that minted the entity, for a genesis; an update names no minter.
    pub fn minter(&self) -> Option<Identity> {
        match &self.body {
            EventBody::Genesis { minter, .. } => Some(*minter),
            EventBody::Update { .. } => None,
        }
    }

    /// One above the greatest parent generation, saturating at `u32::MAX`; genesis is 1.
    pub fn generation(&self) -> u32 {
        if self.is_entity_create() {
            1
        } else {
            self.parent.child_generation()
        }
    }

    pub fn id(&self) -> EventId {
        match &self.body {
            EventBody::Genesis { minter, nonce, timestamp, operations } => {
                EventId::from_genesis_parts(minter, nonce, *timestamp, operations)
            }
            EventBody::Update { nonce, timestamp, operations } => {
                EventId::from_update_parts(&self.entity_id, nonce, *timestamp, operations, &self.parent)
            }
        }
    }

    /// Whether the event's shape is self-consistent: the parent clock is
    /// empty if and only if the body is a genesis, and a genesis names the
    /// entity its own content derives. The second check is what makes
    /// creation uniqueness structural — a different genesis is a different
    /// entity, not a competing claim on the same one.
    pub fn validate_structure(&self) -> Result<(), EventStructureError> {
        match &self.body {
            EventBody::Genesis { .. } => {
                if !self.parent.is_empty() {
                    return Err(EventStructureError::GenesisWithParent);
                }
                let id = self.id();
                if EntityId::from(id.clone()) != self.entity_id {
                    return Err(EventStructureError::GenesisIdMismatch { event: id, claimed: self.entity_id });
                }
                Ok(())
            }
            EventBody::Update { .. } => {
                if self.parent.is_empty() {
                    return Err(EventStructureError::UpdateWithoutParent);
                }
                Ok(())
            }
        }
    }
}

/// 32 fresh random bytes for one event mint.
fn draw_nonce() -> [u8; 32] {
    let mut nonce = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut nonce);
    nonce
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EventFragment {
    pub body: EventBody,
    pub parent: Clock,
    pub attestations: AttestationSet,
}

impl From<Attested<Event>> for EventFragment {
    fn from(attested: Attested<Event>) -> Self {
        Self { body: attested.payload.body, parent: attested.payload.parent, attestations: attested.attestations }
    }
}

impl From<(EntityId, EventFragment)> for Attested<Event> {
    fn from(value: (EntityId, EventFragment)) -> Self {
        let event = Event { entity_id: value.0, body: value.1.body, parent: value.1.parent };
        Attested { payload: event, attestations: value.1.attestations }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct StateFragment {
    pub state: State,
    pub attestations: AttestationSet,
}

impl From<Attested<EntityState>> for StateFragment {
    fn from(attested: Attested<EntityState>) -> Self { Self { state: attested.payload.state, attestations: attested.attestations } }
}
impl From<(EntityId, StateFragment)> for Attested<EntityState> {
    fn from(value: (EntityId, StateFragment)) -> Self {
        let entity_state = EntityState { entity_id: value.0, state: value.1.state };
        Attested { payload: entity_state, attestations: value.1.attestations }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EntityState {
    pub entity_id: EntityId,
    pub state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct State {
    /// The current accumulated state of the entity inclusive of all events up to this point
    pub state_buffers: StateBuffers,
    /// Model-backed memberships established by this entity's causal history.
    pub memberships: BTreeSet<ModelId>,
    /// The materialized head: the concurrent tips (usually one) applied to the state above, each with its
    /// generation; empty before the genesis applies.
    pub head: Clock,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct StateBuffers(pub BTreeMap<String, Vec<u8>>);

impl std::ops::Deref for StateBuffers {
    type Target = BTreeMap<String, Vec<u8>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event({} {} {}{} {})",
            self.id().to_base64_short(),
            self.entity_id.to_base64_short(),
            if self.is_entity_create() { "(genesis) " } else { "" },
            self.parent.to_base64_short(),
            self.operations()
        )
    }
}

impl std::fmt::Display for EventFragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let operations = match &self.body {
            EventBody::Genesis { operations, .. } | EventBody::Update { operations, .. } => operations,
        };
        write!(f, "EventFragment(parent {} operations {})", self.parent, operations)
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "State({:#} buffers {})",
            self.head,
            self.state_buffers.iter().map(|(backend, buf)| format!("{} => {}b", backend, buf.len())).collect::<Vec<_>>().join(" ")
        )
    }
}

impl std::fmt::Display for StateFragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StateFragment(state {} attestations: {})", self.state, self.attestations.len())
    }
}

impl std::fmt::Display for EntityState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EntityState({} {})", self.entity_id.to_base64_short(), self.state)
    }
}

impl From<Event> for Attested<Event> {
    fn from(val: Event) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl From<EntityState> for Attested<EntityState> {
    fn from(val: EntityState) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl Attested<EntityState> {
    pub fn to_parts(self) -> (EntityId, StateFragment) {
        (self.payload.entity_id, StateFragment { state: self.payload.state, attestations: self.attestations })
    }
    pub fn from_parts(entity_id: EntityId, fragment: StateFragment) -> Self {
        Self { payload: EntityState { entity_id, state: fragment.state }, attestations: fragment.attestations }
    }
}

impl Attested<Event> {
    pub fn from_parts(entity_id: EntityId, frag: EventFragment) -> Self {
        Self { payload: Event { entity_id, body: frag.body, parent: frag.parent }, attestations: frag.attestations }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{BackendOperation, CarriedOperation, Membership, Operation};
    use ankurah_core_types::SystemModel;

    #[test]
    fn test_event_id_json_serialization() {
        let id = EventId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ]);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA\"");
        assert_eq!(id, serde_json::from_str(&json).unwrap());
    }

    #[test]
    fn test_event_id_bincode_serialization() {
        let id = EventId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ]);
        let bytes = bincode::serialize(&id).unwrap();
        assert_eq!(
            bytes,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
        );
        assert_eq!(id, bincode::deserialize(&bytes).unwrap());
    }

    fn operations() -> OperationSet {
        OperationSet::from_backends(BTreeMap::from([("lww".to_string(), vec![BackendOperation { diff: vec![1, 2, 3] }])]))
    }

    fn carried() -> CarriedOperations { CarriedOperations::encode(&operations()) }

    fn minter() -> Identity { Identity::Ed25519([9u8; 32]) }

    /// The id vectors of specs/protocol/bytes.md section 5: a genesis and an update over zero, one and two
    /// operations, and over the one operation erased, which leaves each id unchanged.
    #[test]
    fn event_ids_match_the_byte_specification_vectors() {
        let membership = CarriedOperation::encode(&Operation::Membership(Membership::Add(ModelId::System(SystemModel::Model))));
        let backend = CarriedOperation::encode(&Operation::Backend {
            backend: "lww".to_owned(),
            operations: vec![BackendOperation { diff: vec![1, 2, 3] }],
        });
        let erased = CarriedOperation::Erased(membership.digest());
        let minter = Identity::Ed25519([0x11; 32]);
        let (nonce, timestamp) = ([0x22; 32], 1_720_000_000_000);
        let entity_id = EntityId::from_bytes([0x33; 32]);
        let parent = Clock::new(vec![(1, EventId::from_bytes([0x44; 32]))]).unwrap();

        for (operations, genesis, update) in [
            (
                vec![],
                "133fdae5349217e57d706cfb395b259cc94e718562a71779dfd1f0270d8bb649",
                "ea0c549736a30f74f83fc27165938ccd17ac57c5b955fe4a1347f74b32372aa8",
            ),
            (
                vec![membership.clone()],
                "9b8f62522e532428eae37ef651a7a0f495fdbf2b1c5d153a1ee79660fbbd358a",
                "e865a554005226a6dc65c5a8dc039f7d40cd2e658833e741bb6d3a149c0dfe7c",
            ),
            (
                vec![membership, backend],
                "0395ecaf7e93f57b19cf2098a8161c6c0c72b182ff1f5dfc8a0525359cd927a3",
                "76c6835c83eb2aba276df5ad184fca20075a513a9140db37cc00e43e6cb63623",
            ),
            (
                vec![erased],
                "9b8f62522e532428eae37ef651a7a0f495fdbf2b1c5d153a1ee79660fbbd358a",
                "e865a554005226a6dc65c5a8dc039f7d40cd2e658833e741bb6d3a149c0dfe7c",
            ),
        ] {
            let operations = CarriedOperations(operations);
            assert_eq!(hex::encode(EventId::from_genesis_parts(&minter, &nonce, timestamp, &operations).as_bytes()), genesis);
            assert_eq!(hex::encode(EventId::from_update_parts(&entity_id, &nonce, timestamp, &operations, &parent).as_bytes()), update);
        }
    }

    /// An event's id covers each operation's digest and not its bytes, so an
    /// operation whose bytes are replaced by their digest leaves the id unchanged.
    #[test]
    fn erasing_an_operation_leaves_the_event_id_unchanged() {
        let genesis = Event::genesis(minter(), operations());
        let update = Event::update(genesis.entity_id, Clock::singleton(&genesis), operations());
        for event in [genesis, update] {
            let mut erased = event.clone();
            let (EventBody::Genesis { operations, .. } | EventBody::Update { operations, .. }) = &mut erased.body;
            operations.0[0] = CarriedOperation::Erased(operations.0[0].digest());
            assert_ne!(erased.body, event.body);
            assert_eq!(erased.id(), event.id());
            erased.validate_structure().expect("an erased operation leaves the event well formed");
        }
    }

    /// An operation of a kind this build does not know still has a digest, so the
    /// event that carries it still derives its id and passes structural validation.
    #[test]
    fn an_event_holding_an_operation_of_an_unknown_kind_derives_its_id() {
        // Variant index 2, which no build of `Operation` defines yet.
        let operations = CarriedOperations(vec![CarriedOperation::Bytes(vec![2, 0, 0, 0, 7])]);
        let nonce = [6u8; 32];
        let entity_id = EventId::from_genesis_parts(&minter(), &nonce, 0, &operations).into();
        let genesis =
            Event { entity_id, parent: Clock::default(), body: EventBody::Genesis { minter: minter(), nonce, timestamp: 0, operations } };
        genesis.validate_structure().expect("the genesis derives the id it names");
        assert!(genesis.operations().decode().is_err());
    }

    /// The minter is inside the genesis id: the same content minted under two
    /// identities is two entities, so no relay can re-attribute an entity
    /// without changing what it is.
    #[test]
    fn the_minter_is_inside_the_genesis_id() {
        let nonce = [4u8; 32];
        let timestamp = 7u64;
        let carried = carried();
        assert_ne!(
            EventId::from_genesis_parts(&Identity::Ed25519([1u8; 32]), &nonce, timestamp, &carried),
            EventId::from_genesis_parts(&Identity::Ed25519([2u8; 32]), &nonce, timestamp, &carried)
        );

        let operations = operations();
        let event = Event::genesis(minter(), operations.clone());
        assert_eq!(event.minter(), Some(minter()));
        let mut reattributed = event.clone();
        let EventBody::Genesis { minter: named, .. } = &mut reattributed.body else { unreachable!("Event::genesis mints a genesis") };
        *named = Identity::Ed25519([8u8; 32]);
        assert!(
            matches!(reattributed.validate_structure(), Err(EventStructureError::GenesisIdMismatch { .. })),
            "a genesis whose minter was swapped no longer derives the entity it names"
        );
        assert_eq!(Event::update(event.entity_id, Clock::singleton(&event), operations).minter(), None, "an update names no minter");
    }

    #[test]
    fn a_minted_genesis_names_the_entity_its_content_derives() {
        let event = Event::genesis(minter(), operations());
        assert!(event.is_entity_create());
        assert!(event.parent.is_empty());
        assert_eq!(EntityId::from(event.id()), event.entity_id);
        event.validate_structure().expect("a freshly minted genesis is well formed");

        // Two create calls draw two nonces: identical payloads are still two
        // distinct entities.
        let again = Event::genesis(minter(), operations());
        assert_ne!(event.entity_id, again.entity_id);
        assert_ne!(event.nonce(), again.nonce());
    }

    #[test]
    fn structural_validation_refuses_contradictory_shapes() {
        // A genesis that claims some other entity's id.
        let mut genesis = Event::genesis(minter(), operations());
        genesis.entity_id = EntityId::from_bytes([0xABu8; 32]);
        assert!(matches!(genesis.validate_structure(), Err(EventStructureError::GenesisIdMismatch { .. })));

        // A genesis with a parent clock.
        let mut genesis = Event::genesis(minter(), operations());
        genesis.parent = Clock::genesis(EventId::from_bytes([1u8; 32]));
        assert_eq!(genesis.validate_structure(), Err(EventStructureError::GenesisWithParent));

        // An update with no parent: parent is empty if and only if genesis.
        let update = Event::update(EntityId::from_bytes([7u8; 32]), Clock::default(), operations());
        assert_eq!(update.validate_structure(), Err(EventStructureError::UpdateWithoutParent));

        let update = Event::update(EntityId::from_bytes([7u8; 32]), Clock::genesis(EventId::from_bytes([1u8; 32])), operations());
        update.validate_structure().expect("a parented update is well formed");
    }

    /// An event body's first four bytes are its variant index, and every stored and carried event begins with it.
    #[test]
    fn event_body_variant_order_is_pinned() {
        let genesis = Event::genesis(minter(), operations());
        let update = Event::update(genesis.entity_id, Clock::singleton(&genesis), operations());
        assert_eq!(bincode::serialize(&genesis.body).unwrap()[..4], [0, 0, 0, 0], "EventBody::Genesis");
        assert_eq!(bincode::serialize(&update.body).unwrap()[..4], [1, 0, 0, 0], "EventBody::Update");
    }

    #[test]
    fn a_genesis_is_generation_one() {
        let genesis = Event::genesis(minter(), operations());
        assert_eq!(genesis.generation(), 1);
    }

    #[test]
    fn parent_annotations_are_hashed_even_when_the_maximum_does_not_change() {
        let (a, b) = (EventId::from_bytes([1; 32]), EventId::from_bytes([2; 32]));
        let parent = Clock::new(vec![(2, a.clone()), (7, b.clone())]).unwrap();
        let update = Event::update(EntityId::from_bytes([7; 32]), parent, operations());
        let mut changed = update.clone();
        changed.parent = Clock::new(vec![(3, a), (7, b)]).unwrap();
        assert_eq!(update.generation(), 8);
        assert_eq!(changed.generation(), 8);
        assert_ne!(update.id(), changed.id());
    }

    #[test]
    fn generation_is_derived_from_parents_and_saturates() {
        let at = |generation| Clock::new(vec![(generation, EventId::from_bytes([1; 32]))]).unwrap();
        let update = |parent| Event::update(EntityId::from_bytes([7; 32]), parent, operations());
        assert_eq!(update(at(1)).generation(), 2);
        assert_eq!(update(Clock::new(vec![(3, EventId::from_bytes([1; 32])), (7, EventId::from_bytes([2; 32]))]).unwrap()).generation(), 8);
        assert_eq!(update(at(u32::MAX - 1)).generation(), u32::MAX);
        assert_eq!(update(at(u32::MAX)).generation(), u32::MAX);
    }
}
