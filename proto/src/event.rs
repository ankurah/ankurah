use serde::{Deserialize, Serialize};

use crate::{
    auth::{AttestationSet, Attested},
    author::AuthorId,
    clock::Clock,
    event_id::EventId,
    id::EntityId,
    operation::OperationSet,
};

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
/// Both shapes carry a nonce, a timestamp, and an author, and all three are
/// inside the event's id hash. They sit in the variants rather than on
/// [`Event`] so one blob carries a whole body through storage and the wire.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum EventBody {
    Genesis {
        /// The system root's entity id; `None` ONLY for the system root
        /// entity itself, which has no system above it.
        system: Option<EntityId>,
        /// Creator-random, drawn once when the event is minted. Two
        /// `create()` calls with identical payloads draw different nonces and
        /// are therefore different entities; a retry that re-sends the
        /// already-minted event keeps its nonce and is idempotent.
        nonce: [u8; 32],
        /// Unix ms as the creator reported it. Advisory: the same trust level
        /// the timestamp inside a ULID carried. It adds entropy and keeps a
        /// creation-time signal for storage locality.
        timestamp: u64,
        /// Who the creator says wrote this. Always [`AuthorId::Unknown`]
        /// today; see [`AuthorId`].
        author: AuthorId,
        /// The entity's initial property values and model memberships.
        operations: OperationSet,
    },
    Update {
        /// Creator-random, drawn once when the event is minted. An event id
        /// is therefore not computable by anyone who was not sent the event.
        nonce: [u8; 32],
        /// Unix ms as the creator reported it. Advisory; see the genesis
        /// field of the same name.
        timestamp: u64,
        /// Who the creator says wrote this. Always [`AuthorId::Unknown`]
        /// today; see [`AuthorId`].
        author: AuthorId,
        operations: OperationSet,
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
    /// Mint an entity: freeze `operations` into a genesis, draw its nonce and
    /// timestamp, and derive the entity id from the whole of it. The id is
    /// the return value's `entity_id`.
    pub fn genesis(system: Option<EntityId>, author: AuthorId, operations: OperationSet) -> Self {
        let nonce = draw_nonce();
        let timestamp = crate::time::unix_ms_now();
        let entity_id: EntityId = EventId::from_genesis_parts(&system, &nonce, timestamp, &author, &operations).into();
        Event { entity_id, parent: Clock::default(), body: EventBody::Genesis { system, nonce, timestamp, author, operations } }
    }

    /// Mint an update extending `parent`, drawing its nonce and timestamp.
    pub fn update(entity_id: EntityId, parent: Clock, author: AuthorId, operations: OperationSet) -> Self {
        Event {
            entity_id,
            parent,
            body: EventBody::Update { nonce: draw_nonce(), timestamp: crate::time::unix_ms_now(), author, operations },
        }
    }

    pub fn is_entity_create(&self) -> bool { matches!(self.body, EventBody::Genesis { .. }) }

    /// The event's operations, whichever shape carries them.
    pub fn operations(&self) -> &OperationSet {
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

    /// Who the creator says wrote this event.
    pub fn author(&self) -> AuthorId {
        match &self.body {
            EventBody::Genesis { author, .. } | EventBody::Update { author, .. } => *author,
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
            EventBody::Genesis { system, nonce, timestamp, author, operations } => {
                EventId::from_genesis_parts(system, nonce, *timestamp, author, operations)
            }
            EventBody::Update { nonce, timestamp, author, operations } => {
                EventId::from_update_parts(&self.entity_id, author, nonce, *timestamp, operations, &self.parent)
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

impl From<Event> for Attested<Event> {
    fn from(val: Event) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl Attested<Event> {
    pub fn from_parts(entity_id: EntityId, frag: EventFragment) -> Self {
        Self { payload: Event { entity_id, body: frag.body, parent: frag.parent }, attestations: frag.attestations }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_id::{EVENT_TAG, GENESIS_TAG};
    use crate::operation::BackendOperation;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;

    fn operations() -> OperationSet {
        OperationSet::from_backends(BTreeMap::from([("lww".to_string(), vec![BackendOperation { diff: vec![1, 2, 3] }])]))
    }

    /// Both preimages, computed independently, byte for byte. Concatenating
    /// the fields' bincode encodings is the same as encoding the tuple,
    /// because bincode writes no framing between them.
    ///
    /// The tags are written out here as literals rather than read from
    /// [`GENESIS_TAG`] and [`EVENT_TAG`]. Reusing the constants would let a
    /// typo, a swap, or any coordinated edit change both sides of the
    /// comparison and still pass, which is the one thing an independent
    /// recomputation is for.
    #[test]
    fn derivation_preimages_match_the_spec() {
        let genesis_tag: &[u8] = b"org.ankurah.genesis.v0";
        let event_tag: &[u8] = b"org.ankurah.event.v0";
        assert_eq!(GENESIS_TAG, genesis_tag, "the genesis tag is these exact bytes");
        assert_eq!(EVENT_TAG, event_tag, "the update tag is these exact bytes");
        assert_ne!(genesis_tag, event_tag, "the two tags differ, which is what keeps the preimage shapes apart");

        let system = Some(EntityId::from_bytes([9u8; 32]));
        let nonce = [3u8; 32];
        let timestamp = 1_720_000_000_123u64;
        let author = AuthorId::Unknown;
        let operations = operations();

        let mut preimage = genesis_tag.to_vec();
        preimage.extend(bincode::serialize(&(system, nonce, timestamp, author, operations.clone())).unwrap());
        let expected: [u8; 32] = Sha256::digest(&preimage).into();
        assert_eq!(EventId::from_genesis_parts(&system, &nonce, timestamp, &author, &operations), EventId::from_bytes(expected));

        let entity_id = EntityId::from_bytes([7u8; 32]);
        let parent = Clock::genesis(EventId::from_bytes([5u8; 32]));
        let mut preimage = event_tag.to_vec();
        preimage.extend(bincode::serialize(&(entity_id, author, nonce, timestamp, operations.clone(), parent.clone())).unwrap());
        let expected: [u8; 32] = Sha256::digest(&preimage).into();
        assert_eq!(EventId::from_update_parts(&entity_id, &author, &nonce, timestamp, &operations, &parent), EventId::from_bytes(expected));
    }

    /// The two tags keep the two preimage shapes apart even when every hashed
    /// field happens to coincide.
    #[test]
    fn the_two_preimage_shapes_never_collide() {
        let nonce = [1u8; 32];
        let timestamp = 42u64;
        let author = AuthorId::Unknown;
        let operations = operations();
        let genesis = EventId::from_genesis_parts(&None, &nonce, timestamp, &author, &operations);
        let update =
            EventId::from_update_parts(&EntityId::from_bytes([0u8; 32]), &author, &nonce, timestamp, &operations, &Clock::default());
        assert_ne!(genesis, update);
    }

    /// The system root's genesis (`None`) and a non-root genesis under any
    /// system are distinct, and two systems give the same content two ids.
    #[test]
    fn a_genesis_id_binds_its_system() {
        let nonce = [1u8; 32];
        let timestamp = 42u64;
        let author = AuthorId::Unknown;
        let operations = operations();
        let root = EventId::from_genesis_parts(&None, &nonce, timestamp, &author, &operations);
        let under_a = EventId::from_genesis_parts(&Some(EntityId::from_bytes([1u8; 32])), &nonce, timestamp, &author, &operations);
        let under_b = EventId::from_genesis_parts(&Some(EntityId::from_bytes([2u8; 32])), &nonce, timestamp, &author, &operations);
        assert_ne!(root, under_a);
        assert_ne!(under_a, under_b);
    }

    /// Changing the author changes the id, which is what makes the author
    /// unforgeable-in-place rather than an editable annotation.
    #[test]
    fn the_author_is_inside_both_ids() {
        let nonce = [4u8; 32];
        let timestamp = 7u64;
        let operations = operations();
        let author = AuthorId::Id(EntityId::from_bytes([6u8; 32]));
        assert_ne!(
            EventId::from_genesis_parts(&None, &nonce, timestamp, &AuthorId::Unknown, &operations),
            EventId::from_genesis_parts(&None, &nonce, timestamp, &author, &operations)
        );

        let entity_id = EntityId::from_bytes([7u8; 32]);
        let parent = Clock::genesis(EventId::from_bytes([5u8; 32]));
        assert_ne!(
            EventId::from_update_parts(&entity_id, &AuthorId::Unknown, &nonce, timestamp, &operations, &parent),
            EventId::from_update_parts(&entity_id, &author, &nonce, timestamp, &operations, &parent)
        );
    }

    #[test]
    fn a_minted_genesis_names_the_entity_its_content_derives() {
        let event = Event::genesis(Some(EntityId::from_bytes([9u8; 32])), AuthorId::Unknown, operations());
        assert!(event.is_entity_create());
        assert!(event.parent.is_empty());
        assert_eq!(EntityId::from(event.id()), event.entity_id);
        event.validate_structure().expect("a freshly minted genesis is well formed");

        // Two create calls draw two nonces: identical payloads are still two
        // distinct entities.
        let again = Event::genesis(Some(EntityId::from_bytes([9u8; 32])), AuthorId::Unknown, operations());
        assert_ne!(event.entity_id, again.entity_id);
        assert_ne!(event.nonce(), again.nonce());
    }

    #[test]
    fn structural_validation_refuses_contradictory_shapes() {
        // A genesis that claims some other entity's id.
        let mut genesis = Event::genesis(None, AuthorId::Unknown, operations());
        genesis.entity_id = EntityId::from_bytes([0xABu8; 32]);
        assert!(matches!(genesis.validate_structure(), Err(EventStructureError::GenesisIdMismatch { .. })));

        // A genesis with a parent clock.
        let mut genesis = Event::genesis(None, AuthorId::Unknown, operations());
        genesis.parent = Clock::genesis(EventId::from_bytes([1u8; 32]));
        assert_eq!(genesis.validate_structure(), Err(EventStructureError::GenesisWithParent));

        // An update with no parent: parent is empty if and only if genesis.
        let update = Event::update(EntityId::from_bytes([7u8; 32]), Clock::default(), AuthorId::Unknown, operations());
        assert_eq!(update.validate_structure(), Err(EventStructureError::UpdateWithoutParent));

        let update =
            Event::update(EntityId::from_bytes([7u8; 32]), Clock::genesis(EventId::from_bytes([1u8; 32])), AuthorId::Unknown, operations());
        update.validate_structure().expect("a parented update is well formed");
    }

    #[test]
    fn a_genesis_is_generation_one() {
        let genesis = Event::genesis(None, AuthorId::Unknown, operations());
        assert_eq!(genesis.generation(), 1);
    }

    #[test]
    fn parent_annotations_are_hashed_even_when_the_maximum_does_not_change() {
        let (a, b) = (EventId::from_bytes([1; 32]), EventId::from_bytes([2; 32]));
        let parent = Clock::new(vec![(2, a.clone()), (7, b.clone())]).unwrap();
        let update = Event::update(EntityId::from_bytes([7; 32]), parent, AuthorId::Unknown, operations());
        let mut changed = update.clone();
        changed.parent = Clock::new(vec![(3, a), (7, b)]).unwrap();
        assert_eq!(update.generation(), 8);
        assert_eq!(changed.generation(), 8);
        assert_ne!(update.id(), changed.id());
    }

    #[test]
    fn generation_is_derived_from_parents_and_saturates() {
        let at = |generation| Clock::new(vec![(generation, EventId::from_bytes([1; 32]))]).unwrap();
        let update = |parent| Event::update(EntityId::from_bytes([7; 32]), parent, AuthorId::Unknown, operations());
        assert_eq!(update(at(1)).generation(), 2);
        assert_eq!(update(Clock::new(vec![(3, EventId::from_bytes([1; 32])), (7, EventId::from_bytes([2; 32]))]).unwrap()).generation(), 8);
        assert_eq!(update(at(u32::MAX - 1)).generation(), u32::MAX);
        assert_eq!(update(at(u32::MAX)).generation(), u32::MAX);
    }
}
