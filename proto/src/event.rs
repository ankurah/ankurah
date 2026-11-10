use serde::{Deserialize, Serialize};

use crate::{
    attestation::{AttestationSet, Attested},
    clock::Clock,
    event_id::EventId,
    id::EntityId,
    identity::Identity,
    operation::{CarriedOperations, OperationSet},
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{BackendOperation, CarriedOperation};
    use std::collections::BTreeMap;

    fn operations() -> OperationSet {
        OperationSet::from_backends(BTreeMap::from([("lww".to_string(), vec![BackendOperation { diff: vec![1, 2, 3] }])]))
    }

    fn carried() -> CarriedOperations { CarriedOperations::encode(&operations()) }

    fn minter() -> Identity { Identity::Ed25519([9u8; 32]) }

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
