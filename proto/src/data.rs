use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{auth::Attested, author::AuthorId, clock::Clock, collection::CollectionId, id::EntityId, AttestationSet, DecodeError};
use ankurah_core_types::ModelId;

/// Domain tag for genesis event ids. Separates the two preimage shapes so no
/// genesis preimage can ever equal an update preimage, and versions the
/// derivation scheme.
pub const GENESIS_TAG: &[u8] = b"org.ankurah.genesis.v0";

/// Domain tag for update event ids. See [`GENESIS_TAG`].
pub const EVENT_TAG: &[u8] = b"org.ankurah.event.v0";

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct EventId([u8; 32]);

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EventId({})", self.to_base64()) }
}

impl EventId {
    /// The id of a genesis event: `SHA-256(GENESIS_TAG || bincode(system,
    /// nonce, timestamp, author, operations))`. An entity's id IS this value,
    /// so the preimage is a commitment to the whole of what distinguishes one
    /// creation from another.
    ///
    /// Three things are deliberately outside it. `entity_id` is the output.
    /// `parent` is always empty for a genesis, and the tag plus the body shape
    /// already carry that fact. `collection` is envelope attribution, not
    /// identity — the same exclusion update ids already had, and a standing
    /// ruling for this series.
    ///
    /// What pins the model is therefore the `Membership::Add` inside
    /// `operations`, which IS hashed. The envelope `collection` is routing, and
    /// the two are compared only on the two commit funnels
    /// (`Node::commit_remote_transaction` and `Context::commit_local_trx`, both
    /// through `check_membership_admissibility`). The applier paths take the
    /// collection from the envelope and do not compare it: a correctly derived,
    /// structurally valid genesis arriving as a subscription update or an event
    /// bridge materializes its entity in whatever collection the envelope
    /// names. Cross-checking the envelope against the event's own membership on
    /// those paths is identity-02's.
    ///
    /// Binding `system` gives one-id-one-system a hash-level backstop: the
    /// same content under a different system root is a different entity, so
    /// an id cannot be replayed into a foreign system. `None` is the system
    /// root's own genesis, and the Option encoding keeps it distinct from any
    /// `Some` without needing a third tag.
    pub fn from_genesis_parts(
        system: &Option<EntityId>,
        nonce: &[u8; 32],
        timestamp: u64,
        author: &AuthorId,
        operations: &OperationSet,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(GENESIS_TAG);
        hasher.update(bincode::serialize(system).unwrap());
        hasher.update(bincode::serialize(nonce).unwrap());
        hasher.update(bincode::serialize(&timestamp).unwrap());
        hasher.update(bincode::serialize(author).unwrap());
        hasher.update(bincode::serialize(operations).unwrap());
        Self(hasher.finalize().into())
    }

    /// The id of an update event: `SHA-256(EVENT_TAG || bincode(entity_id,
    /// author, nonce, timestamp, operations, parent))`. The collection stays
    /// outside identity, as it always has. The nonce is what makes an event
    /// id unguessable to anyone who was not sent the event.
    pub fn from_update_parts(
        entity_id: &EntityId,
        author: &AuthorId,
        nonce: &[u8; 32],
        timestamp: u64,
        operations: &OperationSet,
        parent: &Clock,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(EVENT_TAG);
        hasher.update(bincode::serialize(entity_id).unwrap());
        hasher.update(bincode::serialize(author).unwrap());
        hasher.update(bincode::serialize(nonce).unwrap());
        hasher.update(bincode::serialize(&timestamp).unwrap());
        hasher.update(bincode::serialize(operations).unwrap());
        hasher.update(bincode::serialize(parent).unwrap());
        Self(hasher.finalize().into())
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
    pub collection: CollectionId,
    /// For a genesis this EQUALS the id derived from the body (checked by
    /// [`Event::validate_structure`]); for an update it names the entity the
    /// event extends.
    pub entity_id: EntityId,
    /// The set of concurrent events (usually only one) which is the precursor
    /// of this event. Empty if and only if the body is a genesis.
    pub parent: Clock,
    pub body: EventBody,
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
        /// The entity's initial property values and its membership.
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

/// An event whose shape contradicts itself. Refused before any staging or
/// storage at the seams that check it.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum EventStructureError {
    #[error("genesis event carries a non-empty parent clock")]
    GenesisWithParent,
    #[error("update event carries an empty parent clock")]
    UpdateWithoutParent,
    #[error("genesis id does not match the claimed entity id (event {event}, claimed {claimed})")]
    GenesisIdMismatch { event: EventId, claimed: EntityId },
}

impl Event {
    /// Mint an entity: freeze `operations` into a genesis, draw its nonce and
    /// timestamp, and derive the entity id from the whole of it. The id is
    /// the return value's `entity_id`.
    pub fn genesis(collection: CollectionId, system: Option<EntityId>, author: AuthorId, operations: OperationSet) -> Self {
        let nonce = draw_nonce();
        let timestamp = crate::time::unix_ms_now();
        let entity_id: EntityId = EventId::from_genesis_parts(&system, &nonce, timestamp, &author, &operations).into();
        Event { collection, entity_id, parent: Clock::default(), body: EventBody::Genesis { system, nonce, timestamp, author, operations } }
    }

    /// Mint an update extending `parent`, drawing its nonce and timestamp.
    pub fn update(collection: CollectionId, entity_id: EntityId, parent: Clock, author: AuthorId, operations: OperationSet) -> Self {
        Event {
            collection,
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

impl From<(EntityId, CollectionId, EventFragment)> for Attested<Event> {
    fn from(value: (EntityId, CollectionId, EventFragment)) -> Self {
        let event = Event { entity_id: value.0, collection: value.1, body: value.2.body, parent: value.2.parent };
        Attested { payload: event, attestations: value.2.attestations }
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
impl From<(EntityId, CollectionId, StateFragment)> for Attested<EntityState> {
    fn from(value: (EntityId, CollectionId, StateFragment)) -> Self {
        let entity_state = EntityState { entity_id: value.0, collection: value.1, state: value.2.state };
        Attested { payload: entity_state, attestations: value.2.attestations }
    }
}

/// Ordered top-level mutations that make up one event's content-addressed
/// payload.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct OperationSet(pub Vec<Operation>);

impl std::fmt::Display for OperationSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OperationSet({})",
            self.0
                .iter()
                .map(|operation| match operation {
                    Operation::Backend { backend, operations } => {
                        format!("{} => {}b", backend, operations.iter().map(|operation| operation.diff.len()).sum::<usize>())
                    }
                    Operation::Membership(Membership::Add(model)) => format!("membership +{model}"),
                })
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

impl std::ops::Deref for OperationSet {
    type Target = [Operation];
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl OperationSet {
    /// Build the event-level operation sequence for backend-generated diffs.
    ///
    /// The input map's stable ordering gives locally generated events a
    /// deterministic operation order.
    pub fn from_backends(backends: BTreeMap<String, Vec<BackendOperation>>) -> Self {
        Self(backends.into_iter().map(|(backend, operations)| Operation::Backend { backend, operations }).collect())
    }

    /// Append one event-level operation.
    pub fn push(&mut self, operation: Operation) { self.0.push(operation); }

    /// Iterate backend operation batches in event order.
    pub fn backends(&self) -> impl Iterator<Item = (&str, &[BackendOperation])> {
        self.0.iter().filter_map(|operation| match operation {
            Operation::Backend { backend, operations } => Some((backend.as_str(), operations.as_slice())),
            Operation::Membership(_) => None,
        })
    }

    /// Iterate all backend diffs addressed to `backend`, preserving event
    /// order even if an untrusted event contains more than one batch.
    pub fn backend_operations<'a>(&'a self, backend: &'a str) -> impl Iterator<Item = &'a BackendOperation> {
        self.0.iter().flat_map(move |operation| match operation {
            Operation::Backend { backend: candidate, operations } if candidate == backend => operations.iter(),
            Operation::Backend { .. } | Operation::Membership(_) => [].iter(),
        })
    }

    /// Iterate explicit membership mutations in event order.
    pub fn memberships(&self) -> impl Iterator<Item = &Membership> {
        self.0.iter().filter_map(|operation| match operation {
            Operation::Membership(membership) => Some(membership),
            Operation::Backend { .. } => None,
        })
    }
}

/// A top-level mutation carried by an entity event.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Operation {
    /// Apply opaque diffs to one property backend.
    Backend {
        /// Registered property backend name.
        backend: String,
        /// Ordered opaque operations understood by that backend.
        operations: Vec<BackendOperation>,
    },
    /// Change the entity's explicit model-backed membership state.
    Membership(Membership),
}

/// An explicit mutation of an entity's model-backed membership state: which
/// model an ENTITY belongs to, asserted in the attested event stream (the
/// sole authority for that fact). Distinct from the catalog's
/// model-property memberships, which are property-to-model records
/// (`_ankurah_model_property`).
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum Membership {
    /// Add the model-backed membership.
    ///
    /// An ordinary operation on any event; the commit funnels currently
    /// admit it only on an entity's first event, exactly one there.
    Add(ModelId),
}

/// An opaque operation generated and interpreted by a named property backend.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub struct BackendOperation {
    /// Opaque diff interpreted by the selected property backend.
    pub diff: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct EntityState {
    pub entity_id: EntityId,
    pub collection: CollectionId,
    pub state: State,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct State {
    /// The current accumulated state of the entity inclusive of all events up to this point
    pub state_buffers: StateBuffers,
    /// Model-backed memberships established by this entity's causal history.
    ///
    /// The commit funnels currently admit exactly one membership, on the
    /// entity's first event, and no later membership mutations.
    pub memberships: BTreeSet<ModelId>,
    /// The set of concurrent events (usually only one) which have been applied to the entity state above
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
            "Event({} {}/{} {}{} {})",
            self.id().to_base64_short(),
            self.collection,
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

impl Attested<Event> {
    pub fn collection(&self) -> &CollectionId { &self.payload.collection }
}

impl From<Event> for Attested<Event> {
    fn from(val: Event) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl From<EntityState> for Attested<EntityState> {
    fn from(val: EntityState) -> Self { Attested { payload: val, attestations: AttestationSet::default() } }
}

impl Attested<EntityState> {
    pub fn to_parts(self) -> (EntityId, CollectionId, StateFragment) {
        (self.payload.entity_id, self.payload.collection, StateFragment { state: self.payload.state, attestations: self.attestations })
    }
    pub fn from_parts(entity_id: EntityId, collection: CollectionId, fragment: StateFragment) -> Self {
        Self { payload: EntityState { entity_id, collection, state: fragment.state }, attestations: fragment.attestations }
    }
}

impl Attested<Event> {
    pub fn from_parts(entity_id: EntityId, collection: CollectionId, frag: EventFragment) -> Self {
        Self { payload: Event { entity_id, collection, body: frag.body, parent: frag.parent }, attestations: frag.attestations }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let parent = Clock::new([EventId::from_bytes([5u8; 32])]);
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
        let parent = Clock::new([EventId::from_bytes([5u8; 32])]);
        assert_ne!(
            EventId::from_update_parts(&entity_id, &AuthorId::Unknown, &nonce, timestamp, &operations, &parent),
            EventId::from_update_parts(&entity_id, &author, &nonce, timestamp, &operations, &parent)
        );
    }

    #[test]
    fn a_minted_genesis_names_the_entity_its_content_derives() {
        let event = Event::genesis("pet".into(), Some(EntityId::from_bytes([9u8; 32])), AuthorId::Unknown, operations());
        assert!(event.is_entity_create());
        assert!(event.parent.is_empty());
        assert_eq!(EntityId::from(event.id()), event.entity_id);
        event.validate_structure().expect("a freshly minted genesis is well formed");

        // Two create calls draw two nonces: identical payloads are still two
        // distinct entities.
        let again = Event::genesis("pet".into(), Some(EntityId::from_bytes([9u8; 32])), AuthorId::Unknown, operations());
        assert_ne!(event.entity_id, again.entity_id);
        assert_ne!(event.nonce(), again.nonce());
    }

    #[test]
    fn structural_validation_refuses_contradictory_shapes() {
        // A genesis that claims some other entity's id.
        let mut genesis = Event::genesis("pet".into(), None, AuthorId::Unknown, operations());
        genesis.entity_id = EntityId::from_bytes([0xABu8; 32]);
        assert!(matches!(genesis.validate_structure(), Err(EventStructureError::GenesisIdMismatch { .. })));

        // A genesis with a parent clock.
        let mut genesis = Event::genesis("pet".into(), None, AuthorId::Unknown, operations());
        genesis.parent = Clock::new([EventId::from_bytes([1u8; 32])]);
        assert_eq!(genesis.validate_structure(), Err(EventStructureError::GenesisWithParent));

        // An update with no parent: parent is empty if and only if genesis.
        let update = Event::update("pet".into(), EntityId::from_bytes([7u8; 32]), Clock::default(), AuthorId::Unknown, operations());
        assert_eq!(update.validate_structure(), Err(EventStructureError::UpdateWithoutParent));

        let update = Event::update(
            "pet".into(),
            EntityId::from_bytes([7u8; 32]),
            Clock::new([EventId::from_bytes([1u8; 32])]),
            AuthorId::Unknown,
            operations(),
        );
        update.validate_structure().expect("a parented update is well formed");
    }
}

#[cfg(test)]
mod operation_wire_tests {
    use super::*;
    use ankurah_core_types::SystemModel;

    /// Variant order is part of the bincode contract for event payloads:
    /// Backend = 0, Membership = 1, and Membership::Add = 0. Payload
    /// encodings inside the variants are pinned by ankurah-core-types' own
    /// golden tests.
    #[test]
    fn operation_variant_order_is_pinned() {
        let backend =
            bincode::serialize(&Operation::Backend { backend: "lww".to_owned(), operations: vec![BackendOperation { diff: vec![7] }] })
                .unwrap();
        assert_eq!(&backend[..4], [0, 0, 0, 0], "Operation::Backend must encode as variant 0");

        let membership = bincode::serialize(&Operation::Membership(Membership::Add(ModelId::System(SystemModel::Model)))).unwrap();
        assert_eq!(&membership[..4], [1, 0, 0, 0], "Operation::Membership must encode as variant 1");
        assert_eq!(&membership[4..8], [0, 0, 0, 0], "Membership::Add must encode as variant 0");

        for bytes in [backend, membership] {
            let round: Operation = bincode::deserialize(&bytes).unwrap();
            assert_eq!(bincode::serialize(&round).unwrap(), bytes);
        }
    }
}
