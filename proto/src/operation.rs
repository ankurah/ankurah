//! The operations an event carries: decoded, as transactions build them and
//! property backends apply them, and carried, as the bytes their author produced.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    domain_tag::DomainTag,
    encoding::{self, UndecodableVariant},
};
use ankurah_core_types::ModelId;

/// Ordered top-level mutations that make up one event's content, decoded: what
/// transactions build and property backends apply.
#[derive(Debug, Clone, PartialEq, Default)]
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

/// The variant index of [`Operation::Membership`], whose [`Membership`] a later build may extend.
const MEMBERSHIP_INDEX: u32 = 1;

/// A top-level mutation carried by an entity event. An event carries each one as
/// its encoding, a [`CarriedOperation`].
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
    Add(ModelId),
    // Index 1 stays free for the variant that removes a membership.
}

/// An opaque operation generated and interpreted by a named property backend.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub struct BackendOperation {
    /// Opaque diff interpreted by the selected property backend.
    pub diff: Vec<u8>,
}

/// One operation as an event carries it: the exact bytes its author produced, or
/// only their digest once the bytes are erased. An event's id covers each
/// operation's digest, so a node that cannot read an operation still derives
/// the id, and erasing the bytes changes no id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CarriedOperation {
    /// The encoding of one [`Operation`], exactly as its author produced it.
    Bytes(Vec<u8>),
    /// The digest of bytes that were erased.
    Erased([u8; 32]),
}

impl CarriedOperation {
    /// `operation` as its author carries it.
    pub fn encode(operation: &Operation) -> Self { Self::Bytes(encoding::encode(operation)) }

    /// The digest an event's id covers for this operation: SHA-256 over
    /// [`DomainTag::Operation`] and the bytes, or what erased bytes left.
    pub fn digest(&self) -> [u8; 32] {
        match self {
            Self::Bytes(bytes) => Sha256::new().chain_update(DomainTag::Operation.encoded()).chain_update(bytes).finalize().into(),
            Self::Erased(digest) => *digest,
        }
    }

    /// The operation these bytes encode.
    pub fn decode(&self) -> Result<Operation, UnreadableOperation> {
        match self {
            Self::Bytes(bytes) => encoding::decode_variant(bytes).map_err(|undecodable| match undecodable {
                UndecodableVariant::Unknown { index } => UnreadableOperation::UnknownKind { indices: vec![index] },
                UndecodableVariant::Malformed(error) => match encoding::unknown_variant_within::<Membership>(bytes, MEMBERSHIP_INDEX) {
                    Some(membership) => UnreadableOperation::UnknownKind { indices: vec![MEMBERSHIP_INDEX, membership] },
                    None => UnreadableOperation::Malformed(error.to_string()),
                },
            }),
            Self::Erased(_) => Err(UnreadableOperation::Erased),
        }
    }
}

/// Why this build cannot read an operation an event carries.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnreadableOperation {
    /// Bytes of an operation kind a later build added: a variant of [`Operation`], or of the
    /// [`Membership`] it holds, that this build does not have. `indices` are the variant indices
    /// that name the kind, the operation's first: `[2]` names an operation kind, `[1, 1]` a
    /// membership kind.
    #[error("an operation of kind {indices:?}, which this build does not know")]
    UnknownKind { indices: Vec<u32> },
    /// Bytes that do not decode as the operation kind they name, as this build reads that kind.
    #[error("an operation whose bytes do not decode: {0}")]
    Malformed(String),
    /// Only the digest of the operation's bytes remains.
    #[error("an operation whose bytes were erased")]
    Erased,
}

/// The operations one event carries, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CarriedOperations(pub Vec<CarriedOperation>);

impl CarriedOperations {
    /// Each of `operations` as its author carries it, in order.
    pub fn encode(operations: &OperationSet) -> Self { Self(operations.iter().map(CarriedOperation::encode).collect()) }

    /// The operations, decoded in order; the first this build cannot read fails the whole.
    pub fn decode(&self) -> Result<OperationSet, UnreadableOperation> {
        self.0.iter().map(CarriedOperation::decode).collect::<Result<_, _>>().map(OperationSet)
    }

    /// Each operation's digest, in order: what an event's id covers in place of the operations.
    pub(crate) fn digests(&self) -> Vec<[u8; 32]> { self.0.iter().map(CarriedOperation::digest).collect() }
}

impl std::fmt::Display for CarriedOperations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.decode() {
            Ok(operations) => write!(f, "{operations}"),
            Err(error) => write!(f, "CarriedOperations({} operations, {error})", self.0.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{decode_exact, encode};
    use ankurah_core_types::SystemModel;

    fn membership() -> Operation { Operation::Membership(Membership::Add(ModelId::System(SystemModel::Model))) }

    /// The operation vectors of specs/protocol/bytes.md section 5: each operation's bytes and its digest.
    #[test]
    fn operation_bytes_and_digests_match_the_byte_specification_vectors() {
        let backend = Operation::Backend { backend: "lww".to_owned(), operations: vec![BackendOperation { diff: vec![1, 2, 3] }] };
        for (operation, bytes, digest) in [
            (membership(), "01000000000000000100000000000000", "76ef9eee987e3dc3a09adbf20c51740e948216e9a8f79bdd1994deffedf5fd98"),
            (
                backend,
                "0000000003000000000000006c777701000000000000000300000000000000010203",
                "75307fbd9932f311bc6799c89a2051231dc6e731410b47a0096484352f219db5",
            ),
        ] {
            let carried = CarriedOperation::encode(&operation);
            assert_eq!(carried, CarriedOperation::Bytes(hex::decode(bytes).unwrap()));
            assert_eq!(hex::encode(carried.digest()), digest);
            assert_eq!(CarriedOperation::Erased(carried.digest()).digest(), carried.digest(), "an erased operation keeps its digest");
        }
    }

    #[test]
    fn a_carried_operation_decodes_to_the_operation_it_encodes() {
        let backend = Operation::Backend { backend: "lww".to_owned(), operations: vec![BackendOperation { diff: vec![1, 2, 3] }] };
        for operation in [backend, membership()] {
            assert_eq!(CarriedOperation::encode(&operation).decode(), Ok(operation));
        }
    }

    #[test]
    fn erased_bytes_and_bytes_of_an_unknown_kind_cannot_be_read() {
        let erased = CarriedOperation::Erased(CarriedOperation::encode(&membership()).digest());
        assert_eq!(erased.decode(), Err(UnreadableOperation::Erased));
        // Variant index 2, which no build of `Operation` defines yet.
        let unknown = CarriedOperation::Bytes(vec![2, 0, 0, 0, 7]);
        assert_eq!(unknown.decode(), Err(UnreadableOperation::UnknownKind { indices: vec![2] }));
        let carried = CarriedOperations(vec![CarriedOperation::encode(&membership()), unknown]);
        assert_eq!(
            carried.decode(),
            Err(UnreadableOperation::UnknownKind { indices: vec![2] }),
            "one unreadable operation fails the event's"
        );
    }

    /// Bytes that name a kind this build knows, or name none, are malformed when they do not decode.
    #[test]
    fn bytes_that_do_not_decode_as_a_known_kind_are_malformed() {
        for bytes in [vec![], vec![1, 0, 0], vec![0, 0, 0, 0, 7], vec![1, 0, 0, 0, 0, 0, 0, 0]] {
            assert!(matches!(CarriedOperation::Bytes(bytes.clone()).decode(), Err(UnreadableOperation::Malformed(_))), "{bytes:?}");
        }
    }

    /// `Membership` index 1 is reserved for the variant that removes a membership, a kind a later
    /// build adds inside the membership operation this build knows.
    #[test]
    fn a_membership_of_the_reserved_removal_index_is_of_a_kind_this_build_does_not_know() {
        let CarriedOperation::Bytes(mut bytes) = CarriedOperation::encode(&membership()) else { unreachable!("encoding yields bytes") };
        // The operation's variant index, then the membership's.
        bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
        assert_eq!(CarriedOperation::Bytes(bytes).decode(), Err(UnreadableOperation::UnknownKind { indices: vec![1, 1] }));
    }

    #[test]
    fn bytes_left_over_after_the_operation_make_it_unreadable() {
        let CarriedOperation::Bytes(mut bytes) = CarriedOperation::encode(&membership()) else { unreachable!("encoding yields bytes") };
        bytes.push(0);
        assert!(matches!(CarriedOperation::Bytes(bytes).decode(), Err(UnreadableOperation::Malformed(_))));
    }

    /// Variant order is part of the bincode contract for event payloads:
    /// Backend = 0, Membership = 1, Membership::Add = 0, and for a carried
    /// operation Bytes = 0, Erased = 1. Payload encodings inside the variants
    /// are pinned by ankurah-core-types' own golden tests.
    #[test]
    fn operation_variant_order_is_pinned() {
        let backend = encode(&Operation::Backend { backend: "lww".to_owned(), operations: vec![BackendOperation { diff: vec![7] }] });
        assert_eq!(&backend[..4], [0, 0, 0, 0], "Operation::Backend must encode as variant 0");

        let membership = encode(&Operation::Membership(Membership::Add(ModelId::System(SystemModel::Model))));
        assert_eq!(&membership[..4], [1, 0, 0, 0], "Operation::Membership must encode as variant 1");
        assert_eq!(&membership[4..8], [0, 0, 0, 0], "Membership::Add must encode as variant 0");

        for bytes in [backend, membership] {
            let round: Operation = decode_exact(&bytes).unwrap();
            assert_eq!(encode(&round), bytes);
        }

        assert_eq!(encode(&CarriedOperation::Bytes(vec![]))[..4], [0, 0, 0, 0], "CarriedOperation::Bytes");
        assert_eq!(encode(&CarriedOperation::Erased([0; 32]))[..4], [1, 0, 0, 0], "CarriedOperation::Erased");
    }
}
