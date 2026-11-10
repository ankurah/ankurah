use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use ankurah_core_types::ModelId;

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
    Add(ModelId),
}

/// An opaque operation generated and interpreted by a named property backend.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub struct BackendOperation {
    /// Opaque diff interpreted by the selected property backend.
    pub diff: Vec<u8>,
}

#[cfg(test)]
mod tests {
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
