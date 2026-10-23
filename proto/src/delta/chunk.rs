use serde::{Deserialize, Serialize};

use crate::EntityId;

/// A fragment of one bincode-encoded DeltaContent, shared by repair and live delivery.
/// Reassemble under the stream and its typed wrapper, then validate and apply once.
/// Offsets start at zero, are contiguous, and stop at `last`; total assembly is capped.
/// The entity and wrapper metadata stay fixed. Assemblies are scoped by their
/// round or live publication/repair wrapper, so baseline and live data for an
/// entity may overlap. Copying bytes returns wire credit, not an application claim.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityDeltaChunk {
    pub entity_id: EntityId,
    pub offset: u64,
    pub bytes: bytes::Bytes,
    pub last: bool,
}
