//! Snapshot-bound state comparison shared by Fetch and SubscribeQuery.
//!
//! An initial request offers a spec and summaries of the requester's state.
//! The responder establishes the authorized result scope and a comparison,
//! then supplies summaries, leaf details and repairs within its own byte and
//! work limits. Follow-ups may compare many regions at different depths.
//!
//! A comparison binds both peers' snapshots, the spec, the authorized Selection,
//! the peer and (for subscriptions) query ID and version. Implementations must
//! retain those snapshots or invalidate the comparison; they must not silently
//! mix revisions. Neither a spec nor a comparison ID grants read permission.
//!
//! Requests within a comparison are sequential. A transport retry reuses the
//! same NodeRequest ID and body; the responder replays that reply rather than
//! advancing twice. If it can no longer replay consistently, it returns
//! ComparisonUnavailable. A fresh follow-up acknowledges receipt of the prior
//! page; only CloseComparison with Applied acknowledges successful application.

pub use ankurah_digest_tree::{AddressRange, TreeHead, TreeSpec};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{Clock, EntityDelta, EntityId, KnownEntity};

/// A responder-issued handle for one comparison, scoped to the requesting peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ComparisonId(Ulid);

impl ComparisonId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl Default for ComparisonId {
    fn default() -> Self { Self::new() }
}

impl std::fmt::Display for ComparisonId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "C-{}", self.0) }
}

/// State the requester can summarize, possibly using a scope of a broader tree.
///
/// Heads use Ristretto255 and may include several depths. Uncovered regions
/// are unknown, not empty. Even a zero-count head says nothing about entities
/// this node holds outside the summarized result. The serving peer validates
/// key types, admission compatibility and authorized bounds before comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownState {
    pub spec: TreeSpec,
    pub heads: Vec<TreeHead>,
}

/// Initial Fetch or SubscribeQuery reply. Later pages reuse this descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateReply {
    /// The spec used to interpret all heads, leaf keys and bounds in this comparison.
    /// An offered spec must be compatible before its summaries can be used.
    pub spec: TreeSpec,
    /// The responder's entire result scope at the captured snapshot, including
    /// its ordered limit. Empty means an empty result; it does not delete entities.
    /// These bounds are interpreted with `spec` and the original Selection.
    pub scope: Vec<AddressRange>,
    pub page: ComparisonReply,
}

/// One bounded reply, potentially spanning many branches and recursion depths.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReply {
    pub comparison: ComparisonId,
    pub items: Vec<StateItem>,
    pub progress: ComparisonProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonProgress {
    /// Answer the outstanding questions and continue. An empty answer list
    /// resumes work the responder deferred under its own budget. Outstanding
    /// questions and omitted regions remain pending across replies.
    Continue,
    /// All result membership and repair data has been supplied. This is not
    /// an acknowledgment that the requester has applied it; CloseComparison
    /// with Applied provides that acknowledgment after successful application.
    Complete,
}

/// A reply may mix equality, refinement, questions and entity repairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateItem {
    /// Both captured snapshots agree on precisely these bounds.
    Equal(AddressRange),
    /// Compare this region locally and answer with a head for the same bounds.
    /// The responder may offer several depths; overlap must not be counted twice.
    Summary(TreeHead),
    /// The responder's complete leaf inventory within this page's bounds.
    Leaves(LeafPage),
    /// Enumerate the requester's leaves within these bounds, in bounded pages.
    NeedLeaves(AddressRange),
    /// Ask for actual clocks, including for entities absent from the query tree
    /// but potentially present elsewhere on the requester.
    NeedHeads(Vec<EntityId>),
    /// Apply through the normal attestation and causal validation path.
    /// `target` is the captured sender head covered by this delta. Application
    /// may retain newer or concurrent local state; sending is not acknowledgment.
    Repair { target: Clock, delta: EntityDelta },
    /// A fragment of a repair too large for one reply. Reassemble and validate
    /// the complete DeltaContent before applying it, using the same path as Repair.
    RepairChunk(RepairChunk),
}

/// Fragments of one bincode-encoded DeltaContent under the current wire version.
///
/// Within a comparison, an entity has one repair target. Chunks start at offset
/// zero and are contiguous; the entity and target stay fixed until `last`.
/// Different entities may interleave. A Complete reply cannot leave a partial
/// repair. Receivers must bound assembly and reject gaps, overlaps and changes
/// of target, rather than applying an incomplete event bridge or snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairChunk {
    pub entity_id: EntityId,
    pub target: Clock,
    pub offset: u64,
    pub bytes: bytes::Bytes,
    pub last: bool,
}

/// Answers for any subset of the outstanding regions/entities in a comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateAnswer {
    /// The requester's digest over the exact bounds named by the head.
    Summary(TreeHead),
    Leaves(LeafPage),
    /// `unknown` explicitly identifies entities for which no clock is held.
    /// Omitted entity IDs remain unanswered, not unknown.
    Heads {
        known: Vec<KnownEntity>,
        unknown: Vec<EntityId>,
    },
}

/// An exhaustive inventory over exactly `bounds`, including gaps between leaves.
///
/// A large requested region is split into disjoint pages in address order. Each
/// page completely describes its bounds; the remaining bounds stay pending.
/// This is a wire page, separate from the tree crate's local snapshot cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafPage {
    pub bounds: AddressRange,
    /// Strictly increasing by canonical key bytes followed by entity ID.
    pub leaves: Vec<KnownLeaf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownLeaf {
    pub key: Vec<u8>,
    /// The actual clock avoids another round trip when constructing an EventBridge.
    pub entity: KnownEntity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOutcome {
    /// Sent only after Complete and successful application of all supplied data.
    Applied,
    /// Release the comparison without claiming any successful application.
    Abandoned,
}
