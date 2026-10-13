//! Snapshot-bound state transfer shared by Fetch and SubscribeQuery.
//!
//! The initial response carries metadata and frame zero. Later frames are pushed
//! through NodeUpdate, correlated with the originating request and comparison.
//! ContinueState acknowledges a processed prefix and supplies bounded answers;
//! it returns Success rather than carrying another frame. Responders choose
//! byte/work/window limits beneath the protocol's hard resource caps.
//!
//! Complete commits the captured result and a standing subscription delivery
//! route. The requester becomes ready after causal repair and local activation,
//! then sends Applied without waiting for its reply. Live frames remain queued
//! until Applied arrives and then continue on the same numbered stream.
//! Snapshot cleanup never silently revokes that standing route: subsequent
//! failure requires RestartRequired or disconnect. Abandoned cancels the exact
//! generation; Applied releases snapshot resources and permits live delivery.
//!
//! Frame acknowledgments are flow control, not global application claims.
//! Peers, connection generations, credentials, snapshots, spec and query version
//! stay bound throughout. Unknown or superseded streams cannot be adopted.

pub use ankurah_digest_tree::{AddressRange, TreeHead, TreeSpec};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

mod compact_initial_page;

use crate::{
    Attested, Clock, EntityDelta, EntityDeltaChunk, EntityId, EntityState, KnownEntity, MembershipChange, QueryId, SubscriptionUpdateItem,
};

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

/// Bounded knowledge of the requester's materialized query result.
/// Regions are disjoint and exact. Omitted regions are unknown, not empty.
/// No region asserts absence of entity state held outside this query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownState {
    pub spec: TreeSpec,
    pub regions: Vec<KnownRegion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnownRegion {
    Summary(TreeHead),
    /// Exhaustive for its bounds; an empty page explicitly describes an empty range.
    /// Replies need only describe changes; Complete confirms unchanged members too.
    Leaves(#[serde(with = "compact_initial_page")] LeafPage),
}

/// Initial response for the full exact materialized selection; frame.sequence must be zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateReply {
    pub spec: TreeSpec,
    pub frame: StateFrame,
}

/// Immutable within one connection generation and comparison.
/// Frame zero is in StateReply; subsequent frames arrive only as NodeUpdate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateFrame {
    pub comparison: ComparisonId,
    pub sequence: u64,
    pub body: StateFrameBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateFrameBody {
    State {
        items: Vec<StateItem>,
        progress: ComparisonProgress,
    },
    /// Subscription only, after Applied. Query id/version are bound by the stream.
    /// All live updates for this generation use this same sequence and byte window.
    /// ContinueState credits Live after application; a stalled stream may require restart.
    Live(Vec<SubscriptionUpdateItem>),
    /// Same entity encoding/assembly as RepairChunk. Relevance stays fixed for all
    /// fragments and is applied only with the complete validated delta. No other
    /// Live item interleaves; the relevance list is independently bounded.
    LiveChunk {
        chunk: EntityDeltaChunk,
        predicate_relevance: Vec<(QueryId, MembershipChange)>,
    },
    /// Mark this generation unavailable and restart; never claim completion.
    RestartRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonProgress {
    /// Further work or questions remain; the responder pushes while credit permits.
    /// On late repair frames this does not revoke a prior Complete handoff.
    Continue,
    /// Initial offers have been sent and the subscription route is established.
    /// This does not claim application: late NeedRepair is allowed. Readiness
    /// requires actual target coverage, no pending repair, and local activation.
    Complete,
}

/// A reply may mix refinement, inventory questions and entity repairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateItem {
    /// Compare this region locally and answer with a head for the same bounds.
    /// The responder may offer several depths; overlap must not be counted twice.
    Summary(TreeHead),
    /// The responder's complete leaf inventory within this page's bounds.
    Leaves(LeafPage),
    /// Enumerate the requester's leaves within these bounds, in bounded pages.
    NeedLeaves(AddressRange),
    /// Apply through the normal attestation and causal validation path.
    /// `target` is the captured sender head covered by this delta. Application
    /// may retain newer or concurrent local state; sending is not acknowledgment.
    Repair { target: Clock, delta: EntityDelta },
    /// A fragment of a repair too large for one reply. Target stays fixed for its
    /// assembly. Complete cannot leave a partial repair; late requested repairs
    /// may span Continue frames, but every required assembly must finish before Applied.
    RepairChunk { target: Clock, chunk: EntityDeltaChunk },
    /// Required causal repair cannot be supplied within policy/history/work limits.
    /// Fail this Fetch or initialization; do not report ready with a known gap.
    RepairUnavailable { entity_id: EntityId, target: Clock },
    /// Exhaustive captured membership with inline snapshots, avoiding a separate
    /// leaf inventory and duplicate repair targets for a cold range.
    Snapshots(SnapshotPage),
}

/// Answers for any subset of the outstanding regions/entities in a comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateAnswer {
    /// The requester's digest over the exact bounds named by the head.
    Summary(TreeHead),
    Leaves(LeafPage),
    /// An already offered snapshot did not incorporate target into local state.
    /// Only the exact offered entity/target is admissible, including after Complete.
    /// Answer with bounded connecting events or RepairUnavailable, never the same snapshot.
    NeedRepair {
        entity_id: EntityId,
        target: Clock,
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

/// An exhaustive range page in canonical address order, including empty gaps.
/// Derive keys and target clocks from these captured snapshots before applying
/// them: a newer or concurrent local state may have a different key or head.
/// Normal attestation, causal validation and target coverage still apply.
/// Oversized snapshots use Leaves plus RepairChunk instead of a partial page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPage {
    pub bounds: AddressRange,
    pub states: Vec<Attested<EntityState>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownLeaf {
    pub key: Vec<u8>,
    /// The actual clock avoids another round trip when constructing an EventBridge.
    pub entity: KnownEntity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOutcome {
    /// After Complete, all offered targets are covered and local result activation
    /// succeeded. Release the snapshot and drain the standing Live stream.
    /// processed_through covers every initial/repair frame, including late repairs
    /// after Complete. It returns frame credit (including frame 0) as well.
    /// The requester need not await the reply before reporting readiness.
    Applied { processed_through: u64 },
    /// Cancel this exact stream/query generation, including after Complete.
    /// Never remove a replacement query generation.
    Abandoned,
}
