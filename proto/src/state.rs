//! Bounded comparison rounds within a standing query's state stream.
//!
//! The initial response carries metadata and frame zero. Later frames are pushed
//! through NodeUpdate, correlated with the originating request and comparison.
//! ContinueState acknowledges a processed prefix and supplies bounded answers;
//! it returns Success rather than carrying another frame. Responders choose
//! byte/work/window limits beneath the protocol's hard resource caps.
//!
//! Live publications may interleave with comparison pages. Each round describes
//! one frozen source revision; later membership decisions must survive its
//! installation. Complete ends enumeration, while Applied confirms actual
//! coverage of that finite round and releases its snapshot. Neither gates live
//! delivery. A standing stream may compare another frozen offer in a later round.
//!
//! Frame acknowledgments are flow control, not global application claims.
//! Peers, connection generations, credentials, snapshots, spec and query version
//! stay bound throughout. Unknown or superseded streams cannot be adopted.

pub use ankurah_digest_tree::{TreeSpan, TreeSpanDigest, TreeSpec};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

mod compact_initial_page;

use crate::{Attested, Clock, DeltaContent, EntityDelta, EntityDeltaChunk, EntityHead, EntityId, EntityState};

/// A responder-issued state stream handle, scoped to one peer connection/query generation.
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
/// Regions are disjoint and exact. Omitted regions are unknown, not empty;
/// an empty region list supplies no result knowledge but still specifies the tree.
/// No region asserts absence of entity state held outside this query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownState {
    pub spec: TreeSpec,
    pub regions: Vec<KnownRegion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KnownRegion {
    Summary(TreeSpanDigest),
    /// All entity heads within these bounds; empty means the range is empty.
    /// An initial inventory uses the request budget, independently of follow-up
    /// page limits. Replies need only describe changes; Complete confirms
    /// unchanged members too.
    Heads(#[serde(with = "compact_initial_page")] TreeSpanInventory),
}

/// One captured query version. Revisions order publications in this stream,
/// not entity histories or other peers. Round numbers start at zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRound {
    pub number: u64,
    pub revision: u64,
}

/// Initial response for the full exact materialized selection. Its round begins
/// before frame zero and every subsequent live publication in this stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateReply {
    pub spec: TreeSpec,
    pub round: StateRound,
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
    /// Accept the requester's already-frozen offer for the next round. This marker
    /// precedes its questions/pages and every live publication after its revision.
    BeginRound(StateRound),
    State {
        round: u64,
        items: Vec<StateItem>,
        progress: ComparisonProgress,
    },
    /// Absolute source membership plus optional causal data. A publication may
    /// span frames. After `complete` and actual target coverage, the receiver
    /// publishes its membership and accepted events together, in revision order.
    Live {
        revision: u64,
        items: Vec<QueryChange>,
        complete: bool,
    },
    /// Same entity encoding as RepairChunk. The membership decision stays fixed
    /// across fragments. `complete` requires the final fragment and finishes this
    /// whole publication, not merely this entity. Comparison frames may interleave.
    LiveChunk {
        revision: u64,
        member: Option<QueryMember>,
        chunk: EntityDeltaChunk,
        complete: bool,
    },
    /// Connecting data for previously offered live targets. Only Repair,
    /// RepairChunk and RepairUnavailable items are permitted; none authors membership.
    LiveRepair {
        items: Vec<StateItem>,
    },
    /// Mark this generation unavailable and restart; never claim completion.
    RestartRequired,
    /// The source cannot prove a gap-free publication history after an uncertain
    /// commit outcome. Recreating this stream cannot repair it; the source node
    /// must be reconstructed before it can serve query state again.
    SourceUnavailable,
}

/// The sender's captured key and target. Keep these separate from the key/head
/// of a newer or concurrently merged local entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMember {
    pub key: Vec<u8>,
    pub head: Clock,
}

/// One absolute membership decision. None means absent, including removals
/// for which the source may not disclose entity data. A Present with no data
/// still requires proof that the local entity covers its target head. An Absent
/// with data must carry a state target; an EventBridge alone has no explicit
/// target and is permitted only with Present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryChange {
    pub entity_id: EntityId,
    pub member: Option<QueryMember>,
    pub delta: Option<DeltaContent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonProgress {
    /// Further work or questions remain; the responder pushes while credit permits.
    /// On late repair frames this does not revoke a prior Complete handoff.
    Continue,
    /// All offers for this finite round have been sent.
    /// This does not claim application: late NeedRepair is allowed. Readiness
    /// requires actual target coverage and local activation. Independently covered
    /// targets no longer need their outstanding repair requests.
    Complete,
}

/// A reply may mix refinement, inventory questions and entity repairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateItem {
    /// Compare this region locally and answer with a digest for the same span.
    /// The responder may offer several depths; overlap must not be counted twice.
    Summary(TreeSpanDigest),
    /// The responder's complete leaf inventory within this page's bounds.
    Leaves(TreeSpanInventory),
    /// Enumerate the requester's leaves within these bounds, in bounded pages.
    NeedLeaves(TreeSpan),
    /// Apply through the normal attestation and causal validation path.
    /// `target` is the captured sender head covered by this delta. Application
    /// may retain newer or concurrent local state; sending is not acknowledgment.
    Repair { target: Clock, delta: EntityDelta },
    /// A fragment of a repair too large for one reply. Target stays fixed for its
    /// assembly. Complete cannot leave a partial repair; late requested repairs
    /// may span Continue frames, but every required assembly must finish before Applied.
    RepairChunk { target: Clock, chunk: EntityDeltaChunk },
    /// Causal repair cannot be supplied within policy/history/work limits.
    /// Fail while this target remains required and uncovered. A retired round or
    /// independently covered target does not become unavailable retroactively.
    RepairUnavailable { entity_id: EntityId, target: Clock },
    /// Exhaustive captured membership with inline snapshots, avoiding a separate
    /// leaf inventory and duplicate repair targets for a cold range.
    Snapshots(SnapshotPage),
}

/// Answers for any subset of the outstanding regions/entities in a comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateAnswer {
    /// The requester's digest over the exact span offered by the responder.
    Summary(TreeSpanDigest),
    Leaves(TreeSpanInventory),
    /// An already offered snapshot did not incorporate target into local state.
    /// Only the exact offered entity/target is admissible, including after Complete.
    /// Answer with bounded connecting events or RepairUnavailable, never the same snapshot.
    NeedRepair {
        entity_id: EntityId,
        target: Clock,
    },
}

/// All keyed entity heads within exactly `bounds`, including empty gaps.
///
/// A large requested span can be split into disjoint inventories in address order.
/// Each inventory completely describes its bounds; the remaining bounds stay pending.
/// This is a wire inventory, separate from the tree crate's local scan page and cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeSpanInventory {
    pub bounds: TreeSpan,
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
    pub bounds: TreeSpan,
    pub states: Vec<Attested<EntityState>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownLeaf {
    /// Canonical property values selected by the TreeSpec's KeySpec. The full
    /// address appends the entity ID; this key is empty for entity-ID-only trees.
    /// It belongs to this tree: another KeySpec can place the same EntityHead
    /// under a different key.
    pub key: Vec<u8>,
    /// The actual clock avoids another round trip when constructing an EventBridge.
    pub entity: EntityHead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOutcome {
    /// After Complete, the round's offered targets are covered and local result
    /// installation succeeded, including live publications through its captured
    /// revision. Later live publications do not hold completion open.
    /// Release only that round's snapshot.
    /// processed_through covers enumeration through Complete and returns frame
    /// credit (including frame 0). Independent live application may already cover
    /// a target whose requested history is still in flight; that work can retire.
    /// Later frames still receive credit but cannot change a retired round.
    /// The requester need not await the reply before reporting readiness.
    Applied { round: u64, processed_through: u64 },
    /// Discard only this round, preserving the standing stream and its live
    /// publications. It may be followed by a new frozen offer and round.
    Cancelled { round: u64 },
    /// Cancel this exact stream/query generation, including after Complete.
    /// Never remove a replacement query generation.
    Abandoned,
}
