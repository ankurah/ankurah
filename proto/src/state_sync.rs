//! Query state synchronization: lets a node bring its copy of a query result up to
//! date with a peer's without resending the entities it already holds.
//!
//! 1. The requester sends Fetch or SubscribeQuery with a `KnownState`: digests of the
//!    key ranges it holds, or the head of every entity in a range.
//! 2. The responder answers with a `StateReply`, whose frame zero begins round zero.
//!    Later frames arrive as `NodeMessage::StateFrame`, numbered by `sequence`.
//! 3. Within a round the responder sends `StateItem`s: digests to compare, head
//!    inventories, requests for the requester's heads, and entity repairs. The
//!    requester replies with `StateFeedback`, returning frame credit and its `StateAnswer`s.
//! 4. Once it has sent everything, the responder marks the round `Complete`. When the
//!    requester's local result covers every repair, it sends `FinishStateRound` with
//!    `Applied`, and the responder releases the round's snapshot.
//! 5. A SubscribeQuery stream stays open: `Live` frames carry each change to the
//!    query's membership in revision order, `RefreshState` begins another round, and
//!    `AbandonStateStream` closes the stream.
//!
//! Both peers enforce the limits in [`limits`]. Byte limits on a message count its
//! work: its encoded bytes plus the bytes its inventories saved by sharing key prefixes.

pub use ankurah_digest_tree::{TreeSpan, TreeSpanDigest, TreeSpec};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

mod inventory;
pub mod limits;
mod validation;
mod work;
pub use inventory::{
    ClockRef, EntityHead, EntityHeadRef, HeadCursor, InventoryError, KeyedEntityHead, KeyedEntityHeadRef, PackedHeads, TreeSpanInventory,
};

use crate::{Clock, DeltaContent, EntityDelta, EntityDeltaChunk, EntityId};

/// Names one state stream. Its responder issues it, and it means nothing outside the
/// connection that opened the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StateStreamId(Ulid);

impl StateStreamId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl Default for StateStreamId {
    fn default() -> Self { Self::new() }
}

impl std::fmt::Display for StateStreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "S-{}", self.0) }
}

/// The requester's account of its query result, for the responder to compare with
/// its own. Spans are disjoint; a range no span covers is unknown, not empty, and an
/// offer with no spans still names the tree its keys belong to. A span describes only
/// entities in the query's result, never entities outside it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownState {
    pub spec: TreeSpec,
    pub spans: Vec<SpanContent>,
}

/// What a requester knows about one span of its result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanContent {
    /// A digest of the span's heads, for the responder to compare with its own.
    Digest(TreeSpanDigest),
    /// Every entity head within the span; an empty inventory says the span is empty.
    /// It may be as large as the offer's limits allow, larger than an inventory page.
    /// The responder sends the heads that differ, and its Complete confirms the rest.
    Heads(TreeSpanInventory),
}

impl SpanContent {
    /// The span this content describes.
    pub fn bounds(&self) -> &TreeSpan {
        match self {
            SpanContent::Digest(digest) => &digest.bounds,
            SpanContent::Heads(inventory) => &inventory.bounds,
        }
    }
}

/// A comparison round's number, counting from zero, and the source revision its
/// snapshot reflects. Revisions order this stream's publications; they say nothing
/// about entity histories or other peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRound {
    pub number: u64,
    pub revision: u64,
}

/// The responder's reply to Fetch or SubscribeQuery: round zero and its first frame.
/// Every live publication of the stream follows the round's revision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateReply {
    pub round: StateRound,
    pub frame: StateFrame,
}

/// One numbered frame of a state stream. A stream and sequence number name exactly one
/// frame, so a resend carries the same bytes. Frame zero travels in the StateReply and
/// every later frame as NodeMessage::StateFrame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateFrame {
    pub stream: StateStreamId,
    pub sequence: u64,
    pub body: StateFrameBody,
}

/// What one frame carries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateFrameBody {
    /// Begins the round a RefreshState asked for, comparing the offer it carried. It
    /// follows every live publication at or before the round's revision and precedes
    /// the round's items and every later publication.
    BeginRound(StateRound),
    /// Items of the round, and whether the responder has now sent them all.
    Round { round: u64, items: Vec<StateItem>, progress: ComparisonProgress },
    /// Part of one live publication: the membership changes the responder's source
    /// made at `revision`, with data for the entities they concern. A publication may
    /// span frames, and `complete` marks its last. The requester installs a complete
    /// publication once its local entities cover every target, in revision order.
    Live { revision: u64, items: Vec<QueryChange>, complete: bool },
    /// One chunk of a live change whose delta is too large for a frame, split as a
    /// repair's delta is. Every chunk repeats the change's membership decision.
    /// `complete` may accompany only the last chunk, and then completes the whole
    /// publication, not only this entity. Round frames may arrive between chunks.
    LiveChunk { revision: u64, member: Option<QueryMember>, chunk: EntityDeltaChunk, complete: bool },
    /// Data toward targets of live publications that the requester asked to repair.
    /// It changes no membership.
    LiveRepair { items: Vec<RepairItem> },
    /// The responder ended the stream. The requester starts over with a new request
    /// and treats nothing it received on this stream as complete.
    RestartRequired,
    /// The source cannot prove a gap-free publication history after an uncertain
    /// commit outcome. A new stream cannot repair that: the source node must be
    /// rebuilt before it can serve query state again.
    SourceUnavailable,
}

/// An entity's membership as the source decided it at a revision: its key in the
/// query's tree, and the head the source held. The requester installs the membership
/// once its own copy of the entity covers that head, and keeps it apart from the key
/// and head of its own copy, which may be newer or concurrent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMember {
    pub key: Vec<u8>,
    pub head: Clock,
}

/// One membership decision of a live publication. `member` is None when the entity is
/// not in the query, which covers a removal whose data the source may not disclose.
/// `delta` optionally carries the entity's data. A member without one still needs a
/// local copy that covers its head. Data for an absent entity must carry state, since
/// an event bridge alone has no target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryChange {
    pub entity_id: EntityId,
    pub member: Option<QueryMember>,
    pub delta: Option<DeltaContent>,
}

/// Whether the responder has sent every item of a round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonProgress {
    /// More items follow, sent as credit allows. Repairs sent after Complete carry
    /// Continue without withdrawing it.
    Continue,
    /// The responder has sent every item of the round. The requester may still ask for
    /// repairs, though a target its copy already covers needs none; the round ends when
    /// the requester's local result covers every target and it reports Applied.
    Complete,
}

/// One item of a round: a question for the requester, heads to adopt, or entity data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateItem {
    /// Asks for the requester's digest of exactly this span. A round may ask about
    /// nested spans at several depths; the requester answers each on its own.
    Digest(TreeSpanDigest),
    /// Every head the responder holds within the inventory's bounds; the requester
    /// adopts them as the round's membership there.
    Heads(TreeSpanInventory),
    /// Asks for the requester's heads within these bounds, answered in pages.
    NeedHeads(TreeSpan),
    /// Data toward one of the round's targets.
    Repair(RepairItem),
}

/// Entity data toward one causal target, sent in a round or a LiveRepair frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepairItem {
    /// Data that brings the entity to `target`, the head the source published. The
    /// requester applies it through the usual attestation and causal checks and may
    /// keep newer or concurrent local state; sending it acknowledges nothing.
    Delta { target: Clock, delta: EntityDelta },
    /// One chunk of a delta too large for a frame; the target stays the same across
    /// its chunks. A responder finishes a round's chunked repairs before marking it
    /// Complete. A repair the requester asks for later may still span frames, and each
    /// one it needs must finish before it reports the round Applied.
    Chunk { target: Clock, chunk: EntityDeltaChunk },
    /// The responder cannot send data toward this target within its policy, history
    /// or work limits. The requester fails if it still needs the target; one its copy
    /// already covers, or one of a retired round, fails nothing.
    Unavailable { entity_id: EntityId, target: Clock },
}

/// One answer to a round, sent in StateFeedback. A feedback may answer any of the
/// round's outstanding questions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateAnswer {
    /// The requester's digest over exactly the span the responder asked about.
    Digest(TreeSpanDigest),
    /// One page of the requester's heads within bounds the responder asked about. A
    /// page that stops short ends at its last head, and the responder asks for the rest.
    Heads(TreeSpanInventory),
    /// The requester's copy did not reach a target the round offered, even after the
    /// data the responder sent. The responder answers with connecting events or
    /// RepairItem::Unavailable, never the same snapshot. Only an offered entity and
    /// target qualify, after Complete too.
    NeedRepair(RepairRequest),
}

/// A requester's request for data connecting its copy of an entity to `target`,
/// a head the responder offered for that entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairRequest {
    pub entity_id: EntityId,
    pub target: Clock,
}

/// The answers one StateFeedback carries, named by what they respond to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FeedbackAnswers {
    /// Answers to this round's questions, including requests to repair the targets
    /// it offered. Answers to a retired round are dropped; a future round is refused.
    Round { round: u64, answers: Vec<StateAnswer> },
    /// Requests for connecting history toward targets of live publications.
    LiveRepair { requests: Vec<RepairRequest> },
}

impl FeedbackAnswers {
    /// How many answers or requests this carries.
    pub fn len(&self) -> usize {
        match self {
            Self::Round { answers, .. } => answers.len(),
            Self::LiveRepair { requests } => requests.len(),
        }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

/// How a requester ends a round, in FinishStateRound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateRoundOutcome {
    /// After Complete, the requester's local result covers every target the round
    /// offered and includes every live publication through the round's revision; the
    /// responder releases the round's snapshot, and later publications never hold the
    /// round open. `processed_through` must reach the frame that completed the round,
    /// and returns credit as StateFeedback does. A repair still in flight for a target
    /// the requester covered by other means may then stop; later frames still receive
    /// credit but cannot change the round. The requester need not wait for the reply
    /// before reporting its query ready.
    Applied { processed_through: u64 },
    /// Discards only this round. The stream and its live publications continue, and
    /// another round may follow.
    Cancelled,
}
