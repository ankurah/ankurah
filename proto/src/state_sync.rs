//! Query state comparison: how a node describes its copy of a query result, so that
//! a peer can compare it with its own and send only the entities the node lacks.
//!
//! A node files its result's entities in a tree under a [`TreeSpec`] and describes the
//! tree as a [`KnownState`]: disjoint spans of addresses, each given as a digest of the
//! heads within it or as an inventory of every head within it.
//!
//! Both peers enforce the limits in [`limits`]. Byte limits on an offer or an inventory
//! page count its work: its encoded bytes plus the bytes its inventories saved by
//! sharing key prefixes.

pub use ankurah_digest_tree::{TreeSpan, TreeSpanDigest, TreeSpec};
use serde::{Deserialize, Serialize};

mod inventory;
pub mod limits;
mod validation;
mod work;
pub use inventory::{
    ClockRef, EntityHead, EntityHeadRef, HeadCursor, InventoryError, KeyedEntityHead, KeyedEntityHeadRef, PackedHeads, TreeSpanInventory,
};

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
