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

/// A node's account of its query result under a [`TreeSpec`], for a peer to compare
/// with its own. Spans are disjoint; a range no span covers is unknown, not empty, and
/// an offer with no spans still names the tree its keys belong to. A span describes
/// only entities in the query's result, never entities outside it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownState {
    pub spec: TreeSpec,
    pub spans: Vec<SpanContent>,
}

/// What a node knows about one span of its result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanContent {
    /// A digest of the span's heads, for a peer to compare with its own.
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

    /// Whether a responder comparing an offer that holds this span pushes
    /// nothing within its bounds: a nonempty digest does not say which entities
    /// differ, so the requester compares the responder's account over the span
    /// itself. Within an empty digest, as within a range an offer leaves out,
    /// the responder pushes every member it holds.
    pub fn withholds_push(&self) -> bool {
        matches!(self, SpanContent::Digest(digest) if digest.digest != ankurah_digest_tree::Digest::empty())
    }
}

/// One question of a CompareSpans request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanQuestion {
    /// The responder's digest of exactly this span.
    Digest(TreeSpan),
    /// The responder's heads within this span from its start, one page of them. A page
    /// that stops short ends at its last head, and the requester asks for the rest.
    Heads(TreeSpan),
}

impl SpanQuestion {
    /// The span this question asks about.
    pub fn span(&self) -> &TreeSpan {
        match self {
            SpanQuestion::Digest(span) | SpanQuestion::Heads(span) => span,
        }
    }
}

/// One answer of a SpansCompared response, in the place of its question.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanAnswer {
    Digest(TreeSpanDigest),
    Heads(TreeSpanInventory),
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_digest_tree::{Digest, HeadHash, Leaf, NodePrefix, Ristretto255};

    fn digest_span(digest: Digest) -> SpanContent {
        SpanContent::Digest(TreeSpanDigest { prefix: NodePrefix::root(), bounds: TreeSpan::all(), digest })
    }

    #[test]
    fn a_responder_withholds_its_push_only_within_a_nonempty_digest() {
        let leaf = Leaf::new(&[], crate::EntityId::from_bytes([1; 32]), HeadHash::of([[2; 32]]));
        assert!(digest_span(Digest::from(leaf.element::<Ristretto255>())).withholds_push());
        assert!(!digest_span(Digest::empty()).withholds_push(), "an empty digest is pushed into like a range left out");
        let empty_inventory = TreeSpanInventory { bounds: TreeSpan::all(), heads: PackedHeads::try_from(Vec::new()).unwrap() };
        assert!(!SpanContent::Heads(empty_inventory).withholds_push(), "an inventory says what it lacks");
    }
}
