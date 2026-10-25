//! The work an offer or an inventory describes: its encoded bytes plus the bytes its
//! inventories saved by sharing key prefixes. Prefix sharing reduces traffic but not
//! what a peer must decode and process, so the protocol's byte limits count work.

use serde::Serialize;

use super::{KnownState, SpanContent, TreeSpanInventory};

fn work(message: &impl Serialize, shared_prefix_bytes: u64) -> bincode::Result<u64> {
    Ok(bincode::serialized_size(message)? + shared_prefix_bytes)
}

impl TreeSpanInventory {
    /// The work of this inventory. One inventory page carries at most `MAX_PAGE_BYTES` of it.
    pub fn work_bytes(&self) -> bincode::Result<u64> { work(self, self.heads.shared_prefix_bytes()) }
}

impl SpanContent {
    /// The work this span adds to an offer that carries it.
    pub fn work_bytes(&self) -> bincode::Result<u64> { work(self, self.shared_prefix_bytes()) }

    fn shared_prefix_bytes(&self) -> u64 {
        match self {
            SpanContent::Heads(inventory) => inventory.heads.shared_prefix_bytes(),
            SpanContent::Digest(_) => 0,
        }
    }
}

impl KnownState {
    /// The work of this offer.
    pub fn work_bytes(&self) -> bincode::Result<u64> { work(self, self.spans.iter().map(SpanContent::shared_prefix_bytes).sum()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Clock, EntityHead, EntityId, EventId, KeyedEntityHead, PackedHeads, TreeSpan, TreeSpanDigest, TreeSpec};
    use ankurah_core_types::KeySpec;
    use ankurah_digest_tree::{Digest, NodePrefix};

    #[test]
    fn an_offers_work_is_its_spans_work_on_top_of_an_offer_with_none() {
        // Keys sharing a long prefix, whose sharing saves bytes that the work still counts.
        let heads = (0..8u8)
            .map(|n| KeyedEntityHead {
                key: [vec![7; 40], vec![n]].concat(),
                entity: EntityHead { entity_id: EntityId::from_bytes([n; 32]), head: Clock::genesis(EventId::from_bytes([n; 32])) },
            })
            .collect::<Vec<_>>();
        let inventory = TreeSpanInventory { bounds: TreeSpan::all(), heads: PackedHeads::try_from(heads).unwrap() };
        assert!(inventory.heads.shared_prefix_bytes() > 0);
        let digest = TreeSpanDigest { prefix: NodePrefix::root(), bounds: TreeSpan::all(), digest: Digest::empty() };
        let spans = vec![SpanContent::Heads(inventory), SpanContent::Digest(digest)];
        let spec = TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True);

        let none = KnownState { spec: spec.clone(), spans: Vec::new() }.work_bytes().unwrap();
        let spans_work: u64 = spans.iter().map(|span| span.work_bytes().unwrap()).sum();
        assert_eq!(KnownState { spec, spans }.work_bytes().unwrap(), none + spans_work);
    }
}
