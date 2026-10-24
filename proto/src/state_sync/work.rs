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
