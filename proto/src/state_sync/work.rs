//! The work a state-sync message describes: its encoded bytes plus the bytes its
//! inventories saved by sharing key prefixes. Prefix sharing reduces traffic but not
//! what a peer must decode and process, so the protocol's byte limits on a message
//! count its work.

use serde::Serialize;

use super::{
    FeedbackAnswers, KnownState, RepairRequest, SpanContent, StateAnswer, StateFrame, StateFrameBody, StateItem, TreeSpanInventory,
};

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

impl StateItem {
    /// The work of this item. A frame's items carry at most `MAX_PAGE_BYTES` of it together.
    pub fn work_bytes(&self) -> bincode::Result<u64> { work(self, self.shared_prefix_bytes()) }

    fn shared_prefix_bytes(&self) -> u64 {
        match self {
            StateItem::Heads(inventory) => inventory.heads.shared_prefix_bytes(),
            StateItem::Digest(_) | StateItem::NeedHeads(_) | StateItem::Repair(_) => 0,
        }
    }
}

impl StateFrame {
    /// The work of this frame.
    pub fn work_bytes(&self) -> bincode::Result<u64> {
        let shared_prefix_bytes = match &self.body {
            StateFrameBody::Round { items, .. } => items.iter().map(StateItem::shared_prefix_bytes).sum(),
            _ => 0,
        };
        work(self, shared_prefix_bytes)
    }
}

impl StateAnswer {
    /// The work of this answer.
    pub fn work_bytes(&self) -> bincode::Result<u64> {
        let shared_prefix_bytes = match self {
            StateAnswer::Heads(inventory) => inventory.heads.shared_prefix_bytes(),
            StateAnswer::Digest(_) | StateAnswer::NeedRepair(_) => 0,
        };
        work(self, shared_prefix_bytes)
    }
}

impl RepairRequest {
    /// The work of this request.
    pub fn work_bytes(&self) -> bincode::Result<u64> { work(self, 0) }
}

impl FeedbackAnswers {
    /// The work of the answers or requests this carries, summed one by one as a
    /// requester fills a feedback. They carry at most `MAX_PAGE_BYTES` of it together.
    pub fn work_bytes(&self) -> bincode::Result<u64> {
        match self {
            FeedbackAnswers::Round { answers, .. } => answers.iter().map(StateAnswer::work_bytes).sum(),
            FeedbackAnswers::LiveRepair { requests } => requests.iter().map(RepairRequest::work_bytes).sum(),
        }
    }
}
