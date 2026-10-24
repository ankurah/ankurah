//! Structural checks a receiver applies to state-sync messages before acting on them.
//! They need no stream state; the checks against what a stream has sent or asked
//! stay with the receiver.

use std::{collections::HashSet, ops::Bound};

use ankurah_digest_tree::NodePrefix;
use anyhow::ensure;

use super::{
    inventory::check_key,
    limits::{MAX_FEEDBACK_ANSWERS, MAX_FRAME_ITEMS, MAX_OFFER_BYTES, MAX_OFFER_HEADS, MAX_OFFER_SPANS, MAX_PAGE_BYTES, MAX_PAGE_HEADS},
    FeedbackAnswers, KnownState, QueryChange, QueryMember, RepairItem, SpanContent, StateAnswer, StateFrameBody, StateItem, StateReply,
    TreeSpan, TreeSpanInventory, TreeSpec,
};

impl StateReply {
    /// The initial reply opens round zero with frame zero.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.round.number == 0 && self.frame.sequence == 0, "initial reply must open round zero with frame zero");
        Ok(())
    }
}

impl KnownState {
    /// Checks the offer against its own spec: at most `MAX_OFFER_SPANS` disjoint spans
    /// within the tree, valid digests and inventories, and within the offer's limits
    /// each entity at most once.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.spans.len() <= MAX_OFFER_SPANS, "offer has too many spans");
        ensure!(self.work_bytes()? <= MAX_OFFER_BYTES as u64, "offer exceeds its work limit");
        let mut heads = 0;
        for span in &self.spans {
            match span {
                SpanContent::Digest(digest) => digest.validate()?,
                SpanContent::Heads(inventory) => {
                    inventory.validate(&self.spec)?;
                    heads += inventory.heads.len();
                }
            }
        }
        ensure!(heads <= MAX_OFFER_HEADS, "offer exceeds its head limit");
        let mut entities = HashSet::with_capacity(heads);
        for span in &self.spans {
            if let SpanContent::Heads(inventory) = span {
                let mut cursor = inventory.heads.cursor();
                while let Some(head) = cursor.next() {
                    ensure!(entities.insert(head.entity.entity_id), "offer lists entity {} twice", head.entity.entity_id);
                }
            }
        }
        check_disjoint_within_tree(self.spans.iter().map(SpanContent::bounds))
    }
}

/// Checks that spans, in any order, are disjoint and lie within the tree's addresses.
fn check_disjoint_within_tree<'a>(spans: impl Iterator<Item = &'a TreeSpan>) -> anyhow::Result<()> {
    let tree = TreeSpan::under(&NodePrefix::root());
    let mut spans: Vec<_> = spans.collect();
    spans.sort_by(|a, b| a.cmp_start(b));
    let mut start = tree.start.clone();
    for span in spans {
        ensure!(
            !matches!(start, Bound::Unbounded) && !span.is_empty() && span.is_subset_of(&TreeSpan::new(start, tree.end.clone())),
            "offer spans overlap or extend past the tree"
        );
        start = match &span.end {
            Bound::Included(end) => Bound::Excluded(end.clone()),
            Bound::Excluded(end) => Bound::Included(end.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
    }
    Ok(())
}

impl StateFrameBody {
    /// Checks a frame's items against the stream's spec and the frame limits.
    pub fn validate(&self, spec: &TreeSpec) -> anyhow::Result<()> {
        match self {
            StateFrameBody::BeginRound(_) | StateFrameBody::RestartRequired | StateFrameBody::SourceUnavailable => {}
            StateFrameBody::Round { items, .. } => {
                ensure!(items.len() <= MAX_FRAME_ITEMS, "frame has too many items");
                for item in items {
                    item.validate(spec)?;
                }
            }
            StateFrameBody::Live { items, .. } => {
                ensure!(items.len() <= MAX_FRAME_ITEMS, "frame has too many items");
                for change in items {
                    change.validate(spec)?;
                }
            }
            StateFrameBody::LiveChunk { member, chunk, complete, .. } => {
                ensure!(chunk.last || !complete, "publication completed inside a chunk");
                ensure!(!chunk.bytes.is_empty(), "empty delta chunk");
                if let Some(member) = member {
                    member.validate(spec)?;
                }
            }
            StateFrameBody::LiveRepair { items } => {
                ensure!(items.len() <= MAX_FRAME_ITEMS, "frame has too many items");
                for item in items {
                    item.validate()?;
                }
            }
        }
        Ok(())
    }
}

impl StateItem {
    fn validate(&self, spec: &TreeSpec) -> anyhow::Result<()> {
        match self {
            StateItem::Digest(digest) => digest.validate()?,
            StateItem::Heads(inventory) => validate_page(inventory, spec)?,
            StateItem::NeedHeads(_) => {}
            StateItem::Repair(item) => item.validate()?,
        }
        Ok(())
    }
}

impl RepairItem {
    /// Checks a repair against its target: delta data must reach the target, and a chunk must carry bytes.
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            RepairItem::Delta { target, delta } => delta.content.check_target(target)?,
            RepairItem::Chunk { chunk, .. } => ensure!(!chunk.bytes.is_empty(), "empty delta chunk"),
            RepairItem::Unavailable { .. } => {}
        }
        Ok(())
    }
}

impl QueryChange {
    /// Checks a live membership decision: a canonical member key, and delta data that has a target.
    pub fn validate(&self, spec: &TreeSpec) -> anyhow::Result<()> {
        if let Some(member) = &self.member {
            member.validate(spec)?;
        }
        if let Some(delta) = &self.delta {
            delta.live_target(self.member.as_ref())?;
        }
        Ok(())
    }
}

impl QueryMember {
    /// Checks that the member's key is canonical under the stream's spec.
    pub fn validate(&self, spec: &TreeSpec) -> anyhow::Result<()> {
        check_key(spec, &self.key)?;
        Ok(())
    }
}

impl StateAnswer {
    /// Checks an answer against the stream's spec and the page limits.
    pub fn validate(&self, spec: &TreeSpec) -> anyhow::Result<()> {
        match self {
            StateAnswer::Digest(digest) => digest.validate()?,
            StateAnswer::Heads(inventory) => validate_page(inventory, spec)?,
            StateAnswer::NeedRepair(_) => {}
        }
        Ok(())
    }
}

impl FeedbackAnswers {
    /// Checks a feedback's answers against the stream's spec and the feedback limits.
    pub fn validate(&self, spec: &TreeSpec) -> anyhow::Result<()> {
        ensure!(self.len() <= MAX_FEEDBACK_ANSWERS, "feedback has too many answers");
        ensure!(self.work_bytes()? <= MAX_PAGE_BYTES as u64, "feedback answers exceed their work limit");
        if let FeedbackAnswers::Round { answers, .. } = self {
            for answer in answers {
                answer.validate(spec)?;
            }
        }
        Ok(())
    }
}

/// An inventory page carries at most `MAX_PAGE_HEADS` heads within `MAX_PAGE_BYTES` of work.
fn validate_page(inventory: &TreeSpanInventory, spec: &TreeSpec) -> anyhow::Result<()> {
    ensure!(inventory.heads.len() <= MAX_PAGE_HEADS, "inventory page has too many heads");
    ensure!(inventory.work_bytes()? <= MAX_PAGE_BYTES as u64, "inventory page exceeds its work limit");
    inventory.validate(spec)?;
    Ok(())
}

#[cfg(test)]
mod tests;
