//! Structural checks a receiver applies to offers and inventory pages before acting on
//! them. Whether a page answers what the receiver asked stays with the receiver.

use std::{collections::HashSet, ops::Bound};

use ankurah_digest_tree::NodePrefix;
use anyhow::ensure;

use super::{
    limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS, MAX_OFFER_SPANS, MAX_PAGE_BYTES, MAX_PAGE_HEADS},
    KnownState, SpanContent, TreeSpan, TreeSpanInventory, TreeSpec,
};

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

impl TreeSpanInventory {
    /// Checks an inventory page: at most `MAX_PAGE_HEADS` heads within `MAX_PAGE_BYTES`
    /// of work, valid under `spec`.
    pub fn validate_page(&self, spec: &TreeSpec) -> anyhow::Result<()> {
        ensure!(self.heads.len() <= MAX_PAGE_HEADS, "inventory page has too many heads");
        ensure!(self.work_bytes()? <= MAX_PAGE_BYTES as u64, "inventory page exceeds its work limit");
        self.validate(spec)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
