//! Comparing a requester's account of its query result with a snapshot of the
//! responder's: which entities the requester lacks or holds at another head,
//! and which of its digest spans differ.

#[cfg(test)]
mod tests;

use super::Snapshot;
use ankurah_digest_tree::{leaf_address, Digest};
use ankurah_proto::{Clock, EntityId, KnownState, SpanContent, TreeSpan, TreeSpanDigest, TreeSpanInventory};
use anyhow::ensure;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
};

/// How a requester's offer differs from a snapshot of the responder's result.
#[derive(Debug, Default)]
pub(crate) struct OfferComparison {
    /// The entities the responder sends the requester's copy toward, each with
    /// the head the offer listed for it: every snapshot member in a range the
    /// offer left unknown or described as empty, every member an inventory
    /// lists at another head or not at all, and every entity an inventory lists
    /// that the snapshot no longer holds.
    pub(crate) lacking: Vec<(EntityId, Option<Clock>)>,
    /// The offer's digest spans whose digest differs from the snapshot's, as
    /// the snapshot digests them. A descent over them finds the rest.
    pub(crate) differing: Vec<TreeSpanDigest>,
}

impl OfferComparison {
    /// Compare `known`, an offer under the snapshot's spec, with `snapshot`.
    pub(crate) async fn new(snapshot: &mut Snapshot, known: KnownState) -> anyhow::Result<Self> {
        ensure!(known.spec == snapshot.spec, "unsupported comparison tree spec");
        known.validate()?;
        let mut spans = known.spans;
        spans.sort_by(|a, b| a.bounds().cmp_start(b.bounds()));
        // An entity an inventory lists may now sit elsewhere in the snapshot; the
        // head the requester listed still lets the responder bridge from it.
        let mut known_heads = HashMap::new();
        for span in &spans {
            if let SpanContent::Heads(inventory) = span {
                let mut cursor = inventory.heads.cursor();
                while let Some(head) = cursor.next() {
                    known_heads.insert(head.entity.entity_id, head.entity.head.to_owned());
                }
            }
        }
        let mut comparison = Self::default();
        let mut digests = Vec::new();
        // Whether the offer's digests alone cover every address so far.
        let mut tiled = true;
        let mut start = TreeSpan::all().start;
        for span in spans {
            let bounds = span.bounds();
            if start != bounds.start {
                tiled = false;
                comparison.lack_all(snapshot, &TreeSpan::new(start, opposite(&bounds.start)), &known_heads);
            }
            start = opposite(&bounds.end);
            match span {
                SpanContent::Digest(digest) => digests.push(digest),
                SpanContent::Heads(inventory) => {
                    tiled = false;
                    comparison.diff(snapshot, &inventory, &known_heads);
                }
            }
        }
        if !matches!(start, Bound::Unbounded) {
            tiled = false;
            comparison.lack_all(snapshot, &TreeSpan::new(start, TreeSpan::all().end), &known_heads);
        }
        // Digests that cover the tree and sum to its root settle the whole offer
        // with one digest read.
        if tiled
            && digests.len() > 1
            && Digest::checked_sum(digests.iter().map(|known| known.digest))? == snapshot.span_digest(TreeSpan::all()).await?.digest
        {
            return Ok(comparison);
        }
        for known in digests {
            if known.digest == Digest::empty() {
                comparison.lack_all(snapshot, &known.bounds, &known_heads);
                continue;
            }
            let local = snapshot.span_digest(known.bounds).await?;
            if local.digest != known.digest {
                comparison.differing.push(local);
            }
        }
        Ok(comparison)
    }

    /// Lack every snapshot member within `bounds`, where the requester holds nothing.
    fn lack_all(&mut self, snapshot: &Snapshot, bounds: &TreeSpan, known_heads: &HashMap<EntityId, Clock>) {
        if bounds.is_empty() {
            return;
        }
        for (entity_id, _) in snapshot.entries_in(bounds, usize::MAX) {
            self.lacking.push((entity_id, known_heads.get(&entity_id).cloned()));
        }
    }

    /// Diff `inventory`, every head the requester holds within its bounds, with
    /// the snapshot's members there.
    fn diff(&mut self, snapshot: &Snapshot, inventory: &TreeSpanInventory, known_heads: &HashMap<EntityId, Clock>) {
        let mut listed = BTreeMap::new();
        let mut cursor = inventory.heads.cursor();
        while let Some(head) = cursor.next() {
            listed.insert(leaf_address(head.key, head.entity.entity_id), (head.entity.entity_id, head.entity.head.to_owned()));
        }
        for (entity_id, entry) in snapshot.entries_in(&inventory.bounds, usize::MAX) {
            match listed.remove(entry.address()) {
                Some((_, head)) if head == entry.head => {}
                _ => self.lacking.push((entity_id, known_heads.get(&entity_id).cloned())),
            }
        }
        // A listed entity the snapshot holds at another address is lacked where
        // it now sits; one it no longer holds has left the result.
        for (entity_id, head) in listed.into_values() {
            if !snapshot.entries.contains_key(&entity_id) {
                self.lacking.push((entity_id, Some(head)));
            }
        }
    }
}

/// The bound that starts where `bound` ends, or ends where it starts.
fn opposite(bound: &Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(value) => Bound::Excluded(value.clone()),
        Bound::Excluded(value) => Bound::Included(value.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
