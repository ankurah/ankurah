//! Comparing a requester's account of its query result with a snapshot of the
//! responder's: which entities the requester lacks or holds at another head,
//! and which of its span digests differ.

#[cfg(test)]
mod tests;

use crate::node::state_sync::tree::Snapshot;
use ankurah_digest_tree::leaf_address;
use ankurah_proto::{Clock, EntityId, KnownState, SpanContent, TreeSpan, TreeSpanDigest, TreeSpanInventory};
use std::collections::{BTreeMap, HashMap};

/// An offer's span digests, compared with a snapshot's digests over the same bounds.
pub(super) struct ComparedDigests {
    /// The offered digests equal to the snapshot's.
    pub(super) matching: Vec<TreeSpanDigest>,
    /// The offered digests that differ from the snapshot's.
    pub(super) differing: Vec<TreeSpanDigest>,
}

/// `known`'s span digests compared with `snapshot`'s over the same bounds;
/// none when `known` describes a span by heads, or describes none. `known` is
/// a valid offer under the snapshot's spec.
pub(super) async fn compare_digests(snapshot: &mut Snapshot, known: &KnownState) -> anyhow::Result<Option<ComparedDigests>> {
    let mut digests = Vec::with_capacity(known.spans.len());
    for span in &known.spans {
        match span {
            SpanContent::Digest(digest) => digests.push(digest),
            SpanContent::Heads(_) => return Ok(None),
        }
    }
    if digests.is_empty() {
        return Ok(None);
    }
    let mut compared = ComparedDigests { matching: Vec::new(), differing: Vec::new() };
    for digest in digests {
        let side = if snapshot.matches(digest).await? { &mut compared.matching } else { &mut compared.differing };
        side.push(digest.clone());
    }
    Ok(Some(compared))
}

/// The entities the responder sends the requester's copy toward, each with the
/// head `known` listed for it: every snapshot member in a range the offer left
/// unknown or described as empty, every member an inventory lists at another
/// head or not at all, and every entity an inventory lists that the snapshot no
/// longer holds. A nonempty digest names no entities, so the requester compares
/// the responder's account over it itself. `known` is a valid offer under the
/// snapshot's spec.
pub(super) fn lacking(snapshot: &Snapshot, known: KnownState) -> Vec<(EntityId, Option<Clock>)> {
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
    let mut lacking = Vec::new();
    let bounds: Vec<_> = spans.iter().map(|span| span.bounds().clone()).collect();
    for gap in TreeSpan::all().gaps(&bounds) {
        lack_all(snapshot, &gap, &known_heads, &mut lacking);
    }
    for span in spans {
        match span {
            span if span.withholds_push() => {}
            SpanContent::Digest(digest) => lack_all(snapshot, &digest.bounds, &known_heads, &mut lacking),
            SpanContent::Heads(inventory) => diff(snapshot, &inventory, &known_heads, &mut lacking),
        }
    }
    lacking
}

/// Lack every snapshot member within `bounds`, where the requester holds nothing.
fn lack_all(snapshot: &Snapshot, bounds: &TreeSpan, known_heads: &HashMap<EntityId, Clock>, lacking: &mut Vec<(EntityId, Option<Clock>)>) {
    for (entity_id, _) in snapshot.entries_in(bounds) {
        lacking.push((entity_id, known_heads.get(&entity_id).cloned()));
    }
}

/// Diff `inventory`, every head the requester holds within its bounds, with the
/// snapshot's members there.
fn diff(
    snapshot: &Snapshot,
    inventory: &TreeSpanInventory,
    known_heads: &HashMap<EntityId, Clock>,
    lacking: &mut Vec<(EntityId, Option<Clock>)>,
) {
    let mut listed = BTreeMap::new();
    let mut cursor = inventory.heads.cursor();
    while let Some(head) = cursor.next() {
        listed.insert(leaf_address(head.key, head.entity.entity_id), (head.entity.entity_id, head.entity.head.to_owned()));
    }
    for (entity_id, entry) in snapshot.entries_in(&inventory.bounds) {
        match listed.remove(entry.address()) {
            Some((_, head)) if head == entry.head => {}
            _ => lacking.push((entity_id, known_heads.get(&entity_id).cloned())),
        }
    }
    // A listed entity the snapshot holds at another address is lacked where
    // it now sits; one it no longer holds has left the result.
    for (entity_id, head) in listed.into_values() {
        if !snapshot.entries.contains_key(&entity_id) {
            lacking.push((entity_id, Some(head)));
        }
    }
}
