//! A snapshot read as protocol spans: span digests, refinements, pages and the initial offer.

use super::Snapshot;
use ankurah_digest_tree::{Digest, NodePrefix};
use ankurah_proto::{
    self as proto,
    state_sync::limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS, MAX_OFFER_SPANS, MAX_PAGE_BYTES, MAX_PAGE_HEADS},
    TreeSpanDigest,
};

/// The encoded size an offer of span digests stays within while its frontier is refined.
pub(super) const FRONTIER_BYTES: u64 = 16 * 1024;
const OFFER_REFINEMENTS: usize = 32;

impl Snapshot {
    pub async fn span_digest(&mut self, bounds: proto::TreeSpan) -> anyhow::Result<TreeSpanDigest> {
        let ranges = match &self.ranges {
            Some(ranges) => ranges.iter().filter_map(|range| range.intersect(&bounds)).collect::<Vec<_>>(),
            None => vec![bounds.clone()],
        };
        if ranges.is_empty() {
            return Ok(TreeSpanDigest { prefix: NodePrefix::root(), bounds, digest: Digest::empty() });
        }
        let tree = self.digests().await?;
        let scope = tree.scope_spans(ranges);
        Ok(TreeSpanDigest { prefix: NodePrefix::root(), bounds, digest: tree.digest(&scope).await? })
    }

    pub async fn offer(&mut self) -> anyhow::Result<proto::KnownState> {
        if let Some(inventory) = self.initial_inventory() {
            return Ok(inventory);
        }
        // A bounded disjoint frontier recognizes both equal inventories and
        // small changed spans without serial refinement exchanges.
        let root = self.span_digest(proto::TreeSpan::all()).await?;
        let mut frontier = vec![(root, true)];
        for _ in 0..OFFER_REFINEMENTS {
            let Some(index) = frontier
                .iter()
                .enumerate()
                .filter(|(_, (head, expandable))| *expandable && head.digest.count() > MAX_PAGE_HEADS as i64)
                .max_by_key(|(_, (head, _))| head.digest.count())
                .map(|(index, _)| index)
            else {
                break;
            };
            frontier[index].1 = false;
            let children = self.refine(&frontier[index].0).await?;
            if children.as_slice() == [frontier[index].0.clone()] {
                continue;
            }
            let mut candidate = frontier.clone();
            candidate.splice(index..=index, children.into_iter().map(|head| (head, true)));
            if candidate.len() > MAX_OFFER_SPANS {
                continue;
            }
            let inventory = proto::KnownState {
                spec: self.spec.clone(),
                spans: candidate.iter().map(|(head, _)| proto::SpanContent::Digest(head.clone())).collect(),
            };
            if bincode::serialized_size(&inventory)? <= FRONTIER_BYTES {
                frontier = candidate;
            }
        }
        Ok(proto::KnownState {
            spec: self.spec.clone(),
            spans: frontier.into_iter().map(|(head, _)| proto::SpanContent::Digest(head)).collect(),
        })
    }

    fn initial_inventory(&self) -> Option<proto::KnownState> {
        if self.members.len() > MAX_OFFER_HEADS {
            return None;
        }
        // The heads' own work bounds the inventory's from below; stop before
        // encoding an inventory it already rules out.
        let mut heads_work = 0usize;
        for entry in self.entries.values() {
            heads_work = heads_work.saturating_add(proto::PackedHeads::head_work_bytes(entry.key().len(), &entry.head));
            if heads_work > MAX_OFFER_BYTES {
                return None;
            }
        }
        let heads = proto::PackedHeads::encode(self.members.values().map(|id| {
            let entry = &self.entries[id];
            (entry.key(), *id, &entry.head)
        }))
        .ok()?;
        let inventory = proto::KnownState {
            spec: self.spec.clone(),
            spans: vec![proto::SpanContent::Heads(proto::TreeSpanInventory { bounds: proto::TreeSpan::all(), heads })],
        };
        inventory.work_bytes().ok().filter(|work| *work <= MAX_OFFER_BYTES as u64).map(|_| inventory)
    }

    pub fn page(&self, bounds: proto::TreeSpan) -> anyhow::Result<proto::TreeSpanInventory> {
        use std::ops::Bound;
        let rows = if bounds.is_empty() {
            Vec::new()
        } else {
            self.members
                .range::<[u8], _>(bounds.bounds())
                .take(MAX_PAGE_HEADS + 1)
                .map(|(_, id)| (*id, &self.entries[id]))
                .collect::<Vec<_>>()
        };
        let mut count = 0;
        let mut heads_work = 0;
        for (index, (_, entry)) in rows.iter().take(MAX_PAGE_HEADS).enumerate() {
            let end = if index + 1 == rows.len() { bounds.end.clone() } else { Bound::Included(entry.address().to_vec()) };
            // An empty inventory over these bounds counts the bounds and the encoding's header.
            let bounds_work =
                proto::TreeSpanInventory { bounds: proto::TreeSpan::new(bounds.start.clone(), end), heads: Default::default() }
                    .work_bytes()?;
            let work = proto::PackedHeads::head_work_bytes(entry.key().len(), &entry.head) as u64;
            if bounds_work + heads_work + work > MAX_PAGE_BYTES as u64 {
                anyhow::ensure!(count > 0, "single head inventory exceeds byte limit");
                break;
            }
            heads_work += work;
            count += 1;
        }
        let end = if count == rows.len() { bounds.end } else { Bound::Included(rows[count - 1].1.address().to_vec()) };
        let heads = proto::PackedHeads::encode(rows.iter().take(count).map(|(id, entry)| (entry.key(), *id, &entry.head)))?;
        let page = proto::TreeSpanInventory { bounds: proto::TreeSpan::new(bounds.start, end), heads };
        anyhow::ensure!(page.work_bytes()? <= MAX_PAGE_BYTES as u64, "head inventory bounds exceed byte limit");
        Ok(page)
    }

    pub async fn refine(&mut self, head: &TreeSpanDigest) -> anyhow::Result<Vec<TreeSpanDigest>> {
        if self.ranges.as_ref().is_some_and(Vec::is_empty) {
            return Ok(vec![TreeSpanDigest { digest: Digest::empty(), ..head.clone() }]);
        }
        let mut children = self.digests().await?.refine(head).await?;
        if self.ranges.is_some() {
            for child in &mut children {
                child.digest = self.span_digest(child.bounds.clone()).await?.digest;
            }
        }
        Ok(children)
    }
}
