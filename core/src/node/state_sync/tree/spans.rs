//! A snapshot read as protocol spans: span digests, refinements, pages, the
//! initial offer, and an account over a peer's offered spans.

use super::Snapshot;
use ankurah_digest_tree::{Digest, NodePrefix};
use ankurah_proto::{
    self as proto,
    state_sync::limits::{MAX_OFFER_BYTES, MAX_OFFER_HEADS, MAX_OFFER_SPANS, MAX_PAGE_BYTES, MAX_PAGE_HEADS},
    SpanContent, TreeSpanDigest,
};

/// The encoded size an offer of span digests stays within while its frontier is refined.
pub(super) const FRONTIER_BYTES: u64 = 16 * 1024;
const OFFER_REFINEMENTS: usize = 32;

/// What an account can still take within an offer's limits.
struct OfferRoom {
    heads: usize,
    work: u64,
}

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

    /// Whether this snapshot's digest over `digest`'s bounds equals `digest`.
    /// A digest counts the leaves it sums, so a snapshot holding another
    /// number of members within the bounds cannot match, and is judged so
    /// without computing a digest.
    pub async fn matches(&mut self, digest: &TreeSpanDigest) -> anyhow::Result<bool> {
        if self.members.range::<[u8], _>(digest.bounds.bounds()).count() as i64 != digest.digest.count() {
            return Ok(false);
        }
        Ok(self.span_digest(digest.bounds.clone()).await?.digest == digest.digest)
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

    /// This snapshot's account over the spans of a peer's offer of span
    /// digests, which a comparison split into the digests `matching` this
    /// snapshot's over the same bounds and those `differing` from it. A
    /// matching digest stays as offered. A differing span lists this
    /// snapshot's heads within it where they fit what an offer's limits leave,
    /// spans with the fewest members first; a span whose heads do not fit gives
    /// this snapshot's digest under the offered prefix, from which the peer's
    /// own tree refines it. Each range the offer left out gets this snapshot's digest. None
    /// when the account would break an offer's limits even with every span
    /// given by a digest.
    pub async fn account_over(
        &mut self,
        matching: Vec<TreeSpanDigest>,
        differing: Vec<TreeSpanDigest>,
    ) -> anyhow::Result<Option<proto::KnownState>> {
        let mut covered: Vec<_> = matching.iter().chain(&differing).map(|digest| digest.bounds.clone()).collect();
        covered.sort_by(|a, b| a.cmp_start(b));
        let left_out = proto::TreeSpan::all().gaps(&covered);
        if covered.len() + left_out.len() > MAX_OFFER_SPANS {
            return Ok(None);
        }
        let mut spans: Vec<_> = matching.into_iter().map(SpanContent::Digest).collect();
        for bounds in left_out {
            spans.push(SpanContent::Digest(self.span_digest(bounds).await?));
        }
        // Each differing span starts as the offered digest, which weighs what
        // this snapshot's digest of the span will: a digest's encoding has a
        // fixed length.
        let mut placed = Vec::with_capacity(differing.len());
        for digest in differing {
            placed.push((spans.len(), digest.clone()));
            spans.push(SpanContent::Digest(digest));
        }
        let mut account = proto::KnownState { spec: self.spec.clone(), spans };
        let Some(work) = (MAX_OFFER_BYTES as u64).checked_sub(account.work_bytes()?) else {
            return Ok(None);
        };
        let mut room = OfferRoom { heads: MAX_OFFER_HEADS, work };
        placed.sort_by_cached_key(|(_, digest)| self.members.range::<[u8], _>(digest.bounds.bounds()).count());
        for (index, offered) in placed {
            if let Some(heads) = self.heads_fitting(&account.spans[index], &mut room)? {
                account.spans[index] = heads;
                continue;
            }
            let digest = self.span_digest(offered.bounds.clone()).await?.digest;
            account.spans[index] = SpanContent::Digest(TreeSpanDigest { digest, ..offered });
        }
        account.spans.sort_by(|a, b| a.bounds().cmp_start(b.bounds()));
        Ok(Some(account))
    }

    /// This snapshot's heads within the bounds of `replaced`, one of an
    /// account's spans, as the span to list in its place, when the account
    /// then stays within `room`, which keeps what the heads leave of it.
    fn heads_fitting(&self, replaced: &SpanContent, room: &mut OfferRoom) -> anyhow::Result<Option<SpanContent>> {
        let bounds = replaced.bounds();
        let heads = || {
            self.members.range::<[u8], _>(bounds.bounds()).map(|(_, id)| {
                let entry = &self.entries[id];
                (entry.key(), *id, &entry.head)
            })
        };
        // The heads may take what the room holds and what `replaced` frees.
        let available = room.work + replaced.work_bytes()?;
        // The heads' own work bounds the inventory's from below; stop before
        // encoding an inventory it already rules out.
        let (count, heads_work) = heads().fold((0usize, 0u64), |(count, heads_work), (key, _, head)| {
            (count + 1, heads_work + proto::PackedHeads::head_work_bytes(key.len(), head) as u64)
        });
        if count > room.heads || heads_work > available {
            return Ok(None);
        }
        let listed = SpanContent::Heads(proto::TreeSpanInventory { bounds: bounds.clone(), heads: proto::PackedHeads::encode(heads())? });
        let Some(left) = available.checked_sub(listed.work_bytes()?) else {
            return Ok(None);
        };
        *room = OfferRoom { heads: room.heads - count, work: left };
        Ok(Some(listed))
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
