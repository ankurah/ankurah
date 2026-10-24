//! The spans a request's limit selects: a walk over leaves in the request's
//! order, tier by tier, through a tree or through any ordered map of leaf
//! addresses, and the cut after the last leaf the limit admits.

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};

use ankurah_core_types::{EntityId, IndexDirection, KeySpec, PropertyId};

use super::reuse::ReusePlan;
use super::{TreeRequest, TreeSpec};
use crate::address::span::prefix_end;
use crate::address::{CanonicalCodec, TreeSpan};
use crate::error::TreeError;

impl TreeSpec {
    /// The spans of leaf addresses that `request` selects from this spec's
    /// members, its membership and its limit resolved over `addresses`, the
    /// members' addresses in a map the caller keeps, without building or
    /// reading digests. An underfilled limit keeps the membership's spans, just
    /// as [`TreeTransaction::scope`](crate::TreeTransaction::scope) does.
    ///
    /// The caller must supply this spec's admitted members, with exactly one
    /// canonical address (complete key followed by entity ID) per entity. Only
    /// addresses encountered while selecting the limit are validated; checking
    /// admission and uniqueness requires the caller's maintained membership index.
    pub fn selected_spans<K: Borrow<[u8]> + Ord, V>(
        &self,
        request: &TreeRequest,
        addresses: &BTreeMap<K, V>,
    ) -> Result<Vec<TreeSpan>, TreeError> {
        let ReusePlan { mut spans, directions, id_direction, .. } = self.plan(request).ok_or(TreeError::UnsupportedSelection)?;
        let Some(limit) = request.limit else { return Ok(spans) };
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut walk = TierWalk::new(self.key_spec(), &directions, id_direction, limit, &spans);
        let mut last = None;
        while let Some(scan) = walk.next_scan() {
            let mut entries = addresses.range::<[u8], _>(scan.bounds.bounds());
            let mut first = None;
            let mut count = 0;
            for _ in 0..scan.limit {
                let next = match scan.direction {
                    IndexDirection::Asc => entries.next(),
                    IndexDirection::Desc => entries.next_back(),
                };
                let Some((address, _)) = next else { break };
                let address: &[u8] = address.borrow();
                address_parts(self.key_spec(), address)?;
                first.get_or_insert(address);
                if scan.leaves {
                    last = Some(address);
                }
                count += 1;
            }
            walk.advance(first, count)?;
        }
        if walk.seen == limit {
            let cut = order_cut(self.key_spec(), &directions, id_direction, last.expect("a positive limit was filled"), true)?;
            spans = TreeSpan::intersect_all(&spans, &cut);
        }
        Ok(spans)
    }
}

/// Traversal decisions shared by stored leaves and an already ordered address
/// map. Each tuple tier has its own direction; entity IDs break final ties.
pub(crate) struct TierWalk<'a> {
    spec: &'a KeySpec<PropertyId>,
    directions: &'a [IndexDirection],
    id_direction: IndexDirection,
    limit: usize,
    seen: usize,
    tiers: Vec<Tier>,
}
/// The spans of one key part's tier still to walk, the part's depth, and
/// the place of the next span to walk among them.
struct Tier {
    spans: Vec<TreeSpan>,
    depth: usize,
    next: usize,
}

/// One scan a walk asks for: a span, the direction to read it in, how many
/// entries to read, and whether they are the leaves the walk selects or only
/// the first entry, which names the next tier.
pub(crate) struct TierScan {
    pub(crate) bounds: TreeSpan,
    pub(crate) direction: IndexDirection,
    pub(crate) limit: usize,
    pub(crate) leaves: bool,
}

impl<'a> TierWalk<'a> {
    pub(crate) fn new(
        spec: &'a KeySpec<PropertyId>,
        directions: &'a [IndexDirection],
        id_direction: IndexDirection,
        limit: usize,
        spans: &[TreeSpan],
    ) -> Self {
        Self { spec, directions, id_direction, limit, seen: 0, tiers: vec![Tier { spans: spans.to_vec(), depth: 0, next: 0 }] }
    }

    pub(crate) fn next_scan(&mut self) -> Option<TierScan> {
        if self.seen == self.limit {
            return None;
        }
        loop {
            let tier = self.tiers.last()?;
            if tier.next == tier.spans.len() {
                self.tiers.pop();
                continue;
            }
            let direction = self.directions.get(tier.depth).copied().unwrap_or(self.id_direction);
            let index = match direction {
                IndexDirection::Asc => tier.next,
                IndexDirection::Desc => tier.spans.len() - tier.next - 1,
            };
            let leaves = tier.depth == self.spec.keyparts.len();
            return Some(TierScan {
                bounds: tier.spans[index].clone(),
                direction,
                limit: if leaves { self.limit - self.seen } else { 1 },
                leaves,
            });
        }
    }

    pub(crate) fn advance(&mut self, first: Option<&[u8]>, count: usize) -> Result<(), TreeError> {
        let tier = self.tiers.last_mut().expect("advance follows a scan");
        if tier.depth == self.spec.keyparts.len() {
            self.seen += count;
            tier.next += 1;
            return Ok(());
        }
        let Some(address) = first else {
            tier.next += 1;
            return Ok(());
        };
        let parts = address_parts(self.spec, address)?;
        let prefix = parts[..=tier.depth].concat();
        let inside = TreeSpan::intersect_all(&tier.spans, &[TreeSpan::starting_with(&prefix)]);
        let direction = self.directions[tier.depth];
        let rest = match direction {
            IndexDirection::Asc => prefix_end(&prefix).map(|end| TreeSpan::new(Included(end), Unbounded)),
            IndexDirection::Desc => Some(TreeSpan::new(Unbounded, Excluded(prefix))),
        };
        tier.spans = rest.map(|rest| TreeSpan::intersect_all(&tier.spans, &[rest])).unwrap_or_default();
        tier.next = 0;
        let depth = tier.depth + 1;
        self.tiers.push(Tier { spans: inside, depth, next: 0 });
        Ok(())
    }
}

fn address_parts<'a>(spec: &KeySpec<PropertyId>, address: &'a [u8]) -> Result<Vec<&'a [u8]>, TreeError> {
    let key_len = address.len().checked_sub(EntityId::BYTE_LEN).ok_or(TreeError::InvalidKey("truncated leaf address"))?;
    CanonicalCodec::split(spec, &address[..key_len])
}

// A lexicographic cut has one interval for each earlier differing tuple part
// and one for the entity-id tie. All intervals use ascending physical bytes.
pub(crate) fn order_cut(
    spec: &KeySpec<PropertyId>,
    directions: &[IndexDirection],
    id_direction: IndexDirection,
    address: &[u8],
    before: bool,
) -> Result<Vec<TreeSpan>, TreeError> {
    let parts = address_parts(spec, address)?;
    let mut prefix = Vec::new();
    let mut spans = Vec::new();
    for (part, direction) in parts.iter().zip(directions) {
        let value = [prefix.as_slice(), part].concat();
        let tier = TreeSpan::starting_with(&prefix);
        let smaller = before == (*direction == IndexDirection::Asc);
        let span = if smaller {
            Some(TreeSpan::new(tier.start, Excluded(value.clone())))
        } else {
            prefix_end(&value).map(|start| TreeSpan::new(Included(start), tier.end))
        };
        if let Some(span) = span {
            spans.push(span);
        }
        prefix = value;
    }
    let tier = TreeSpan::starting_with(&prefix);
    let address = address.to_vec();
    let smaller = before == (id_direction == IndexDirection::Asc);
    let edge = if smaller {
        TreeSpan::new(tier.start, if before { Included(address) } else { Excluded(address) })
    } else {
        TreeSpan::new(if before { Included(address) } else { Excluded(address) }, tier.end)
    };
    spans.push(edge);
    Ok(TreeSpan::normalize(spans))
}

#[cfg(test)]
mod tests;
