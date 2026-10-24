//! A frozen view of a resident's tree, selected for one query.

use super::{key_paths, request_for, serves, Entries, Members, TreeEntry};
use crate::schema::catalog::CatalogManager;
use ankql::ast::{Resolved, Selection};
use ankurah_core_types::PropertyPath;
use ankurah_digest_tree::{CanonicalCodec, TreeRequest, TreeTransaction};
use ankurah_proto::{self as proto, EntityId, TreeSpec};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct Snapshot {
    // A peer may use a compatible admission without changing resident identity.
    pub spec: TreeSpec,
    pub(crate) resident: Option<crate::node::state_sync::ResidentPin>,
    pub(super) physical_spec: TreeSpec,
    pub entries: Arc<Entries>,
    pub(super) tree: TreeTransaction,
    pub(super) ranges: Option<Vec<proto::TreeSpan>>,
    pub(super) members: Arc<Members>,
    pub(super) missing_counts: Vec<usize>,
}

#[derive(Debug, thiserror::Error)]
#[error("selection cannot be represented by this tree's keys")]
pub(crate) struct UnsupportedTreeSelection;

impl Snapshot {
    pub fn cannot_select(&self, selection: &Selection<Resolved>, catalog: &CatalogManager) -> bool {
        if selection.limit == Some(0) || (self.physical_spec.predicate() == &selection.predicate && selection.limit.is_none()) {
            return false;
        }
        match request_for(selection, catalog) {
            Ok(request) if self.physical_spec.serves(&request).is_some() => {
                selection.limit.is_some() && self.has_descending_nulls(selection)
            }
            _ => true,
        }
    }

    /// Select a query view without changing the maintained tree. Scope gaps are
    /// empty query addresses even when the resident contains other leaves there.
    pub async fn select(mut self, selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<Self> {
        anyhow::ensure!(self.ranges.is_none(), "a query view cannot be selected again");
        if self.physical_spec.predicate() == &selection.predicate && selection.limit.is_none() {
            return Ok(self);
        }
        if selection.limit == Some(0) {
            self.restrict(Vec::new());
            return Ok(self);
        }
        anyhow::ensure!(!self.cannot_select(selection, catalog), UnsupportedTreeSelection);
        self.select_request(&request_for(selection, catalog)?).await
    }

    pub(super) fn has_descending_nulls(&self, selection: &Selection<Resolved>) -> bool {
        let paths = key_paths(&self.physical_spec);
        selection.order_by.iter().flatten().filter(|item| item.direction == ankql::ast::OrderDirection::Desc).any(|item| {
            item.path != PropertyPath::id()
                && !(item.path.subpath.is_empty()
                    && selection.predicate.assume_null(&[item.path.property_id()]) == ankql::ast::Predicate::False)
                && paths.iter().position(|path| path == &item.path).is_none_or(|index| self.missing_counts[index] != 0)
        })
    }

    pub(super) async fn select_request(mut self, request: &TreeRequest) -> anyhow::Result<Self> {
        let ranges = self.physical_spec.selected_spans(request, &self.members)?;
        self.restrict(ranges);
        Ok(self)
    }

    pub fn with_spec(mut self, spec: TreeSpec, selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.physical_spec.key_spec() == spec.key_spec()
                && serves(&self.physical_spec, selection, catalog)
                && serves(&spec, selection, catalog),
            "incompatible comparison tree spec"
        );
        self.spec = spec;
        Ok(self)
    }

    fn restrict(&mut self, ranges: Vec<proto::TreeSpan>) {
        let mut members = Members::new();
        for range in ranges.iter().filter(|range| !range.is_empty()) {
            members.extend(self.members.range((range.start.clone(), range.end.clone())).map(|(address, id)| (address.clone(), *id)));
        }
        self.entries = Arc::new(members.values().map(|id| (*id, self.entries[id].clone())).collect());
        self.members = Arc::new(members);
        self.ranges = Some(ranges);
    }

    /// Conservative resident accounting for head/key metadata and digest-map rows.
    pub fn accounted_bytes(&self) -> usize {
        self.entries.values().fold(4096usize, |total, entry| {
            total
                .saturating_add(1024)
                .saturating_add(entry.key.capacity().saturating_mul(4))
                .saturating_add(entry.head.len().saturating_mul(36))
        })
    }

    /// Order a view using its indexed key parts, without reading entity properties.
    pub fn ordered_ids(&self, selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<Vec<EntityId>> {
        if selection.limit == Some(0) {
            return Ok(Vec::new());
        }
        let limit = selection.limit.map_or(usize::MAX, |limit| usize::try_from(limit).unwrap_or(usize::MAX));
        if selection.order_by.as_ref().is_none_or(Vec::is_empty) && self.physical_spec.predicate() == &selection.predicate {
            return Ok(self.entries.keys().copied().take(limit).collect());
        }
        let mut membership = selection.clone();
        membership.order_by = None;
        membership.limit = None;
        let ranges = if self.physical_spec.predicate() == &membership.predicate {
            vec![proto::TreeSpan::all()]
        } else {
            self.physical_spec.selected_spans(&request_for(&membership, catalog)?, &self.members)?
        };
        let paths = key_paths(&self.physical_spec);
        let order = selection
            .order_by
            .iter()
            .flatten()
            .map(|item| {
                let part = if item.path == PropertyPath::id() {
                    None
                } else {
                    Some(paths.iter().position(|path| path == &item.path).ok_or(UnsupportedTreeSelection)?)
                };
                Ok((part, item.direction.clone()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut rows = Vec::new();
        for range in ranges.iter().filter(|range| !range.is_empty()) {
            for (_, id) in self.members.range((range.start.clone(), range.end.clone())) {
                let entry = &self.entries[id];
                let parts = if order.is_empty() { Vec::new() } else { CanonicalCodec::split(self.physical_spec.key_spec(), &entry.key)? };
                rows.push((*id, parts));
            }
        }
        rows.sort_unstable_by(|(left_id, left), (right_id, right)| {
            for (part, direction) in &order {
                let (compared, missing) = match part {
                    Some(index) => (left[*index].cmp(right[*index]), left[*index][0] == 0 || right[*index][0] == 0),
                    None => (left_id.cmp(right_id), false),
                };
                // Storage places missing values first in both directions.
                let compared = if *direction == ankql::ast::OrderDirection::Desc && !missing { compared.reverse() } else { compared };
                if !compared.is_eq() {
                    return compared;
                }
            }
            left_id.cmp(right_id)
        });
        rows.truncate(limit);
        Ok(rows.into_iter().map(|(id, _)| id).collect())
    }

    pub fn members(&self) -> Members { (*self.members).clone() }

    pub fn query_members(&self) -> BTreeMap<EntityId, proto::QueryMember> {
        self.members
            .iter()
            .map(|(address, id)| {
                (
                    *id,
                    proto::QueryMember { key: address[..address.len() - EntityId::BYTE_LEN].to_vec(), head: self.entries[id].head.clone() },
                )
            })
            .collect()
    }

    pub fn entries_in(&self, bounds: &proto::TreeSpan, limit: usize) -> impl Iterator<Item = (&[u8], &TreeEntry)> {
        self.members
            .range((bounds.start.clone(), bounds.end.clone()))
            .take(limit)
            .map(|(address, id)| (address.as_slice(), &self.entries[id]))
    }
}
