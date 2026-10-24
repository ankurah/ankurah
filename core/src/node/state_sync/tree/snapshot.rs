//! A frozen view of a resident's tree, selected for one query.

use super::{build_tree_over, key_paths, request_for, serves, tree_keys_rank_like_storage, Entries, Members, TreeEntry};
use crate::schema::catalog::CatalogManager;
use ankql::ast::{Resolved, Selection};
use ankurah_core_types::PropertyPath;
use ankurah_digest_tree::{CanonicalCodec, TreeRequest, TreeTransaction};
use ankurah_proto::{self as proto, EntityId, TreeSpec};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};

/// A frozen view of a resident's tree: the entries and members it held when
/// the view was taken, narrowed to one query's spans once selected, and
/// digests consistent with them.
pub(crate) struct Snapshot {
    /// The spec a comparison describes this view under: the tree's own, or a
    /// compatible spec the peer chose.
    pub spec: TreeSpec,
    /// The resident this view came from, kept alive while the view is.
    pub(crate) resident: Option<crate::node::state_sync::ResidentPin>,
    /// The spec the tree is built under, which addresses and digests follow.
    pub(super) physical_spec: Arc<TreeSpec>,
    /// Every entry the tree held, whatever spans the view is narrowed to.
    physical_entries: Arc<Entries>,
    pub entries: Arc<Entries>,
    digests: SnapshotDigests,
    /// The spans a selection narrowed the view to; none while the view is the whole tree.
    pub(super) ranges: Option<Vec<proto::TreeSpan>>,
    pub(super) members: Arc<Members>,
}

/// Where a snapshot reads its digests.
enum SnapshotDigests {
    /// A read view of digests over exactly the snapshot's entries.
    Read(TreeTransaction),
    /// Not read yet: the first digest read asks `resident` for its digest tree
    /// at `version`, and otherwise builds one from the snapshot's entries.
    Deferred { version: u64, resident: Option<DigestRequests> },
}

/// How a snapshot asks the resident it came from for the resident's digests.
#[derive(Clone)]
pub(crate) struct DigestRequests(mpsc::WeakSender<DigestRequest>);

/// One snapshot's request for its resident's digests, as of the tree version
/// the snapshot was taken at.
pub(crate) struct DigestRequest {
    pub(super) version: u64,
    response: oneshot::Sender<Option<TreeTransaction>>,
}

impl DigestRequests {
    /// Requests sent on `sender`, held only weakly: once the resident stops,
    /// its snapshots build their own digests.
    pub(crate) fn new(sender: &mpsc::Sender<DigestRequest>) -> Self { Self(sender.downgrade()) }

    async fn at(&self, version: u64) -> Option<TreeTransaction> {
        let sender = self.0.upgrade()?;
        let (response, answer) = oneshot::channel();
        sender.send(DigestRequest { version, response }).await.ok()?;
        drop(sender);
        answer.await.ok().flatten()
    }
}

impl DigestRequest {
    pub(super) fn answer(self, digests: Option<TreeTransaction>) { let _ = self.response.send(digests); }
}

#[derive(Debug, thiserror::Error)]
#[error("selection cannot be represented by this tree's keys")]
pub(crate) struct UnsupportedTreeSelection;

impl Snapshot {
    pub(super) fn new(
        spec: Arc<TreeSpec>,
        entries: Arc<Entries>,
        members: Arc<Members>,
        digests: Option<TreeTransaction>,
        version: u64,
    ) -> Self {
        let digests = match digests {
            Some(tree) => SnapshotDigests::Read(tree),
            None => SnapshotDigests::Deferred { version, resident: None },
        };
        Self {
            spec: (*spec).clone(),
            resident: None,
            physical_spec: spec,
            physical_entries: entries.clone(),
            entries,
            digests,
            ranges: None,
            members,
        }
    }

    /// Let this snapshot's first digest read ask its resident through `requests`.
    pub(crate) fn asking(mut self, requests: &DigestRequests) -> Self {
        if let SnapshotDigests::Deferred { resident, .. } = &mut self.digests {
            *resident = Some(requests.clone());
        }
        self
    }

    /// The digests this snapshot reads: its resident's, when the resident still
    /// holds this snapshot's entries, and otherwise digests built from them.
    pub(super) async fn digests(&mut self) -> anyhow::Result<&mut TreeTransaction> {
        if let SnapshotDigests::Deferred { version, resident } = &self.digests {
            let shared = match resident {
                Some(resident) => resident.at(*version).await,
                None => None,
            };
            let tree = match shared {
                Some(tree) => tree,
                None => build_tree_over(&self.physical_spec, &self.physical_entries).await?.begin(),
            };
            self.digests = SnapshotDigests::Read(tree);
        }
        match &mut self.digests {
            SnapshotDigests::Read(tree) => Ok(tree),
            SnapshotDigests::Deferred { .. } => unreachable!("a deferred read was resolved above"),
        }
    }

    pub fn cannot_select(&self, selection: &Selection<Resolved>, catalog: &CatalogManager) -> bool {
        if selection.limit == Some(0) || (self.physical_spec.predicate() == &selection.predicate && selection.limit.is_none()) {
            return false;
        }
        match request_for(selection, catalog) {
            Ok(request) if self.physical_spec.serves(&request).is_some() => {
                selection.limit.is_some() && !tree_keys_rank_like_storage(selection)
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
            members.extend(self.members.range::<[u8], _>(range.bounds()).map(|(address, id)| (address.clone(), *id)));
        }
        self.entries = Arc::new(members.values().map(|id| (*id, self.entries[id].clone())).collect());
        self.members = Arc::new(members);
        self.ranges = Some(ranges);
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
            for (_, id) in self.members.range::<[u8], _>(range.bounds()) {
                let entry = &self.entries[id];
                let parts = if order.is_empty() { Vec::new() } else { CanonicalCodec::split(self.physical_spec.key_spec(), entry.key())? };
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

    /// Each member's address and entity id, in address order.
    pub fn members(&self) -> impl Iterator<Item = (&[u8], EntityId)> + '_ {
        self.members.iter().map(|(address, id)| (address.as_ref(), *id))
    }

    /// Each member as a query reports it: its key and Clock, by entity id.
    pub fn query_members(&self) -> BTreeMap<EntityId, proto::QueryMember> {
        self.entries.iter().map(|(id, entry)| (*id, proto::QueryMember { key: entry.key().to_vec(), head: entry.head.clone() })).collect()
    }

    /// At most `limit` members within `bounds`, in address order.
    pub fn entries_in(&self, bounds: &proto::TreeSpan, limit: usize) -> impl Iterator<Item = (EntityId, &TreeEntry)> {
        self.members.range::<[u8], _>(bounds.bounds()).take(limit).map(|(_, id)| (*id, &self.entries[id]))
    }
}
