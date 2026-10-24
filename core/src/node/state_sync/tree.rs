use std::{collections::BTreeMap, sync::Arc};

use ankql::ast::{Resolved, Selection};
use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyPath, ValueType};
use ankurah_digest_tree::{
    leaf_address, CanonicalCodec, Digest, DigestTree, HeadHash, LeafChange, LeafInput, NodePrefix, PropertyTypes, TreeError, TreeRequest,
    TreeTransaction,
};
use ankurah_proto::{self as proto, Clock, EntityId, TreeSpanDigest, TreeSpec};

use crate::schema::catalog::CatalogManager;
#[cfg(test)]
use crate::{entity::TemporaryEntity, selection::filter::ValueLookup};
#[cfg(test)]
use ankurah_proto::{Attested, EntityState};

type Entries = BTreeMap<EntityId, TreeEntry>;
type Members = BTreeMap<Vec<u8>, EntityId>;

const OFFER_BYTES: u64 = 16 * 1024;
const OFFER_SPANS: usize = 128;
const OFFER_REFINEMENTS: usize = 32;
const BULK_REFRESH_CHANGES: usize = 128;
/// Builds and bulk rebuilds over at least this many leaves run on a blocking
/// thread: over memory maps they never yield, and at about 5 µs to hash a
/// leaf, or 2.4 µs to decode a stored one, they hold a thread for milliseconds.
#[cfg(not(target_arch = "wasm32"))]
const BLOCKING_BUILD_LEAVES: usize = 1024;

/// The entity metadata retained with a leaf. Payloads remain in entity/storage ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TreeEntry {
    pub entity_id: EntityId,
    pub key: Vec<u8>,
    pub head: Clock,
}

impl TreeEntry {
    fn address(&self) -> Vec<u8> { leaf_address(&self.key, self.entity_id) }

    fn leaf(&self) -> LeafInput {
        LeafInput { key: self.key.clone(), entity_id: self.entity_id, head: HeadHash::of(self.head.ids().map(|id| id.clone().to_bytes())) }
    }
}

/// A resident's fixed-spec index, updated by its reactor maintenance subscription.
/// Initialization and every subsequent update publish a complete digest and matching heads.
#[derive(Default)]
pub(crate) struct QueryTree(tokio::sync::Mutex<Option<Index>>);

struct Index {
    tree: DigestTree,
    entries: Arc<Entries>,
    members: Arc<Members>,
    missing_counts: Vec<usize>,
}

pub(crate) struct Snapshot {
    // A peer may use a compatible admission without changing resident identity.
    pub spec: TreeSpec,
    pub(crate) resident: Option<super::resident::ResidentPin>,
    physical_spec: TreeSpec,
    pub entries: Arc<Entries>,
    tree: TreeTransaction,
    ranges: Option<Vec<proto::TreeSpan>>,
    members: Arc<Members>,
    missing_counts: Vec<usize>,
}

#[derive(Debug, thiserror::Error)]
#[error("selection cannot be represented by this tree's keys")]
pub(crate) struct UnsupportedTreeSelection;

struct CatalogTypes<'a>(&'a CatalogManager);
impl PropertyTypes for CatalogTypes<'_> {
    fn value_type(&self, path: &PropertyPath) -> Option<ValueType> {
        if !path.subpath.is_empty() {
            return None;
        }
        match path.property_id() {
            proto::PropertyId::Id => Some(ValueType::EntityId),
            proto::PropertyId::System(property) => Some(crate::schema::resolver::system_property_value_type(property)),
            proto::PropertyId::EntityId(id) => {
                self.0.property_by_id(&id).ok().flatten().and_then(|property| ValueType::from_property_str(&property.value_type))
            }
        }
    }
}

impl QueryTree {
    pub fn spec(selection: &Selection<Resolved>, catalog: &CatalogManager) -> TreeSpec {
        // Entity-ID keys also support exact selections with expressions or JSON
        // paths that the scalar range planner cannot address.
        TreeSpec::for_selection(selection, &CatalogTypes(catalog)).unwrap_or_else(|error| {
            tracing::debug!(%error, "query uses entity-id digest keys");
            TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone())
        })
    }

    pub fn offered_spec(selection: &Selection<Resolved>, catalog: &CatalogManager, known: &proto::KnownState) -> anyhow::Result<TreeSpec> {
        anyhow::ensure!(Self::serves(&known.spec, selection, catalog), "unsupported comparison tree spec");
        Ok(known.spec.clone())
    }

    pub fn request(selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<TreeRequest> {
        let mut membership = selection.clone();
        if membership.limit.is_none() {
            // Unbounded query ordering does not affect digest membership and
            // must not require types for otherwise unused ordering paths.
            membership.order_by = None;
        }
        Ok(TreeRequest::for_selection(&membership, &CatalogTypes(catalog))?)
    }

    pub fn serves(spec: &TreeSpec, selection: &Selection<Resolved>, catalog: &CatalogManager) -> bool {
        let types = CatalogTypes(catalog);
        if Self::key_paths(spec).iter().zip(&spec.key_spec().keyparts).any(|(path, part)| types.value_type(path) != Some(part.value_type))
            || CanonicalCodec::encode_nullable(spec.key_spec(), &vec![None; spec.key_spec().keyparts.len()]).is_err()
        {
            return false;
        }
        // Unknown scalar types and opaque predicates use exact admission with
        // entity-ID keys. Limited views requiring property ordering are
        // selected through storage before their metadata is scoped.
        if spec.predicate() == &selection.predicate && (selection.limit.is_none() || spec.key_spec().keyparts.is_empty()) {
            return true;
        }
        Self::request(selection, catalog).ok().is_some_and(|request| spec.serves(&request).is_some())
    }

    pub fn key_paths(spec: &TreeSpec) -> Vec<PropertyPath> { spec.key_spec().keyparts.iter().map(IndexKeyPart::path).collect() }

    pub async fn refresh_entries(&self, spec: TreeSpec, entries: Vec<TreeEntry>) -> anyhow::Result<Snapshot> {
        let entries = checked_entries(&spec, entries)?;
        let mut current = self.0.lock().await;
        match current.as_mut() {
            Some(index) => {
                anyhow::ensure!(index.tree.spec() == &spec, "different TreeSpec requires a different tree");
                let mut changes = BTreeMap::new();
                for (id, before) in index.entries.iter() {
                    let after = entries.get(id);
                    if after != Some(before) {
                        changes.insert(*id, after.cloned());
                    }
                }
                for (id, after) in &entries {
                    if !index.entries.contains_key(id) {
                        changes.insert(*id, Some(after.clone()));
                    }
                }
                index.apply(changes).await?;
            }
            None => *current = Some(Index::build(spec, entries).await?),
        }
        Ok(current.as_ref().unwrap().snapshot())
    }

    pub async fn apply_entries(&self, entries: Vec<TreeEntry>, removed: Vec<EntityId>) -> anyhow::Result<Snapshot> {
        let mut current = self.0.lock().await;
        let index = current.as_mut().ok_or_else(|| anyhow::anyhow!("query tree is not initialized"))?;
        let entries = checked_entries(index.tree.spec(), entries)?;
        let mut changes = BTreeMap::new();
        for id in removed {
            if index.entries.contains_key(&id) {
                changes.insert(id, None);
            }
        }
        for (id, entry) in entries {
            if index.entries.get(&id) != Some(&entry) {
                changes.insert(id, Some(entry));
            } else {
                changes.remove(&id);
            }
        }
        index.apply(changes).await?;
        Ok(index.snapshot())
    }

    pub async fn current_snapshot(&self) -> anyhow::Result<Snapshot> {
        let current = self.0.lock().await;
        Ok(current.as_ref().ok_or_else(|| anyhow::anyhow!("query tree is not initialized"))?.snapshot())
    }

    #[cfg(test)]
    pub async fn refresh(&self, spec: TreeSpec, states: Vec<Attested<EntityState>>) -> anyhow::Result<Snapshot> {
        let entries = states.into_iter().map(|state| entry_from_state(&spec, &state)).collect::<anyhow::Result<Vec<_>>>()?;
        self.refresh_entries(spec, entries).await
    }
}

fn checked_entries(spec: &TreeSpec, entries: Vec<TreeEntry>) -> anyhow::Result<Entries> {
    let mut checked = Entries::new();
    for entry in entries {
        CanonicalCodec::split(spec.key_spec(), &entry.key)?;
        anyhow::ensure!(checked.insert(entry.entity_id, entry).is_none(), "duplicate tree entity");
    }
    Ok(checked)
}

fn missing_parts(spec: &TreeSpec, entry: &TreeEntry) -> Vec<usize> {
    CanonicalCodec::split(spec.key_spec(), &entry.key)
        .expect("tree entries have validated canonical keys")
        .iter()
        .enumerate()
        .filter_map(|(index, part)| (part[0] == 0).then_some(index))
        .collect()
}

impl Index {
    async fn build(spec: TreeSpec, entries: Entries) -> anyhow::Result<Self> {
        let leaves: Vec<LeafInput> = entries.values().map(TreeEntry::leaf).collect();
        let tree = run_blocking_if_large(leaves.len(), build_tree(spec.clone(), leaves)).await??;
        let mut missing_counts = vec![0; spec.key_spec().keyparts.len()];
        for entry in entries.values() {
            for index in missing_parts(&spec, entry) {
                missing_counts[index] += 1;
            }
        }
        let members = entries.values().map(|entry| (entry.address(), entry.entity_id)).collect();
        Ok(Self { tree, entries: Arc::new(entries), members: Arc::new(members), missing_counts })
    }

    async fn apply(&mut self, changes: BTreeMap<EntityId, Option<TreeEntry>>) -> anyhow::Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let entries = &self.entries;
        let leaf_changes = changes
            .iter()
            .map(|(id, after)| LeafChange { before: entries.get(id).map(TreeEntry::leaf), after: after.as_ref().map(TreeEntry::leaf) });
        let mut tx = self.tree.begin();
        if changes.len() >= BULK_REFRESH_CHANGES && changes.len() >= entries.len().div_ceil(4) {
            // A batch this large costs less to fold in by rebuilding every
            // tree node from the stored leaves than change by change.
            let leaf_changes: Vec<LeafChange> = leaf_changes.collect();
            let leaves = entries.len() + leaf_changes.len();
            tx = run_blocking_if_large(leaves, async move { tx.rebuild_with_changes(leaf_changes).await.map(|()| tx) }).await??;
        } else {
            tx.apply_changes(leaf_changes).await?;
        }
        // Memory commit has no suspension after publication; metadata is replaced
        // under the same owner lock before another reader can capture the index.
        tx.commit().await?;
        let entries = Arc::make_mut(&mut self.entries);
        let members = Arc::make_mut(&mut self.members);
        for (id, after) in changes {
            if let Some(before) = entries.remove(&id) {
                members.remove(&before.address());
                for index in missing_parts(self.tree.spec(), &before) {
                    self.missing_counts[index] -= 1;
                }
            }
            if let Some(after) = after {
                members.insert(after.address(), id);
                for index in missing_parts(self.tree.spec(), &after) {
                    self.missing_counts[index] += 1;
                }
                entries.insert(id, after);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            spec: self.tree.spec().clone(),
            resident: None,
            physical_spec: self.tree.spec().clone(),
            tree: self.tree.begin(),
            ranges: None,
            entries: self.entries.clone(),
            members: self.members.clone(),
            missing_counts: self.missing_counts.clone(),
        }
    }
}

/// Await `work`, an operation on a tree over memory maps that hashes or decodes
/// about `leaves` leaves, on a blocking thread when they number at least
/// [`BLOCKING_BUILD_LEAVES`], so that it holds no executor thread while it runs.
#[cfg(not(target_arch = "wasm32"))]
async fn run_blocking_if_large<R: Send + 'static>(
    leaves: usize,
    work: impl std::future::Future<Output = R> + Send + 'static,
) -> anyhow::Result<R> {
    if leaves < BLOCKING_BUILD_LEAVES {
        return Ok(work.await);
    }
    Ok(tokio::task::spawn_blocking(move || futures::executor::block_on(work)).await?)
}

/// Await `work` where it is: wasm32 has no thread to block.
#[cfg(target_arch = "wasm32")]
async fn run_blocking_if_large<R>(_leaves: usize, work: impl std::future::Future<Output = R>) -> anyhow::Result<R> { Ok(work.await) }

/// A tree defined by `spec` holding `leaves`.
async fn build_tree(spec: TreeSpec, leaves: Vec<LeafInput>) -> Result<DigestTree, TreeError> {
    let tree = DigestTree::new(spec);
    let mut tx = tree.begin();
    tx.build(leaves).await?;
    tx.commit().await?;
    Ok(tree)
}

#[cfg(test)]
fn entry_from_state(spec: &TreeSpec, state: &Attested<EntityState>) -> anyhow::Result<TreeEntry> {
    let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
    let values = QueryTree::key_paths(spec).iter().map(|path| entity.value_at(path)).collect::<Vec<_>>();
    Ok(TreeEntry {
        entity_id: state.payload.entity_id,
        key: CanonicalCodec::encode_nullable(spec.key_spec(), &values)?,
        head: state.payload.state.head.clone(),
    })
}

impl Snapshot {
    pub fn cannot_select(&self, selection: &Selection<Resolved>, catalog: &CatalogManager) -> bool {
        if selection.limit == Some(0) || (self.physical_spec.predicate() == &selection.predicate && selection.limit.is_none()) {
            return false;
        }
        match QueryTree::request(selection, catalog) {
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
        self.select_request(&QueryTree::request(selection, catalog)?).await
    }

    fn has_descending_nulls(&self, selection: &Selection<Resolved>) -> bool {
        let paths = QueryTree::key_paths(&self.physical_spec);
        selection.order_by.iter().flatten().filter(|item| item.direction == ankql::ast::OrderDirection::Desc).any(|item| {
            item.path != PropertyPath::id()
                && !(item.path.subpath.is_empty()
                    && selection.predicate.assume_null(&[item.path.property_id()]) == ankql::ast::Predicate::False)
                && paths.iter().position(|path| path == &item.path).is_none_or(|index| self.missing_counts[index] != 0)
        })
    }

    async fn select_request(mut self, request: &TreeRequest) -> anyhow::Result<Self> {
        let ranges = self.physical_spec.selected_spans(request, &self.members)?;
        self.restrict(ranges);
        Ok(self)
    }

    pub fn with_spec(mut self, spec: TreeSpec, selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.physical_spec.key_spec() == spec.key_spec()
                && QueryTree::serves(&self.physical_spec, selection, catalog)
                && QueryTree::serves(&spec, selection, catalog),
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

    pub async fn span_digest(&mut self, bounds: proto::TreeSpan) -> anyhow::Result<TreeSpanDigest> {
        let ranges = match &self.ranges {
            Some(ranges) => ranges.iter().filter_map(|range| range.intersect(&bounds)).collect::<Vec<_>>(),
            None => vec![bounds.clone()],
        };
        if ranges.is_empty() {
            return Ok(TreeSpanDigest { prefix: NodePrefix::root(), bounds, digest: Digest::empty() });
        }
        let tree = &mut self.tree;
        let scope = tree.scope_spans(ranges);
        Ok(TreeSpanDigest { prefix: NodePrefix::root(), bounds, digest: tree.digest(&scope).await? })
    }

    pub async fn offer(&mut self) -> anyhow::Result<proto::KnownState> {
        if let Some(inventory) = self.initial_inventory() {
            return Ok(inventory);
        }
        // A bounded disjoint frontier recognizes both equal inventories and
        // small changed spans without serial refinement exchanges.
        let root = self.span_digest(super::whole()).await?;
        let mut frontier = vec![(root, true)];
        for _ in 0..OFFER_REFINEMENTS {
            let Some(index) = frontier
                .iter()
                .enumerate()
                .filter(|(_, (head, expandable))| *expandable && head.digest.count() > super::HEAD_PAGE as i64)
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
            if candidate.len() > OFFER_SPANS {
                continue;
            }
            let inventory = proto::KnownState {
                spec: self.spec.clone(),
                spans: candidate.iter().map(|(head, _)| proto::SpanContent::Digest(head.clone())).collect(),
            };
            if bincode::serialized_size(&inventory)? <= OFFER_BYTES {
                frontier = candidate;
            }
        }
        Ok(proto::KnownState {
            spec: self.spec.clone(),
            spans: frontier.into_iter().map(|(head, _)| proto::SpanContent::Digest(head)).collect(),
        })
    }

    fn initial_inventory(&self) -> Option<proto::KnownState> {
        if self.members.len() > super::INITIAL_INVENTORY_HEADS {
            return None;
        }
        let mut minimum_bytes = 0usize;
        for (address, id) in self.members.iter() {
            let head = &self.entries[id].head;
            minimum_bytes = minimum_bytes.saturating_add(address.len()).saturating_add(head.len().saturating_mul(32));
            if minimum_bytes > super::INITIAL_INVENTORY_BYTES {
                return None;
            }
        }
        let heads = proto::PackedHeads::from_rows(
            self.members.iter().map(|(address, id)| (address[..address.len() - EntityId::BYTE_LEN].as_ref(), *id, &self.entries[id].head)),
        );
        let inventory = proto::KnownState {
            spec: self.spec.clone(),
            spans: vec![proto::SpanContent::Heads(proto::TreeSpanInventory { bounds: super::whole(), heads })],
        };
        super::budget::offer_bytes(&inventory).ok().filter(|size| *size <= super::INITIAL_INVENTORY_BYTES as u64).map(|_| inventory)
    }

    pub fn page(&self, bounds: proto::TreeSpan) -> anyhow::Result<proto::TreeSpanInventory> {
        use std::ops::Bound;
        let empty = match (&bounds.start, &bounds.end) {
            (Bound::Included(start), Bound::Included(end)) => start > end,
            (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => start >= end,
            _ => false,
        };
        let rows = if empty {
            Vec::new()
        } else {
            self.members.range((bounds.start.clone(), bounds.end.clone())).take(super::HEAD_PAGE + 1).collect::<Vec<_>>()
        };
        let mut count = 0;
        let mut bytes = 0;
        for (index, (address, id)) in rows.iter().take(super::HEAD_PAGE).enumerate() {
            let head = &self.entries[*id].head;
            let end = if index + 1 == rows.len() { bounds.end.clone() } else { Bound::Included((*address).clone()) };
            // Preserve the decoded-work budget even when repeated keys compress well.
            let overhead = 4 + bincode::serialized_size(&proto::TreeSpan::new(bounds.start.clone(), end))? + 8;
            let size = 8 + address.len() as u64 + 8 + 36 * head.len() as u64;
            if overhead + bytes + size > super::PAGE_BYTES as u64 {
                anyhow::ensure!(count > 0, "single head inventory exceeds byte limit");
                break;
            }
            bytes += size;
            count += 1;
        }
        let end = if count == rows.len() { bounds.end } else { Bound::Included(rows[count - 1].0.clone()) };
        let heads = proto::PackedHeads::from_rows(
            rows.iter()
                .take(count)
                .map(|(address, id)| (address[..address.len() - EntityId::BYTE_LEN].as_ref(), **id, &self.entries[*id].head)),
        );
        let page = proto::TreeSpanInventory { bounds: proto::TreeSpan::new(bounds.start, end), heads };
        anyhow::ensure!(
            bincode::serialized_size(&proto::StateAnswer::Heads(page.clone()))? <= super::PAGE_BYTES as u64,
            "head inventory bounds exceed byte limit"
        );
        Ok(page)
    }

    pub async fn refine(&mut self, head: &TreeSpanDigest) -> anyhow::Result<Vec<TreeSpanDigest>> {
        if self.ranges.as_ref().is_some_and(Vec::is_empty) {
            return Ok(vec![TreeSpanDigest { digest: Digest::empty(), ..head.clone() }]);
        }
        let mut children = self.tree.refine(head).await?;
        if self.ranges.is_some() {
            for child in &mut children {
                child.digest = self.span_digest(child.bounds.clone()).await?.digest;
            }
        }
        Ok(children)
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
            vec![super::whole()]
        } else {
            self.physical_spec.selected_spans(&QueryTree::request(&membership, catalog)?, &self.members)?
        };
        let paths = QueryTree::key_paths(&self.physical_spec);
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

#[cfg(test)]
mod tests;
