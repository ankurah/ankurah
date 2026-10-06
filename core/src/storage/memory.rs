//! An in-memory engine with the commit log and tree storage, so the core's
//! digest-tree code can be tested without a database. It keeps nothing
//! durable, and runs the conformance suite like any other engine.

use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    ops::Bound,
    sync::{Arc, Mutex},
};

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelId};
use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use super::{
    log::{CommitLog, LogError, LogIncarnation, LogPage, LogPosition, LogRow},
    tree::{
        leaf_address, AddressRange, BuildStatus, FoldedRow, HashedIndex, NodePrefix, NodeRow, Tombstone, TreeBatch, TreeBatchOutcome,
        TreeCell, TreeId, TreeRead, TreeRegistration, TreeStorage, TreeStorageError,
    },
    CommittedEntityWrite, StorageCommitOutcome, StorageCommitResult, StorageEngine, StorageTransaction,
};
use crate::{
    entity::TemporaryEntity,
    error::{MutationError, RetrievalError},
    selection::filter::evaluate_predicate,
};

pub(crate) struct MemoryStorageEngine {
    store: Mutex<Store>,
}

/// Everything a commit must see at once: entity states, events, the log, and
/// the registered trees whose keys its log rows carry.
struct Store {
    states: BTreeMap<EntityId, Attested<EntityState>>,
    events: BTreeMap<EventId, Attested<Event>>,
    log: Log,
    trees: BTreeMap<TreeId, Registered>,
    next_tree: u64,
    /// The prune horizon's offset in the log's incarnation.
    prune_horizon: u64,
}

struct Log {
    incarnation: LogIncarnation,
    /// The offset the next commit takes; every offset below it is settled.
    next: u64,
    /// The lowest retained offset.
    floor: u64,
    rows: BTreeMap<(u64, EntityId), LogRow>,
}

struct Registered {
    index: HashedIndex,
    contents: Arc<AsyncMutex<TreeContents>>,
}

/// One tree's cell and rows. A batch holds the lock from start to finish, so
/// its reads form a snapshot and it can write in place.
struct TreeContents {
    /// Set when the tree is unregistered or the store reset; handles then fail.
    removed: bool,
    cell: TreeCell,
    tables: Tables,
}

#[derive(Default)]
struct Tables {
    folded: BTreeMap<Vec<u8>, FoldedRow>,
    /// The entity-to-keys lookup, kept in step with `folded` by `set_row`.
    keys: BTreeMap<EntityId, BTreeSet<Vec<u8>>>,
    tombstones: BTreeMap<Vec<u8>, Tombstone>,
    /// Node rows keyed by [`NodePrefix::to_bytes`], so map order is prefix order.
    nodes: BTreeMap<Vec<u8>, NodeRow>,
}

impl Default for MemoryStorageEngine {
    fn default() -> Self { Self { store: Mutex::new(Store::new()) } }
}

impl MemoryStorageEngine {
    pub(crate) fn new() -> Self { Self::default() }

    fn contents(&self, tree: TreeId) -> Result<Arc<AsyncMutex<TreeContents>>, TreeStorageError> {
        let store = self.store.lock().unwrap();
        store.trees.get(&tree).map(|registered| registered.contents.clone()).ok_or(TreeStorageError::UnknownTree(tree))
    }
}

impl Store {
    fn new() -> Self {
        let mut store = Self {
            states: BTreeMap::new(),
            events: BTreeMap::new(),
            log: Log::new(),
            trees: BTreeMap::new(),
            next_tree: 0,
            prune_horizon: 0,
        };
        // Created with the store, the entity-id tree has every commit to fold
        // and nothing to fill.
        store.add_tree(HashedIndex::EntityId, BuildStatus::Ready);
        store
    }

    /// Empty the store under a new log incarnation, leaving only a fresh
    /// entity-id tree, and return the removed trees' contents for marking.
    /// Tree ids keep counting, so a stale handle never reaches a new tree.
    fn reset(&mut self) -> Vec<Arc<AsyncMutex<TreeContents>>> {
        let removed = mem::take(&mut self.trees).into_values().map(|registered| registered.contents).collect();
        self.states.clear();
        self.events.clear();
        self.log = Log::new();
        self.prune_horizon = 0;
        self.add_tree(HashedIndex::EntityId, BuildStatus::Ready);
        removed
    }

    fn stable(&self) -> LogPosition { LogPosition::new(self.log.incarnation, self.log.next) }

    /// Register a tree at the stable position, from which every commit carries
    /// its keys.
    fn add_tree(&mut self, index: HashedIndex, status: BuildStatus) -> TreeRegistration {
        let id = TreeId(self.next_tree);
        self.next_tree += 1;
        let cell = TreeCell { generation: 0, status, folded: self.stable() };
        self.prune_horizon = self.prune_horizon.max(self.log.next);
        let contents = Arc::new(AsyncMutex::new(TreeContents { removed: false, cell, tables: Tables::default() }));
        self.trees.insert(id, Registered { index: index.clone(), contents });
        TreeRegistration { id, index }
    }

    fn commit(
        &mut self,
        states: Vec<(Clock, Attested<EntityState>)>,
        events: Vec<Attested<Event>>,
    ) -> Result<StorageCommitOutcome, MutationError> {
        // The commit's order is fixed here, so it takes its position even if it
        // aborts below; readers meet gaps as they will on other engines.
        let position = self.stable();
        self.log.next += 1;

        let observed: BTreeMap<_, _> =
            states.iter().map(|(_, state)| (state.payload.entity_id, self.states.get(&state.payload.entity_id).cloned())).collect();
        let current_head = |id: &EntityId| observed[id].as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default();
        if states.iter().any(|(expected, state)| current_head(&state.payload.entity_id) != *expected) {
            return Ok(StorageCommitOutcome::Conflict { observed });
        }

        let rows = states.iter().map(|(_, state)| self.log_row(position, &state.payload)).collect::<Result<Vec<_>, _>>()?;
        let mut entities = Vec::with_capacity(states.len());
        for (_, state) in states {
            let entity_id = state.payload.entity_id;
            let canonical_changed =
                observed[&entity_id].as_ref().is_none_or(|previous| previous.payload.state.head != state.payload.state.head);
            entities.push(CommittedEntityWrite { entity_id, canonical_changed });
            self.states.insert(entity_id, state);
        }
        for event in events {
            self.events.insert(event.payload.id(), event);
        }
        let logged = !rows.is_empty();
        for row in rows {
            self.log.rows.insert((position.offset(), row.entity_id), row);
        }
        Ok(StorageCommitOutcome::Committed(StorageCommitResult { entities, position: logged.then_some(position) }))
    }

    /// The log row for one entity state, with its keys under every tree.
    fn log_row(&self, position: LogPosition, state: &EntityState) -> Result<LogRow, MutationError> {
        let entity = TemporaryEntity::new(state.entity_id, &state.state).map_err(MutationError::RetrievalError)?;
        let mut keys = BTreeMap::new();
        for (id, tree) in &self.trees {
            let tree_keys = tree.index.keys(state.entity_id, &entity).map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
            keys.insert(*id, tree_keys);
        }
        Ok(LogRow { position, entity_id: state.entity_id, head: state.state.head.clone(), keys })
    }
}

impl Log {
    fn new() -> Self { Self { incarnation: LogIncarnation::mint(), next: 0, floor: 0, rows: BTreeMap::new() } }

    fn check(&self, position: LogPosition) -> Result<(), LogError> {
        match position.incarnation() == self.incarnation {
            true => Ok(()),
            false => Err(LogError::IncarnationMismatch { requested: position.incarnation(), current: self.incarnation }),
        }
    }

    fn read(&self, from: LogPosition, limit: usize) -> Result<LogPage, LogError> {
        self.check(from)?;
        if from.offset() < self.floor {
            return Err(LogError::BelowRetentionFloor { floor: LogPosition::new(self.incarnation, self.floor) });
        }
        let mut rows: Vec<LogRow> = Vec::new();
        for ((offset, _), row) in self.rows.range((from.offset(), EntityId::from_bytes([0; 32]))..) {
            if let Some(last) = rows.last().filter(|last| rows.len() >= limit && last.position.offset() != *offset) {
                let next = last.position.next();
                return Ok(LogPage { rows, next });
            }
            rows.push(row.clone());
        }
        Ok(LogPage { rows, next: LogPosition::new(self.incarnation, self.next.max(from.offset())) })
    }

    fn discard_below(&mut self, position: LogPosition) -> Result<(), LogError> {
        self.check(position)?;
        let floor = position.offset().min(self.next);
        if floor > self.floor {
            self.rows = self.rows.split_off(&(floor, EntityId::from_bytes([0; 32])));
            self.floor = floor;
        }
        Ok(())
    }
}

impl TreeContents {
    fn live(&self, tree: TreeId) -> Result<&Tables, TreeStorageError> {
        match self.removed {
            false => Ok(&self.tables),
            true => Err(TreeStorageError::UnknownTree(tree)),
        }
    }

    fn remove(&mut self) {
        self.removed = true;
        self.tables = Tables::default();
    }
}

impl Tables {
    /// Write or remove the folded row at `address`, keeping the entity-to-keys
    /// lookup in step, and return the row it replaced.
    fn set_row(&mut self, address: Vec<u8>, row: Option<FoldedRow>) -> Option<FoldedRow> {
        match row {
            Some(row) => {
                self.keys.entry(row.entity_id).or_default().insert(row.key.clone());
                self.folded.insert(address, row)
            }
            None => {
                let previous = self.folded.remove(&address)?;
                if let Some(keys) = self.keys.get_mut(&previous.entity_id) {
                    keys.remove(&previous.key);
                    if keys.is_empty() {
                        self.keys.remove(&previous.entity_id);
                    }
                }
                Some(previous)
            }
        }
    }

    fn row(&self, key: &[u8], entity_id: EntityId) -> Option<FoldedRow> { self.folded.get(&leaf_address(key, entity_id)).cloned() }

    fn entity_rows(&self, entity_id: EntityId) -> Vec<FoldedRow> {
        let mut rows: Vec<FoldedRow> = self.keys.get(&entity_id).into_iter().flatten().filter_map(|key| self.row(key, entity_id)).collect();
        rows.sort_by_cached_key(FoldedRow::address);
        rows
    }

    fn rows(&self, range: &AddressRange, limit: usize) -> Vec<FoldedRow> { values_in(&self.folded, range, limit) }

    fn tombstones(&self, range: &AddressRange, limit: usize) -> Vec<Tombstone> { values_in(&self.tombstones, range, limit) }

    fn node(&self, prefix: &NodePrefix) -> Option<NodeRow> { self.nodes.get(&prefix.to_bytes()).cloned() }

    fn children(&self, prefix: &NodePrefix) -> Vec<(NodePrefix, NodeRow)> {
        let end = prefix.after_subtree().map_or(Bound::Unbounded, |after| Bound::Excluded(after.to_bytes()));
        let mut start = Bound::Excluded(prefix.to_bytes());
        let mut children = Vec::new();
        // The first stored row past each child's subtree is the next child.
        while let Some((bytes, node)) = self.nodes.range((start.clone(), end.clone())).next() {
            let child = NodePrefix::from_bytes(bytes).expect("node keys are encoded prefixes");
            let after = child.after_subtree();
            children.push((child, node.clone()));
            match after {
                Some(after) => start = Bound::Included(after.to_bytes()),
                None => break,
            }
        }
        children
    }
}

/// The first `limit` values of `map` whose keys lie in `range`.
fn values_in<V: Clone>(map: &BTreeMap<Vec<u8>, V>, range: &AddressRange, limit: usize) -> Vec<V> {
    // `BTreeMap::range` panics on an inverted range; such a range holds nothing.
    let inverted = match (&range.start, &range.end) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Excluded(start), Bound::Excluded(end)) => start >= end,
        (Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => start > end,
    };
    if inverted {
        return Vec::new();
    }
    map.range((range.start.clone(), range.end.clone())).take(limit).map(|(_, value)| value.clone()).collect()
}

pub(crate) struct MemoryTransaction<'a> {
    engine: &'a MemoryStorageEngine,
    states: Vec<(Clock, Attested<EntityState>)>,
    events: Vec<Attested<Event>>,
}

#[async_trait]
impl StorageTransaction for MemoryTransaction<'_> {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError> {
        self.events.extend_from_slice(events);
        Ok(())
    }

    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError> {
        match self.states.iter_mut().find(|(_, previous)| previous.payload.entity_id == state.payload.entity_id) {
            Some((_, previous)) if previous.payload.state.head != *expected_head => {
                Err(MutationError::InvalidUpdate("state does not follow the preceding transaction write"))
            }
            Some((_, previous)) => {
                *previous = state.clone();
                Ok(())
            }
            None => {
                self.states.push((expected_head.clone(), state.clone()));
                Ok(())
            }
        }
    }

    async fn commit(self) -> Result<StorageCommitOutcome, MutationError> {
        if self.states.is_empty() && self.events.is_empty() {
            return Ok(StorageCommitOutcome::Committed(StorageCommitResult::default()));
        }
        self.engine.store.lock().unwrap().commit(self.states, self.events)
    }
}

#[async_trait]
impl StorageEngine for MemoryStorageEngine {
    type Value = crate::value::Value;
    type Transaction<'a> = MemoryTransaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> { MemoryTransaction { engine: self, states: Vec::new(), events: Vec::new() } }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        self.store.lock().unwrap().states.get(&id).cloned().ok_or(RetrievalError::EntityNotFound(id))
    }

    async fn fetch_states(&self, selection: &Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        if selection.order_by.is_some() || selection.limit.is_some() {
            return Err(RetrievalError::Other("the in-memory engine answers only unordered, unlimited selections".into()));
        }
        let predicate = selection.predicate.assume_null(&[]);
        let states: Vec<_> = self.store.lock().unwrap().states.values().cloned().collect();
        let mut matching = Vec::new();
        for state in states {
            let matches = match &predicate {
                Predicate::MemberOf(model) => state.payload.state.memberships.contains(model),
                predicate => {
                    let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
                    evaluate_predicate(&entity, predicate)?
                }
            };
            if matches {
                matching.push(state);
            }
        }
        Ok(matching)
    }

    async fn get_events(&self, ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let store = self.store.lock().unwrap();
        Ok(ids.into_iter().filter_map(|id| store.events.get(&id).cloned()).collect())
    }

    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        Ok(self.store.lock().unwrap().events.values().filter(|event| event.payload.entity_id == id).cloned().collect())
    }

    async fn delete_all(&self) -> Result<bool, MutationError> {
        let (any_deleted, removed) = {
            let mut store = self.store.lock().unwrap();
            let any_deleted = !store.states.is_empty() || !store.events.is_empty() || !store.log.rows.is_empty();
            (any_deleted, store.reset())
        };
        for contents in removed {
            contents.lock().await.remove();
        }
        Ok(any_deleted)
    }

    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> {
        let store = self.store.lock().unwrap();
        let models: BTreeSet<ModelId> = store.states.values().flat_map(|state| state.payload.state.memberships.iter().copied()).collect();
        Ok(models.into_iter().collect())
    }
}

#[async_trait]
impl CommitLog for MemoryStorageEngine {
    async fn stable_position(&self) -> Result<LogPosition, LogError> { Ok(self.store.lock().unwrap().stable()) }

    /// Nothing here outlives the process, and a store that starts again
    /// starts a new incarnation, so within an incarnation every settled
    /// commit is as durable as this store gets.
    async fn durable_position(&self) -> Result<LogPosition, LogError> { Ok(self.store.lock().unwrap().stable()) }

    async fn retention_floor(&self) -> Result<LogPosition, LogError> {
        let store = self.store.lock().unwrap();
        Ok(LogPosition::new(store.log.incarnation, store.log.floor))
    }

    async fn discard_log_below(&self, position: LogPosition) -> Result<(), LogError> {
        self.store.lock().unwrap().log.discard_below(position)
    }

    async fn read_log(&self, from: LogPosition, limit: usize) -> Result<LogPage, LogError> {
        self.store.lock().unwrap().log.read(from, limit)
    }
}

#[async_trait]
impl TreeStorage for MemoryStorageEngine {
    type Reader<'a> = MemoryTreeReader;
    type Batch<'a> = MemoryTreeBatch<'a>;

    async fn register_tree(&self, index: HashedIndex) -> Result<TreeRegistration, TreeStorageError> {
        let mut store = self.store.lock().unwrap();
        if let Some((id, _)) = store.trees.iter().find(|(_, registered)| registered.index == index) {
            return Ok(TreeRegistration { id: *id, index });
        }
        Ok(store.add_tree(index, BuildStatus::Building))
    }

    async fn unregister_tree(&self, tree: TreeId) -> Result<(), TreeStorageError> {
        let contents = {
            let mut store = self.store.lock().unwrap();
            match store.trees.get(&tree) {
                None => return Err(TreeStorageError::UnknownTree(tree)),
                Some(registered) if registered.index == HashedIndex::EntityId => return Err(TreeStorageError::PermanentTree),
                Some(_) => store.trees.remove(&tree).expect("the tree is registered").contents,
            }
        };
        contents.lock().await.remove();
        Ok(())
    }

    async fn trees(&self) -> Result<Vec<TreeRegistration>, TreeStorageError> {
        let store = self.store.lock().unwrap();
        Ok(store.trees.iter().map(|(id, registered)| TreeRegistration { id: *id, index: registered.index.clone() }).collect())
    }

    async fn prune_horizon(&self) -> Result<LogPosition, TreeStorageError> {
        let store = self.store.lock().unwrap();
        Ok(LogPosition::new(store.log.incarnation, store.prune_horizon))
    }

    async fn reader(&self, tree: TreeId) -> Result<MemoryTreeReader, TreeStorageError> {
        Ok(MemoryTreeReader { tree, contents: self.contents(tree)? })
    }

    async fn batch(&self, tree: TreeId) -> Result<MemoryTreeBatch<'_>, TreeStorageError> {
        let contents = self.contents(tree)?.lock_owned().await;
        contents.live(tree)?;
        let begun = contents.cell;
        Ok(MemoryTreeBatch { engine: self, tree, contents, begun, undo: Vec::new(), horizon: None, committed: false })
    }
}

pub(crate) struct MemoryTreeReader {
    tree: TreeId,
    contents: Arc<AsyncMutex<TreeContents>>,
}

#[async_trait]
impl TreeRead for MemoryTreeReader {
    async fn cell(&self) -> Result<TreeCell, TreeStorageError> {
        let contents = self.contents.lock().await;
        contents.live(self.tree)?;
        Ok(contents.cell)
    }

    async fn row(&self, key: &[u8], entity_id: EntityId) -> Result<Option<FoldedRow>, TreeStorageError> {
        Ok(self.contents.lock().await.live(self.tree)?.row(key, entity_id))
    }

    async fn entity_rows(&self, entity_id: EntityId) -> Result<Vec<FoldedRow>, TreeStorageError> {
        Ok(self.contents.lock().await.live(self.tree)?.entity_rows(entity_id))
    }

    async fn rows(&self, range: &AddressRange, limit: usize) -> Result<Vec<FoldedRow>, TreeStorageError> {
        Ok(self.contents.lock().await.live(self.tree)?.rows(range, limit))
    }

    async fn tombstones(&self, range: &AddressRange, limit: usize) -> Result<Vec<Tombstone>, TreeStorageError> {
        Ok(self.contents.lock().await.live(self.tree)?.tombstones(range, limit))
    }

    async fn node(&self, prefix: &NodePrefix) -> Result<Option<NodeRow>, TreeStorageError> {
        Ok(self.contents.lock().await.live(self.tree)?.node(prefix))
    }

    async fn children(&self, prefix: &NodePrefix) -> Result<Vec<(NodePrefix, NodeRow)>, TreeStorageError> {
        Ok(self.contents.lock().await.live(self.tree)?.children(prefix))
    }
}

/// A batch writes in place under the tree's lock and keeps what each write
/// replaced, so a conflict or a drop can put everything back.
pub(crate) struct MemoryTreeBatch<'a> {
    engine: &'a MemoryStorageEngine,
    tree: TreeId,
    contents: OwnedMutexGuard<TreeContents>,
    /// The cell when the batch began, which the commit compares.
    begun: TreeCell,
    /// What each write replaced, oldest first.
    undo: Vec<Undo>,
    /// The prune horizon this batch raises when it commits.
    horizon: Option<LogPosition>,
    committed: bool,
}

enum Undo {
    Row(Vec<u8>, Option<FoldedRow>),
    Tombstone(Vec<u8>, Option<Tombstone>),
    Node(Vec<u8>, Option<NodeRow>),
    Cell(TreeCell),
    Tables(Tables),
}

impl MemoryTreeBatch<'_> {
    fn check(&self, position: LogPosition) -> Result<(), TreeStorageError> {
        let expected = self.contents.cell.folded.incarnation();
        match position.incarnation() == expected {
            true => Ok(()),
            false => Err(TreeStorageError::IncarnationMismatch { expected, found: position.incarnation() }),
        }
    }

    fn set_cell(&mut self, cell: TreeCell) {
        self.undo.push(Undo::Cell(self.contents.cell));
        self.contents.cell = cell;
    }

    fn raise_horizon(&mut self, position: LogPosition) {
        if self.horizon.is_none_or(|horizon| horizon < position) {
            self.horizon = Some(position);
        }
    }

    fn roll_back(&mut self) {
        while let Some(undo) = self.undo.pop() {
            let tables = &mut self.contents.tables;
            match undo {
                Undo::Row(address, previous) => {
                    tables.set_row(address, previous);
                }
                Undo::Tombstone(address, Some(previous)) => {
                    tables.tombstones.insert(address, previous);
                }
                Undo::Tombstone(address, None) => {
                    tables.tombstones.remove(&address);
                }
                Undo::Node(key, Some(previous)) => {
                    tables.nodes.insert(key, previous);
                }
                Undo::Node(key, None) => {
                    tables.nodes.remove(&key);
                }
                Undo::Cell(cell) => self.contents.cell = cell,
                Undo::Tables(previous) => *tables = previous,
            }
        }
        self.horizon = None;
    }
}

impl Drop for MemoryTreeBatch<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.roll_back();
        }
    }
}

#[async_trait]
impl TreeRead for MemoryTreeBatch<'_> {
    async fn cell(&self) -> Result<TreeCell, TreeStorageError> { Ok(self.contents.cell) }

    async fn row(&self, key: &[u8], entity_id: EntityId) -> Result<Option<FoldedRow>, TreeStorageError> {
        Ok(self.contents.tables.row(key, entity_id))
    }

    async fn entity_rows(&self, entity_id: EntityId) -> Result<Vec<FoldedRow>, TreeStorageError> {
        Ok(self.contents.tables.entity_rows(entity_id))
    }

    async fn rows(&self, range: &AddressRange, limit: usize) -> Result<Vec<FoldedRow>, TreeStorageError> {
        Ok(self.contents.tables.rows(range, limit))
    }

    async fn tombstones(&self, range: &AddressRange, limit: usize) -> Result<Vec<Tombstone>, TreeStorageError> {
        Ok(self.contents.tables.tombstones(range, limit))
    }

    async fn node(&self, prefix: &NodePrefix) -> Result<Option<NodeRow>, TreeStorageError> { Ok(self.contents.tables.node(prefix)) }

    async fn children(&self, prefix: &NodePrefix) -> Result<Vec<(NodePrefix, NodeRow)>, TreeStorageError> {
        Ok(self.contents.tables.children(prefix))
    }
}

#[async_trait]
impl TreeBatch for MemoryTreeBatch<'_> {
    async fn put_row(&mut self, row: FoldedRow) -> Result<(), TreeStorageError> {
        self.check(row.position)?;
        let address = row.address();
        let previous = self.contents.tables.set_row(address.clone(), Some(row));
        self.undo.push(Undo::Row(address, previous));
        Ok(())
    }

    async fn delete_row(&mut self, key: &[u8], entity_id: EntityId) -> Result<(), TreeStorageError> {
        let address = leaf_address(key, entity_id);
        let previous = self.contents.tables.set_row(address.clone(), None);
        self.undo.push(Undo::Row(address, previous));
        Ok(())
    }

    async fn put_tombstone(&mut self, tombstone: Tombstone) -> Result<(), TreeStorageError> {
        self.check(tombstone.position)?;
        let address = tombstone.address();
        let previous = self.contents.tables.tombstones.insert(address.clone(), tombstone);
        self.undo.push(Undo::Tombstone(address, previous));
        Ok(())
    }

    async fn prune_tombstones(&mut self, below: LogPosition) -> Result<(), TreeStorageError> {
        self.check(below)?;
        let tombstones = &mut self.contents.tables.tombstones;
        let pruned: Vec<Vec<u8>> =
            tombstones.iter().filter(|(_, tombstone)| tombstone.position < below).map(|(address, _)| address.clone()).collect();
        for address in pruned {
            let previous = tombstones.remove(&address);
            self.undo.push(Undo::Tombstone(address, previous));
        }
        self.raise_horizon(below);
        Ok(())
    }

    async fn put_node(&mut self, prefix: NodePrefix, node: NodeRow) -> Result<(), TreeStorageError> {
        self.check(node.watermark)?;
        let key = prefix.to_bytes();
        let previous = self.contents.tables.nodes.insert(key.clone(), node);
        self.undo.push(Undo::Node(key, previous));
        Ok(())
    }

    async fn delete_node(&mut self, prefix: &NodePrefix) -> Result<(), TreeStorageError> {
        let key = prefix.to_bytes();
        let previous = self.contents.tables.nodes.remove(&key);
        self.undo.push(Undo::Node(key, previous));
        Ok(())
    }

    async fn set_folded(&mut self, position: LogPosition) -> Result<(), TreeStorageError> {
        self.check(position)?;
        let cell = self.contents.cell;
        if position < cell.folded {
            return Err(TreeStorageError::FoldBackwards { current: cell.folded, requested: position });
        }
        self.set_cell(TreeCell { folded: position, ..cell });
        Ok(())
    }

    async fn publish(&mut self) -> Result<(), TreeStorageError> {
        let cell = self.contents.cell;
        self.set_cell(TreeCell { status: BuildStatus::Ready, ..cell });
        Ok(())
    }

    async fn restart(&mut self, folded: LogPosition) -> Result<(), TreeStorageError> {
        self.check(folded)?;
        let previous = mem::take(&mut self.contents.tables);
        self.undo.push(Undo::Tables(previous));
        let cell = self.contents.cell;
        self.set_cell(TreeCell { generation: cell.generation + 1, status: BuildStatus::Building, folded });
        self.raise_horizon(folded);
        Ok(())
    }

    async fn commit(mut self, expected: &TreeCell) -> Result<TreeBatchOutcome, TreeStorageError> {
        if *expected != self.begun {
            self.roll_back();
            return Ok(TreeBatchOutcome::Conflict { observed: self.begun });
        }
        {
            let mut store = self.engine.store.lock().unwrap();
            let registered =
                store.trees.get(&self.tree).is_some_and(|tree| Arc::ptr_eq(&tree.contents, OwnedMutexGuard::mutex(&self.contents)));
            if !registered {
                drop(store);
                self.roll_back();
                return Err(TreeStorageError::UnknownTree(self.tree));
            }
            if let Some(horizon) = self.horizon {
                store.prune_horizon = store.prune_horizon.max(horizon.offset());
            }
        }
        self.committed = true;
        Ok(TreeBatchOutcome::Committed(self.contents.cell))
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryStorageEngine;

    crate::tree_storage_conformance!(MemoryStorageEngine::new());
}
