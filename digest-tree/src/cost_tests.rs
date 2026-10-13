//! Count the ordered-map work, independently of an engine's timing or caches.
use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, Mutex};

use ankql::ast::Predicate;
use ankurah_core_types::{EntityId, IndexKeyPart, KeySpec, PropertyId, Value, ValueType};
use async_trait::async_trait;

use crate::*;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Cost {
    gets: usize,
    scans: usize,
    read_rows: usize,
    read_bytes: usize,
    puts: usize,
    deletes: usize,
    appends: usize,
    write_bytes: usize,
}
type Counts = Arc<Mutex<[Cost; 3]>>;
struct Counted {
    inner: MemoryTransaction,
    counts: Counts,
    suspend_after_put: bool,
}
impl Counted {
    fn scanned(&self, map: MapName, rows: &[(Vec<u8>, Vec<u8>)]) {
        let mut counts = self.counts.lock().unwrap();
        let cost = &mut counts[map as usize];
        cost.scans += 1;
        cost.read_rows += rows.len();
        cost.read_bytes += rows.iter().map(|(key, value)| key.len() + value.len()).sum::<usize>();
    }
}
#[async_trait]
impl MapAccess for Counted {
    fn poison(&mut self) { self.inner.poison(); }

    fn watch_structure(&mut self) { self.inner.watch_structure(); }
    fn change_structure(&mut self) { self.inner.change_structure(); }
    async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> {
        let value = self.inner.get(map, key).await?;
        let mut counts = self.counts.lock().unwrap();
        let cost = &mut counts[map as usize];
        cost.gets += 1;
        cost.read_rows += usize::from(value.is_some());
        cost.read_bytes += value.as_ref().map_or(0, |value| key.len() + value.len());
        Ok(value)
    }
    async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        let rows = self.inner.scan(map, bounds, limit).await?;
        self.scanned(map, &rows);
        Ok(rows)
    }
    async fn scan_reverse(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        let rows = self.inner.scan_reverse(map, bounds, limit).await?;
        self.scanned(map, &rows);
        Ok(rows)
    }
    async fn insert(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        {
            let mut counts = self.counts.lock().unwrap();
            counts[map as usize].puts += 1;
            counts[map as usize].write_bytes += key.len() + value.len();
        }
        self.inner.insert(map, key, value).await
    }
    async fn put_if(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<(), TreeError> {
        {
            let mut counts = self.counts.lock().unwrap();
            counts[map as usize].puts += 1;
            counts[map as usize].write_bytes += key.len() + value.len();
        }
        self.inner.put_if(map, key, expected, value).await?;
        if self.suspend_after_put {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    async fn delete_if(&mut self, map: MapName, key: &[u8], expected: &[u8]) -> Result<(), TreeError> {
        {
            let mut counts = self.counts.lock().unwrap();
            counts[map as usize].deletes += 1;
            counts[map as usize].write_bytes += key.len();
        }
        self.inner.delete_if(map, key, expected).await
    }
    async fn append(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        {
            let mut counts = self.counts.lock().unwrap();
            counts[map as usize].appends += 1;
            counts[map as usize].write_bytes += key.len() + 8 + value.len();
        }
        self.inner.append(map, key, value).await
    }
}

#[async_trait]
impl MapTransaction for Counted {
    async fn commit(self) -> Result<(), TreeError> { self.inner.commit().await }
}

fn spec() -> TreeSpec {
    TreeSpec::new(
        KeySpec::new(vec![IndexKeyPart::asc(PropertyId::EntityId(EntityId::from_bytes([1; 32])), ValueType::I64)]),
        Predicate::True,
    )
}
fn leaf(value: i64, version: u8) -> LeafData {
    let mut id = [0; 32];
    id[..8].copy_from_slice(&value.to_be_bytes());
    LeafData {
        key: CanonicalCodec.encode_prefix(spec().key_spec(), &[Value::I64(value)]).unwrap(),
        entity_id: EntityId::from_bytes(id),
        head: HeadHash::of([[version; 32]]),
    }
}
fn begin(maps: &MemoryMaps, counts: &Counts) -> TreeTransaction<Ristretto255, Counted> {
    let mut tx = TreeTransaction::new(Counted { inner: maps.begin(), counts: counts.clone(), suspend_after_put: false });
    tx.spec = Some(spec());
    tx
}
async fn populated(size: i64) -> (MemoryMaps, Counts) {
    let maps = MemoryMaps::new();
    let counts = Counts::default();
    let mut tx = begin(&maps, &counts);
    tx.build((0..size).map(|n| leaf(n, 0))).await.unwrap();
    tx.commit().await.unwrap();
    reset(&counts);
    (maps, counts)
}
fn reset(counts: &Counts) { *counts.lock().unwrap() = Default::default(); }

#[tokio::test]
async fn extending_a_compressed_branch_preserves_existing_node_rows() {
    for size in [256, 4096] {
        let (maps, counts) = populated(size).await;
        let mut tx = begin(&maps, &counts);
        let root = tx.node_base(&NodePrefix::root()).await.unwrap();
        reset(&counts);
        tx.apply_change(LeafChange { before: None, after: Some(leaf(1_000_000, 0)) }).await.unwrap();
        let measured = counts.lock().unwrap().clone();
        println!("gap insertion in {size} leaves: {measured:?}");
        assert_eq!(measured[MapName::Leaves as usize].read_rows, 0, "a new branch does not read existing leaves");
        assert_eq!(measured[MapName::Nodes as usize].deletes, 0, "existing summaries stay in place");
        assert!(measured[MapName::Nodes as usize].puts <= 2, "only the new branches need baselines");
        assert_eq!(tx.node_base(&NodePrefix::root()).await.unwrap(), root);
        tx.commit().await.unwrap();
        assert_eq!(begin(&maps, &counts).audit().await.unwrap().leaves, size as usize + 1);
    }
}

#[tokio::test]
async fn changing_only_a_head_leaves_lookup_and_multiplicity_untouched() {
    let (maps, counts) = populated(4096).await;
    let mut extra = leaf(5000, 0);
    extra.entity_id = leaf(100, 0).entity_id;
    let mut setup = begin(&maps, &counts);
    setup.apply_change(LeafChange { before: None, after: Some(extra) }).await.unwrap();
    setup.commit().await.unwrap();
    let mut tx = begin(&maps, &counts);
    reset(&counts);
    tx.apply_change(LeafChange { before: Some(leaf(100, 0)), after: Some(leaf(100, 1)) }).await.unwrap();
    let measured = counts.lock().unwrap().clone();
    println!("head replacement in 4097 leaves: {measured:?}");
    assert_eq!(measured[MapName::Lookup as usize], Cost::default());
    assert_eq!(measured[MapName::Leaves as usize].gets, 1);
    assert_eq!(measured[MapName::Leaves as usize].puts, 1);
    assert_eq!(measured[MapName::Leaves as usize].deletes, 0);
    assert_eq!(measured[MapName::Nodes as usize].puts, 0);
    assert_eq!(measured[MapName::Nodes as usize].deletes, 0);
    tx.commit().await.unwrap();
    begin(&maps, &counts).audit().await.unwrap();
}

#[tokio::test]
async fn bounded_reads_do_not_load_delta_history_from_an_excluded_branch() {
    let (maps, counts) = populated(128).await;
    let range = AddressRange::new(Included(leaf(0, 0).key), Excluded(leaf(2, 0).key));
    let mut tx = begin(&maps, &counts);
    let scope = tx.scope_ranges([range.clone()]);
    assert_eq!(tx.count(&scope).await.unwrap(), 2);
    let baseline = counts.lock().unwrap().clone();
    drop(tx);
    let mut tx = begin(&maps, &counts);
    for version in 1..=200 {
        tx.apply_change(LeafChange { before: Some(leaf(100, version - 1)), after: Some(leaf(100, version)) }).await.unwrap();
    }
    tx.commit().await.unwrap();
    let mut tx = begin(&maps, &counts);
    let scope = tx.scope_ranges([range]);
    reset(&counts);
    assert_eq!(tx.count(&scope).await.unwrap(), 2);
    let measured = counts.lock().unwrap().clone();
    println!("two-leaf count after 200 excluded updates: {measured:?}");
    assert_eq!(measured[MapName::Leaves as usize], baseline[MapName::Leaves as usize]);
    // Topology discovery may encounter a delta key to skip its entire run,
    // but must not read a number of rows proportional to the delta history.
    assert!(measured[MapName::Nodes as usize].read_rows <= baseline[MapName::Nodes as usize].read_rows + 2);
}

/// Stop after the adapter buffers a conditional write, before maintenance
/// finishes the related maps. Dropping this future models cancellation.
fn cancel_after_yield(future: impl std::future::Future) {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(future.as_mut().poll(&mut context).is_pending());
}

#[tokio::test]
async fn cancelled_mutations_cannot_commit_partial_work() {
    for operation in ["head", "build", "compact"] {
        let (maps, counts) = populated(128).await;
        let mut tx = begin(&maps, &counts);
        tx.apply_change(LeafChange { before: Some(leaf(100, 0)), after: Some(leaf(100, 1)) }).await.unwrap();
        tx.commit().await.unwrap();
        let mut snapshot = begin(&maps, &counts);
        let original = snapshot.node(&NodePrefix::root()).await.unwrap();
        let mut tx = begin(&maps, &counts);
        tx.transaction.suspend_after_put = true;
        match operation {
            "head" => cancel_after_yield(tx.apply_change(LeafChange { before: Some(leaf(100, 1)), after: Some(leaf(100, 2)) })),
            "build" => cancel_after_yield(tx.build((0..2048).map(|n| leaf(n, 2)))),
            "compact" => cancel_after_yield(tx.compact_node(&NodePrefix::root())),
            _ => unreachable!(),
        }
        // Even a later successful operation must not clear the poison.
        tx.transaction.suspend_after_put = false;
        tx.apply_change(LeafChange { before: None, after: None }).await.unwrap();
        assert!(matches!(tx.commit().await, Err(TreeError::Conflict)), "{operation}");
        let mut current = begin(&maps, &counts);
        assert_eq!(current.node(&NodePrefix::root()).await.unwrap(), original, "{operation}");
        assert_eq!(current.audit().await.unwrap().leaves, 128);
    }
}

#[tokio::test]
async fn branch_insertion_and_concurrent_head_updates_retry_safely() {
    for insertion_first in [true, false] {
        let (maps, counts) = populated(256).await;
        let insertion = LeafChange { before: None, after: Some(leaf(1_000_000, 0)) };
        let replacement = LeafChange { before: Some(leaf(100, 0)), after: Some(leaf(100, 1)) };
        let mut a = begin(&maps, &counts);
        let mut b = begin(&maps, &counts);
        a.apply_change(insertion.clone()).await.unwrap();
        b.apply_change(replacement.clone()).await.unwrap();
        let retry = if insertion_first {
            a.commit().await.unwrap();
            assert!(matches!(b.commit().await, Err(TreeError::Conflict)));
            replacement
        } else {
            b.commit().await.unwrap();
            assert!(matches!(a.commit().await, Err(TreeError::Conflict)));
            insertion
        };
        let mut tx = begin(&maps, &counts);
        tx.apply_change(retry).await.unwrap();
        tx.commit().await.unwrap();
        let mut tx = begin(&maps, &counts);
        assert_eq!(tx.audit().await.unwrap().leaves, 257);
        assert_eq!(tx.leaf(&leaf(100, 1).key, leaf(100, 1).entity_id).await.unwrap().unwrap().head, leaf(100, 1).head);
    }
}
