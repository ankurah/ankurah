use std::future::Future;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::*;
use crate::common::{asc, item, tree_of, ItemTree, Property, Tag};
use crate::{MemoryStore, Ristretto255, Store, StoreTrx};
use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use futures::executor::block_on;

type TestRow = Row<Property, Tag, Ristretto255>;

/// A row of another kind than its key names is damage, which reads report.
#[test]
fn a_row_of_another_kind_than_its_key_is_inconsistent() {
    let store = MemoryStore::<RowKey, TestRow>::new();
    let mut trx = block_on(store.begin());
    let metadata = Row::Meta(Meta { key_spec: KeySpec::new(vec![]), group: 1 });
    block_on(trx.write(vec![WriteItem::Put(RowKey::Node(NodePrefix::root()), metadata)])).unwrap();
    block_on(trx.commit()).unwrap();

    let mut rows = TreeRows::new(block_on(store.begin_read()).unwrap());
    assert!(matches!(block_on(rows.node(&NodePrefix::root())), Err(Error::Inconsistent(_))));
    assert!(matches!(block_on(crate::tree::trie::Trie::new(&mut rows).root()), Err(Error::Inconsistent(_))));
}

/// 100 members under keys 0 to 99, a root and two buckets, whose first
/// bucket's row records where the root branches: by that record the bucket's
/// children are the bucket itself and its sibling.
async fn a_bucket_given_its_parents_record() -> ItemTree {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..100).map(|n| item!(n, n as i64, b"")));

    let Some(Row::Node(root)) = tree.stored_row(&RowKey::Node(NodePrefix::root())).await else { panic!("a root node") };
    let first = root.branches_at.clone().expect("a root of 100 members branches").child(false);
    let Some(Row::Node(bucket)) = tree.stored_row(&RowKey::Node(first.clone())).await else { panic!("a first bucket") };

    tree.damage(vec![WriteItem::Put(RowKey::Node(first), Row::Node(NodeRow { branches_at: root.branches_at, ..bucket }))]).await;
    tree
}

/// Far past the few milliseconds a walk over these 100 members takes.
const DEADLINE: Duration = Duration::from_secs(5);

/// What `walk` gives, run on a thread of its own, or past [`DEADLINE`] a
/// failure: over the memory store a walk that never ends never yields
/// either, so no timer sharing the walk's thread could end it.
fn before_deadline<T: Send + 'static>(walk: impl Future<Output = T> + Send + 'static) -> T {
    let (done, outcome) = mpsc::channel();
    thread::spawn(move || done.send(block_on(walk)));

    outcome.recv_timeout(DEADLINE).unwrap_or_else(|_| panic!("the walk over a record outside its node gave nothing within {DEADLINE:?}"))
}

/// A count across the bucket would follow its record back to the bucket
/// for ever; it reports the damage instead.
#[test]
fn a_read_refuses_a_record_of_where_a_node_branches_that_lies_outside_it() {
    let tree = block_on(a_bucket_given_its_parents_record());

    let count = before_deadline(async move { tree.view().count(0i64..5).await });
    assert!(matches!(count, Err(Error::Inconsistent(_))));
}

/// An insert beneath the bucket would walk from the bucket to the bucket for
/// ever, its lock held; it reports the damage instead.
#[test]
fn a_commit_refuses_a_record_of_where_a_node_branches_that_lies_outside_it() {
    let tree = block_on(a_bucket_given_its_parents_record());

    let mut trx = tree.transaction();
    trx.insert(item!(500, 2, b"")).unwrap();

    assert!(matches!(before_deadline(trx.commit()), Err(Error::Inconsistent(_))));
}
