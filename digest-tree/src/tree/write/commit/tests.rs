use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;

use crate::common::{asc, counted, item, ten_items, Property};

/// A commit too small to lay the tree out anew reads no node but those its
/// walks reach: an insert into a tree of one bucket reads the root once.
#[tokio::test]
async fn a_commit_node_by_node_reads_the_root_once() {
    let (tree, counts) = counted(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), ten_items()).await;

    let mut trx = tree.transaction();
    trx.insert(item!(10, 10, b"")).unwrap();

    let before = counts.nodes_read();
    trx.commit().await.unwrap();

    assert_eq!(counts.nodes_read() - before, 1);
}
