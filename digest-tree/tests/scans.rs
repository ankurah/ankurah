//! Scans: range reads of a view's members, each a stream in the range's
//! direction that reads a batch at a time, each batch under the tree's read
//! lock for its own read alone.

mod common;

use std::ops::Bound;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::test_support::member_key;
use ankurah_digest_tree::{leaf_address, Error, Members, Ristretto255, ScanRange, Span};
use common::{asc, counted, item, tree_of, FailingStore, FailingTree, Id, Item, ItemStore, ItemTree, Property, Tag};
use futures::{StreamExt, TryStreamExt};

/// Item `n` of the long reads' tree: 2,500 items over 300 keys, so that each
/// key holds several members and a span can end among one key's members.
fn numbered(n: u64) -> Item { item!(n, (n % 300) as i64, &n.to_be_bytes()) }

/// Range reads of a tree large enough for one to reach its largest batch: of
/// a view of the whole tree and of a view of several spans, each over the
/// whole view and over a span that cuts it, and each in both directions. The
/// spans end between keys and among one key's members, so they cut buckets.
/// Each read yields exactly the members a model of the tree holds within its
/// spans, in its direction, so one view read both ways gives the same members
/// in opposite orders.
#[tokio::test]
async fn a_range_read_yields_every_member_within_its_spans_in_its_direction() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), (0..2500).map(numbered));
    let address = |n: u64| leaf_address(&member_key(&spec, &numbered(n)).unwrap(), &Id(n));

    let mut model: Vec<(Vec<u8>, Tag)> = (0..2500).map(|n| (address(n), Tag(n.to_be_bytes().to_vec()))).collect();
    model.sort_by(|a, b| a.0.cmp(&b.0));

    let several = vec![
        tree.span_for(10i64..90).unwrap(),
        // From after the fifth of key 112's members to the seventh of key
        // 279's.
        Span::new(Bound::Excluded(address(1312)), Bound::Included(address(2079))),
        tree.span_for((Bound::Excluded(285i64), Bound::Unbounded)).unwrap(),
    ];

    // From the third of key 50's members to short of the eighth of key 190's.
    let cutting = Span::new(Bound::Included(address(650)), Bound::Excluded(address(2290)));

    for spans in [vec![Span::all()], several] {
        let view = tree.select(spans.clone());

        for scanned in [Span::all(), cutting.clone()] {
            let within = |address: &Vec<u8>| scanned.contains(address) && spans.iter().any(|span| span.contains(address));
            let forward: Vec<(Vec<u8>, Tag)> = model.iter().filter(|(address, _)| within(address)).cloned().collect();
            let reverse: Vec<(Vec<u8>, Tag)> = forward.iter().rev().cloned().collect();

            for (range, expected) in [(ScanRange::forward(scanned.clone()), forward), (ScanRange::reverse(scanned.clone()), reverse)] {
                let read: Vec<(Vec<u8>, Tag)> =
                    view.range(range.clone()).map_ok(|leaf| (leaf_address(&leaf.key, &leaf.id), leaf.payload)).try_collect().await.unwrap();

                assert_eq!(read, expected, "{range:?} of {spans:?}");
            }
        }
    }
}

/// A Rust range of values reads the members whose values lie within it,
/// forward unless turned around, its included end covering every member that
/// holds that value.
#[tokio::test]
async fn a_range_of_values_reads_the_members_holding_them() {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..100).map(|n| item!(n, n as i64, b"")));

    let forward: Vec<Id> = tree.view().range(tree.span_for(10i64..20).unwrap()).map_ok(|leaf| leaf.id).try_collect().await.unwrap();
    let reverse = tree.view().range(ScanRange::reverse(tree.span_for(10i64..=20).unwrap()));
    let reverse: Vec<Id> = reverse.map_ok(|leaf| leaf.id).try_collect().await.unwrap();

    assert_eq!(forward, (10..20).map(Id).collect::<Vec<_>>());
    assert_eq!(reverse, (10..=20).rev().map(Id).collect::<Vec<_>>());
}

/// A range read reads only the batches it yields from: the first of 64
/// members, as many as a bucket holds, each after it twice as many as the one
/// before up to 1,024, and with each one member past it, which tells the read
/// whether more remain. So a read stopped after three members has read only
/// its first batch, 65 rows; one stopped after 65 members, two batches; and
/// one stopped after 2,000 members, six, the last two of 1,024.
#[tokio::test]
async fn a_range_read_stopped_early_reads_only_the_batches_it_yields_from() {
    let (tree, store) = counted(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..5000).map(|n| item!(n, n as i64, b""))).await;

    for (stopped_after, rows) in [(3, 65), (65, 65 + 129), (2000, 65 + 129 + 257 + 513 + 1025 + 1025)] {
        let before = store.rows_read();

        let read: Vec<Id> = tree.view().range(..).take(stopped_after).map_ok(|leaf| leaf.id).try_collect().await.unwrap();

        assert_eq!(read, (0..stopped_after as u64).map(Id).collect::<Vec<_>>());
        assert_eq!(store.rows_read() - before, rows, "stopped after {stopped_after}");
    }
}

/// A failed read ends a range read: the stream yields the members its first
/// batch read, then the store's failure, once, and then nothing more, even
/// once the store reads again.
#[tokio::test]
async fn a_failed_read_ends_the_stream_after_yielding_its_failure() {
    let store = FailingStore::new();
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree = FailingTree::build(store.clone(), spec, (0..100).map(|n| item!(n, n as i64, b""))).await.unwrap();
    let mut scan = tree.view().range(..);

    // The first batch's 64 members, read before the store fails.
    for n in 0..64 {
        assert_eq!(scan.next().await.unwrap().unwrap().id, Id(n));
    }

    store.fail_reads(true);

    match scan.next().await {
        Some(Err(Error::Store(failure))) => assert!(failure.is_retryable()),
        other => panic!("expected the store's failure, got {other:?}"),
    }

    store.fail_reads(false);
    assert!(scan.next().await.is_none(), "the stream ended with its failure");
}

/// A range read is `Unpin`, as stream adapters that poll a stream without
/// pinning it require: its batch under way sits pinned in a box of its own.
#[test]
fn a_range_read_needs_no_pinning() {
    fn is_unpin<T: Unpin>() {}

    is_unpin::<Members<Property, Id, Tag, Ristretto255, ItemStore>>();
}
