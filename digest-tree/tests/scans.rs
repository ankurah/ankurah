//! Scans of a view's members: a stream in the view's direction that reads a
//! batch at a time, each batch under the tree's read lock for its own read
//! alone.

mod common;

use std::ops::Bound;

use ankurah_digest_tree::{leaf_address, DigestTree, Error, Ristretto255, Row, ScanDirection, Span, ViewPlan};
use common::{built, counted, integer_key, integer_spec, item, FailingReads, Id, Item, Spec, Tag};
use futures::{StreamExt, TryStreamExt};

/// A tree of items over a store whose reads can fail.
type FailingReadsTree = DigestTree<Spec, Id, Tag, Ristretto255, FailingReads<Row<Spec, Tag, Ristretto255>>>;

/// Item `n` of the long scans' tree: 2,500 items over 300 keys, so that each
/// key holds several members and a span can end among one key's members.
fn numbered(n: u64) -> Item { item(n, Some((n % 300) as i64), &n.to_be_bytes()) }

/// The address of item `n` of the long scans' tree.
fn address(n: u64) -> Vec<u8> { leaf_address(&integer_key(Some((n % 300) as i64)), &Id(n)) }

/// Scans of a tree large enough for a scan to reach its largest batch, in
/// both directions, of a view of the whole tree and of a view of several
/// spans, each over the whole view and over a span that cuts it. The spans
/// end between keys and among one key's members, so they cut buckets. Each
/// scan yields exactly the members a model of the tree holds within its
/// spans, in the view's order.
#[tokio::test]
async fn a_scan_yields_every_member_within_its_spans_in_the_views_order() {
    let tree = built::<Ristretto255>(integer_spec(), (0..2500).map(numbered)).await;

    let mut model: Vec<(Vec<u8>, Tag)> = (0..2500).map(|n| (address(n), Tag(n.to_be_bytes().to_vec()))).collect();
    model.sort_by(|a, b| a.0.cmp(&b.0));

    let key = |key: i64| integer_key(Some(key));
    let several = vec![
        Span::for_key_range(Bound::Included(&key(10)), Bound::Excluded(&key(90))),
        // From after the fifth of key 112's members to the seventh of key
        // 279's.
        Span::new(Bound::Excluded(address(1312)), Bound::Included(address(2079))),
        Span::for_key_range(Bound::Excluded(&key(285)), Bound::Unbounded),
    ];

    // From the third of key 50's members to short of the eighth of key 190's.
    let cutting = Span::new(Bound::Included(address(650)), Bound::Excluded(address(2290)));

    for spans in [vec![Span::all()], several] {
        for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
            let view = tree.select(&ViewPlan { spans: spans.clone(), direction, limit: None }).await.unwrap();

            for scanned in [Span::all(), cutting.clone()] {
                let read: Vec<(Vec<u8>, Tag)> =
                    view.members(&scanned).map_ok(|leaf| (leaf_address(&leaf.key, &leaf.id), leaf.payload)).try_collect().await.unwrap();

                let within = |address: &Vec<u8>| scanned.contains(address) && spans.iter().any(|span| span.contains(address));
                let mut expected: Vec<(Vec<u8>, Tag)> = model.iter().filter(|(address, _)| within(address)).cloned().collect();

                if direction == ScanDirection::Reverse {
                    expected.reverse();
                }

                assert_eq!(read, expected, "{direction:?} over {scanned:?} of {spans:?}");
            }
        }
    }
}

/// A scan reads only the batches it yields from: the first of 64 members, as
/// many as a bucket holds, each after it twice as many as the one before up
/// to 1,024, and with each one member past it, which tells the scan whether
/// more remain. So a scan stopped after three members has read only its
/// first batch, 65 rows; one stopped after 65 members, two batches; and one
/// stopped after 2,000 members, six, the last two of 1,024.
#[tokio::test]
async fn a_scan_stopped_early_reads_only_the_batches_it_yields_from() {
    let (tree, store) = counted(integer_spec(), (0..5000).map(|n| item(n, Some(n as i64), b""))).await;

    for (stopped_after, rows) in [(3, 65), (65, 65 + 129), (2000, 65 + 129 + 257 + 513 + 1025 + 1025)] {
        let before = store.rows_read();
        let read: Vec<Id> = tree.view().members(&Span::all()).take(stopped_after).map_ok(|leaf| leaf.id).try_collect().await.unwrap();

        assert_eq!(read, (0..stopped_after as u64).map(Id).collect::<Vec<_>>());
        assert_eq!(store.rows_read() - before, rows, "stopped after {stopped_after}");
    }
}

/// A failed read ends a scan: the stream yields the members its first batch
/// read, then the store's failure, once, and then nothing more, even once
/// the store reads again.
#[tokio::test]
async fn a_failed_read_ends_the_scan_after_yielding_its_failure() {
    let store = FailingReads::new();
    let tree = FailingReadsTree::build(store.clone(), integer_spec(), (0..100).map(|n| item(n, Some(n as i64), b""))).await.unwrap();
    let mut scan = tree.view().members(&Span::all());

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
    assert!(scan.next().await.is_none(), "the scan ended with its failure");
}
