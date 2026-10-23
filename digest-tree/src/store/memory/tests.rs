use super::*;
use std::ops::Bound::{Excluded, Included, Unbounded};
use MapName::{Leaves, Lookup, Nodes};

/// Some entries, as text.
fn text(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(String, String)> {
    entries.into_iter().map(|(key, value)| (String::from_utf8(key).unwrap(), String::from_utf8(value).unwrap())).collect()
}

/// The keys of some entries, as text.
fn keys(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<String> { text(entries).into_iter().map(|(key, _)| key).collect() }

#[tokio::test]
async fn each_conditional_write_succeeds_on_its_condition_and_conflicts_otherwise() {
    let maps = MemoryMaps::new();
    let mut transaction = maps.begin();
    assert_eq!(transaction.get(Leaves, b"k").await.unwrap(), None);
    transaction.insert(Leaves, b"k", b"first").await.unwrap();
    assert_eq!(transaction.get(Leaves, b"k").await.unwrap(), Some(b"first".to_vec()), "its own write");
    transaction.put_if(Leaves, b"k", Some(b"first"), b"second").await.unwrap();
    transaction.put_if(Leaves, b"new", None, b"value").await.unwrap();
    transaction.put_if(Leaves, b"gone", None, b"value").await.unwrap();
    transaction.delete_if(Leaves, b"gone", b"value").await.unwrap();
    transaction.put_if(Leaves, b"", None, b"").await.unwrap();
    transaction.commit().await.unwrap();

    let mut transaction = maps.begin();
    assert_eq!(transaction.get(Leaves, b"k").await.unwrap(), Some(b"second".to_vec()));
    assert_eq!(transaction.get(Leaves, b"new").await.unwrap(), Some(b"value".to_vec()));
    assert_eq!(transaction.get(Leaves, b"gone").await.unwrap(), None);
    assert_eq!(transaction.get(Leaves, b"").await.unwrap(), Some(Vec::new()), "the empty key and the empty value are values");
    assert_eq!(transaction.get(Lookup, b"k").await.unwrap(), None, "each map has keys of its own");

    // A condition that fails conflicts at once and fails the commit, so
    // nothing the transaction wrote lands.
    for condition in [
        "insert needs the key absent",
        "put_if of absent finds a value",
        "put_if finds another value",
        "put_if of a value finds none",
        "delete_if finds another value",
        "delete_if finds none",
    ] {
        let mut transaction = maps.begin();
        transaction.insert(Lookup, b"written first", b"x").await.unwrap();
        let failed = match condition {
            "insert needs the key absent" => transaction.insert(Leaves, b"k", b"again").await,
            "put_if of absent finds a value" => transaction.put_if(Leaves, b"k", None, b"x").await,
            "put_if finds another value" => transaction.put_if(Leaves, b"k", Some(b"other"), b"x").await,
            "put_if of a value finds none" => transaction.put_if(Leaves, b"absent", Some(b"x"), b"y").await,
            "delete_if finds another value" => transaction.delete_if(Leaves, b"k", b"other").await,
            _ => transaction.delete_if(Leaves, b"absent", b"x").await,
        };
        assert!(matches!(failed, Err(TreeError::Conflict)), "{condition}");
        assert!(matches!(transaction.commit().await, Err(TreeError::Conflict)), "{condition}");
        assert_eq!(maps.begin().get(Lookup, b"written first").await.unwrap(), None, "{condition}");
    }
    assert_eq!(maps.begin().get(Leaves, b"k").await.unwrap(), Some(b"second".to_vec()));
}

/// A transaction reads the maps it began with, whatever commits land
/// meanwhile, and its own writes over them, in gets and scans alike.
#[tokio::test]
async fn a_transaction_reads_the_maps_it_began_with_and_its_own_writes() {
    let maps = MemoryMaps::new();
    let mut setup = maps.begin();
    setup.insert(Nodes, b"a", b"1").await.unwrap();
    setup.insert(Nodes, b"c", b"3").await.unwrap();
    setup.commit().await.unwrap();

    let mut reader = maps.begin();
    let mut writer = maps.begin();
    writer.put_if(Nodes, b"a", Some(b"1"), b"one").await.unwrap();
    writer.insert(Nodes, b"b", b"2").await.unwrap();
    writer.delete_if(Nodes, b"c", b"3").await.unwrap();
    writer.commit().await.unwrap();

    let all = (Unbounded, Unbounded);
    assert_eq!(reader.get(Nodes, b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(text(reader.scan(Nodes, all, usize::MAX).await.unwrap()), [("a".into(), "1".into()), ("c".into(), "3".into())]);

    // A new key, a replaced value and a removal, over what it began with.
    reader.insert(Nodes, b"d", b"4").await.unwrap();
    reader.put_if(Nodes, b"a", Some(b"1"), b"uno").await.unwrap();
    reader.delete_if(Nodes, b"c", b"3").await.unwrap();
    assert_eq!(text(reader.scan(Nodes, all, usize::MAX).await.unwrap()), [("a".into(), "uno".into()), ("d".into(), "4".into())]);
    assert_eq!(keys(reader.scan(Nodes, all, 1).await.unwrap()), ["a"], "the limit counts what it sees");
    assert_eq!(reader.get(Nodes, b"c").await.unwrap(), None);

    // The writer changed what it expected, so none of its writes land.
    assert!(matches!(reader.commit().await, Err(TreeError::Conflict)));
    let after = text(maps.begin().scan(Nodes, all, usize::MAX).await.unwrap());
    assert_eq!(after, [("a".into(), "one".into()), ("b".into(), "2".into())]);
}

/// Two transactions that write one key, in different maps from their
/// other writes: the first commit lands whole, and the second fails whole.
#[tokio::test]
async fn a_commit_applies_all_its_writes_across_the_maps_or_none() {
    let maps = MemoryMaps::new();
    let mut first = maps.begin();
    let mut second = maps.begin();
    first.insert(Leaves, b"first row", b"1").await.unwrap();
    first.put_if(Lookup, b"state", None, b"first").await.unwrap();
    second.insert(Leaves, b"second row", b"2").await.unwrap();
    second.put_if(Lookup, b"state", None, b"second").await.unwrap();
    first.commit().await.unwrap();
    assert!(matches!(second.commit().await, Err(TreeError::Conflict)), "it expected no state");

    let mut after = maps.begin();
    assert_eq!(after.get(Lookup, b"state").await.unwrap(), Some(b"first".to_vec()));
    assert_eq!(after.get(Leaves, b"first row").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(after.get(Leaves, b"second row").await.unwrap(), None, "its other write did not land either");
}

#[tokio::test]
async fn appends_under_one_prefix_take_distinct_keys_within_its_range() {
    let maps = MemoryMaps::new();
    let (mut first, mut second) = (maps.begin(), maps.begin());
    first.append(Nodes, b"p/", b"one").await.unwrap();
    second.append(Nodes, b"p/", b"two").await.unwrap();
    first.append(Nodes, b"q/", b"other").await.unwrap();
    second.commit().await.unwrap();
    first.commit().await.unwrap();
    let under_p = maps.begin().scan(Nodes, (Included(b"p/"), Excluded(b"p0")), usize::MAX).await.unwrap();
    assert_eq!(under_p.len(), 2, "appends never conflict");
    assert!(under_p.iter().all(|(key, _)| key.starts_with(b"p/") && key.len() > 2));
    assert_ne!(under_p[0].0, under_p[1].0);
    let values: Vec<&[u8]> = under_p.iter().map(|(_, value)| value.as_slice()).collect();
    assert_eq!(values, [b"one".as_slice(), b"two"], "in the order appended");
}

#[tokio::test]
async fn scans_follow_the_bounds_and_the_limit_in_key_order() {
    let maps = MemoryMaps::new();
    let mut setup = maps.begin();
    for key in [b"d", b"b", b"a", b"c"] {
        setup.insert(Leaves, key, key).await.unwrap();
    }
    setup.commit().await.unwrap();
    let mut map = maps.begin();
    assert_eq!(keys(map.scan(Leaves, (Unbounded, Unbounded), usize::MAX).await.unwrap()), ["a", "b", "c", "d"]);
    assert_eq!(keys(map.scan(Leaves, (Included(b"b"), Excluded(b"d")), usize::MAX).await.unwrap()), ["b", "c"]);
    assert_eq!(keys(map.scan(Leaves, (Excluded(b"b"), Included(b"d")), usize::MAX).await.unwrap()), ["c", "d"]);
    assert_eq!(keys(map.scan(Leaves, (Excluded(b"b"), Excluded(b"c")), usize::MAX).await.unwrap()), [] as [&str; 0]);
    assert_eq!(keys(map.scan(Leaves, (Included(b"bb"), Unbounded), usize::MAX).await.unwrap()), ["c", "d"], "a bound need not be a key");

    assert_eq!(keys(map.scan(Leaves, (Unbounded, Unbounded), 2).await.unwrap()), ["a", "b"]);
    assert_eq!(keys(map.scan(Leaves, (Excluded(b"b"), Unbounded), 1).await.unwrap()), ["c"], "the next page");
    assert_eq!(keys(map.scan(Leaves, (Unbounded, Unbounded), 0).await.unwrap()), [] as [&str; 0]);

    // Inverted bounds hold nothing rather than panicking.
    assert_eq!(keys(map.scan(Leaves, (Included(b"d"), Excluded(b"b")), usize::MAX).await.unwrap()), [] as [&str; 0]);
    assert_eq!(keys(map.scan(Leaves, (Excluded(b"b"), Excluded(b"b")), usize::MAX).await.unwrap()), [] as [&str; 0]);
    assert_eq!(keys(map.scan(Leaves, (Included(b"b"), Included(b"b")), usize::MAX).await.unwrap()), ["b"]);
}

#[tokio::test]
async fn bounded_counts_match_scans_with_own_writes_and_borrowed_views() {
    async fn borrowed_count<T: MapAccess>(mut transaction: T, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> usize {
        transaction.count_up_to(map, bounds, limit).await.unwrap()
    }
    let maps = MemoryMaps::new();
    let mut setup = maps.begin();
    for map in [Leaves, Lookup, Nodes] {
        for key in [b"a", b"c", b"e"] {
            setup.insert(map, key, key).await.unwrap();
        }
    }
    setup.commit().await.unwrap();
    let mut reader = maps.begin();
    let mut later = maps.begin();
    for map in [Leaves, Lookup, Nodes] {
        later.insert(map, b"z", b"later").await.unwrap();
        reader.insert(map, b"b", b"new").await.unwrap();
        reader.put_if(map, b"a", Some(b"a"), b"changed").await.unwrap();
        reader.delete_if(map, b"c", b"c").await.unwrap();
        reader.insert(map, b"d", b"temporary").await.unwrap();
        reader.delete_if(map, b"d", b"temporary").await.unwrap();
    }
    later.commit().await.unwrap();
    let bounds: &[ByteBounds<'_>] = &[
        (Unbounded, Unbounded),
        (Included(b"a"), Excluded(b"e")),
        (Excluded(b"a"), Included(b"e")),
        (Included(b"bb"), Unbounded),
        (Included(b"b"), Included(b"b")),
        (Excluded(b"b"), Excluded(b"b")),
        (Included(b"e"), Excluded(b"a")),
        (Included(b"c"), Included(b"d")),
        (Included(b"z"), Unbounded),
    ];
    for map in [Leaves, Lookup, Nodes] {
        assert_eq!(reader.count_up_to(map, (Unbounded, Unbounded), usize::MAX).await.unwrap(), 3);
        for &bounds in bounds {
            for limit in [0, 1, 2, 3, usize::MAX] {
                let scanned = reader.scan(map, bounds, limit).await.unwrap().len();
                assert_eq!(reader.count_up_to(map, bounds, limit).await.unwrap(), scanned, "{map:?} {bounds:?} {limit}");
                assert_eq!(borrowed_count(&mut reader, map, bounds, limit).await, scanned, "borrowed {map:?} {bounds:?} {limit}");
            }
        }
    }
    assert_eq!(maps.begin().count_up_to(Leaves, (Unbounded, Unbounded), usize::MAX).await.unwrap(), 4);
}

/// A commit reports the first failure its transaction met: a failed condition
/// as a conflict, even when an operation poisons the transaction after it,
/// and a poisoned operation as poisoned, even when a condition fails after it.
#[tokio::test]
async fn a_commit_reports_the_first_failure_its_transaction_met() {
    let maps = MemoryMaps::new();
    let mut setup = maps.begin();
    setup.insert(Leaves, b"k", b"v").await.unwrap();
    setup.commit().await.unwrap();

    let mut conflicted = maps.begin();
    assert!(matches!(conflicted.insert(Leaves, b"k", b"again").await, Err(TreeError::Conflict)));
    conflicted.poison(Failure::Poisoned);
    assert!(matches!(conflicted.commit().await, Err(TreeError::Conflict)));

    let mut poisoned = maps.begin();
    poisoned.poison(Failure::Poisoned);
    assert!(matches!(poisoned.insert(Leaves, b"k", b"again").await, Err(TreeError::Conflict)));
    assert!(matches!(poisoned.commit().await, Err(TreeError::Poisoned)));
}
