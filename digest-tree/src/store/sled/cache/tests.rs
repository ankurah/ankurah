use super::*;

#[test]
fn the_least_recently_used_row_leaves_first() {
    let mut recent = Recent::<i32, &str>::new(2);
    recent.put(1, "one");
    recent.put(2, "two");

    assert_eq!(recent.get(&1), Some("one"), "using 1 makes 2 the least recently used");
    recent.put(3, "three");

    assert_eq!((recent.get(&1), recent.get(&2), recent.get(&3)), (Some("one"), None, Some("three")));
    recent.put(1, "uno");

    assert_eq!(recent.get(&1), Some("uno"), "a put replaces the row");
    recent.remove(&1);

    assert_eq!((recent.get(&1), recent.rows.len(), recent.uses.len()), (None, 1, 1));
}

/// A peek finds a recent row without making it the most recently used.
#[test]
fn a_peek_leaves_recency_alone() {
    let mut recent = Recent::<i32, &str>::new(2);
    recent.put(1, "one");
    recent.put(2, "two");

    assert_eq!(recent.peek(&1), Some(&"one"));
    recent.put(3, "three");

    assert_eq!((recent.peek(&1), recent.peek(&2), recent.peek(&3)), (None, Some(&"two"), Some(&"three")));
}

/// A read that began before a write fills nothing: what it read from disk
/// may be what the write replaced.
#[test]
fn a_read_that_a_write_overtook_fills_nothing() {
    let mut cache = Cache::<&str>::new(4);
    let noted = cache.writes();

    cache.apply(vec![WriteItem::Put(RowKey::Meta, "new")]);
    cache.fill(noted, [(RowKey::Meta, "old")]);

    assert_eq!(cache.get(&RowKey::Meta), Some("new"));
    cache.fill(cache.writes(), [(RowKey::Leaf(vec![1]), "read")]);

    assert_eq!(cache.get(&RowKey::Leaf(vec![1])), Some("read"));
    cache.apply(vec![WriteItem::Delete(RowKey::Leaf(vec![1]))]);

    assert_eq!(cache.get(&RowKey::Leaf(vec![1])), None);
}
