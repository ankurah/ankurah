use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};

use super::*;
use crate::test_helpers::map_conformance::fixtures::{clear, delete, entries, everything, found, get, key, put, range, ranged};

/// Entries of one map an engine has stored: each key's text with its number.
macro_rules! stored {
    ($($key:expr => $n:expr),* $(,)?) => {{
        let stored: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::from([$((key!($key), vec![$n])),*]);
        stored
    }};
}

/// Pending writes holding `writes`, folded in their order.
fn pending(writes: Vec<MapWrite>) -> PendingWrites {
    let mut pending = PendingWrites::default();
    pending.add(writes);
    pending
}

/// The answer an engine gives `read` from `stored`, the entries of the one
/// map it reads, as every map read is answered: one value per key, or the
/// first `limit` entries between the bounds in the read's direction.
fn answer_from(stored: &BTreeMap<Vec<u8>, Vec<u8>>, read: MapRead) -> MapRows {
    match read {
        MapRead::Get { keys, .. } => MapRows::Got(keys.iter().map(|key| stored.get(key).cloned()).collect()),
        MapRead::Range { start, end, direction, limit, .. } => {
            let within = stored
                .iter()
                .filter(|(key, _)| (start.as_ref(), end.as_ref()).contains(key))
                .map(|(key, value)| (key.clone(), value.clone()));
            MapRows::Ranged(match direction {
                ScanDirection::Forward => within.take(limit).collect(),
                ScanDirection::Reverse => within.rev().take(limit).collect(),
            })
        }
    }
}

/// Answer `reads` through `pending` over `stored`, the entries of the one
/// map they read: the answers, and the reads of `stored` they needed.
fn batch_through(pending: &PendingWrites, stored: &BTreeMap<Vec<u8>, Vec<u8>>, reads: Vec<MapRead>) -> (Vec<MapRows>, Vec<MapRead>) {
    let (asked, overlay) = pending.read(reads);
    let answers = overlay.answer(asked.iter().map(|read| answer_from(stored, read.clone())).collect());

    (answers, asked)
}

/// Answer `read` alone through `pending` over `stored`: the answer, and the
/// read of `stored` it needed, if it needed one.
fn read_through(pending: &PendingWrites, stored: &BTreeMap<Vec<u8>, Vec<u8>>, read: MapRead) -> (MapRows, Option<MapRead>) {
    let (mut answers, mut asked) = batch_through(pending, stored, vec![read]);
    assert!(asked.len() <= 1, "one read needs at most one read of what is stored");

    (answers.remove(0), asked.pop())
}

/// The pending writes to map `map`, each key's text with its number, or
/// `None` where it is deleted; and whether the map was cleared.
fn changes_to(pending: &mut PendingWrites, map: &str) -> (bool, Vec<(String, Option<u8>)>) {
    let changes = pending.take().remove(&MapId::new(map)).unwrap_or_default();
    let keys = changes.keys.into_iter().map(|(key, value)| (String::from_utf8(key).unwrap(), value.map(|value| value[0]))).collect();
    (changes.cleared, keys)
}

#[test]
fn the_last_write_under_a_key_is_the_one_pending() {
    let mut writes = pending(vec![put!("m", "a", 1), put!("m", "b", 1), delete!("m", "c")]);
    writes.add(vec![put!("m", "a", 2), delete!("m", "b"), put!("m", "c", 3)]);

    let expected = vec![(String::from("a"), Some(2)), (String::from("b"), None), (String::from("c"), Some(3))];
    assert_eq!(changes_to(&mut writes, "m"), (false, expected));
}

#[test]
fn a_clear_drops_the_writes_to_its_map_before_it_and_keeps_those_after() {
    let mut writes = pending(vec![put!("m", "a", 1), delete!("m", "b"), put!("n", "a", 9), clear!("m"), put!("m", "c", 3)]);

    let (cleared, keys) = changes_to(&mut writes, "m");
    assert!(cleared);
    assert_eq!(keys, [(String::from("c"), Some(3))]);

    writes.add(vec![put!("n", "a", 9), clear!("m"), put!("m", "c", 3)]);
    assert_eq!(changes_to(&mut writes, "n"), (false, vec![(String::from("a"), Some(9))]), "another map's writes stand");
}

#[test]
fn taking_the_pending_writes_leaves_none() {
    let mut writes = pending(vec![put!("m", "a", 1), clear!("n")]);

    assert_eq!(writes.take().len(), 2);
    assert!(writes.take().is_empty());
}

#[test]
fn a_get_sees_a_pending_put_over_the_stored_value_and_a_pending_delete_hiding_it() {
    let writes = pending(vec![put!("m", "a", 10), delete!("m", "b"), put!("m", "d", 4)]);
    let stored = stored!["a" => 1, "b" => 2, "c" => 3];

    let (rows, asked) = read_through(&writes, &stored, get!("m", ["a", "b", "c", "d", "e"]));

    assert_eq!(found(rows), [Some(10), None, Some(3), Some(4), None]);
    assert_eq!(asked, Some(get!("m", ["c", "e"])), "only the keys the pending writes leave unwritten are read");
}

#[test]
fn a_get_of_keys_no_write_touched_reads_each_stored_value_in_order() {
    let writes = pending(vec![put!("n", "a", 10)]);
    let stored = stored!["a" => 1, "b" => 2];

    let (rows, asked) = read_through(&writes, &stored, get!("m", ["b", "z", "a", "b"]));

    assert_eq!(found(rows), [Some(2), None, Some(1), Some(2)]);
    assert_eq!(asked, Some(get!("m", ["b", "z", "a", "b"])));
}

#[test]
fn a_get_of_a_cleared_map_sees_only_its_later_writes_and_reads_nothing_stored() {
    let writes = pending(vec![clear!("m"), put!("m", "b", 20)]);
    let stored = stored!["a" => 1, "b" => 2];

    let (rows, asked) = read_through(&writes, &stored, get!("m", ["a", "b", "c"]));

    assert_eq!(found(rows), [None, Some(20), None]);
    assert_eq!(asked, None);
}

#[test]
fn a_range_sees_pending_puts_replacing_and_joining_stored_entries_and_pending_deletes_hiding_them_either_way() {
    let writes = pending(vec![put!("m", "a", 10), put!("m", "bb", 22), delete!("m", "c"), delete!("m", "z")]);
    let stored = stored!["a" => 1, "b" => 2, "c" => 3, "d" => 4];
    let written = entries!["a" => 10, "b" => 2, "bb" => 22, "d" => 4];

    let (forward, _) = read_through(&writes, &stored, everything!("m", ScanDirection::Forward));
    let (reverse, _) = read_through(&writes, &stored, everything!("m", ScanDirection::Reverse));

    assert_eq!(ranged(forward), written);
    assert_eq!(ranged(reverse), written.into_iter().rev().collect::<Vec<_>>());
}

#[test]
fn a_range_gives_exactly_its_limit_when_pending_deletes_hide_the_first_stored_entries_either_way() {
    let writes = pending(vec![delete!("m", "a"), delete!("m", "b"), delete!("m", "d"), delete!("m", "e")]);
    let stored = stored!["a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5, "f" => 6];

    let (forward, _) = read_through(&writes, &stored, range!("m", Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 2));
    let (reverse, _) = read_through(&writes, &stored, range!("m", Bound::Unbounded, Bound::Unbounded, ScanDirection::Reverse, 2));

    assert_eq!(ranged(forward), entries!["c" => 3, "f" => 6]);
    assert_eq!(ranged(reverse), entries!["f" => 6, "c" => 3]);
}

#[test]
fn a_range_reads_as_many_stored_entries_more_than_its_limit_as_pending_deletes_lie_within_its_bounds() {
    let writes = pending(vec![delete!("m", "a"), delete!("m", "c"), delete!("m", "x"), put!("m", "b", 2)]);

    let (_, asked) = read_through(&writes, &stored![], range!("m", Bound::Excluded("a"), Bound::Included("m"), ScanDirection::Forward, 3));

    assert_eq!(asked, Some(range!("m", Bound::Excluded("a"), Bound::Included("m"), ScanDirection::Forward, 4)));
}

#[test]
fn a_range_of_a_cleared_map_sees_only_its_later_writes_and_reads_nothing_stored() {
    let writes = pending(vec![put!("m", "a", 10), clear!("m"), put!("m", "c", 30), put!("m", "b", 20)]);
    let stored = stored!["a" => 1, "b" => 2, "d" => 4];

    let (forward, asked) = read_through(&writes, &stored, range!("m", Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 1));
    let (reverse, _) = read_through(&writes, &stored, everything!("m", ScanDirection::Reverse));

    assert_eq!(ranged(forward), entries!["b" => 20]);
    assert_eq!(ranged(reverse), entries!["c" => 30, "b" => 20]);
    assert_eq!(asked, None);
}

#[test]
fn a_range_with_no_limit_or_bounds_that_hold_nothing_reads_nothing() {
    let writes = pending(vec![put!("m", "a", 10)]);
    let stored = stored!["a" => 1, "b" => 2];

    for read in [
        range!("m", Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 0),
        range!("m", Bound::Included("b"), Bound::Included("a"), ScanDirection::Forward, 5),
        range!("m", Bound::Included("a"), Bound::Excluded("a"), ScanDirection::Reverse, 5),
        range!("m", Bound::Excluded("a"), Bound::Included("a"), ScanDirection::Forward, 5),
    ] {
        let (rows, asked) = read_through(&writes, &stored, read.clone());
        assert_eq!((ranged(rows), asked), (entries![], None), "{read:?}");
    }
}

/// Every case over three keys: each stored or not, each pending a put, a
/// delete or neither, the map cleared before or not, every kind of bound at
/// each key, both directions and limits up to past every key. Each read
/// through the pending writes gives what a model that applies the writes to
/// the stored entries gives, from one read of what is stored.
#[test]
fn every_read_over_three_keys_sees_the_stored_entries_as_the_pending_writes_leave_them() {
    let keys = ["a", "b", "c"];
    let bounds: Vec<Bound<&str>> =
        std::iter::once(Bound::Unbounded).chain(keys.iter().flat_map(|key| [Bound::Included(*key), Bound::Excluded(*key)])).collect();

    for held in 0..(1usize << keys.len()) {
        let stored: BTreeMap<Vec<u8>, Vec<u8>> = keys
            .iter()
            .enumerate()
            .filter(|(index, _)| held & (1 << index) != 0)
            .map(|(index, key)| (key!(*key), vec![1 + index as u8]))
            .collect();

        for written in 0..3usize.pow(keys.len() as u32) {
            for cleared in [false, true] {
                let mut writes = if cleared { vec![clear!("m")] } else { Vec::new() };
                let mut model = if cleared { BTreeMap::new() } else { stored.clone() };

                for (index, key) in keys.iter().enumerate() {
                    match written / 3usize.pow(index as u32) % 3 {
                        1 => {
                            writes.push(put!("m", *key, 10 + index as u8));
                            model.insert(key!(*key), vec![10 + index as u8]);
                        }

                        2 => {
                            writes.push(delete!("m", *key));
                            model.remove(&key!(*key));
                        }

                        _ => {}
                    }
                }

                let writes = pending(writes);
                for (start, end) in bounds.iter().flat_map(|start| bounds.iter().map(move |end| (*start, *end))) {
                    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
                        for limit in 0..=keys.len() + 1 {
                            let read = range!("m", start, end, direction, limit);
                            assert_eq!(
                                read_through(&writes, &stored, read.clone()).0,
                                answer_from(&model, read.clone()),
                                "{read:?} over {stored:?}"
                            );
                        }
                    }
                }

                let read = get!("m", ["a", "b", "c", "d"]);
                assert_eq!(read_through(&writes, &stored, read.clone()).0, answer_from(&model, read), "a get over {stored:?}");
            }
        }
    }
}

#[test]
fn a_batch_is_answered_in_its_order_each_read_by_the_pending_writes_alone_or_over_what_is_stored() {
    let writes = pending(vec![put!("m", "a", 10), clear!("n"), put!("n", "b", 20)]);
    let stored = stored!["a" => 1, "b" => 2, "c" => 3];

    let reads = vec![
        get!("m", ["a"]),
        everything!("m"),
        get!("n", ["a", "b"]),
        get!("m", ["c", "a"]),
        range!("m", Bound::Included("b"), Bound::Included("a"), ScanDirection::Forward, 5),
    ];
    let (answers, asked) = batch_through(&writes, &stored, reads);

    assert_eq!(found(answers[0].clone()), [Some(10)]);
    assert_eq!(ranged(answers[1].clone()), entries!["a" => 10, "b" => 2, "c" => 3]);
    assert_eq!(found(answers[2].clone()), [None, Some(20)]);
    assert_eq!(found(answers[3].clone()), [Some(3), Some(10)]);
    assert_eq!(ranged(answers[4].clone()), entries![]);
    assert_eq!(asked, [everything!("m"), get!("m", ["c"])], "the reads of what is stored, in the batch's order, only where one is needed");
}

#[test]
fn a_get_of_no_keys_asks_nothing_of_what_is_stored() {
    let (answer, asked) = read_through(&pending(Vec::new()), &stored![], get!("m", []));

    assert_eq!(found(answer), []);
    assert_eq!(asked, None);
}

#[test]
#[should_panic(expected = "a get of 1 keys with 2 values")]
fn a_stored_answer_with_another_number_of_values_than_asked_is_an_engine_bug() {
    let writes = pending(Vec::new());
    let (_, overlay) = writes.read(vec![get!("m", ["a"])]);

    overlay.answer(vec![MapRows::Got(vec![None, None])]);
}

#[test]
#[should_panic(expected = "a get of 1 keys with 2 values")]
fn a_stored_answer_under_pending_writes_with_another_number_of_values_than_asked_is_an_engine_bug() {
    let writes = pending(vec![put!("m", "b", 1)]);
    let (_, overlay) = writes.read(vec![get!("m", ["a", "b"])]);

    overlay.answer(vec![MapRows::Got(vec![None, None])]);
}

#[test]
#[should_panic(expected = "rows of another kind")]
fn a_stored_answer_of_another_kind_than_its_read_is_an_engine_bug() {
    let writes = pending(Vec::new());
    let (_, overlay) = writes.read(vec![get!("m", ["a"])]);

    overlay.answer(vec![MapRows::Ranged(Vec::new())]);
}

#[test]
#[should_panic(expected = "rows of another kind")]
fn a_stored_answer_under_pending_writes_of_another_kind_than_its_read_is_an_engine_bug() {
    let writes = pending(vec![put!("m", "b", 1)]);
    let (_, overlay) = writes.read(vec![everything!("m")]);

    overlay.answer(vec![MapRows::Got(Vec::new())]);
}

#[test]
#[should_panic(expected = "an engine answered 1 reads of what it has stored with 2 answers")]
fn stored_answers_of_another_number_than_the_reads_of_what_is_stored_are_an_engine_bug() {
    let writes = pending(Vec::new());
    let (_, overlay) = writes.read(vec![get!("m", ["a"])]);

    overlay.answer(vec![MapRows::Got(vec![None]), MapRows::Got(vec![None])]);
}
