//! Reads of maps: gets and ranges of what is committed, maps kept apart by
//! their ids, and a transaction's reads of its own writes.

use std::ops::Bound;

use super::fixtures::{clear, commit, committed, delete, entries, everything, found, get, lock, only, put, range, ranged, read};
use crate::storage::{MapRows, MapSnapshot, ScanDirection, StorageEngine, StorageTransaction};

/// A get answers each key it asks for, in order, a key twice or one the map
/// lacks among them, with the last value put under it: a put replaces the
/// value, and a delete removes it.
pub async fn a_get_answers_each_key_with_the_last_value_put_under_it<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    assert_eq!(found(committed(engine, get!("m", ["a"])).await?), [None], "a map nothing was written to");

    commit(engine, vec![put!("m", "a", 1), put!("m", "b", 1)]).await?;
    commit(engine, vec![put!("m", "a", 2), delete!("m", "b"), delete!("m", "c")]).await?;

    assert_eq!(found(committed(engine, get!("m", ["b", "a", "c", "a"])).await?), [None, Some(2), None, Some(2)]);
    assert!(found(committed(engine, get!("m", [])).await?).is_empty(), "a get of no keys");
    Ok(())
}

/// A range gives the first entries between its bounds, every kind of bound,
/// in key order or reversed, as many as its limit asks. Keys order by their
/// bytes, unsigned: the empty key first, a key before the keys it begins, and
/// a byte past 0x7F after every ASCII one.
pub async fn a_range_gives_the_first_entries_between_its_bounds_either_way<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let keys = ["", "a", "a\0", "ab", "b", "é"];
    commit(engine, keys.iter().zip(1..).map(|(key, n)| put!("m", *key, n)).collect()).await?;

    let all: Vec<(String, u8)> = keys.iter().zip(1..).map(|(key, n)| (String::from(*key), n)).collect();
    let mut snapshot = engine.map_snapshot(&[]).await?;

    for (start, end, expected) in [
        (Bound::Unbounded, Bound::Unbounded, &all[..]),
        (Bound::Included("a"), Bound::Included("b"), &all[1..5]),
        (Bound::Excluded("a"), Bound::Included("b"), &all[2..5]),
        (Bound::Included("a"), Bound::Excluded("b"), &all[1..4]),
        (Bound::Excluded("a"), Bound::Excluded("ab"), &all[2..3]),
        (Bound::Included("aa"), Bound::Excluded("é"), &all[3..5]),
        (Bound::Included("ab"), Bound::Unbounded, &all[3..]),
        (Bound::Unbounded, Bound::Excluded("a"), &all[..1]),
    ] {
        let reversed: Vec<_> = expected.iter().rev().cloned().collect();

        for limit in [usize::MAX, 2, 1, 0] {
            let reads =
                vec![range!("m", start, end, ScanDirection::Forward, limit), range!("m", start, end, ScanDirection::Reverse, limit)];
            let [forward, reverse]: [MapRows; 2] = snapshot.map_read(reads).await?.try_into().expect("two answers");

            assert_eq!(ranged(forward), expected.iter().take(limit).cloned().collect::<Vec<_>>(), "{start:?} to {end:?}, {limit}");
            assert_eq!(ranged(reverse), reversed.iter().take(limit).cloned().collect::<Vec<_>>(), "{start:?} to {end:?} reversed, {limit}");
        }
    }

    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        for (start, end) in [
            (Bound::Included("b"), Bound::Included("a")),
            (Bound::Included("a"), Bound::Excluded("a")),
            (Bound::Excluded("a"), Bound::Included("a")),
            (Bound::Excluded("a"), Bound::Excluded("a")),
            (Bound::Unbounded, Bound::Excluded("")),
        ] {
            let rows = only(snapshot.map_read(vec![range!("m", start, end, direction, usize::MAX)]).await?);
            assert_eq!(ranged(rows), entries![], "{start:?} to {end:?} holds nothing, {direction:?}");
        }
    }

    Ok(())
}

/// Maps whose ids and keys run into each other stay apart: a write to one
/// changes no other, a read of one gives nothing of another, and a clear of
/// one leaves the others whole. So do a map whose id ends in the greatest
/// byte and the map whose id follows it.
pub async fn maps_are_kept_apart_by_their_ids<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let (greatest, next) = (b"m\xff", b"n\0");
    commit(engine, vec![put!("m", "ab", 1), put!("ma", "b", 2), put!("mab", "", 3), put!("", "mab", 4)]).await?;
    commit(engine, vec![put!(greatest, "a", 5), put!(next, "a", 6)]).await?;

    assert_eq!(ranged(committed(engine, everything!("m")).await?), entries!["ab" => 1]);
    assert_eq!(ranged(committed(engine, everything!("ma")).await?), entries!["b" => 2]);
    assert_eq!(ranged(committed(engine, everything!("mab")).await?), entries!["" => 3]);
    assert_eq!(ranged(committed(engine, everything!("")).await?), entries!["mab" => 4]);
    assert_eq!(ranged(committed(engine, everything!(greatest, ScanDirection::Reverse)).await?), entries!["a" => 5]);
    assert_eq!(ranged(committed(engine, everything!(next)).await?), entries!["a" => 6]);
    assert_eq!(ranged(committed(engine, everything!("n")).await?), entries![], "a map nothing was written to");
    assert_eq!(found(committed(engine, get!("ma", ["ab", "b"])).await?), [None, Some(2)]);

    commit(engine, vec![clear!("m"), clear!(greatest)]).await?;

    assert_eq!(ranged(committed(engine, everything!("m")).await?), entries![]);
    assert_eq!(ranged(committed(engine, everything!("ma")).await?), entries!["b" => 2]);
    assert_eq!(ranged(committed(engine, everything!("mab")).await?), entries!["" => 3]);
    assert_eq!(ranged(committed(engine, everything!("")).await?), entries!["mab" => 4]);
    assert_eq!(ranged(committed(engine, everything!(greatest)).await?), entries![]);
    assert_eq!(ranged(committed(engine, everything!(next)).await?), entries!["a" => 6]);
    Ok(())
}

/// A transaction's reads see its own writes before it commits, its puts and
/// deletes alike, in gets and in ranges both ways at any limit, whether it
/// reads alone or with locks. Nothing else sees them until it commits.
pub async fn a_transactions_reads_see_its_own_writes<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    commit(engine, vec![put!("m", "a", 1), put!("m", "b", 2), put!("m", "c", 3), put!("m", "d", 4)]).await?;

    let mut trx = engine.transaction();
    trx.map_write(vec![delete!("m", "b"), put!("m", "c", 30), put!("m", "bb", 22)]).await?;
    trx.map_write(vec![put!("m", "e", 5)]).await?;

    let written = entries!["a" => 1, "bb" => 22, "c" => 30, "d" => 4, "e" => 5];
    let reversed: Vec<_> = written.iter().rev().cloned().collect();

    let trx: &mut dyn StorageTransaction = &mut trx;
    assert_eq!(found(read(trx, get!("m", ["b", "c", "bb", "z"])).await?), [None, Some(30), Some(22), None]);

    for limit in [usize::MAX, 2, 1] {
        let forward = read(trx, range!("m", Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, limit)).await?;
        let reverse = read(trx, range!("m", Bound::Unbounded, Bound::Unbounded, ScanDirection::Reverse, limit)).await?;

        assert_eq!(ranged(forward), written.iter().take(limit).cloned().collect::<Vec<_>>(), "{limit}");
        assert_eq!(ranged(reverse), reversed.iter().take(limit).cloned().collect::<Vec<_>>(), "reversed, {limit}");
    }

    let bounded = range!("m", Bound::Excluded("a"), Bound::Included("d"), ScanDirection::Reverse, usize::MAX);
    let locked = only(trx.map_lock_and_read(&[lock!("l")], vec![bounded]).await?);
    assert_eq!(ranged(locked), entries!["d" => 4, "c" => 30, "bb" => 22]);

    assert_eq!(ranged(committed(engine, everything!("m")).await?), entries!["a" => 1, "b" => 2, "c" => 3, "d" => 4]);
    Ok(())
}

/// A clear removes every key its map holds, those its transaction wrote to
/// it before included, and the transaction's later writes to it stand: a put,
/// a clear and a put to one map leave the last put, before the commit and
/// after it. Other maps keep theirs.
pub async fn a_put_a_clear_and_a_put_to_one_map_leave_the_last<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    commit(engine, vec![put!("m", "a", 1), put!("m", "b", 2), put!("n", "a", 9)]).await?;

    let mut trx = engine.transaction();
    trx.map_write(vec![put!("m", "c", 3)]).await?;
    trx.map_write(vec![clear!("m"), put!("m", "d", 4)]).await?;

    assert_eq!(ranged(read(&mut trx, everything!("m")).await?), entries!["d" => 4]);
    assert_eq!(found(read(&mut trx, get!("m", ["a", "c", "d"])).await?), [None, None, Some(4)]);
    assert_eq!(ranged(read(&mut trx, everything!("n")).await?), entries!["a" => 9]);

    trx.commit().await?.committed()?;

    assert_eq!(ranged(committed(engine, everything!("m")).await?), entries!["d" => 4]);
    assert_eq!(ranged(committed(engine, everything!("n")).await?), entries!["a" => 9]);
    Ok(())
}
