//! The record cache's rules, each on its own: rows true at one generation per
//! tree, changes made only against the generation they were read at, and a
//! budget kept by dropping the rows used least recently.

use std::convert::Infallible;

use super::*;
use crate::sync::storage_adapter::layout::log::changed;

/// The rows map of the tree numbered `n`.
macro_rules! tree {
    ($n:expr) => {
        MapId::new([b'r', $n])
    };
}

fn cache(bytes: usize) -> CacheState { CacheState::new(CacheSettings { bytes }) }

/// The bytes the cache keeps under `key` of `tree` at `generation`, if it
/// answers.
fn kept(cache: &mut CacheState, tree: &MapId, generation: u64, key: &[u8]) -> Option<Vec<u8>> {
    cache.get(tree, generation, key, |bytes| Ok::<_, Infallible>(bytes.to_vec()))
}

#[test]
fn rows_answer_only_at_the_generation_they_are_kept_at() {
    let mut cache = cache(1024);
    cache.land(&tree!(1), 3, changed![b"a" => &b"1"[..]]);

    assert_eq!(kept(&mut cache, &tree!(1), 3, b"a"), Some(b"1".to_vec()));
    assert_eq!(kept(&mut cache, &tree!(1), 2, b"a"), None, "an older generation");
    assert_eq!(kept(&mut cache, &tree!(1), 4, b"a"), None, "a later generation");
    assert_eq!(kept(&mut cache, &tree!(2), 3, b"a"), None, "another tree's rows map");
}

#[test]
fn a_landing_moves_the_rows_only_from_the_generation_before_it() {
    let mut cache = cache(1024);
    cache.land(&tree!(1), 0, changed![b"a" => &b"0"[..], b"b" => &b"0"[..]]);

    cache.land(&tree!(1), 1, changed![b"a" => &b"1"[..], b"b" => None]);
    assert_eq!(cache.generation(&tree!(1)), Some(1));
    assert_eq!((kept(&mut cache, &tree!(1), 1, b"a"), kept(&mut cache, &tree!(1), 1, b"b")), (Some(b"1".to_vec()), None));

    cache.land(&tree!(1), 3, changed![b"a" => &b"3"[..]]);
    cache.land(&tree!(1), 1, changed![b"a" => &b"x"[..]]);
    assert_eq!(kept(&mut cache, &tree!(1), 1, b"a"), Some(b"1".to_vec()), "landings read against other generations are dropped");

    cache.land(&tree!(2), 7, changed![b"a" => &b"7"[..]]);
    assert_eq!(kept(&mut cache, &tree!(2), 7, b"a"), Some(b"7".to_vec()), "a tree with nothing kept takes any landing");

    cache.land(&tree!(3), 0, changed![b"a" => &b"0"[..]]);
    cache.land(&tree!(3), 0, changed![b"a" => &b"x"[..]]);
    assert_eq!(kept(&mut cache, &tree!(3), 0, b"a"), Some(b"0".to_vec()), "a creation lands only on a tree with nothing kept");
}

#[test]
fn a_fill_keeps_a_row_only_at_the_generation_kept_or_on_a_tree_with_nothing_kept() {
    let mut cache = cache(1024);
    cache.fill(&tree!(1), 2, b"a".to_vec(), b"2".to_vec(), 1);
    cache.fill(&tree!(1), 2, b"b".to_vec(), b"2".to_vec(), 1);
    cache.fill(&tree!(1), 1, b"c".to_vec(), b"1".to_vec(), 1);

    assert_eq!(cache.generation(&tree!(1)), Some(2));
    assert_eq!(kept(&mut cache, &tree!(1), 2, b"b"), Some(b"2".to_vec()));
    assert_eq!(kept(&mut cache, &tree!(1), 2, b"c"), None, "a fill read at another generation is dropped");
    assert_eq!(kept(&mut cache, &tree!(1), 1, b"c"), None);
}

#[test]
fn catching_up_and_forgetting_happen_only_from_the_generation_they_were_read_against() {
    let mut cache = cache(1024);
    cache.land(&tree!(1), 2, changed![b"a" => &b"2"[..], b"b" => &b"2"[..]]);

    cache.catch_up(&tree!(1), 1, 5, changed![b"a" => &b"x"[..]]);
    cache.forget(&tree!(1), 1);
    assert_eq!(kept(&mut cache, &tree!(1), 2, b"a"), Some(b"2".to_vec()), "read against generation 1, while the rows are at 2");

    cache.catch_up(&tree!(1), 2, 5, changed![b"a" => &b"5"[..], b"b" => None]);
    assert_eq!((kept(&mut cache, &tree!(1), 5, b"a"), kept(&mut cache, &tree!(1), 5, b"b")), (Some(b"5".to_vec()), None));

    cache.forget(&tree!(1), 5);
    assert_eq!(cache.generation(&tree!(1)), None);
    assert_eq!(cache.used, 0);
}

#[test]
fn over_budget_the_rows_used_least_recently_go() {
    // Each row costs its key's byte and its own four bytes.
    let mut cache = cache(15);
    cache.land(&tree!(1), 0, changed![b"a" => &b"1111"[..], b"b" => &b"2222"[..], b"c" => &b"3333"[..]]);
    assert!(kept(&mut cache, &tree!(1), 0, b"a").is_some());

    cache.land(&tree!(2), 0, changed![b"d" => &b"4444"[..]]);

    assert_eq!(kept(&mut cache, &tree!(1), 0, b"b"), None, "the row used least recently");
    assert!(kept(&mut cache, &tree!(1), 0, b"a").is_some() && kept(&mut cache, &tree!(1), 0, b"c").is_some());
    assert!(kept(&mut cache, &tree!(2), 0, b"d").is_some());
    assert_eq!(cache.used, 15);
}

#[test]
fn a_tree_whose_last_row_goes_has_no_generation() {
    let mut cache = cache(5);
    cache.land(&tree!(1), 4, changed![b"a" => &b"1111"[..]]);
    cache.land(&tree!(2), 0, changed![b"b" => &b"2222"[..]]);

    assert_eq!(cache.generation(&tree!(1)), None, "its last row evicted");

    cache.land(&tree!(2), 1, changed![b"b" => None]);
    assert_eq!(cache.generation(&tree!(2)), None, "its last row deleted");
}

#[test]
fn a_budget_of_zero_keeps_nothing() {
    let mut cache = cache(0);
    cache.land(&tree!(1), 0, changed![b"a" => &b"1"[..]]);
    cache.fill(&tree!(1), 0, b"b".to_vec(), b"1".to_vec(), 1);

    assert_eq!(cache.generation(&tree!(1)), None);
    assert_eq!(kept(&mut cache, &tree!(1), 0, b"a"), None);
}

#[test]
fn a_row_that_doesnt_decode_is_not_answered_and_its_trees_rows_are_forgotten() {
    let mut cache = cache(1024);
    cache.land(&tree!(1), 0, changed![b"a" => &b"1"[..], b"b" => &b"2"[..]]);
    cache.land(&tree!(2), 0, changed![b"c" => &b"3"[..]]);

    assert!(cache.get(&tree!(1), 0, b"a", |_| Err::<Vec<u8>, _>("damaged")).is_none());

    assert_eq!(cache.generation(&tree!(1)), None);
    assert_eq!(kept(&mut cache, &tree!(2), 0, b"c"), Some(b"3".to_vec()), "another tree's rows stay");
    assert_eq!(cache.used, 2);
}

#[test]
fn a_row_is_decoded_once_and_answered_as_its_type_alone() {
    let mut cache = cache(1024);
    cache.land(&tree!(1), 0, changed![b"a" => &b"1"[..]]);

    let mut decodes = 0;
    for _ in 0..3 {
        let row = cache.get(&tree!(1), 0, b"a", |bytes| {
            decodes += 1;
            Ok::<_, Infallible>(bytes.to_vec())
        });
        assert_eq!(row, Some(b"1".to_vec()));
    }

    assert_eq!(decodes, 1);
    assert_eq!(cache.get(&tree!(1), 0, b"a", |_| Ok::<u8, Infallible>(0)), None, "a row decoded as another type");
}
