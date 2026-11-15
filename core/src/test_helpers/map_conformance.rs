//! What every storage engine promises of its ordered maps and locks, checked
//! over any engine: each case is a function a test runs once, over a fresh
//! engine that holds nothing. An engine's tests reach it through
//! `ankurah-core`'s `test-helpers` feature, and run every case through
//! [`every_case`]. An engine whose locks under different names keep apart,
//! rather than one writer for the whole store, runs the cases of
//! [`every_case_for_engines_that_lock_by_name`] too, and one whose snapshots
//! keep their state across commits those of
//! [`every_case_for_engines_whose_snapshots_keep_their_state`].
//!
//! The cases write numbers under keys of text, so that what they read back
//! reads plainly. Some watch a transaction wait on another by polling it once.
//! On an engine whose calls complete at once when nothing holds them, as the
//! test engine's do, a call still pending after that poll is waiting; on any
//! other, it may be pending for its own work alone, and the engine's own
//! tests prove that it waits.

mod commits;
pub mod fixtures;
mod locks;
mod reads;
mod resets;
mod snapshots;

pub use commits::*;
pub use locks::*;
pub use reads::*;
pub use resets::*;
pub use snapshots::*;

/// Every case every engine runs, by name, handed to `$run`, a macro of an
/// engine's tests that wraps each in the engine's own test of it:
/// `every_case!(on_a_fresh_database)` calls
/// `on_a_fresh_database!(a_get_answers_each_key_with_the_last_value_put_under_it, ...)`.
/// Every such case is listed here, so that every engine runs it.
#[doc(hidden)]
#[macro_export]
macro_rules! __map_conformance_every_case {
    ($run:ident) => {
        $run!(
            a_get_answers_each_key_with_the_last_value_put_under_it,
            a_range_gives_the_first_entries_between_its_bounds_either_way,
            maps_are_kept_apart_by_their_ids,
            a_transactions_reads_see_its_own_writes,
            a_put_a_clear_and_a_put_to_one_map_leave_the_last,
            map_writes_land_with_the_transactions_entity_records_or_not_at_all,
            a_state_set_after_a_map_call_is_refused,
            a_dropped_transaction_lands_none_of_its_map_writes,
            a_poisoned_transaction_fails_every_later_call_and_its_commit,
            a_map_write_dropped_part_way_leaves_the_transaction_unable_to_commit,
            a_lock_waits_for_the_transaction_holding_it_to_end,
            a_lock_name_taken_twice_or_out_of_order_fails_the_call,
            a_snapshot_sees_one_committed_state_while_a_transaction_commits,
            delete_all_empties_every_map_and_lands_nothing_of_an_open_map_transaction,
            a_transaction_that_touches_no_map_commits_as_without_maps,
        );
    };
}

#[doc(inline)]
pub use crate::__map_conformance_every_case as every_case;

/// Every case of an engine whose locks under different names keep apart, by
/// name, handed to `$run` as [`every_case`] hands its own. An engine with one
/// writer for the whole store makes every writer wait behind an open map
/// transaction, and runs none of these.
#[doc(hidden)]
#[macro_export]
macro_rules! __map_conformance_every_case_for_engines_that_lock_by_name {
    ($run:ident) => {
        $run!(
            a_lock_under_another_name_doesnt_wait_for_the_one_held,
            a_transaction_that_touches_no_map_doesnt_wait_for_an_open_map_transaction,
        );
    };
}

#[doc(inline)]
pub use crate::__map_conformance_every_case_for_engines_that_lock_by_name as every_case_for_engines_that_lock_by_name;

/// Every case of an engine whose snapshots keep their state across commits,
/// by name, handed to `$run` as [`every_case`] hands its own. An engine whose
/// commits wait for the snapshots open under their names, or whose snapshots
/// fail retryably once a commit moved what they read, runs none of these.
#[doc(hidden)]
#[macro_export]
macro_rules! __map_conformance_every_case_for_engines_whose_snapshots_keep_their_state {
    ($run:ident) => {
        $run!(a_snapshot_keeps_its_state_across_a_commit_under_its_names);
    };
}

#[doc(inline)]
pub use crate::__map_conformance_every_case_for_engines_whose_snapshots_keep_their_state as every_case_for_engines_whose_snapshots_keep_their_state;
