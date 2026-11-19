//! Storage-adapter scenarios shared by core and integration tests. Core runs
//! the full suite in both digest groups on its test engine. Integration tests
//! select scenarios that exercise composition with real storage engines and
//! reach them through `ankurah-core`'s `test-helpers` feature.
//!
//! The scenarios keep trees of items, each with an id, a number it is keyed
//! by, and a label, as [`fixtures`] makes and checks them. Some watch a
//! storage transaction wait for a tree's lock by polling its call once. On an
//! engine whose calls complete at once when nothing holds them, as the test
//! engine's do, a call still pending after that poll is waiting; on any other,
//! it may be pending for its own work alone, and the engine's own tests prove
//! that it waits.

use std::future::Future;
use std::sync::Arc;

use crate::storage::StorageEngine;

pub(crate) mod cache_model;
mod captures;
mod changes;
pub mod fixtures;
mod lifecycle;
mod reads;
mod refusals;
mod store_contract;

pub use cache_model::*;
pub use captures::*;
pub use changes::*;
pub use lifecycle::*;
pub use reads::*;
pub use refusals::*;
pub use store_contract::*;

#[doc(hidden)]
pub use ankurah_digest_tree::test_support::store_conformance::every_case as __every_store_conformance_case;

/// The full suite, each test with the attribute `$test` and run
/// over a fresh store, the test database the expression `$db` makes: every
/// scenario once in each digest group, in the modules `lthash16` and
/// `ristretto255`, and every store case once, in `store_contract`.
/// `suite!(#[tokio::test], Database::start()?)` runs it on the databases
/// `Database::start` makes, in the module it is called in. Every scenario is
/// listed here, and every store case through the crate's own list.
#[doc(hidden)]
#[macro_export]
macro_rules! __digest_trees_conformance_suite {
    (#[$test:meta], $db:expr) => {
        $crate::__digest_trees_conformance_in_both_groups!(
            [#[$test] $db]
            a_created_tree_opens_by_its_id_holding_its_members,
            changes_land_when_their_storage_transaction_commits,
            a_dropped_tree_goes_when_its_storage_transaction_commits,
            several_trees_and_entity_records_land_in_one_storage_transaction_or_none_do,
            a_second_transaction_waits_for_the_first_to_end_and_sees_its_commit,
            several_writers_on_one_node_committing_to_two_trees_at_once_land_every_commit,
            an_apply_that_fails_leaves_its_transaction_unable_to_commit,
            of_two_creations_racing_the_second_finds_the_tree_the_first_created,
            a_creation_racing_a_drop_creates_the_tree_anew_once_the_drop_commits,
            a_drop_racing_a_commit_drops_the_tree_once_the_commit_lands,
            a_commit_racing_a_drop_finds_its_tree_retired_and_writes_nothing,
            a_rebuild_preserves_handles_and_removes_obsolete_rows,
            a_rebuild_of_an_absent_tree_creates_it,
            a_rebuild_whose_transaction_is_dropped_preserves_the_old_tree,
            a_rebuild_whose_commit_conflicts_preserves_the_old_tree,
            a_failed_rebuild_preserves_the_old_tree,
            a_rebuild_restores_a_tree_marked_invalid,
            a_rebuild_refuses_a_damaged_entry_without_replacing_it,
            a_rebuild_refuses_a_damaged_row_without_changing_stored_state,
            late_landings_do_not_restore_rows_removed_by_a_rebuild,
            a_rebuild_refuses_a_different_key_spec,
            a_rebuild_refuses_generation_overflow,
            a_reset_under_open_handles_retires_their_trees,
            a_reset_between_an_apply_and_its_commit_waits_for_the_commit_or_fails_it,
            a_range_reads_later_batches_see_a_commit_made_between_them,
            a_tree_that_refuses_its_changes_is_marked_invalid,
            a_tree_its_owner_reports_refused_is_marked_invalid,
            a_refusal_whose_transaction_is_dropped_leaves_the_tree_valid,
            a_damaged_row_fails_reads_and_marks_the_tree_invalid_at_its_next_commit,
            a_tree_whose_entry_is_damaged_refuses_its_changes_until_it_is_dropped,
            reads_commits_landings_and_evictions_in_any_order_never_serve_a_row_of_another_generation,
            a_capture_survives_flushes_and_other_managers_commits,
        );

        $crate::sync::conformance::__every_store_conformance_case!(
            [#[$test] $db] $crate::__digest_trees_conformance_store_cases_once,
            a_store_owns_its_own_transactions_and_no_others
        );
    };
}

#[doc(inline)]
pub use crate::__digest_trees_conformance_suite as suite;

/// Runs each scenario, a function generic over the digest group alone, once
/// in each group, each run a test with the attribute `$test`, in a module
/// named for its group: `in_both_groups!(#[tokio::test] a_story)` makes
/// `lthash16::a_story`, which runs `a_story::<LtHash16>()` of the module it is
/// called in, and `ristretto255::a_story`. Given the test database in brackets
/// before them, as [`suite`] gives them, the scenarios are the suite's, each
/// run over a fresh store the expression `$db` makes.
#[doc(hidden)]
#[macro_export]
macro_rules! __digest_trees_conformance_in_both_groups {
    (#[$test:meta] $($scenario:ident),* $(,)?) => {
        mod lthash16 {
            $(#[$test]
            async fn $scenario() -> $crate::sync::conformance::__runner::Result<()> {
                super::$scenario::<$crate::sync::conformance::__runner::LtHash16>().await
            })*
        }

        mod ristretto255 {
            $(#[$test]
            async fn $scenario() -> $crate::sync::conformance::__runner::Result<()> {
                super::$scenario::<$crate::sync::conformance::__runner::Ristretto255>().await
            })*
        }
    };

    ([#[$test:meta] $db:expr] $($scenario:ident),* $(,)?) => {
        mod lthash16 {
            use super::*;

            $(#[$test]
            async fn $scenario() -> $crate::sync::conformance::__runner::Result<()> {
                $crate::sync::conformance::$scenario::<$crate::sync::conformance::__runner::LtHash16, _>(&$db).await
            })*
        }

        mod ristretto255 {
            use super::*;

            $(#[$test]
            async fn $scenario() -> $crate::sync::conformance::__runner::Result<()> {
                $crate::sync::conformance::$scenario::<$crate::sync::conformance::__runner::Ristretto255, _>(&$db).await
            })*
        }
    };
}

#[doc(inline)]
pub use crate::__digest_trees_conformance_in_both_groups as in_both_groups;

/// Runs each of the crate's store conformance cases once, as [`suite`] gives
/// them, each a test with the attribute `$test` over a fresh store the
/// expression `$db` makes, in the module `store_contract`.
#[doc(hidden)]
#[macro_export]
macro_rules! __digest_trees_conformance_store_cases_once {
    ([#[$test:meta] $db:expr] $($case:ident),* $(,)?) => {
        mod store_contract {
            use super::*;

            $(#[$test]
            async fn $case() -> $crate::sync::conformance::__runner::Result<()> {
                $crate::sync::conformance::$case(&$db).await
            })*
        }
    };
}

/// Types named by the generated suite tests.
#[doc(hidden)]
pub mod __runner {
    pub use ankurah_digest_tree::{LtHash16, Ristretto255};
    pub use anyhow::Result;
}

/// The test database a scenario runs on: one database, fresh for the
/// scenario, which opens an engine handle on it for each node of the
/// scenario.
///
/// A database that many processes share, such as a server's, gives each
/// node an engine of its own, with its own connections, as a node in another
/// process would have; one kept in one process's memory gives each node the
/// one engine it has.
pub trait TestDatabase {
    /// The engine a node reaches the database through.
    type Engine: StorageEngine + 'static;

    /// An engine handle on the test database, for another node of the
    /// scenario.
    fn engine(&self) -> impl Future<Output = anyhow::Result<Arc<Self::Engine>>>;

    /// Whether a tree's read, begun on the database, goes on seeing the state
    /// it began at while a commit to the tree lands. A server's snapshot does;
    /// an engine whose commits wait for the reads open on their tree, or whose
    /// reads can't outlive the state they began at, doesn't, and a scenario
    /// ends a tree's reads there before it commits to the tree.
    fn keeps_a_reads_state_across_commits(&self) -> bool;
}
