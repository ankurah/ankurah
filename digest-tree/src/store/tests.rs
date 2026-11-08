//! The store contract's conformance suite, run against the in-memory
//! reference store.

use crate::common::{Property, Tag};
use crate::test_support::store_conformance::{self, every_case};
use crate::{MemoryStore, Ristretto255, Row, RowKey};

/// The rows the suite writes.
type Rows = Row<Property, Tag, Ristretto255>;

/// Runs each case against a fresh memory store.
macro_rules! against_a_memory_store {
    ($($case:ident),* $(,)?) => {
        $(#[tokio::test]
        async fn $case() { store_conformance::$case(MemoryStore::<RowKey, Rows>::new()).await })*
    };
}

every_case!(against_a_memory_store);

#[tokio::test]
async fn a_store_owns_its_own_transactions_and_no_others() {
    store_conformance::a_store_owns_its_own_transactions_and_no_others(MemoryStore::<RowKey, Rows>::new(), MemoryStore::new()).await
}
