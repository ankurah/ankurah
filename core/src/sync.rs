//! Resident digest trees, query views, and their storage-engine adapter.
//!
//! Each engine adapter scopes its reads and writes to one tree's maps. The
//! adapters use a shared StorageCache for their records and apply tree
//! changes atomically through the caller's storage transaction.
//!
//! TreeManager keeps a weak registry of live trees and retains recent trees
//! in a strong LRU. A tree stays maintained while the LRU or any caller owns
//! it, including through the digest crate's views, streams and transactions.

// Resident trees stay keyed by resolved Selection. TreeManager::view matches
// exactly and returns the digest crate's existing view. Query callers
// apply LIMIT; views and maintenance cover full predicate membership.
// TODO(fwd:TwVZVJaD): let view lookup choose a compatible tree under another
// Selection, without adding a second resident entry. Cases: different LIMITs,
// proven equivalent predicates, compatible orderings and exact span restrictions.
// LIMIT constrains reads, never tree membership. Reverse/prefix reuse must
// preserve null placement and entity-ID tie order; KeySpec::matches alone is
// insufficient. TreeManager owns this matching.

mod error;
mod manager;
mod member;
pub mod storage_adapter;

pub use error::{Error, TreePart};
pub use manager::TreeManager;
pub use member::{EntityHead, EntityMemberId, EntityVersion, TreeProperty};
pub use storage_adapter::{
    AppliedTrees, CacheSettings, EngineAdapter, StorageCache, StorageEngineSnapshot, StorageEngineTrx, TreeChange, TreeId, TreeLanding,
};

#[cfg(any(test, feature = "test-helpers"))]
pub use storage_adapter::conformance;
