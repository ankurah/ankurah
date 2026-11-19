//! Resident digest trees, query views, and their storage-engine adapter.
//!
//! Each engine adapter scopes its reads and writes to one tree's maps. The
//! adapters share a StorageCache for their records. TreeManager reconciles
//! canonical entity changes through engine-local continuation cursors, then
//! persists each tree's records and cursor together. Its write-behind policy
//! bounds how long completed maintenance remains only in memory.
//!
//! TreeManager keeps a weak registry of live trees and retains recent trees
//! in a strong LRU. A tree stays maintained while the LRU or any caller owns
//! it, including through the digest crate's views, streams and transactions.

// Resident trees stay keyed by resolved Selection. TreeManager::view matches
// exactly and returns the digest crate's existing view. LIMIT is part of
// membership: the first N matches in query order, with entity-ID tie breaking.
// Future view lookup may reuse a compatible tree under another Selection, but
// must prove membership coverage as well as key compatibility. Reverse/prefix
// reuse must also preserve null placement and entity-ID tie order;
// KeySpec::matches alone is insufficient. TreeManager owns this matching.

mod error;
mod manager;
mod member;
pub mod storage_adapter;

pub use error::{Error, TreePart};
pub use manager::{TreeManager, WriteBehind};
pub use member::{EntityHead, EntityMemberId, TreeProperty};
pub use storage_adapter::{
    AppliedTrees, CacheSettings, EngineAdapter, StorageCache, StorageEngineSnapshot, StorageEngineTrx, TreeChange, TreeId, TreeLanding,
};

#[cfg(any(test, feature = "test-helpers"))]
pub use storage_adapter::conformance;
