//! A conformance-test node's engine and cache, used to create and maintain
//! trees without depending on TreeManager's entity types or residency policy.

use std::sync::Arc;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_digest_tree::{Created, DigestGroup, DigestTree, Encode, Member, MemberId, Payload};

use crate::error::{MutationError, RetrievalError};
use crate::storage::StorageEngine;
use crate::sync::storage_adapter;
use crate::sync::{AppliedTrees, CacheSettings, EngineAdapter, StorageCache, TreeChange, TreeId};

pub struct TestStores<SE> {
    pub(crate) storage: Arc<SE>,
    pub(crate) cache: Arc<StorageCache>,
}

impl<SE> Clone for TestStores<SE> {
    fn clone(&self) -> Self { Self { storage: Arc::clone(&self.storage), cache: Arc::clone(&self.cache) } }
}

impl<SE: StorageEngine + 'static> TestStores<SE> {
    pub fn new(storage: Arc<SE>, cache: CacheSettings) -> Self { Self { storage, cache: Arc::new(StorageCache::new(cache)) } }

    pub async fn create<K, I, L, G, M>(
        &self,
        trx: &mut SE::Transaction<'_>,
        id: TreeId,
        key_spec: KeySpec<K>,
        members: impl IntoIterator<Item = M>,
    ) -> Result<Created<K, I, L, G, EngineAdapter<SE, K, L, G>>, MutationError>
    where
        K: Encode + Clone + Send + Sync + 'static,
        I: MemberId,
        L: Encode + Payload,
        G: DigestGroup,
        M: Member<K, Id = I, Payload = L>,
    {
        storage_adapter::create(Arc::clone(&self.storage), Arc::clone(&self.cache), trx, id, key_spec, members).await
    }

    pub async fn open<K, I, L, G>(&self, id: &TreeId) -> Result<DigestTree<K, I, L, G, EngineAdapter<SE, K, L, G>>, RetrievalError>
    where
        K: Encode + Clone + Send + Sync + 'static,
        I: MemberId,
        L: Encode + Payload,
        G: DigestGroup,
    {
        storage_adapter::open(Arc::clone(&self.storage), Arc::clone(&self.cache), id).await
    }

    pub async fn rebuild<K, I, L, G, M>(
        &self,
        trx: &mut SE::Transaction<'_>,
        id: TreeId,
        key_spec: KeySpec<K>,
        members: impl IntoIterator<Item = M>,
    ) -> Result<Created<K, I, L, G, EngineAdapter<SE, K, L, G>>, MutationError>
    where
        K: Encode + Clone + PartialEq + Send + Sync + 'static,
        I: MemberId,
        L: Encode + Payload,
        G: DigestGroup,
        M: Member<K, Id = I, Payload = L>,
    {
        storage_adapter::rebuild(Arc::clone(&self.storage), Arc::clone(&self.cache), trx, id, key_spec, members).await
    }

    pub async fn apply<K, I, L, G>(
        &self,
        trx: &mut SE::Transaction<'_>,
        changes: Vec<TreeChange<SE, K, I, L, G>>,
    ) -> Result<AppliedTrees<SE, K, I, L, G>, MutationError>
    where
        K: Encode + Clone + Send + Sync + 'static,
        I: MemberId,
        L: Encode + Payload,
        G: DigestGroup,
    {
        storage_adapter::apply(&self.storage, trx, changes).await
    }

    pub async fn drop(&self, trx: &mut SE::Transaction<'_>, id: &TreeId) -> Result<(), MutationError> {
        storage_adapter::drop(trx, id).await
    }
}
