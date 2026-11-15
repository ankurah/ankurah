//! The crate's store conformance suite, run on a digest tree's store.

use std::collections::BTreeMap;
use std::sync::Arc;

use ankurah_digest_tree::test_support::store_conformance::{self, Owner};
use ankurah_digest_tree::{DigestGroup, Ristretto255, Row, RowKey, StoreError};

use super::fixtures::{stores, Label, Number};
use super::TestDatabase;
use crate::storage::{MapWrite, StorageEngine, StorageTransaction};
use crate::sync::error::transaction_store_failure;
use crate::sync::storage_adapter::layout::entry::{self, TreeEntry};
use crate::sync::storage_adapter::layout::names::{trees_map, Incarnation};
use crate::sync::storage_adapter::Inner;
use crate::sync::{EngineAdapter, StorageEngineTrx, TreeId};

type Store<SE, G> = EngineAdapter<SE, Number, Label, G>;

/// A tree's store on an engine, and the owner of the storage transactions
/// lent to it: each one takes the tree's lock and reads its entry as it
/// begins, as a change through the storage adapter does.
struct TreeOwner<SE: StorageEngine + 'static, G: DigestGroup> {
    engine: Arc<SE>,
    store: Store<SE, G>,

    /// What the test database says of its reads' state across commits.
    keeps_a_reads_state_across_commits: bool,
}

impl<SE: StorageEngine + 'static, G: DigestGroup> TreeOwner<SE, G> {
    /// Tree `id` on the engine of a node of `db`, whose entry is written
    /// and whose store holds no rows yet.
    async fn new(db: &impl TestDatabase<Engine = SE>, id: &str) -> anyhow::Result<Self> {
        let engine = &db.engine().await?;
        let (id, incarnation) = (TreeId::new(id), Incarnation::mint());
        let entry = TreeEntry { incarnation, generation: 0, valid: true };

        let mut trx = engine.transaction();
        trx.map_write(vec![MapWrite::Put { map: trees_map(), key: id.entry_key(), value: entry.encode() }]).await?;
        trx.commit().await?.committed()?;

        let fixtures = stores(engine);
        let store = EngineAdapter::new(Inner::new(fixtures.storage, fixtures.cache, id, incarnation));
        Ok(Self { engine: Arc::clone(engine), store, keeps_a_reads_state_across_commits: db.keeps_a_reads_state_across_commits() })
    }
}

/// A storage transaction that holds the tree's lock, with what it found
/// under it.
struct TrxUnderLock<'o, SE: StorageEngine + 'o> {
    trx: SE::Transaction<'o>,
    locked: u64,
}

impl<'o, SE: StorageEngine + 'static, G: DigestGroup> Owner<RowKey, Row<Number, Label, G>> for &'o TreeOwner<SE, G> {
    type Store = Store<SE, G>;
    type Transaction = TrxUnderLock<'o, SE>;

    fn store(&self) -> Store<SE, G> { self.store.clone() }

    async fn begin(&self) -> Result<TrxUnderLock<'o, SE>, StoreError> {
        let owner: &'o TreeOwner<SE, G> = self;
        let tree = owner.store.inner();

        let mut trx = owner.engine.transaction();
        let found = trx
            .map_lock_and_read(std::slice::from_ref(&tree.lock), vec![entry::read([&tree.id])])
            .await
            .map_err(transaction_store_failure)?;
        let locked = tree.generation_found(found.into_iter().next())?;

        Ok(TrxUnderLock { trx, locked })
    }

    fn lend<'a>(&self, trx: &'a mut TrxUnderLock<'o, SE>) -> StorageEngineTrx<'a, SE, Number, Label, G> {
        StorageEngineTrx::new(self.store.clone(), &mut trx.trx, Some(trx.locked), BTreeMap::new())
    }

    async fn commit(&self, trx: TrxUnderLock<'o, SE>) -> Result<(), StoreError> {
        trx.trx.commit().await.and_then(|outcome| outcome.committed()).map_err(transaction_store_failure)?;
        Ok(())
    }

    fn keeps_a_reads_state_across_commits(&self) -> bool { self.keeps_a_reads_state_across_commits }
}

/// Each case of the crate's, on a fresh tree's store, in one digest group,
/// since the cases write only metadata rows, which hold no digest.
macro_rules! on_a_stored_tree {
    ($($case:ident),* $(,)?) => {
        $(#[doc = concat!("The crate's store conformance case [`", stringify!($case), "`](store_conformance::", stringify!($case), "), on a digest tree's store.")]
        pub async fn $case<D: TestDatabase>(db: &D) -> anyhow::Result<()> {
            store_conformance::$case(&TreeOwner::<_, Ristretto255>::new(db, "tree").await?).await;
            Ok(())
        })*
    };
}

store_conformance::every_case!(on_a_stored_tree);

/// The crate's store conformance case
/// [`a_store_owns_its_own_transactions_and_no_others`](store_conformance::a_store_owns_its_own_transactions_and_no_others),
/// on two digest trees' stores.
pub async fn a_store_owns_its_own_transactions_and_no_others<D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let (owner, other) = (TreeOwner::<_, Ristretto255>::new(db, "tree").await?, TreeOwner::new(db, "other").await?);

    store_conformance::a_store_owns_its_own_transactions_and_no_others(&owner, &other).await;
    Ok(())
}
