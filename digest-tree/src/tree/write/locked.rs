//! A tree on a store with no other owner: created, and its transactions
//! committed, each in a transaction of the store's own.

use ankurah_core_types::key_spec::KeySpec;

use super::Transaction;
use crate::tree::DigestTree;
use crate::{DigestGroup, Error, Locked, Member, MemberId, OrderedMap, Payload, Row, RowKey};

impl<K, I, L, G, M> DigestTree<K, I, L, G, Locked<M>>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    M: OrderedMap<Key = RowKey, Value = Row<K, L, G>>,
{
    /// A new tree keyed by `key_spec`, holding exactly `members`, on a store
    /// with no other owner, which holds no rows yet: the tree
    /// [`create_in`](Self::create_in) makes, in a transaction of the store's
    /// own that it commits at once. It refuses what `create_in` refuses, and
    /// writes nothing then; a commit the store fails is [`Error::Store`].
    pub async fn create<Mb: Member<K, Id = I, Payload = L>>(
        store: Locked<M>,
        key_spec: KeySpec<K>,
        members: impl IntoIterator<Item = Mb>,
    ) -> Result<Self, Error> {
        // Collected before the first await, so that the caller's iterator is
        // never held across one.
        let members: Vec<Mb> = members.into_iter().collect();

        let mut trx = store.begin().await;
        let created = Self::create_in(store.clone(), &mut trx, key_spec, members).await?;
        trx.commit().await?;

        Ok(created.landed())
    }
}

impl<K, I, L, G, M> Transaction<K, I, L, G, Locked<M>>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    M: OrderedMap<Key = RowKey, Value = Row<K, L, G>>,
{
    /// Commit every change in one batch on a store with no other owner, all
    /// of it or none: the transaction [prepared](Self::prepare), its new
    /// versions hashed before the store's transaction begins, then
    /// [applied](crate::Prepared::apply) in a transaction of the store's own,
    /// which it commits. It refuses what `apply` refuses, and writes nothing
    /// then; a commit the store fails is [`Error::Store`].
    pub async fn commit(self) -> Result<(), Error> {
        let store = self.tree().store().clone();
        let prepared = self.prepare().await;

        let mut trx = store.begin().await;
        let applied = prepared.apply(&mut trx).await?;
        trx.commit().await?;
        applied.landed();

        Ok(())
    }
}
