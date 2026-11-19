//! Rebuilding an existing tree from a complete set of members, through the
//! same ordinary row writes as incremental maintenance.

use std::ops::Bound;

use ankurah_core_types::key_spec::KeySpec;

use super::{create, Created};
use crate::key::check_spec;
use crate::tree::rows::TreeRows;
use crate::{DigestGroup, DigestTree, Error, Member, MemberId, Payload, Row, RowKey, ScanDirection, Store, StoreTrx, WriteItem};

impl<K, I, L, G, S> DigestTree<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// Rebuild the store's existing tree to hold exactly `members`, through
    /// its owner's transaction. The stored key spec and digest group must
    /// match, so existing handles remain usable after the owner commits. This
    /// also replaces legacy version 1 keys with the current ordering codec.
    /// [`Created::landed`] then returns a handle to the rebuilt tree.
    ///
    /// Every old row is read, and the new rows and deletions of obsolete rows
    /// are written in one batch. The store's normal transaction and caching
    /// rules apply. The owner supplies a complete, current set of members and
    /// coordinates it with other changes to the records the tree describes.
    ///
    /// A store without metadata is [`Error::NoTree`], another digest group is
    /// [`Error::WrongGroup`], and another key spec is [`Error::KeySpecMismatch`].
    /// An unknown key encoding is [`Error::WrongKeyEncoding`].
    /// Invalid members are refused as in [`create_in`](Self::create_in), and
    /// an unreadable old row fails the rebuild. These failures write nothing.
    pub async fn rebuild_in<M: Member<K, Id = I, Payload = L>>(
        store: S,
        mut trx: S::Trx<'_>,
        key_spec: KeySpec<K>,
        members: impl IntoIterator<Item = M>,
    ) -> Result<Created<K, I, L, G, S>, Error>
    where
        K: PartialEq,
    {
        let members: Vec<M> = members.into_iter().collect();
        if !store.owns(&trx) {
            return Err(Error::OtherTree);
        }
        check_spec(&key_spec)?;

        let mut rows = TreeRows::new(&mut trx);
        let meta = rows.meta().await?.ok_or(Error::NoTree)?;
        if meta.group != G::ROW_VERSION {
            return Err(Error::WrongGroup);
        }
        if meta.key_encoding != 1 && meta.key_encoding != ankurah_core_types::ordering::KEY_ENCODING_VERSION {
            return Err(Error::WrongKeyEncoding { found: meta.key_encoding });
        }
        if meta.key_spec != key_spec {
            return Err(Error::KeySpecMismatch);
        }

        let rebuilt = create::rows::<K, I, L, G, M>(&key_spec, members).await?;
        for (key, _) in rows.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await? {
            rows.delete(key);
        }
        for item in rebuilt {
            match item {
                WriteItem::Put(key, row) => rows.put(key, row),
                WriteItem::Delete(key) => rows.delete(key),
            }
        }

        // The overlay leaves one operation per key, including deletions of
        // old nodes and members absent from the rebuilt tree.
        let batch = rows.into_writes();
        let pending = trx.write(batch).await?;
        Ok(Created { tree: Self::holding(store, key_spec), pending })
    }
}
