//! What every [`Store`] promises a tree, checked over any store and the
//! owner of its transactions: each case is a function a test runs once, over a
//! fresh store that holds no rows.
//!
//! The cases write rows of one kind under keys of every kind, since a store
//! keeps each row whole whatever it holds: the tree's metadata row, told apart
//! by the number it holds.
//!
//! Some race two transactions, or a read and a commit, by polling one of them
//! once. On a store whose calls complete at once when nothing holds them, as
//! a store in memory's do, a call still pending after that poll is waiting;
//! on any other, it may be pending for its own work alone, and the store's
//! own tests prove that it waits.

use std::future::Future;
use std::ops::Bound;
use std::pin::pin;
use std::task::Poll;

use ankurah_core_types::key_spec::KeySpec;

use super::poll_once;
use crate::store::Meta;
use crate::{
    DigestGroup, Locked, LockedTrx, NodePrefix, OrderedMap, ReadTrx, Row, RowKey, ScanDirection, Store, StoreError, StoreTrx, WriteItem,
};

/// Every case that runs on one owner, by name, handed to `$run`, a macro of a
/// store's tests that wraps each in the store's own test of it:
/// `every_case!(against_a_store)` calls
/// `against_a_store!(a_row_reads_back_after_a_put_and_is_gone_after_a_delete, ...)`.
/// Tokens in brackets ahead of `$run` go ahead of the names, for what `$run`
/// needs besides them: `every_case!([a_store()] on)` calls
/// `on!([a_store()] a_row_reads_back_after_a_put_and_is_gone_after_a_delete, ...)`.
/// Names after `$run` join the list, for cases of the store's own that it
/// runs beside them, such as its run of
/// [`a_store_owns_its_own_transactions_and_no_others`](crate::test_support::store_conformance::a_store_owns_its_own_transactions_and_no_others), which takes two
/// owners. Every case on one owner is listed here, so that every store runs
/// it.
#[doc(hidden)]
#[macro_export]
macro_rules! __store_conformance_every_case {
    ($([$($with:tt)*])? $run:path $(, $also:ident)* $(,)?) => {
        $run!(
            $([$($with)*])?
            a_row_reads_back_after_a_put_and_is_gone_after_a_delete,
            a_range_reads_the_rows_within_its_bounds_in_order,
            bounds_that_hold_nothing_read_nothing,
            a_read_of_many_keys_answers_each_in_order,
            every_put_and_delete_of_a_write_lands,
            clones_of_a_store_share_its_rows,
            a_transactions_reads_see_its_own_write,
            a_write_lands_when_its_transaction_commits,
            a_dropped_transaction_lands_nothing,
            two_transactions_commit_as_if_one_followed_the_other,
            a_read_sees_one_committed_state,
            $($also),*
        );
    };
}

#[doc(inline)]
pub use crate::__store_conformance_every_case as every_case;

/// The owner of a store's transactions, as the cases need it: the store, and
/// transactions on it to begin, lend to a writer, and commit.
pub trait Owner<K, V> {
    /// The store the owner's transactions are on.
    type Store: Store<K, V>;

    /// The owner's own transaction, which it lends to a writer as the store's.
    type Transaction;

    /// The store.
    fn store(&self) -> Self::Store;

    /// Begin a transaction.
    fn begin(&self) -> impl Future<Output = Result<Self::Transaction, StoreError>>;

    /// The transaction as the store's, lent to a writer until it is dropped,
    /// for one commit or one creation, as a tree's owner lends it: the writer
    /// reads its own write through the lending it wrote with, since a store
    /// may keep, in what it lends, what the writer's reads answer from until
    /// it writes.
    fn lend<'a>(&self, trx: &'a mut Self::Transaction) -> <Self::Store as Store<K, V>>::Trx<'a>;

    /// Commit the transaction: its writes land, all of them or none.
    fn commit(&self, trx: Self::Transaction) -> impl Future<Output = Result<(), StoreError>>;

    /// Whether a read of the store goes on seeing the state it began at while
    /// a transaction's commit lands, the commit not waiting for it. A
    /// database's snapshot does; a store whose commits wait for the reads
    /// open on it, or whose reads fail retryably once a commit moved what
    /// they read, doesn't.
    fn keeps_a_reads_state_across_commits(&self) -> bool;
}

/// A store with no other owner owns its own transactions.
impl<M: OrderedMap> Owner<M::Key, M::Value> for Locked<M> {
    type Store = Self;
    type Transaction = LockedTrx<M>;

    fn store(&self) -> Self { self.clone() }

    async fn begin(&self) -> Result<LockedTrx<M>, StoreError> { Ok(Locked::begin(self).await) }

    fn lend<'a>(&self, trx: &'a mut LockedTrx<M>) -> &'a mut LockedTrx<M> { trx }

    async fn commit(&self, trx: LockedTrx<M>) -> Result<(), StoreError> { trx.commit().await }

    /// A commit waits for the reads open on the store before it applies its
    /// batch.
    fn keeps_a_reads_state_across_commits(&self) -> bool { false }
}

/// A metadata row holding `n`, so that rows tell apart.
macro_rules! row {
    ($n:expr) => {
        Row::Meta(Meta { key_spec: KeySpec::new(Vec::new()), group: $n })
    };
}

/// The number a row the cases wrote holds.
fn held<K, L, G: DigestGroup>(row: Row<K, L, G>) -> u8 {
    match row {
        Row::Meta(meta) => meta.group,
        _ => panic!("the store returned a row of another kind than it was given"),
    }
}

fn held_rows<K, L, G: DigestGroup>(rows: Vec<(RowKey, Row<K, L, G>)>) -> Vec<(RowKey, u8)> {
    rows.into_iter().map(|(key, row)| (key, held(row))).collect()
}

/// Keys of each kind, in key order: kinds sort together, in declared order.
fn keys() -> Vec<RowKey> {
    vec![
        RowKey::Meta,
        RowKey::Node(NodePrefix::root()),
        RowKey::Node(NodePrefix::root().child(false)),
        RowKey::Node(NodePrefix::root().child(false).child(true)),
        RowKey::Node(NodePrefix::root().child(true)),
        RowKey::Leaf(vec![]),
        RowKey::Leaf(vec![1]),
        RowKey::Leaf(vec![1, 0]),
        RowKey::Leaf(vec![2]),
        RowKey::Leaf(vec![0xFF, 0xFF]),
    ]
}

/// Every key of [`keys`], the nth holding `n`, as a [`filled`] store holds them.
fn filling() -> Vec<(RowKey, u8)> { keys().into_iter().zip(0..).collect() }

/// Write `batch` in one transaction of `owner`'s, and commit it.
async fn commit<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: &O, batch: Vec<WriteItem<RowKey, Row<K, L, G>>>) {
    let mut trx = owner.begin().await.unwrap();
    let pending = owner.lend(&mut trx).write(batch).await.unwrap();
    owner.commit(trx).await.unwrap();

    owner.store().landed(pending);
}

/// Commit a row under every key of [`keys`], the nth holding `n`.
async fn filled<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: &O) {
    commit(owner, keys().into_iter().zip(0..).map(|(key, n)| WriteItem::Put(key, row!(n))).collect()).await
}

/// The number the row under `key` holds, through a read of its own.
async fn read_one<K, L, G: DigestGroup, S: Store<RowKey, Row<K, L, G>>>(store: &S, key: &RowKey) -> Option<u8> {
    store.begin_read().await.unwrap().get(key).await.unwrap().map(held)
}

/// Every row between the bounds, read `page` rows at a time from the last
/// key each read gave, as a tree reads a range: until a read gives fewer rows
/// than it was asked, which holds every row left. No read may give more.
async fn read_all<K, L, G: DigestGroup>(
    rows: &mut impl ReadTrx<RowKey, Row<K, L, G>>,
    (start, end): (Bound<&RowKey>, Bound<&RowKey>),
    direction: ScanDirection,
    page: usize,
) -> Vec<(RowKey, u8)> {
    let mut read: Vec<(RowKey, u8)> = Vec::new();
    loop {
        let (from, to) = match (read.last(), direction) {
            (None, _) => (start.cloned(), end.cloned()),
            (Some((last, _)), ScanDirection::Forward) => (Bound::Excluded(last.clone()), end.cloned()),
            (Some((last, _)), ScanDirection::Reverse) => (start.cloned(), Bound::Excluded(last.clone())),
        };

        let rows = rows.range(from.as_ref(), to.as_ref(), direction, page).await.unwrap();
        assert!(rows.len() <= page, "a read gave {} rows where {page} were asked", rows.len());

        let last_page = rows.len() < page;
        read.extend(held_rows(rows));

        if last_page {
            return read;
        }
    }
}

/// A committed put reads back, a later put replaces it, and a delete removes
/// it; deleting a row that isn't there changes nothing.
pub async fn a_row_reads_back_after_a_put_and_is_gone_after_a_delete<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    let store = owner.store();
    assert_eq!(read_one(&store, &RowKey::Meta).await, None);

    commit(&owner, vec![WriteItem::Put(RowKey::Meta, row!(1))]).await;
    assert_eq!(read_one(&store, &RowKey::Meta).await, Some(1));

    commit(&owner, vec![WriteItem::Put(RowKey::Meta, row!(2))]).await;
    assert_eq!(read_one(&store, &RowKey::Meta).await, Some(2), "a put replaces the row");

    commit(&owner, vec![WriteItem::Delete(RowKey::Meta)]).await;
    assert_eq!(read_one(&store, &RowKey::Meta).await, None);

    commit(&owner, vec![WriteItem::Delete(RowKey::Meta)]).await;
    assert_eq!(read_one(&store, &RowKey::Meta).await, None, "deleting an absent row changes nothing");
}

/// A range read gives the rows between its bounds in key order or reversed,
/// every kind of bound, and fewer than it asks only where fewer lie between
/// them; a reader that reads on from the last key it was given reads each row
/// once, however few rows it asks for at a time.
pub async fn a_range_reads_the_rows_within_its_bounds_in_order<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    filled(&owner).await;
    let (keys, all) = (keys(), filling());
    let mut read = owner.store().begin_read().await.unwrap();

    let (first_leaf, second_leaf, last_leaf) = (&keys[5], &keys[6], &keys[8]);
    for (bounds, expected) in [
        ((Bound::Unbounded, Bound::Unbounded), &all[..]),
        ((Bound::Included(first_leaf), Bound::Included(last_leaf)), &all[5..9]),
        ((Bound::Excluded(first_leaf), Bound::Included(last_leaf)), &all[6..9]),
        ((Bound::Included(first_leaf), Bound::Excluded(last_leaf)), &all[5..8]),
        ((Bound::Excluded(first_leaf), Bound::Excluded(second_leaf)), &all[6..6]),
        ((Bound::Included(second_leaf), Bound::Unbounded), &all[6..]),
        ((Bound::Unbounded, Bound::Excluded(first_leaf)), &all[..5]),
    ] {
        let reversed: Vec<_> = expected.iter().rev().cloned().collect();

        for page in [usize::MAX, 3, 1] {
            assert_eq!(read_all(&mut read, bounds, ScanDirection::Forward, page).await, expected, "{bounds:?}, {page} at a time");
            assert_eq!(read_all(&mut read, bounds, ScanDirection::Reverse, page).await, reversed, "{bounds:?} reversed, {page} at a time");
        }
    }

    let none = read.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 0).await.unwrap();
    assert!(none.is_empty(), "a read of no rows gives none");
}

/// Bounds whose start lies past their end, or on it with either excluded,
/// hold no key, and a read between them gives nothing.
pub async fn bounds_that_hold_nothing_read_nothing<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    filled(&owner).await;
    let (low, high) = (RowKey::Leaf(vec![1]), RowKey::Leaf(vec![2]));
    let mut read = owner.store().begin_read().await.unwrap();

    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        for (start, end) in [
            (Bound::Included(&high), Bound::Included(&low)),
            (Bound::Included(&low), Bound::Excluded(&low)),
            (Bound::Excluded(&low), Bound::Included(&low)),
            (Bound::Excluded(&low), Bound::Excluded(&low)),
        ] {
            assert!(read.range(start, end, direction, usize::MAX).await.unwrap().is_empty(), "{start:?} to {end:?}");
        }
    }
}

/// A read of many keys answers each in turn, one per key and in the order
/// asked, a key twice or a key the store lacks among them.
pub async fn a_read_of_many_keys_answers_each_in_order<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    filled(&owner).await;
    let (keys, absent) = (keys(), RowKey::Leaf(vec![3]));
    let asked = [keys[6].clone(), absent, keys[1].clone(), keys[6].clone()];

    let mut read = owner.store().begin_read().await.unwrap();
    let found: Vec<Option<u8>> = read.get_many(&asked).await.unwrap().into_iter().map(|row| row.map(held)).collect();

    assert_eq!(found, [Some(6), None, Some(1), Some(6)]);
    assert!(read.get_many(&[]).await.unwrap().is_empty());
}

/// Every put and delete of a transaction's write lands when it commits.
pub async fn every_put_and_delete_of_a_write_lands<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    filled(&owner).await;
    let keys = keys();

    commit(
        &owner,
        vec![
            WriteItem::Delete(keys[1].clone()),
            WriteItem::Put(keys[2].clone(), row!(20)),
            WriteItem::Delete(keys[3].clone()),
            WriteItem::Put(keys[4].clone(), row!(40)),
        ],
    )
    .await;

    let mut read = owner.store().begin_read().await.unwrap();
    let nodes = read_all(&mut read, (Bound::Included(&keys[1]), Bound::Included(&keys[4])), ScanDirection::Forward, usize::MAX).await;

    assert_eq!(nodes, [(keys[2].clone(), 20), (keys[4].clone(), 40)]);
}

/// Clones of a store are the same store: what a transaction on one writes,
/// the others read, and each owns the transaction.
pub async fn clones_of_a_store_share_its_rows<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    let (store, clone) = (owner.store(), owner.store().clone());
    let leaf = RowKey::Leaf(vec![7]);

    let mut trx = owner.begin().await.unwrap();
    let mut lent = owner.lend(&mut trx);
    assert!(store.owns(&lent) && clone.owns(&lent));

    let pending = lent.write(vec![WriteItem::Put(leaf.clone(), row!(7))]).await.unwrap();
    drop(lent);
    owner.commit(trx).await.unwrap();
    store.landed(pending);

    assert_eq!(read_one(&clone, &leaf).await, Some(7));
}

/// A transaction's reads see its own write before it commits, its puts and
/// deletes alike, in reads of one key, of many, and of ranges both ways
/// however few rows each asks for.
pub async fn a_transactions_reads_see_its_own_write<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    filled(&owner).await;
    let keys = keys();
    let added = RowKey::Leaf(vec![1, 5]);

    let mut trx = owner.begin().await.unwrap();
    let mut lent = owner.lend(&mut trx);
    let batch =
        vec![WriteItem::Delete(keys[2].clone()), WriteItem::Put(keys[6].clone(), row!(60)), WriteItem::Put(added.clone(), row!(99))];
    lent.write(batch).await.unwrap();

    let mut expected = filling();
    expected.remove(2);
    expected[5].1 = 60;
    expected.insert(7, (added.clone(), 99));
    let reversed: Vec<_> = expected.iter().rev().cloned().collect();

    assert_eq!(lent.get(&keys[2]).await.unwrap().map(held), None, "deleted");
    assert_eq!(lent.get(&keys[6]).await.unwrap().map(held), Some(60), "replaced");
    assert_eq!(lent.get(&added).await.unwrap().map(held), Some(99), "added");

    let many: Vec<Option<u8>> =
        lent.get_many(&[added, keys[2].clone(), keys[3].clone()]).await.unwrap().into_iter().map(|row| row.map(held)).collect();
    assert_eq!(many, [Some(99), None, Some(3)]);

    for page in [usize::MAX, 2, 1] {
        assert_eq!(
            read_all(&mut lent, (Bound::Unbounded, Bound::Unbounded), ScanDirection::Forward, page).await,
            expected,
            "{page} at a time"
        );
        assert_eq!(
            read_all(&mut lent, (Bound::Unbounded, Bound::Unbounded), ScanDirection::Reverse, page).await,
            reversed,
            "reversed, {page} at a time"
        );
    }
}

/// A transaction's write lands only when it commits: a read before then
/// sees the rows as they were, and a read or a transaction begun after it
/// committed sees its write.
pub async fn a_write_lands_when_its_transaction_commits<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    let (store, leaf) = (owner.store(), RowKey::Leaf(vec![7]));
    commit(&owner, vec![WriteItem::Put(leaf.clone(), row!(1))]).await;

    let mut trx = owner.begin().await.unwrap();
    let pending = owner.lend(&mut trx).write(vec![WriteItem::Put(leaf.clone(), row!(2))]).await.unwrap();
    assert_eq!(read_one(&store, &leaf).await, Some(1), "a read before the commit");

    owner.commit(trx).await.unwrap();
    store.landed(pending);
    assert_eq!(read_one(&store, &leaf).await, Some(2), "a read after the commit");

    let mut next = owner.begin().await.unwrap();
    assert_eq!(owner.lend(&mut next).get(&leaf).await.unwrap().map(held), Some(2), "a transaction after the commit");
}

/// A transaction dropped uncommitted lands nothing, and holds nothing that
/// keeps the next transaction from beginning and committing.
pub async fn a_dropped_transaction_lands_nothing<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    let (store, leaf) = (owner.store(), RowKey::Leaf(vec![7]));
    commit(&owner, vec![WriteItem::Put(leaf.clone(), row!(1))]).await;

    let mut trx = owner.begin().await.unwrap();
    owner.lend(&mut trx).write(vec![WriteItem::Put(leaf.clone(), row!(2)), WriteItem::Put(RowKey::Meta, row!(2))]).await.unwrap();
    drop(trx);

    assert_eq!((read_one(&store, &leaf).await, read_one(&store, &RowKey::Meta).await), (Some(1), None));

    commit(&owner, vec![WriteItem::Put(leaf.clone(), row!(3))]).await;
    assert_eq!(read_one(&store, &leaf).await, Some(3));
}

/// Two transactions on one store commit as if one followed the other: one
/// that reads a row and writes it again from what it read, and another that
/// writes the row in the meantime. No write lands beneath an open
/// transaction: the other waits until the first has ended, or one of the two
/// fails to commit. Either way the row ends as some order of the commits that
/// succeeded leaves it.
pub async fn two_transactions_commit_as_if_one_followed_the_other<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    let (store, leaf) = (owner.store(), RowKey::Leaf(vec![7]));
    commit(&owner, vec![WriteItem::Put(leaf.clone(), row!(1))]).await;

    let mut first = owner.begin().await.unwrap();
    let read = owner.lend(&mut first).get(&leaf).await.unwrap().map(held).expect("the row");

    let other = async {
        let mut trx = owner.begin().await?;
        let pending = owner.lend(&mut trx).write(vec![WriteItem::Put(leaf.clone(), row!(10))]).await?;
        owner.commit(trx).await?;
        store.landed(pending);

        Ok::<_, StoreError>(())
    };

    let mut other = pin!(other);
    let other_done = match poll_once(other.as_mut()) {
        Poll::Ready(outcome) => Some(outcome.is_ok()),
        Poll::Pending => None,
    };

    let mut lent = owner.lend(&mut first);
    assert_eq!(lent.get(&leaf).await.unwrap().map(held), Some(read), "no other write landed beneath the open transaction");

    let pending = lent.write(vec![WriteItem::Put(leaf.clone(), row!(read + 1))]).await.unwrap();
    drop(lent);

    let first_ok = owner.commit(first).await.is_ok();
    if first_ok {
        store.landed(pending);
    }

    let other_ok = match other_done {
        Some(ok) => ok,
        None => other.await.is_ok(),
    };

    // The first read 1 before the other committed, so only the first, then
    // the other, is an order of both.
    let expected = match (first_ok, other_ok) {
        (true, true) => 10,
        (true, false) => 2,
        (false, true) => 10,
        (false, false) => 1,
    };

    assert_eq!(read_one(&store, &leaf).await, Some(expected), "the first committed: {first_ok}; the other: {other_ok}");
}

/// A read open across another transaction's commit sees one committed state
/// throughout: the rows before the commit, never some of each. On a store
/// that keeps a read's state across commits, the commit completes first; on
/// any other the commit waits for the read, or the read fails retryably. A
/// read begun once the commit has completed sees it.
pub async fn a_read_sees_one_committed_state<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O) {
    let (store, one, two) = (owner.store(), RowKey::Leaf(vec![1]), RowKey::Leaf(vec![2]));
    commit(&owner, vec![WriteItem::Put(one.clone(), row!(1)), WriteItem::Put(two.clone(), row!(1))]).await;

    let mut read = store.begin_read().await.unwrap();
    assert_eq!(read.get(&one).await.unwrap().map(held), Some(1));
    let committing = commit(&owner, vec![WriteItem::Put(one.clone(), row!(2)), WriteItem::Put(two.clone(), row!(2))]);

    if owner.keeps_a_reads_state_across_commits() {
        committing.await;
        assert_eq!(read.get(&two).await.unwrap().map(held), Some(1), "the state the read began at, across the commit");
    } else {
        let mut committing = pin!(committing);
        let committed = poll_once(committing.as_mut()).is_ready();

        match read.get(&two).await {
            Ok(row) => assert_eq!(row.map(held), Some(1), "one state throughout the read"),
            Err(failure) => assert!(failure.is_retryable(), "{failure:?}"),
        }

        drop(read);
        if !committed {
            committing.await;
        }
    }

    assert_eq!((read_one(&store, &one).await, read_one(&store, &two).await), (Some(2), Some(2)), "a read after the commit");
}

/// A store owns its own transactions, and its clones' alike, and no other
/// store's: `other` is another store, holding no rows either. One
/// transaction is open at a time, since an owner may make a transaction on
/// one store wait for one open on another, as a database with one writer
/// does.
pub async fn a_store_owns_its_own_transactions_and_no_others<K, L, G: DigestGroup, O: Owner<RowKey, Row<K, L, G>>>(owner: O, other: O) {
    let (store, other_store) = (owner.store(), other.store());

    let mut trx = owner.begin().await.unwrap();
    assert!(store.owns(&owner.lend(&mut trx)));
    assert!(store.clone().owns(&owner.lend(&mut trx)));
    assert!(!other_store.owns(&owner.lend(&mut trx)));
    drop(trx);

    let mut others = other.begin().await.unwrap();
    assert!(!store.owns(&other.lend(&mut others)));
}
