//! A scan of a view's members: a stream that reads them in the view's
//! direction a batch at a time, each batch under the tree's read lock for its
//! own read alone.

use std::future::Future;
use std::mem;
use std::ops::Bound;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::vec;

use futures_core::Stream;

use super::first_members;
use super::view::Leaf;
use crate::span::split_address;
use crate::store::LeafRow;
use crate::tree::rows::StagedRows;
use crate::tree::trie::BUCKET_CAPACITY;
use crate::tree::DigestTree;
use crate::{DigestGroup, Error, MemberId, Payload, Row, RowKey, ScanDirection, Span, Store};

/// How many members a scan's first batch yields: as many as a bucket holds,
/// so that a span refined down to one bucket is read in one hold of the lock.
const FIRST_BATCH: usize = BUCKET_CAPACITY as usize;

/// The most members one batch yields. Each batch yields twice as many as the
/// one before, up to this, so that a long scan takes the lock few times and
/// no one hold of it keeps a commit waiting long.
const LARGEST_BATCH: usize = 1024;

/// The members of a view within a span, as a stream in the view's direction:
/// what [`DigestTreeView::members`](crate::DigestTreeView::members) returns,
/// whose docs say what it promises. It owns a handle to the tree, so its
/// caller can keep it or move it freely.
pub struct Members<K, I, L, G: DigestGroup, S> {
    tree: DigestTree<K, I, L, G, S>,
    direction: ScanDirection,

    /// How many members the next batch yields.
    batch_size: usize,

    progress: Progress<I, L>,
}

/// How far a scan has got.
enum Progress<I, L> {
    /// Yielding the members a batch read, none before the first, then
    /// reading on through `unread`: the spans still to read, in the order the
    /// scan reads them, the first starting just past the last member read.
    /// `unread` is empty once a batch has read to the end.
    Yielding { read: vec::IntoIter<Leaf<I, L>>, unread: Vec<Span> },

    /// Reading a batch.
    Reading(BatchRead<I, L>),

    /// Every member yielded, or a read failed.
    Ended,
}

/// A batch being read: a future that owns everything it reads with, so that
/// the stream borrows nothing.
type BatchRead<I, L> = Pin<Box<dyn Future<Output = Result<Batch<I, L>, Error>> + Send>>;

/// The members one batch read, and the spans still to read after them.
struct Batch<I, L> {
    members: Vec<Leaf<I, L>>,
    unread: Vec<Span>,
}

impl<K, I, L, G, S> Members<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// A scan of `tree`'s members within `spans`, which are disjoint and in
    /// address order, read in `direction`. It reads nothing until polled.
    pub(super) fn new(tree: DigestTree<K, I, L, G, S>, mut spans: Vec<Span>, direction: ScanDirection) -> Self {
        if direction == ScanDirection::Reverse {
            spans.reverse();
        }

        let progress = Progress::Yielding { read: Vec::new().into_iter(), unread: spans };
        Self { tree, direction, batch_size: FIRST_BATCH, progress }
    }
}

impl<K, I, L, G, S> Stream for Members<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    type Item = Result<Leaf<I, L>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let scan = &mut *self;

        loop {
            match &mut scan.progress {
                Progress::Yielding { read, unread } => {
                    if let Some(member) = read.next() {
                        return Poll::Ready(Some(Ok(member)));
                    }

                    if unread.is_empty() {
                        scan.progress = Progress::Ended;
                        return Poll::Ready(None);
                    }

                    let batch = read_batch(scan.tree.clone(), mem::take(unread), scan.direction, scan.batch_size);
                    scan.batch_size = (scan.batch_size * 2).min(LARGEST_BATCH);
                    scan.progress = Progress::Reading(Box::pin(batch));
                }

                Progress::Reading(batch) => match ready!(batch.as_mut().poll(cx)) {
                    Ok(Batch { members, unread }) => scan.progress = Progress::Yielding { read: members.into_iter(), unread },

                    Err(error) => {
                        scan.progress = Progress::Ended;
                        return Poll::Ready(Some(Err(error)));
                    }
                },

                Progress::Ended => return Poll::Ready(None),
            }
        }
    }
}

/// A scan never pins what it holds: the batch it is reading is pinned in a
/// box of its own. So it is `Unpin` whatever its members hold, as stream
/// adapters that poll their stream without pinning it require.
impl<K, I, L, G: DigestGroup, S> Unpin for Members<K, I, L, G, S> {}

/// A scan's next batch: at most `size` members within `unread`, the spans
/// still to read in the order the scan reads them, read in `direction` under
/// the tree's read lock.
async fn read_batch<K, I, L, G, S>(
    tree: DigestTree<K, I, L, G, S>,
    unread: Vec<Span>,
    direction: ScanDirection,
    size: usize,
) -> Result<Batch<I, L>, Error>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    // One member past the batch tells whether any lie beyond it, so that the
    // scan ends without a read that finds nothing.
    let mut leaves = {
        let _read = tree.inner.lock.read().await;
        first_members(&StagedRows::new(&tree.inner.store), &unread, direction, size + 1).await?
    };

    let unread = match leaves.len() > size {
        true => {
            leaves.truncate(size);
            let (last, _) = leaves.last().expect("a batch yields at least one member");
            past(unread, last, direction)
        }

        false => Vec::new(),
    };

    let members = leaves.into_iter().map(|(address, row)| leaf(&address, row)).collect::<Result<_, _>>()?;
    Ok(Batch { members, unread })
}

/// The spans of `unread`, in the order the scan reads them, from just past
/// `last` on: the span that holds `last`, cut to the addresses after it in
/// `direction`, and every span after that one.
fn past(mut unread: Vec<Span>, last: &[u8], direction: ScanDirection) -> Vec<Span> {
    let holding = unread.iter().position(|span| span.contains(last)).expect("a member read lies within the spans it was read from");
    unread.drain(..holding);

    let after = Bound::Excluded(last.to_vec());
    match direction {
        ScanDirection::Forward => unread[0].start = after,
        ScanDirection::Reverse => unread[0].end = after,
    }

    unread
}

/// The member at `address`, as its row holds it. The tree stores only
/// addresses it formed from a key and an id, so one that doesn't split into
/// them, or holds no id, means its rows are damaged.
fn leaf<I: MemberId, L, G: DigestGroup>(address: &[u8], row: LeafRow<L, G>) -> Result<Leaf<I, L>, Error> {
    let (key, id) = split_address(address)?;
    let id = I::from_bytes(&id).ok_or(Error::Inconsistent("a stored address holds no member id"))?;

    Ok(Leaf { key, id, payload: row.payload })
}
