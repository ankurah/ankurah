//! The stream a range read returns: a view's members within a range, read in
//! the range's direction a batch at a time, a scan, each batch in a read of
//! the store of its own.

use std::future::Future;
use std::mem;
use std::ops::Bound;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::vec;

use futures_core::Stream;

use super::view::Leaf;
use crate::tree::trie::BUCKET_CAPACITY;
use crate::tree::DigestTree;
use crate::{DigestGroup, Error, IntoSpan, MemberId, Payload, Row, RowKey, ScanDirection, Span, Store};

/// How many members a scan's first batch yields: as many as a bucket holds,
/// so that a range within one bucket is read in one read of the store.
const FIRST_BATCH: usize = BUCKET_CAPACITY as usize;

/// The most members one batch yields. Each batch yields twice as many as the
/// one before, up to this, so that a long scan begins few reads of the store
/// and no one read is open long.
const LARGEST_BATCH: usize = 1024;

/// What a [range read](crate::DigestTreeView::range) covers and which way it
/// reads: a span of addresses, or a key range of values the view lowers under
/// its tree's key spec ([`IntoSpan`]), and the direction, as a store's
/// [range read](crate::ReadTrx::range) takes its bounds and a direction.
///
/// Anything that names a span reads forward as it is: `view.range(..)` reads
/// every member, `view.range(2013..)` those whose first key part holds 2013
/// or more, and a [`Span`] the addresses it holds; [`reverse`](Self::reverse)
/// turns any of them around.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRange<R = Span> {
    /// What is read: a span, or a key range not yet lowered.
    pub range: R,

    /// The order the addresses are read in.
    pub direction: ScanDirection,
}

impl<R> ScanRange<R> {
    /// `range`, read from its smallest address up.
    pub fn forward(range: R) -> Self { Self { range, direction: ScanDirection::Forward } }

    /// `range`, read from its largest address down.
    pub fn reverse(range: R) -> Self { Self { range, direction: ScanDirection::Reverse } }
}

/// What a [range read](crate::DigestTreeView::range) takes: a [`ScanRange`],
/// or anything that names a span ([`IntoSpan`]), read forward.
pub trait IntoScanRange {
    /// What the read covers, before the view lowers it to a span.
    type Range: IntoSpan;

    /// The range and the direction it is read in.
    fn into_scan_range(self) -> ScanRange<Self::Range>;
}

impl<R: IntoSpan> IntoScanRange for ScanRange<R> {
    type Range = R;

    fn into_scan_range(self) -> Self { self }
}

impl<R: IntoSpan> IntoScanRange for R {
    type Range = R;

    fn into_scan_range(self) -> ScanRange<R> { ScanRange::forward(self) }
}

/// The members of a view within a range, as a stream in the range's
/// direction: what [`DigestTreeView::range`](crate::DigestTreeView::range)
/// returns, whose docs say what it promises. It owns a handle to the tree, so
/// its caller can keep it or move it freely.
pub struct MemberStream<K, I, L, G: DigestGroup, S> {
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

    /// Reading a batch, which holds its read of the store until it is read.
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

impl<K, I, L, G, S> MemberStream<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// A scan of `tree`'s members within `unread`, a view's spans cut to the
    /// range read, disjoint and in address order, read in `direction`. It
    /// reads nothing until polled.
    pub(super) fn new(tree: DigestTree<K, I, L, G, S>, mut unread: Vec<Span>, direction: ScanDirection) -> Self {
        if direction == ScanDirection::Reverse {
            unread.reverse();
        }

        let progress = Progress::Yielding { read: Vec::new().into_iter(), unread };
        Self { tree, direction, batch_size: FIRST_BATCH, progress }
    }
}

impl<K, I, L, G, S> Stream for MemberStream<K, I, L, G, S>
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
/// box of its own. So it is `Unpin` whatever its members and its store hold,
/// as stream adapters that poll their stream without pinning it require.
impl<K, I, L, G: DigestGroup, S> Unpin for MemberStream<K, I, L, G, S> {}

/// A scan's next batch: at most `size` members within `unread`, the spans
/// still to read in the order the scan reads them, read in `direction` in a
/// read of the store of its own, which ends before the batch yields its first
/// member.
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
    // One member past the batch tells whether any lie beyond it, so that a
    // batch that reaches the end of the range ends the scan without another
    // read.
    let mut leaves = tree.read().await?.first_leaves(&unread, direction, size + 1).await?;

    let unread = match leaves.len() > size {
        true => {
            leaves.truncate(size);
            let (last, _) = leaves.last().expect("a batch yields at least one member");
            past(unread, last, direction)?
        }

        false => Vec::new(),
    };

    let members = leaves.into_iter().map(|(address, row)| Leaf::from_row(&address, row)).collect::<Result<_, _>>()?;
    Ok(Batch { members, unread })
}

/// The spans of `unread`, in the order the scan reads them, from just past
/// `last` on: the span that holds `last`, cut to the addresses after it in
/// `direction`, and every span after that one. A read refuses a store's rows
/// outside the range it asked for, so a `last` outside every span means the
/// tree's own reads broke.
fn past(mut unread: Vec<Span>, last: &[u8], direction: ScanDirection) -> Result<Vec<Span>, Error> {
    let holding = unread
        .iter()
        .position(|span| span.contains(last))
        .ok_or(Error::Inconsistent("a member read lies outside the spans it was read from"))?;

    unread.drain(..holding);

    let after = Bound::Excluded(last.to_vec());
    match direction {
        ScanDirection::Forward => unread[0].start = after,
        ScanDirection::Reverse => unread[0].end = after,
    }

    Ok(unread)
}

#[cfg(test)]
mod tests;
