use std::cmp::Ordering;
use std::future::poll_fn;
use std::iter::Peekable;
use std::mem;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::vec;

use futures_core::Stream;

use super::{same_payload, ComparisonError, Descent, Difference, PageGetter};
use crate::span::leaf_address;
use crate::{DigestGroup, DigestTreeView, Error, Leaf, MemberId, MemberStream, Payload, Row, RowKey, Span, Store};

/// An incremental comparison, made by [`DigestTreeView::compare`]. It owns
/// the getter and a view, and reads only while [`Self::next_batch`] is
/// awaited. Dropping it releases both without further reads.
///
/// Differences pair leaves at the same address, not across the whole
/// comparison. A member that moved can appear twice; order across batches
/// follows the descent, not member ids or addresses. Global pairing and
/// duplicate-id detection belong to [`DigestTreeView::differences`].
///
/// Memory holds one admitted group of pages, pending descent spans, one
/// bounded local range batch and the next output batch. The getter bounds
/// the pages it accepts; the caller chooses how many spans to ask together
/// and differences to yield, and may stop between batches. Pending spans
/// grow with descent depth and admitted subdivisions, not with all leaves
/// already visited.
pub struct Comparison<K, I, L, G: DigestGroup, S, P> {
    view: DigestTreeView<K, I, L, G, S>,
    getter: P,
    descent: Descent,
    listed: vec::IntoIter<(Span, Vec<Leaf<I, L>>)>,
    members: Option<Members<K, I, L, G, S>>,
    output: Vec<Difference<I, L>>,
    batch_size: NonZeroUsize,
    ended: bool,
}

impl<K, I, L, G, S, P> Comparison<K, I, L, G, S, P>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
    P: PageGetter<I, L, G>,
{
    pub(super) fn new(view: DigestTreeView<K, I, L, G, S>, getter: P, batch_size: NonZeroUsize) -> Self {
        let descent = Descent::new(view.spans().to_vec(), Some(batch_size));
        Self { view, getter, descent, listed: Vec::new().into_iter(), members: None, output: Vec::new(), batch_size, ended: false }
    }

    /// The next nonempty batch of at most the chosen batch size, or `None`
    /// once every differing span has been read. Failure ends the comparison;
    /// batches returned before it remain observations, not a completed result.
    ///
    /// Canceling this future retains its progress and unreturned differences
    /// in the cursor. Awaiting it again resumes local reads and may repeat a
    /// canceled getter call. A getter must tolerate that repeated request.
    /// A pending local read may hold the store's read handle until this
    /// future is resumed or the cursor dropped, as [`MemberStream`] does.
    /// No local read remains open when a batch is returned.
    pub async fn next_batch(&mut self) -> Result<Option<Vec<Difference<I, L>>>, ComparisonError<P::Error>> {
        if self.ended {
            return Ok(None);
        }
        let result = self.fill_batch().await;
        if result.is_err() {
            self.ended = true;
            self.members = None;
            self.listed = Vec::new().into_iter();
            self.descent.frontier.clear();
            self.output.clear();
        }
        result
    }

    async fn fill_batch(&mut self) -> Result<Option<Vec<Difference<I, L>>>, ComparisonError<P::Error>> {
        loop {
            if self.output.len() == self.batch_size.get() {
                return Ok(Some(mem::take(&mut self.output)));
            }
            if let Some(members) = &mut self.members {
                match members.next().await? {
                    Some(difference) => self.output.push(difference),
                    None => self.members = None,
                }
                continue;
            }
            if let Some((span, theirs)) = self.listed.next() {
                self.members =
                    Some(Members { stream: self.view.range(span)?, ours: None, ours_ended: false, theirs: theirs.into_iter().peekable() });
                continue;
            }
            match self.descent.advance(&self.view, &mut self.getter).await? {
                Some(listed) => self.listed = listed.into_iter(),
                None => {
                    self.ended = true;
                    return Ok((!self.output.is_empty()).then(|| mem::take(&mut self.output)));
                }
            }
        }
    }
}

/// Merge one ordered remote page with an ordered, bounded scan of our
/// members. The scan can be much larger than the remote page, including a
/// completely empty page covering our entire tree.
struct Members<K, I, L, G: DigestGroup, S> {
    stream: MemberStream<K, I, L, G, S>,
    ours: Option<Leaf<I, L>>,
    ours_ended: bool,
    theirs: Peekable<vec::IntoIter<Leaf<I, L>>>,
}

impl<K, I, L, G, S> Members<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    async fn next(&mut self) -> Result<Option<Difference<I, L>>, Error> {
        loop {
            if self.ours.is_none() && !self.ours_ended {
                self.ours = poll_fn(|cx| Pin::new(&mut self.stream).poll_next(cx)).await.transpose()?;
                self.ours_ended = self.ours.is_none();
            }

            let order = match (self.ours.as_ref(), self.theirs.peek()) {
                (None, None) => return Ok(None),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(ours), Some(theirs)) => leaf_address(&ours.key, &ours.id).cmp(&leaf_address(&theirs.key, &theirs.id)),
            };
            let difference = match order {
                Ordering::Less => Difference { ours: self.ours.take(), theirs: None },
                Ordering::Greater => Difference { ours: None, theirs: self.theirs.next() },
                Ordering::Equal => {
                    let ours = self.ours.take().expect("both sides hold this address");
                    let theirs = self.theirs.next().expect("both sides hold this address");
                    if same_payload(&ours, &theirs) {
                        continue;
                    }
                    Difference { ours: Some(ours), theirs: Some(theirs) }
                }
            };
            return Ok(Some(difference));
        }
    }
}
