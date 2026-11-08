//! Comparing a view with another tree's. Whatever holds that tree only reads
//! it: asked about spans, it answers its page at each, what the tree holds
//! there one level down. The view does the comparing: it compares each
//! page's digests with its own digests of the same spans, asks for the pages
//! beneath those that differ, a whole level per request, and, where a page
//! holds members, pairs them with its own. How much to ask of the other tree
//! is the getter's to judge.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;

use serde::{Deserialize, Serialize};

use super::{DigestTreeView, Leaf};
use crate::span::leaf_address;
use crate::{DigestGroup, Error, MemberId, Payload, Row, RowKey, Span, SpanDigest, Store};

/// What gets the other tree's pages for one comparison: across the wire, a
/// connection to the node that holds the tree, or one that asks several
/// nodes; in one process, that tree's own view, as tests do. Nothing about a
/// getter is a tree: it answers one read, a page for each span asked, in
/// order, and compares nothing.
///
/// The comparison asks its getter for one whole level of the descent per
/// call, and sets no bounds of its own. Any judgment of the other side is
/// the getter's, over the getter's own lifetime: its account of the pages it
/// fetched, its patience, what it admits and its budget. A getter ends the
/// comparison by returning its error, which the comparison returns as
/// [`ComparisonError::Getter`]. A getter for trusted trees in one process
/// need judge nothing, and may serve one comparison after another.
///
/// The comparison checks that each page fits the span it answers. A page is
/// the other tree's claim about itself, which no check can verify: what a
/// difference may change, which side's version wins or whether a member the
/// other side lacks should go, is the caller's to decide.
pub trait PageGetter<I: MemberId, L: Payload, G: DigestGroup> {
    /// How getting pages fails.
    type Error;

    /// The other tree's page at each of `spans`, in the order given: one
    /// level of the comparison's descent.
    fn get_pages(&mut self, spans: Vec<Span>) -> impl Future<Output = Result<Vec<Page<I, L, G>>, Self::Error>> + Send;
}

/// A getter lent to a comparison is a getter too, so that its owner still
/// holds it once the comparison is done: to read what it counted, or to lend
/// it to the next.
impl<I: MemberId, L: Payload, G: DigestGroup, P: PageGetter<I, L, G>> PageGetter<I, L, G> for &mut P {
    type Error = P::Error;

    fn get_pages(&mut self, spans: Vec<Span>) -> impl Future<Output = Result<Vec<Page<I, L, G>>, Self::Error>> + Send {
        (**self).get_pages(spans)
    }
}

/// A tree's page at a span: what it holds there, one level down. What
/// [`PageGetter::get_pages`] answers for each span asked, and what crosses
/// the wire.
///
/// A page is the unit by which a comparison narrows: each request to the
/// other tree answers one level of the descent, a page for each span still in
/// question, and the comparison asks next only beneath the spans whose digests
/// differ. So a page is an answer, not a record. The tree builds it from its
/// rows when asked, from the nodes beneath the span and, where the span
/// reaches a bucket, the members within it, and neither stores nor keeps it:
/// its store holds rows by key, so that a change to one member rewrites only
/// that member's row and the rows of the nodes on its path
/// ([`Row`](crate::Row)).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "I: Serialize, L: Serialize", deserialize = "I: Deserialize<'de>, L: Deserialize<'de>"))]
pub enum Page<I, L, G: DigestGroup> {
    /// Where the tree has structure beneath the span: the span's
    /// subdivision, its span digests one level down, in address order,
    /// disjoint, together covering the span and each narrower than it, with
    /// an empty digest for a part no subtree reaches.
    Digests(Vec<SpanDigest<G>>),

    /// Where it has no structure beneath the span, in this crate's trees a
    /// bucket: every member the tree holds within the span, in address order.
    Members(Vec<Leaf<I, L>>),
}

/// A member whose two sides differ: the view's leaf and the other tree's,
/// either absent where that side doesn't hold the member. A member that
/// moved to another key is one difference, our leaf at the old key and
/// theirs at the new. A difference with only the other tree's leaf can name
/// a member this tree holds at another address, within a span the two trees
/// digest alike, which the comparison never reads: the tree keeps no index by
/// id, so a caller that inserts the other tree's leaf checks its own records
/// for the member first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Difference<I, L> {
    /// The view's leaf for the member, if it holds one.
    pub ours: Option<Leaf<I, L>>,

    /// The other tree's leaf for the member, if it holds one.
    pub theirs: Option<Leaf<I, L>>,
}

/// Why a comparison failed, by whose part failed: the view reading its own
/// tree, the getter getting pages, or a page the getter got.
#[derive(Debug, thiserror::Error)]
pub enum ComparisonError<E> {
    /// The view couldn't read its own tree.
    #[error("couldn't read the view's own tree")]
    View(#[from] Error),

    /// The getter couldn't get the pages asked, for its own reason.
    #[error("couldn't get the other tree's pages")]
    Getter(#[source] E),

    /// A page the getter got doesn't fit the span it answers, by the rule it
    /// breaks.
    #[error("a page doesn't fit the span it answers: {0}")]
    MalformedPage(PageMisfit),

    /// A member was seen at two addresses during the comparison, so its two
    /// sides can't be paired: one of the trees changed while it was
    /// compared, or the other tree holds the member twice. Comparing again
    /// tells which: a change settles, and a member held twice stays.
    #[error("a member was seen at two addresses during the comparison")]
    Unsettled {
        /// The member's id, as its bytes.
        member: Vec<u8>,
    },
}

/// How the pages a getter got don't fit the spans they answer: the rule a
/// comparison refuses them by, so that every exchange narrows every span
/// that differs and a page pairs only with the members of its own span.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PageMisfit {
    /// Another number of pages than spans asked.
    #[error("another number of pages than spans asked")]
    WrongNumberOfPages,

    /// A subspan of a subdivision reaching outside the span asked.
    #[error("a subspan outside the span asked")]
    SubspanOutside,

    /// A subspan of a subdivision as wide as the span asked, or empty, where
    /// the page beneath would make no progress.
    #[error("a subspan as wide as the span asked, or empty")]
    SubspanNotNarrower,

    /// A subdivision's subspans out of address order, or overlapping.
    #[error("subspans out of address order, or overlapping")]
    SubspansOutOfOrder,

    /// A subdivision's subspans leaving part of the span asked uncovered,
    /// where members of the other tree could hide.
    #[error("subspans that leave part of the span asked uncovered")]
    SubspansLeaveGap,

    /// A member outside the span asked.
    #[error("a member outside the span asked")]
    MemberOutside,

    /// Members out of address order, or one address twice.
    #[error("members out of address order, or one address twice")]
    MembersOutOfOrder,
}

impl<K, I, L, G, S> DigestTreeView<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// Every member whose two sides differ, ours and the other tree's, in the
    /// order of their ids' bytes, found by descending the other tree: the
    /// view asks `getter` for its pages at the spans the view covers,
    /// compares each page's digests with its own digests of the same spans,
    /// asks again beneath those that differ, a whole level per call, and pairs
    /// the members of each page of members with its own within that span, by
    /// address and then by id, so that a member that moved to another key is
    /// one difference. Two leaves are the same member unchanged when their
    /// addresses and their payloads' digest bytes agree. Where the other
    /// tree's digest of a span is empty, every member of ours within it
    /// differs, with nothing more to ask. The comparison is no snapshot: each
    /// read sees one state of either tree, and a change landing during the
    /// descent can make a span that agreed differ, or the reverse, so a
    /// caller that must be sure compares again. How much of the other tree
    /// the comparison asks for, and how long it waits, is the getter's to
    /// judge ([`PageGetter`]). A page that doesn't fit the span it answers
    /// is [`ComparisonError::MalformedPage`], so that every exchange narrows
    /// every span that differs and a page pairs only with the members of its
    /// own span. Pairing by id takes one leaf per member on each side, so a
    /// member seen at two addresses in the other tree's pages, or across two
    /// reads of this view's tree, leaves the comparison
    /// [`ComparisonError::Unsettled`], and two leaves for one member within
    /// one read of this view's tree are [`Error::Inconsistent`].
    pub async fn differences<P: PageGetter<I, L, G>>(&self, mut getter: P) -> Result<Vec<Difference<I, L>>, ComparisonError<P::Error>> {
        let mut by_id: BTreeMap<Vec<u8>, Difference<I, L>> = BTreeMap::new();
        let mut frontier: Vec<Span> = self.spans().to_vec();

        // The ids of every member each side has shown among the spans
        // compared, which are disjoint, so an id seen twice was seen at two
        // addresses.
        let (mut theirs_seen, mut ours_seen) = (HashSet::new(), HashSet::new());

        while !frontier.is_empty() {
            let pages = pages_at(&mut getter, &frontier).await?;

            let mut subspans: Vec<SpanDigest<G>> = Vec::new();
            let mut listed: Vec<(Span, Vec<Leaf<I, L>>)> = Vec::new();
            for (span, page) in frontier.into_iter().zip(pages) {
                match page {
                    Page::Digests(theirs) => {
                        fits_as_subdivision(&span, &theirs).map_err(ComparisonError::MalformedPage)?;
                        subspans.extend(theirs);
                    }

                    Page::Members(theirs) => {
                        fits_as_members(&span, &theirs).map_err(ComparisonError::MalformedPage)?;
                        see_once(&mut theirs_seen, &theirs)?;
                        listed.push((span, theirs));
                    }
                }
            }

            let ours = self.span_digests(subspans.iter().map(|theirs| theirs.bounds.clone()).collect()).await?;
            frontier = Vec::new();
            for (theirs, ours) in subspans.into_iter().zip(ours) {
                if theirs.digest == ours.digest {
                    continue;
                }

                match theirs.digest.count() == 0 {
                    true => listed.push((theirs.bounds, Vec::new())),
                    false => frontier.push(theirs.bounds),
                }
            }

            let spans: Vec<Span> = listed.iter().map(|(span, _)| span.clone()).collect();
            let ours_each = self.members_within_each(&spans).await?;

            // One read sees one state of this tree, so a member twice within
            // it is one the tree holds twice; across reads, it may have moved.
            let mut in_this_read = HashSet::new();
            if ours_each.iter().flatten().any(|leaf| !in_this_read.insert(leaf.id.to_bytes())) {
                return Err(Error::Inconsistent("a member is held under two keys").into());
            }

            for mine in &ours_each {
                see_once(&mut ours_seen, mine)?;
            }

            for ((_, theirs), mine) in listed.into_iter().zip(ours_each) {
                record_differences(&mut by_id, mine, theirs);
            }
        }

        Ok(by_id.into_values().collect())
    }
}

/// The other tree's page at each of `spans`, a whole level of the descent,
/// in one request: how the request travels, whole or in parts, is the
/// getter's to choose. An answer with another number of pages than spans
/// asked is [`PageMisfit::WrongNumberOfPages`].
async fn pages_at<I: MemberId, L: Payload, G: DigestGroup, P: PageGetter<I, L, G>>(
    getter: &mut P,
    spans: &[Span],
) -> Result<Vec<Page<I, L, G>>, ComparisonError<P::Error>> {
    let pages = getter.get_pages(spans.to_vec()).await.map_err(ComparisonError::Getter)?;
    if pages.len() != spans.len() {
        return Err(ComparisonError::MalformedPage(PageMisfit::WrongNumberOfPages));
    }

    Ok(pages)
}

/// Whether `subdivision` fits as the page beneath `span`: its spans within
/// `span` and narrower than it, in address order and disjoint, and together
/// covering it. If it doesn't, the rule it breaks, checked in that order.
fn fits_as_subdivision<G: DigestGroup>(span: &Span, subdivision: &[SpanDigest<G>]) -> Result<(), PageMisfit> {
    let bounds: Vec<Span> = subdivision.iter().map(|digest| digest.bounds.clone()).collect();

    if bounds.iter().any(|sub| !sub.is_subset_of(span)) {
        return Err(PageMisfit::SubspanOutside);
    }

    if bounds.iter().any(|sub| sub.is_empty() || sub == span) {
        return Err(PageMisfit::SubspanNotNarrower);
    }

    if bounds.windows(2).any(|pair| pair[0].cmp_start(&pair[1]).is_ge() || pair[0].intersect(&pair[1]).is_some()) {
        return Err(PageMisfit::SubspansOutOfOrder);
    }

    if !span.gaps(&bounds).is_empty() {
        return Err(PageMisfit::SubspansLeaveGap);
    }

    Ok(())
}

/// Whether `members` fit as the page of members within `span`: each within
/// it, in address order. If they don't, the rule they break.
fn fits_as_members<I: MemberId, L>(span: &Span, members: &[Leaf<I, L>]) -> Result<(), PageMisfit> {
    let mut last: Option<Vec<u8>> = None;

    for leaf in members {
        let address = leaf_address(&leaf.key, &leaf.id);
        if !span.contains(&address) {
            return Err(PageMisfit::MemberOutside);
        }

        if last.as_ref().is_some_and(|last| *last >= address) {
            return Err(PageMisfit::MembersOutOfOrder);
        }

        last = Some(address);
    }

    Ok(())
}

/// Note `leaves`' ids among those `seen` so far in a comparison: an id seen
/// before leaves the comparison unsettled.
fn see_once<I: MemberId, L, E>(seen: &mut HashSet<Vec<u8>>, leaves: &[Leaf<I, L>]) -> Result<(), ComparisonError<E>> {
    for leaf in leaves {
        let member = leaf.id.to_bytes().into_owned();
        if !seen.insert(member.clone()) {
            return Err(ComparisonError::Unsettled { member });
        }
    }

    Ok(())
}

/// Record every member whose two sides differ within one span, `mine` and
/// `theirs` being each side's members there.
fn record_differences<I: MemberId, L: Payload>(
    by_id: &mut BTreeMap<Vec<u8>, Difference<I, L>>,
    mine: Vec<Leaf<I, L>>,
    theirs: Vec<Leaf<I, L>>,
) {
    let (mine, theirs) = (by_address(mine), by_address(theirs));

    for (address, leaf) in &mine {
        if !theirs.get(address).is_some_and(|theirs| same_payload(leaf, theirs)) {
            by_id.entry(leaf.id.to_bytes().into_owned()).or_insert(Difference { ours: None, theirs: None }).ours = Some(leaf.clone());
        }
    }

    for (address, leaf) in &theirs {
        if !mine.get(address).is_some_and(|ours| same_payload(leaf, ours)) {
            by_id.entry(leaf.id.to_bytes().into_owned()).or_insert(Difference { ours: None, theirs: None }).theirs = Some(leaf.clone());
        }
    }
}

fn by_address<I: MemberId, L>(leaves: Vec<Leaf<I, L>>) -> BTreeMap<Vec<u8>, Leaf<I, L>> {
    leaves.into_iter().map(|leaf| (leaf_address(&leaf.key, &leaf.id), leaf)).collect()
}

fn same_payload<I, L: Payload>(a: &Leaf<I, L>, b: &Leaf<I, L>) -> bool { a.payload.digest_bytes() == b.payload.digest_bytes() }
