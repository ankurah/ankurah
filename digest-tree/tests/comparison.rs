//! Finding the members that differ from another tree's: what the exchange
//! costs, what comes back, what a window compares, and what the comparison
//! refuses.

mod common;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeInclusive};

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{
    ComparisonError, Difference, Digest, DigestGroup, DigestTree, Error, LtHash16, MemberId, MemoryStore, Page, PageGetter, PageMisfit,
    Ristretto255, RowKey, Span, SpanDigest,
};
use common::{
    album, asc, counted, failing_store, item, no_albums, no_items, stored_rows, ten_items, tree_of, Album, FailingTree, Id, ItemTree,
    Property, Title, Tree, View,
};

/// A getter that records how many spans each request to it names.
struct Recording<G: DigestGroup = Ristretto255> {
    view: View<G>,
    requests: Vec<usize>,
}

impl<G: DigestGroup> Recording<G> {
    fn of(view: View<G>) -> Self { Self { view, requests: Vec::new() } }
}

impl<G: DigestGroup> PageGetter<Id, Title, G> for Recording<G> {
    type Error = Error;

    async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, G>>, Error> {
        self.requests.push(spans.len());
        self.view.pages(spans).await
    }
}

fn keyspec() -> KeySpec<Property> { KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]) }

/// Albums `ids`, out one a year from 1700 on.
fn albums(ids: RangeInclusive<u64>) -> Vec<Album> { ids.map(|n| album!(n, 1700 + n as i32, format!("Album {n}"))).collect() }

/// Three hundred albums on each side, one retitled on theirs.
async fn trees_differing_in_one_album() -> (Tree, Tree) {
    let (ours, theirs) = (tree_of!(keyspec(), albums(1..=300)), tree_of!(keyspec(), albums(1..=300)));

    let mut trx = theirs.transaction();
    trx.update(album!(150, 1850, "Album 150"), album!(150, 1850, "Album 150 (Live)")).unwrap();
    trx.commit().await.unwrap();

    (ours, theirs)
}

/// One difference among three hundred members costs one request per level
/// of the other tree, each naming the one span that differs, down to the
/// members of the one bucket that holds it.
#[tokio::test]
async fn a_comparison_descends_a_level_per_exchange() {
    let (ours, theirs) = trees_differing_in_one_album().await;
    let mut getter = Recording::of(theirs.view());

    let differences = ours.view().differences(&mut getter).await.unwrap();
    let requests = getter.requests;

    assert_eq!(differences.len(), 1);
    assert_eq!(differences[0].theirs.as_ref().unwrap().payload, "Album 150 (Live)");
    assert!(requests.len() >= 2, "the root's page, then at least one level: {requests:?}");
    assert!(requests.iter().all(|&named| named == 1), "the one differing span per request: {requests:?}");
}

/// Two differences in different subtrees descend together: each request
/// names every span that differed, so the second difference costs spans in
/// a request, not requests.
#[tokio::test]
async fn differences_descend_together_a_request_per_level() {
    let (ours, theirs) = trees_differing_in_one_album().await;
    let mut one = Recording::of(theirs.view());
    ours.view().differences(&mut one).await.unwrap();
    let one = one.requests;

    let mut trx = theirs.transaction();
    trx.update(album!(20, 1720, "Album 20"), album!(20, 1720, "Album 20 (Live)")).unwrap();
    trx.commit().await.unwrap();
    let mut two = Recording::of(theirs.view());

    let differences = ours.view().differences(&mut two).await.unwrap();
    let two = two.requests;

    assert_eq!(differences.len(), 2);
    assert!(two.len() <= one.len() + 1, "one difference: {one:?}; two: {two:?}");
    assert!(two.iter().sum::<usize>() > one.iter().sum::<usize>(), "one difference: {one:?}; two: {two:?}");
}

/// A tree with nothing beneath its root, a bucket's worth of members or
/// fewer, answers them in its first page, so the comparison is one exchange
/// whatever the view holds.
#[tokio::test]
async fn a_tree_with_nothing_beneath_its_root_answers_its_members_at_once() {
    let (ours, theirs): (Tree, Tree) = (tree_of!(keyspec(), albums(1..=300)), tree_of!(keyspec(), albums(1..=10)));
    let mut getter = Recording::of(theirs.view());

    let differences = ours.view().differences(&mut getter).await.unwrap();

    assert_eq!(differences.len(), 290);
    assert!(differences.iter().all(|difference| difference.theirs.is_none()));
    assert_eq!(getter.requests, [1]);
}

/// A view holding nothing descends the other tree whole, a level per
/// exchange, and every member of that tree is a difference.
#[tokio::test]
async fn an_empty_tree_descends_the_other_tree_whole() {
    let empty: Tree = DigestTree::create(MemoryStore::new(), keyspec(), no_albums!()).await.unwrap();
    let theirs: Tree = tree_of!(keyspec(), albums(1..=300));
    let mut getter = Recording::of(theirs.view());

    let differences = empty.view().differences(&mut getter).await.unwrap();
    let requests = getter.requests;

    assert_eq!(differences.len(), 300);
    assert!(differences.iter().all(|difference| difference.ours.is_none()));
    assert_eq!(requests[0], 1, "{requests:?}");
    assert!(requests.windows(2).all(|pair| pair[1] <= 2 * pair[0]), "two subspans at most per span that differs: {requests:?}");
}

/// Each request is one level of the descent, however many spans the level
/// holds, so a getter that counts its requests counts levels: twelve
/// thousand members end in a level of 160 buckets, asked in one request.
#[tokio::test]
async fn a_request_asks_for_a_whole_level() {
    let empty: Tree<LtHash16> = DigestTree::create(MemoryStore::new(), keyspec(), no_albums!()).await.unwrap();
    let theirs: Tree<LtHash16> = tree_of!(keyspec(), albums(1..=12_000));
    let mut getter = Recording::of(theirs.view());

    let differences = empty.view().differences(&mut getter).await.unwrap();
    let requests = getter.requests;

    assert_eq!(differences.len(), 12_000);
    assert_eq!(requests.last(), Some(&160), "{requests:?}");
}

/// A request for the pages at many spans reads each node of the tree twice
/// at most, once as it finds each span's smallest subtree and once as it
/// digests the parts beneath, however many of the spans lie beneath the
/// node: the spans share the walks rather than each walking from the root.
#[tokio::test]
async fn a_request_for_many_pages_reads_each_node_twice_at_most() {
    let (tree, counts) = counted(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..2000).map(|n| item!(n, n as i64, b""))).await;
    let nodes = stored_rows(tree.store()).await.iter().filter(|(key, _)| matches!(key, RowKey::Node(_))).count();
    let spans: Vec<Span> = (0..128i64).map(|i| tree.span_for(15 * i..15 * (i + 1)).unwrap()).collect();

    let before = counts.nodes_read();
    tree.view().pages(spans).await.unwrap();
    let read = counts.nodes_read() - before;

    assert!(read <= 2 * nodes, "{read} node rows read from a tree of {nodes} nodes");
}

/// A tree that holds nothing digests any number of spans with one read: its
/// root holds no members, so nothing beneath it is read. Against an empty
/// tree, a page of many subspans costs the comparison no more reads.
#[tokio::test]
async fn an_empty_tree_digests_any_number_of_spans_with_one_read() {
    let (tree, counts) = counted(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), no_items!()).await;
    let spans: Vec<Span> = (0..100i64).map(|i| tree.span_for(2 * i..2 * i + 1).unwrap()).collect();
    let view = tree.select(spans).unwrap();

    let before = counts.reads();
    assert_eq!(view.count(..).await.unwrap(), 0);

    assert_eq!(counts.reads() - before, 1);
}

/// The member a comparison was left unsettled by, as its id's bytes.
fn unsettled_by<T: Debug, E: Debug>(compared: Result<T, ComparisonError<E>>) -> Vec<u8> {
    match compared {
        Err(ComparisonError::Unsettled { member }) => member,
        other => panic!("expected the comparison unsettled, got {other:?}"),
    }
}

/// A member the other tree holds at two addresses, in one page or across
/// two, can't be paired: the comparison is unsettled, naming it, and
/// comparing again leaves it so, since a member held twice stays.
#[tokio::test]
async fn a_member_the_other_tree_holds_twice_leaves_the_comparison_unsettled() {
    let (ours, theirs): (Tree, Tree) = (tree_of!(keyspec(), albums(1..=10)), tree_of!(keyspec(), albums(1..=10)));
    let mut trx = theirs.transaction();
    trx.insert(album!(3, 1709, "Album 3")).unwrap();
    trx.commit().await.unwrap();

    assert_eq!(unsettled_by(ours.view().differences(theirs.view()).await), Id(3).to_bytes().into_owned(), "in one page");
    assert_eq!(unsettled_by(ours.view().differences(theirs.view()).await), Id(3).to_bytes().into_owned(), "and again");

    let (ours, theirs) = trees_differing_in_one_album().await;
    let mut trx = theirs.transaction();
    trx.insert(album!(150, 1999, "Album 150 (Live)")).unwrap();
    trx.commit().await.unwrap();

    assert_eq!(unsettled_by(ours.view().differences(theirs.view()).await), Id(150).to_bytes().into_owned(), "across pages");
}

/// Two leaves for one member within one read of the view's own tree are
/// that tree holding the member twice: damage, which comparing again won't
/// settle.
#[tokio::test]
async fn a_member_the_views_own_tree_holds_twice_is_inconsistent() {
    let (ours, theirs): (Tree, Tree) = (tree_of!(keyspec(), albums(1..=10)), tree_of!(keyspec(), albums(1..=10)));
    let mut trx = ours.transaction();
    trx.insert(album!(3, 1709, "Album 3")).unwrap();
    trx.commit().await.unwrap();

    assert!(matches!(ours.view().differences(theirs.view()).await, Err(ComparisonError::View(Error::Inconsistent(_)))));
}

/// A commit that moves one of the view's members while the comparison runs
/// can show the member at its old address in one read and at its new one in
/// a later read: the comparison is unsettled, not the tree damaged, and
/// comparing again settles.
#[tokio::test]
async fn a_member_moved_during_the_comparison_leaves_it_unsettled() {
    // Answers as their view does, and on its second request first moves an
    // album of ours from 1500, which no subtree of theirs reaches, so that
    // the first exchange read it there, to 1850, beside the album that
    // differs.
    struct MovingOursMidway {
        theirs: View,
        ours: Tree,
        requests: usize,
    }

    impl PageGetter<Id, Title, Ristretto255> for MovingOursMidway {
        type Error = Error;

        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
            self.requests += 1;
            if self.requests == 2 {
                let mut trx = self.ours.transaction();
                trx.update(album!(999, 1500, "Album 999"), album!(999, 1850, "Album 999"))?;
                trx.commit().await?;
            }

            self.theirs.pages(spans).await
        }
    }

    let (ours, theirs) = trees_differing_in_one_album().await;
    let mut trx = ours.transaction();
    trx.insert(album!(999, 1500, "Album 999")).unwrap();
    trx.commit().await.unwrap();

    let getter = MovingOursMidway { theirs: theirs.view(), ours: ours.clone(), requests: 0 };
    assert_eq!(unsettled_by(ours.view().differences(getter).await), Id(999).to_bytes().into_owned());

    let settled = ours.view().differences(theirs.view()).await.unwrap();
    assert!(settled
        .iter()
        .any(|difference| difference.ours.as_ref().is_some_and(|leaf| leaf.id == Id(999)) && difference.theirs.is_none()));
}

/// Each difference carries a member's two sides: a retitled member both, a
/// member only the other tree holds just theirs, and a member that moved to
/// another key both, at its two keys, as one difference.
#[tokio::test]
async fn differences_pair_each_members_two_sides() {
    let (ours, theirs) = trees_differing_in_one_album().await;

    let mut trx = theirs.transaction();
    trx.insert(album!(301, 2001, "Album 301")).unwrap();
    trx.update(album!(20, 1720, "Album 20"), album!(20, 2020, "Album 20")).unwrap();
    trx.commit().await.unwrap();

    let differences = ours.view().differences(theirs.view()).await.unwrap();
    let by_id: BTreeMap<u64, &Difference<Id, Title>> =
        differences.iter().map(|difference| (difference.ours.as_ref().or(difference.theirs.as_ref()).unwrap().id.0, difference)).collect();

    assert_eq!(differences.len(), 3);
    assert_eq!(by_id[&150].ours.as_ref().unwrap().payload, "Album 150");
    assert_eq!(by_id[&150].theirs.as_ref().unwrap().payload, "Album 150 (Live)");
    assert_eq!(by_id[&301].ours, None);
    assert_eq!(by_id[&301].theirs.as_ref().unwrap().payload, "Album 301");
    assert_ne!(by_id[&20].ours.as_ref().unwrap().key, by_id[&20].theirs.as_ref().unwrap().key);
}

/// A getter's failure ends the comparison, as the getter's own error.
#[tokio::test]
async fn a_getters_failure_ends_the_comparison() {
    /// Why a getter whose node is away gets no pages.
    #[derive(Debug)]
    struct Away;

    struct Disconnected;

    impl PageGetter<Id, Title, Ristretto255> for Disconnected {
        type Error = Away;

        async fn get_pages(&mut self, _: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Away> { Err(Away) }
    }

    let (ours, _) = trees_differing_in_one_album().await;

    assert!(matches!(ours.view().differences(Disconnected).await, Err(ComparisonError::Getter(Away))));
}

/// A failure of the view's own reads ends the comparison as the view's,
/// told apart from a getter's.
#[tokio::test]
async fn the_views_own_failure_ends_the_comparison() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let (store, faults) = failing_store();
    let ours = FailingTree::create(store, spec.clone(), ten_items()).await.unwrap();
    let theirs: ItemTree = tree_of!(spec, ten_items());

    faults.fail_reads(true);

    assert!(matches!(ours.view().differences(theirs.view()).await, Err(ComparisonError::View(Error::Store(_)))));
}

/// Trees that agree compare in one exchange, with nothing to report.
#[tokio::test]
async fn trees_that_agree_differ_nowhere() {
    let (ours, theirs): (Tree, Tree) = (tree_of!(keyspec(), albums(1..=300)), tree_of!(keyspec(), albums(1..=300)));
    let mut getter = Recording::of(theirs.view());

    assert_eq!(ours.view().differences(&mut getter).await.unwrap(), []);
    assert_eq!(getter.requests, [1]);
}

/// A view over a window compares what it covers, whether the other view is
/// the same window or the whole tree: a difference outside the window is
/// neither reported nor descended into. The whole tree, compared with a
/// window, finds every member of its own outside the window missing there.
#[tokio::test]
async fn a_view_over_a_window_compares_what_it_covers() {
    let (ours, theirs) = trees_differing_in_one_album().await;

    let mut trx = theirs.transaction();
    trx.update(album!(300, 2000, "Album 300"), album!(300, 2000, "Album 300 (Live)")).unwrap();
    trx.commit().await.unwrap();

    let (our_window, mut their_window) = (ours.select(..=1900).unwrap(), theirs.select(..=1900).unwrap());

    assert_eq!(our_window.differences(&mut their_window).await.unwrap().len(), 1);
    assert_eq!(our_window.differences(theirs.view()).await.unwrap().len(), 1);
    assert_eq!(ours.view().differences(theirs.view()).await.unwrap().len(), 2);
    assert_eq!(
        ours.view().differences(&mut their_window).await.unwrap().len(),
        101,
        "one retitled within the window, a hundred missing beyond it"
    );
}

/// The rule a comparison refused a page by.
fn misfit<T: Debug, E: Debug>(compared: Result<T, ComparisonError<E>>) -> PageMisfit {
    match compared {
        Err(ComparisonError::MalformedPage(misfit)) => misfit,
        other => panic!("expected a page refused, got {other:?}"),
    }
}

/// Both public comparison modes reject the same malformed pages. The
/// incremental one also remains ended after its error.
async fn incremental_misfit<P: PageGetter<Id, Title, Ristretto255>>(view: View, getter: P) -> PageMisfit
where P::Error: Debug {
    let mut comparison = view.compare(getter, NonZeroUsize::new(8).unwrap());
    loop {
        match comparison.next_batch().await {
            Ok(Some(_)) => {}
            result => {
                let misfit = misfit(result);
                assert!(comparison.next_batch().await.unwrap().is_none());
                return misfit;
            }
        }
    }
}

/// An answer that names another number of pages than spans asked is
/// refused: the comparison could pair no page with its span.
#[tokio::test]
async fn an_answer_of_another_number_of_pages_is_refused() {
    struct OneShort(View);

    impl PageGetter<Id, Title, Ristretto255> for OneShort {
        type Error = Error;

        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
            let mut pages = self.0.pages(spans).await?;
            pages.pop();

            Ok(pages)
        }
    }

    let (ours, theirs) = trees_differing_in_one_album().await;

    assert_eq!(misfit(ours.view().differences(OneShort(theirs.view())).await), PageMisfit::WrongNumberOfPages);
    assert_eq!(incremental_misfit(ours.view(), OneShort(theirs.view())).await, PageMisfit::WrongNumberOfPages);
}

/// A getter that answers its root's page whatever span it is asked: once
/// the comparison asks beneath the root, the digests it gets lie outside
/// the span asked, and so do the members where the tree is a bucket. Either
/// page doesn't fit the span it answers, and the comparison says so rather
/// than compare what it was given.
#[tokio::test]
async fn a_page_outside_the_span_asked_is_refused() {
    struct RootsPage(View);

    impl PageGetter<Id, Title, Ristretto255> for RootsPage {
        type Error = Error;

        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
            self.0.pages(spans.iter().map(|_| Span::all()).collect()).await
        }
    }

    let (ours, theirs) = trees_differing_in_one_album().await;
    assert_eq!(misfit(ours.view().differences(RootsPage(theirs.view())).await), PageMisfit::SubspanOutside, "digests");
    assert_eq!(incremental_misfit(ours.view(), RootsPage(theirs.view())).await, PageMisfit::SubspanOutside);

    let small: Tree = tree_of!(keyspec(), albums(1..=10));
    let window = ours.select(..=1705).unwrap();
    assert_eq!(misfit(window.differences(RootsPage(small.view())).await), PageMisfit::MemberOutside, "members");
    assert_eq!(incremental_misfit(window, RootsPage(small.view())).await, PageMisfit::MemberOutside);
}

/// A subdivision with a subspan as wide as the span asked, or with an empty
/// one, is refused: asking beneath either would make no progress, and a
/// getter could keep the comparison asking forever.
#[tokio::test]
async fn a_subdivision_that_doesnt_narrow_the_span_asked_is_refused() {
    struct Itself;

    impl PageGetter<Id, Title, Ristretto255> for Itself {
        type Error = Error;

        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
            Ok(spans.into_iter().map(|bounds| Page::Digests(vec![SpanDigest { bounds, digest: Digest::empty() }])).collect())
        }
    }

    struct WithAnEmptySubspan(View);

    impl PageGetter<Id, Title, Ristretto255> for WithAnEmptySubspan {
        type Error = Error;

        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
            let mut pages = self.0.pages(spans).await?;
            for page in &mut pages {
                if let Page::Digests(subspans) = page {
                    let nothing = Span::new(Bound::Excluded(Vec::new()), Bound::Excluded(Vec::new()));
                    subspans.insert(0, SpanDigest { bounds: nothing, digest: Digest::empty() });
                }
            }

            Ok(pages)
        }
    }

    let (ours, theirs) = trees_differing_in_one_album().await;

    assert_eq!(misfit(ours.view().differences(Itself).await), PageMisfit::SubspanNotNarrower, "the span itself");
    assert_eq!(incremental_misfit(ours.view(), Itself).await, PageMisfit::SubspanNotNarrower);
    assert_eq!(
        misfit(ours.view().differences(WithAnEmptySubspan(theirs.view())).await),
        PageMisfit::SubspanNotNarrower,
        "an empty subspan"
    );
    assert_eq!(incremental_misfit(ours.view(), WithAnEmptySubspan(theirs.view())).await, PageMisfit::SubspanNotNarrower);
}

/// A subdivision that leaves part of its span uncovered would hide the
/// members there: it is refused.
#[tokio::test]
async fn a_subdivision_that_doesnt_cover_its_span_is_refused() {
    struct Dropping(View);

    impl PageGetter<Id, Title, Ristretto255> for Dropping {
        type Error = Error;

        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
            let mut pages = self.0.pages(spans).await?;
            for page in &mut pages {
                if let Page::Digests(subspans) = page {
                    subspans.pop();
                }
            }

            Ok(pages)
        }
    }

    let (ours, theirs) = trees_differing_in_one_album().await;

    assert_eq!(misfit(ours.view().differences(Dropping(theirs.view())).await), PageMisfit::SubspansLeaveGap);
    assert_eq!(incremental_misfit(ours.view(), Dropping(theirs.view())).await, PageMisfit::SubspansLeaveGap);
}

/// A page whose digests or members come out of address order is refused, and
/// so is a subdivision whose subspans overlap.
#[tokio::test]
async fn a_page_out_of_address_order_is_refused() {
    struct Backwards(View);

    impl PageGetter<Id, Title, Ristretto255> for Backwards {
        type Error = Error;

        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
            let mut pages = self.0.pages(spans).await?;
            for page in &mut pages {
                match page {
                    Page::Digests(subspans) => subspans.reverse(),
                    Page::Members(members) => members.reverse(),
                }
            }

            Ok(pages)
        }
    }

    // Each subdivision's first subspan reaching on to the second's first
    // address.
    struct Overlapping(View);

    impl PageGetter<Id, Title, Ristretto255> for Overlapping {
        type Error = Error;

        async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
            let mut pages = self.0.pages(spans).await?;
            for page in &mut pages {
                if let Page::Digests(subspans) = page {
                    let Bound::Included(second) = subspans[1].bounds.start.clone() else {
                        panic!("a subspan after the first starts at an address")
                    };
                    subspans[0].bounds.end = Bound::Included(second);
                }
            }

            Ok(pages)
        }
    }

    let (ours, theirs) = trees_differing_in_one_album().await;
    assert_eq!(misfit(ours.view().differences(Backwards(theirs.view())).await), PageMisfit::SubspansOutOfOrder, "digests");
    assert_eq!(misfit(ours.view().differences(Overlapping(theirs.view())).await), PageMisfit::SubspansOutOfOrder, "overlapping");
    assert_eq!(incremental_misfit(ours.view(), Backwards(theirs.view())).await, PageMisfit::SubspansOutOfOrder);
    assert_eq!(incremental_misfit(ours.view(), Overlapping(theirs.view())).await, PageMisfit::SubspansOutOfOrder);

    let small: Tree = tree_of!(keyspec(), albums(1..=10));
    assert_eq!(misfit(ours.view().differences(Backwards(small.view())).await), PageMisfit::MembersOutOfOrder, "members");
    assert_eq!(incremental_misfit(ours.view(), Backwards(small.view())).await, PageMisfit::MembersOutOfOrder);
}
