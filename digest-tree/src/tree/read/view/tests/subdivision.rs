//! How a view answers its page at a span, and its digests of spans.

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;

use super::super::*;
use crate::common::{asc, item, tree_of, ItemTree, Property};
use crate::Page;

/// The page beneath a branch is the span's subdivision, and a view digests
/// nothing outside what it covers: the same subspans as the whole tree's
/// page, those outside the view empty.
#[tokio::test]
async fn a_page_within_a_view_digests_nothing_outside_what_it_covers() {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..100).map(|n| item!(n, n as i64, b"")));
    let low = tree.span_for(..10i64).unwrap();
    let view = tree.select(low.clone()).unwrap();

    let Page::Digests(within) = view.pages(vec![Span::all()]).await.unwrap().pop().unwrap() else {
        panic!("a root of a hundred is a branch")
    };
    let Page::Digests(whole) = tree.view().pages(vec![Span::all()]).await.unwrap().pop().unwrap() else {
        panic!("a root of a hundred is a branch")
    };

    assert_eq!(within.iter().map(|subspan| subspan.digest.count()).sum::<u64>(), 10);
    assert_eq!(whole.iter().map(|subspan| subspan.digest.count()).sum::<u64>(), 100);
    assert_eq!(
        within.iter().map(|subspan| &subspan.bounds).collect::<Vec<_>>(),
        whole.iter().map(|subspan| &subspan.bounds).collect::<Vec<_>>()
    );
    assert!(within.iter().filter(|subspan| subspan.bounds.intersect(&low).is_none()).all(|subspan| subspan.digest.count() == 0));
}

/// The page beneath a bucket is its members.
#[tokio::test]
async fn a_bucket_answers_its_members() {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..10).map(|n| item!(n, n as i64, b"")));

    let Page::Members(members) = tree.view().pages(vec![Span::all()]).await.unwrap().pop().unwrap() else {
        panic!("a root of ten is a bucket")
    };

    assert_eq!(members.len(), 10);
}

/// A view digests each span a comparison names, in order and in one read:
/// the same as the other tree's digest where their members agree, another
/// where they don't.
#[tokio::test]
async fn span_digests_answer_each_span_asked_in_order() {
    let keyspec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let ours: ItemTree = tree_of!(keyspec.clone(), (0..100).map(|n| item!(n, n as i64, b"")));
    let theirs: ItemTree = tree_of!(keyspec, (0..100).map(|n| item!(n, n as i64, b"")));

    let mut trx = theirs.transaction();
    trx.update(item!(42, 42, b""), item!(42, 42, b"changed")).unwrap();
    trx.commit().await.unwrap();

    let root = ours.view().span_digest(&Span::all()).await.unwrap();
    let Page::Digests(subspans) = ours.view().pages(vec![Span::all()]).await.unwrap().pop().unwrap() else { panic!("a branch") };
    let spans: Vec<Span> = subspans.iter().map(|subspan| subspan.bounds.clone()).collect();
    let answers = theirs.view().span_digests(spans.clone()).await.unwrap();

    assert_eq!(answers.iter().map(|answer| answer.bounds.clone()).collect::<Vec<_>>(), spans);
    assert_eq!(ours.view().span_digests(vec![root.bounds.clone()]).await.unwrap()[0].digest, root.digest);
    assert_ne!(theirs.view().span_digests(vec![root.bounds]).await.unwrap()[0].digest, root.digest);

    let differing: Vec<_> =
        subspans.iter().zip(&answers).filter(|(ours, theirs)| ours.digest != theirs.digest).map(|(ours, _)| ours).collect();
    assert_eq!(differing.len(), 1);
    assert!(ours.span_for(42i64..=42).unwrap().is_subset_of(&differing[0].bounds), "the subspan holding key 42");
}

/// A span digest is the members within its span and nothing of the tree
/// around them: a tree of just those members and a broader tree's view of the
/// same span digest it alike, though the two trees are shaped differently
/// there.
#[tokio::test]
async fn trees_holding_the_same_members_within_a_span_digest_it_alike() {
    let keyspec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let dedicated: ItemTree = tree_of!(keyspec.clone(), (10..20).map(|n| item!(n, n as i64, b"")));
    let broader: ItemTree = tree_of!(keyspec, (0..1000).map(|n| item!(n, n as i64, b"")));
    let span = broader.span_for(10i64..20).unwrap();

    let ours = dedicated.view().span_digest(&span).await.unwrap();
    let theirs = broader.select(span.clone()).unwrap().span_digest(&span).await.unwrap();

    assert_eq!(ours.digest.count(), 10);
    assert_eq!(ours, theirs);
}
