//! How span digests are named and refined.

use super::super::*;
use crate::tree::fixtures::{built, integer_spec, item};
use crate::{NodePrefix, Ristretto255};

/// A span digest names the deepest node whose subtree holds its span.
#[tokio::test]
async fn a_span_digest_is_named_with_the_smallest_subtree_holding_its_span() {
    let tree = built::<Ristretto255>(integer_spec(), (0..100).map(|n| item!(n, Some(i64::from(n)), b""))).await;
    let view = tree.view();
    assert_eq!(view.span_digest(&Span::all()).await.unwrap().prefix, NodePrefix::root());

    let seven = integer_spec().collate([7]);
    let one = Span::for_key_range(seven.clone()..=seven);
    let named = view.span_digest(&one).await.unwrap();

    let buckets: Vec<NodePrefix> = tree.node_prefixes().await.into_iter().filter(|node| *node != NodePrefix::root()).collect();
    let holding: Vec<&NodePrefix> = buckets.iter().filter(|bucket| one.is_subset_of(&bucket.span())).collect();

    assert_eq!(holding, [&named.prefix], "the one bucket that holds key 7");
    assert_eq!(named.digest.count(), 1);
}

/// Refining within a view leaves out what the view doesn't cover.
#[tokio::test]
async fn refining_within_a_view_leaves_out_what_it_doesnt_cover() {
    let tree = built::<Ristretto255>(integer_spec(), (0..100).map(|n| item!(n, Some(i64::from(n)), b""))).await;
    let low = Span::for_key_range(..integer_spec().collate([10]));

    let view = tree.select([low.clone()]);
    let root = view.span_digest(&Span::all()).await.unwrap();
    assert_eq!(root.digest.count(), 10);

    let refined = view.refine(&root).await.unwrap();
    assert!(refined.iter().all(|part| part.bounds.intersect(&low).is_some()), "{refined:?}");
    assert_eq!(refined.iter().map(|part| part.digest.count()).sum::<u64>(), 10);

    let whole = tree.view().refine(&tree.view().span_digest(&Span::all()).await.unwrap()).await.unwrap();
    assert!(whole.len() > refined.len(), "the whole tree refines into the part above key 10 too");
}

#[tokio::test]
async fn a_bucket_refines_into_nothing_and_a_malformed_span_digest_is_refused() {
    let tree = built::<Ristretto255>(integer_spec(), (0..10).map(|n| item!(n, Some(i64::from(n)), b""))).await;
    let view = tree.view();
    let root = view.span_digest(&Span::all()).await.unwrap();
    assert!(view.refine(&root).await.unwrap().is_empty(), "a root of ten members is a bucket");

    let outside = SpanDigest { prefix: NodePrefix::root().child(true), ..root };
    assert!(matches!(view.refine(&outside).await, Err(Error::MalformedSpanDigest(_))));
}
