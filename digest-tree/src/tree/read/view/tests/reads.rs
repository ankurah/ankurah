//! Every read of a view, checked against a model of the members.

use std::ops::Bound;

use super::super::*;
use crate::fixtures::{block_on, leaf_element, Id, Tag};
use crate::span::{key_prefix, split_address};
use crate::tree::fixtures::{built, integer_key, integer_spec, item, Tree, View};
use crate::{leaf_address, NodePrefix, Ristretto255, ViewPlan};
use futures::TryStreamExt;
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

/// The members a test put in a tree, in address order: what reads are
/// checked against.
struct Model(Vec<(Vec<u8>, Id, Tag)>);

impl Model {
    fn of(members: &[(u32, Option<i64>, Vec<u8>)]) -> Self {
        let mut rows: Vec<(Vec<u8>, Id, Tag)> =
            members.iter().map(|(id, key, payload)| (leaf_address(&integer_key(*key), &Id(*id)), Id(*id), Tag(payload.clone()))).collect();

        rows.sort_by(|a, b| a.0.cmp(&b.0));
        Self(rows)
    }

    /// The members within `span` that `spans` cover.
    fn within<'a>(&'a self, span: &'a Span, spans: &'a [Span]) -> impl Iterator<Item = &'a (Vec<u8>, Id, Tag)> + 'a {
        self.0.iter().filter(move |(address, ..)| span.contains(address) && spans.iter().any(|covered| covered.contains(address)))
    }

    /// The digest of the members within `span` that `spans` cover.
    fn digest(&self, span: &Span, spans: &[Span]) -> Digest<Ristretto255> {
        let mut digest = Digest::empty();
        for (address, id, payload) in self.within(span, spans) {
            let (key, _) = split_address(address).unwrap();
            digest.add_element(&leaf_element::<Ristretto255>(&key, &id.to_bytes(), &payload.0)).unwrap();
        }

        digest
    }
}

/// A bound at a key, or at one member's address within it, so that spans cut
/// buckets mid-key as well as between keys.
fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
    (0..3u8, proptest::option::weighted(0.9, -3i64..3), proptest::option::of(0..300u32)).prop_map(|(kind, key, id)| {
        let at = match id {
            Some(id) => leaf_address(&integer_key(key), &Id(id)),
            None => key_prefix(&integer_key(key)),
        };

        match kind {
            0 => Bound::Unbounded,
            1 => Bound::Included(at),
            _ => Bound::Excluded(at),
        }
    })
}

fn span() -> impl Strategy<Value = Span> { (bound(), bound()).prop_map(|(start, end)| Span::new(start, end)) }

/// Distinct ids with keys from a narrow range, so that keys repeat, and
/// enough members for the trie to branch.
fn members() -> impl Strategy<Value = Vec<(u32, Option<i64>, Vec<u8>)>> {
    proptest::collection::btree_map(
        0..300u32,
        (proptest::option::weighted(0.85, -3i64..3), proptest::collection::vec(any::<u8>(), 0..3)),
        0..260,
    )
    .prop_map(|members| members.into_iter().map(|(id, (key, payload))| (id, key, payload)).collect())
}

/// Check every read of `view`, which covers `spans` of a tree holding
/// `model` and reads its members in `direction`, over `span`.
async fn reads_match_the_model(tree: &Tree, view: &View, model: &Model, spans: &[Span], direction: ScanDirection, span: &Span) {
    assert_eq!(view.len().await.unwrap(), model.within(&Span::all(), spans).count() as u64);
    assert_eq!(view.count(span).await.unwrap(), model.within(span, spans).count() as u64);

    // Members, scanned in the view's direction.
    let read: Vec<(Vec<u8>, Tag)> =
        view.members(span).map_ok(|leaf| (leaf_address(&leaf.key, &leaf.id), leaf.payload)).try_collect().await.unwrap();

    let mut expected: Vec<(Vec<u8>, Tag)> =
        model.within(span, spans).map(|(address, _, payload)| (address.clone(), payload.clone())).collect();

    if direction == ScanDirection::Reverse {
        expected.reverse();
    }

    assert_eq!(read, expected, "members within {span:?}, read {direction:?}");

    for (address, id, payload) in model.0.iter().step_by(17) {
        let covered = spans.iter().any(|covered| covered.contains(address));
        let (key, _) = split_address(address).unwrap();
        let leaf = view.member(&key, id).await.unwrap();

        assert_eq!(leaf.map(|leaf| (leaf.key, leaf.payload)), covered.then(|| (key, payload.clone())));
    }

    // The span's digest, named with the smallest subtree that contains it.
    let span_digest = view.span_digest(span).await.unwrap();
    span_digest.validate().unwrap();
    assert_eq!(span_digest.digest, model.digest(span, spans));

    if !span.is_empty() {
        for node in tree.node_prefixes().await {
            if node != span_digest.prefix && is_beneath(&node, &span_digest.prefix) {
                assert!(!span.is_subset_of(&node.span()), "the deeper {node:?} holds {span:?}");
            }
        }
    }

    // Its refinement: disjoint parts of its bounds, each with its own digest,
    // which together sum to it and cover every member within it.
    let refined = view.refine(&span_digest).await.unwrap();
    if refined.is_empty() {
        let reached = tree
            .node_prefixes()
            .await
            .into_iter()
            .any(|node| node != span_digest.prefix && is_beneath(&node, &span_digest.prefix) && node.span().intersect(span).is_some());

        let in_view = spans.iter().any(|covered| covered.intersect(span).is_some());
        assert!(!reached || !in_view, "only bounds outside the view, or no subtree beneath that reaches them, refine into nothing");
        return;
    }

    let mut sum = Digest::empty();
    for part in &refined {
        part.validate().unwrap();
        assert!(part.prefix != span_digest.prefix || part.bounds != span_digest.bounds, "{part:?} makes progress");
        assert!(part.bounds.is_subset_of(span), "{part:?} within {span:?}");
        assert_eq!(part.digest, model.digest(&part.bounds, spans), "{part:?}");
        sum.add(&part.digest).unwrap();
    }

    assert_eq!(sum, span_digest.digest);

    for (address, ..) in model.within(span, spans) {
        assert_eq!(refined.iter().filter(|part| part.bounds.contains(address)).count(), 1, "{address:02x?}");
    }
}

/// Whether `node` is `prefix` or lies beneath it.
fn is_beneath(node: &NodePrefix, prefix: &NodePrefix) -> bool { prefix.contains(node) }

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, rng_seed: RngSeed::Fixed(0x7ead_0001), ..ProptestConfig::default() })]

    /// Every read of a view of the whole tree, read forward, and of a view
    /// of some spans, read in either direction, against a model of the
    /// members.
    #[test]
    fn every_read_matches_a_model_of_the_members(
        members in members(),
        selected in proptest::collection::vec(span(), 0..3),
        spans in proptest::collection::vec(span(), 1..4),
        reverse in any::<bool>(),
    ) {
        block_on(async {
            let model = Model::of(&members);
            let tree = built::<Ristretto255>(integer_spec(), members.iter().map(|(id, key, payload)| item(*id, *key, payload))).await;

            let (view, covered, direction) = match selected.is_empty() {
                true => (tree.view(), vec![Span::all()], ScanDirection::Forward),
                false => {
                    let direction = if reverse { ScanDirection::Reverse } else { ScanDirection::Forward };
                    let plan = ViewPlan { spans: selected.clone(), direction, limit: None };

                    (tree.select(&plan).await.unwrap(), Span::normalize(selected.clone()), direction)
                }
            };

            for span in spans.iter().chain([&Span::all()]) {
                reads_match_the_model(&tree, &view, &model, &covered, direction, span).await;
            }
        });
    }
}
