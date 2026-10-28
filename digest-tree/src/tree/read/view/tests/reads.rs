//! Every read of a view, checked against a model of the members.

use std::ops::Bound;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use futures::TryStreamExt;
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

use super::super::*;
use crate::common::{asc, item, tree_of, Id, ItemTree, ItemView, Property, Tag};
use crate::fixtures::{block_on, leaf_element};
use crate::key::member_key;
use crate::span::{key_prefix, split_address};
use crate::{leaf_address, NodePrefix, Ristretto255, ScanRange};

/// The key of a member holding `value` for property `A`, ascending.
fn key_of(value: Option<i64>) -> Vec<u8> {
    member_key(&KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), &item!(0, value, b"")).unwrap()
}

/// The members a test put in a tree, in address order: what reads are
/// checked against.
struct Model(Vec<(Vec<u8>, Id, Tag, Option<i64>)>);

impl Model {
    fn of(members: &[(u64, Option<i64>, Vec<u8>)]) -> Self {
        let mut rows: Vec<(Vec<u8>, Id, Tag, Option<i64>)> =
            members.iter().map(|(id, key, payload)| (leaf_address(&key_of(*key), &Id(*id)), Id(*id), Tag(payload.clone()), *key)).collect();

        rows.sort_by(|a, b| a.0.cmp(&b.0));
        Self(rows)
    }

    /// The members within `span` that `spans` cover.
    fn within<'a>(&'a self, span: &'a Span, spans: &'a [Span]) -> impl Iterator<Item = &'a (Vec<u8>, Id, Tag, Option<i64>)> + 'a {
        self.0.iter().filter(move |(address, ..)| span.contains(address) && spans.iter().any(|covered| covered.contains(address)))
    }

    /// The digest of the members within `span` that `spans` cover.
    fn digest(&self, span: &Span, spans: &[Span]) -> Digest<Ristretto255> {
        let mut digest = Digest::empty();
        for (address, id, payload, _) in self.within(span, spans) {
            let (key, _) = split_address(address).unwrap();
            digest.add_element(&leaf_element::<Ristretto255>(&key, &id.to_bytes(), &payload.0)).unwrap();
        }

        digest
    }
}

/// A bound at a key, or at one member's address within it, so that spans cut
/// buckets mid-key as well as between keys.
fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
    (0..3u8, proptest::option::weighted(0.9, -3i64..3), proptest::option::of(0..300u64)).prop_map(|(kind, key, id)| {
        let at = match id {
            Some(id) => leaf_address(&key_of(key), &Id(id)),
            None => key_prefix(&key_of(key)),
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
fn members() -> impl Strategy<Value = Vec<(u64, Option<i64>, Vec<u8>)>> {
    proptest::collection::btree_map(
        0..300u64,
        (proptest::option::weighted(0.85, -3i64..3), proptest::collection::vec(any::<u8>(), 0..3)),
        0..260,
    )
    .prop_map(|members| members.into_iter().map(|(id, (key, payload))| (id, key, payload)).collect())
}

/// Check every read of `view`, which covers `spans` of a tree holding
/// `model`, over `span`.
async fn reads_match_the_model(tree: &ItemTree, view: &ItemView, model: &Model, spans: &[Span], span: &Span) {
    assert_eq!(view.len().await.unwrap(), model.within(&Span::all(), spans).count() as u64);
    assert_eq!(view.count(span).await.unwrap(), model.within(span, spans).count() as u64);

    // Members, read both ways: the same members in opposite orders.
    let forward: Vec<(Vec<u8>, Tag)> =
        model.within(span, spans).map(|(address, _, payload, _)| (address.clone(), payload.clone())).collect();
    let reverse: Vec<(Vec<u8>, Tag)> = forward.iter().rev().cloned().collect();

    for (range, expected) in [(ScanRange::forward(span.clone()), forward), (ScanRange::reverse(span.clone()), reverse)] {
        let read: Vec<(Vec<u8>, Tag)> =
            view.range(range.clone()).map_ok(|leaf| (leaf_address(&leaf.key, &leaf.id), leaf.payload)).try_collect().await.unwrap();

        assert_eq!(read, expected, "{range:?}");
    }

    // Lookups name a key's values, so only members that hold one.
    for (address, id, payload, value) in model.0.iter().step_by(17) {
        let Some(value) = value else { continue };
        let covered = spans.iter().any(|covered| covered.contains(address));
        let (key, _) = split_address(address).unwrap();
        let leaf = view.member([*value], id).await.unwrap();

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

    /// Every read of a view of the whole tree, and of a view of some spans,
    /// against a model of the members.
    #[test]
    fn every_read_matches_a_model_of_the_members(members in members(), selected in proptest::collection::vec(span(), 0..3), spans in proptest::collection::vec(span(), 1..4)) {
        block_on(async {
            let model = Model::of(&members);
            let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), members.iter().map(|(id, key, payload)| item!(*id, *key, payload)));

            let (view, covered) = match selected.is_empty() {
                true => (tree.view(), vec![Span::all()]),
                false => (tree.select(selected.clone()), Span::normalize(selected.clone())),
            };

            for span in spans.iter().chain([&Span::all()]) {
                reads_match_the_model(&tree, &view, &model, &covered, span).await;
            }
        });
    }
}
