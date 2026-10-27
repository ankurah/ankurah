use super::group::GroupOperations;
use super::*;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::Index;
use proptest::test_runner::RngSeed;

/// A leaf's element in ristretto255, from its key, id and payload.
fn element(key: &[u8], id: &[u8], payload: &[u8]) -> <Ristretto255 as DigestGroup>::Element {
    Ristretto255::hash_leaf(&leaf_encoding(key, id, payload))
}

fn digest_of(elements: &[<Ristretto255 as DigestGroup>::Element]) -> Digest<Ristretto255> {
    let mut digest = Digest::empty();
    for element in elements {
        digest.add_element(element).unwrap();
    }

    digest
}

#[test]
fn the_empty_digest_is_the_identity_and_removing_every_member_empties_a_digest() {
    let (a, b) = (element(b"a", b"1", b""), element(b"b", b"2", b""));
    let digest = digest_of(&[a, b]);
    let mut sum = digest;
    sum.add(&Digest::empty()).unwrap();
    assert_eq!(sum, digest);

    let mut nothing = digest;
    nothing.sub_element(&a).unwrap();
    nothing.sub_element(&b).unwrap();

    assert_eq!(nothing, Digest::empty());
    assert_eq!(Digest::<Ristretto255>::empty().count(), 0);
}

#[test]
fn adding_and_subtracting_elements_counts_members() {
    let (a, b) = (element(b"a", b"1", b""), element(b"b", b"2", b""));
    let mut digest = Digest::<Ristretto255>::empty();
    digest.add_element(&a).unwrap();
    digest.add_element(&b).unwrap();

    assert_eq!(digest.count(), 2);
    digest.sub_element(&a).unwrap();

    assert_eq!(digest, Digest::of_element(b));
}

/// A count past the range of `u64`, or below zero, comes only from rows that
/// disagree, so the arithmetic reports the tree inconsistent rather than wrap.
#[test]
fn counts_out_of_range_report_the_tree_inconsistent() {
    let full = Digest::<Ristretto255> { element: Ristretto255::zero(), count: u64::MAX };
    let one = Digest::<Ristretto255>::of_element(element(b"a", b"1", b""));

    let (mut sum, mut with_element) = (full, full);
    assert!(matches!(sum.add(&one), Err(Error::Inconsistent(_))));
    assert!(matches!(with_element.add_element(&one.element), Err(Error::Inconsistent(_))));

    let mut without_element = Digest::<Ristretto255>::empty();
    assert!(matches!(without_element.sub_element(&one.element), Err(Error::Inconsistent(_))));

    // A refused operation leaves the digest as it was.
    assert_eq!((sum, with_element, without_element), (full, full, Digest::empty()));
}

#[test]
fn a_digest_prints_its_canonical_encoding_and_count() {
    let empty = format!("{:?}", Digest::<Ristretto255>::empty());
    assert_eq!(empty, format!("Digest {{ element: {}, count: 0 }}", "00".repeat(32)));
}

/// A leaf's parts, owned so proptest can generate and shrink them.
#[derive(Clone, Debug)]
struct LeafParts {
    key: Vec<u8>,
    id: [u8; 16],
    payload: Vec<u8>,
}

impl LeafParts {
    fn element(&self) -> <Ristretto255 as DigestGroup>::Element { element(&self.key, &self.id, &self.payload) }
}

fn leaf_parts() -> impl Strategy<Value = LeafParts> {
    (vec(any::<u8>(), 0..12), any::<[u8; 16]>(), vec(any::<u8>(), 0..12)).prop_map(|(key, id, payload)| LeafParts { key, id, payload })
}

/// Sum consecutive runs of `digests`, cutting before each position in `cuts`:
/// one level of a tree whose tree nodes hold consecutive leaves.
fn group_runs(digests: &[Digest<Ristretto255>], cuts: &[Index]) -> Vec<Digest<Ristretto255>> {
    let mut at: Vec<usize> = cuts.iter().map(|cut| cut.index(digests.len() + 1)).collect();
    at.extend([0, digests.len()]);
    at.sort_unstable();
    at.dedup();

    at.windows(2)
        .map(|run| {
            let mut sum = Digest::empty();
            digests[run[0]..run[1]].iter().for_each(|digest| sum.add(digest).unwrap());
            sum
        })
        .collect()
}

fn sum(digests: &[Digest<Ristretto255>]) -> Digest<Ristretto255> { group_runs(digests, &[]).pop().unwrap_or_else(Digest::empty) }

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, rng_seed: RngSeed::Fixed(0xd16e_0001), ..ProptestConfig::default() })]

    /// Two trees that group the same leaves differently, with different
    /// bucket boundaries and different depths, hold the same digest at the
    /// root, and it equals the sum of the leaves taken one by one.
    #[test]
    fn any_grouping_gives_the_same_digest(
        leaves in vec(leaf_parts(), 0..24),
        first in (vec(any::<Index>(), 0..6), vec(any::<Index>(), 0..3)),
        second in vec(any::<Index>(), 0..8),
    ) {
        let digests: Vec<_> = leaves.iter().map(|leaf| Digest::of_element(leaf.element())).collect();
        let flat = digest_of(&leaves.iter().map(LeafParts::element).collect::<Vec<_>>());

        prop_assert_eq!(sum(&group_runs(&group_runs(&digests, &first.0), &first.1)), flat);
        prop_assert_eq!(sum(&group_runs(&digests, &second)), flat);
        prop_assert_eq!(flat.count(), leaves.len() as u64);
    }

    /// Leaves summed in any order give the same digest.
    #[test]
    fn insertion_order_never_matters((leaves, reordered) in vec(leaf_parts(), 0..24).prop_flat_map(|leaves| (Just(leaves.clone()), Just(leaves).prop_shuffle()))) {
        let in_order = digest_of(&leaves.iter().map(LeafParts::element).collect::<Vec<_>>());
        let reordered = digest_of(&reordered.iter().map(LeafParts::element).collect::<Vec<_>>());

        prop_assert_eq!(in_order, reordered);
    }

    /// A change to one member, of its payload or its key, updates a digest by
    /// subtracting the old leaf's element and adding the new one, and the
    /// result equals the digest recomputed from scratch, with the same count.
    #[test]
    fn subtracting_the_old_leaf_and_adding_the_new_equals_recomputation(
        leaves in vec(leaf_parts(), 1..16),
        changed in any::<Index>(),
        new_key in vec(any::<u8>(), 0..12),
        new_payload in vec(any::<u8>(), 0..12),
    ) {
        let i = changed.index(leaves.len());
        let before = digest_of(&leaves.iter().map(LeafParts::element).collect::<Vec<_>>());

        let mut after = leaves.clone();
        after[i].key = new_key;
        after[i].payload = new_payload;

        let mut updated = before;
        updated.sub_element(&leaves[i].element()).unwrap();
        updated.add_element(&after[i].element()).unwrap();

        prop_assert_eq!(updated, digest_of(&after.iter().map(LeafParts::element).collect::<Vec<_>>()));
        prop_assert_eq!(updated.count(), leaves.len() as u64);
    }
}
