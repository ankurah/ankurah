//! The oracles: whatever transactions a tree commits, it holds what a build
//! over its final members holds, and a large transaction, laid out anew,
//! holds what the same changes committed a few at a time hold.

use std::collections::BTreeMap;

use crate::fixtures::{block_on, Item, Spec};
use crate::tree::fixtures::{assert_same_tree, built, by_id_spec, integer_spec, item, Tree};
use crate::{DigestGroup, LtHash16, Ristretto255};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

/// A member under the integer spec, as a test describes it.
#[derive(Clone, Debug)]
struct Planned {
    id: u32,
    key: Option<i64>,
    payload: Vec<u8>,
}

impl Planned {
    fn item(&self) -> Item { item(self.id, self.key, &self.payload) }
}

/// Members with ids below `ids`, keys from a narrow range so that many share
/// one, and short payloads.
fn planned(ids: u32) -> impl Strategy<Value = Planned> {
    (0..ids, proptest::option::weighted(0.85, -3i64..3), proptest::collection::vec(any::<u8>(), 0..3))
        .prop_map(|(id, key, payload)| Planned { id, key, payload })
}

/// A change a test plans: a member for the tree to hold as given, or an id
/// for it to hold no longer.
#[derive(Clone, Debug)]
enum PlannedChange {
    Hold(Planned),
    Remove(u32),
}

/// Commit `changes` to `tree` in one transaction, each naming the version
/// `model`, the members the tree holds, has for its member, as an owner
/// would, and bring `model` up to date. A member to hold is an update if the
/// model holds its id and an insert if not; a removal of an id the model
/// doesn't hold names no version, so it is left out.
async fn commit<G: DigestGroup>(tree: &Tree<G>, model: &mut BTreeMap<u32, Planned>, changes: &[PlannedChange]) {
    let mut trx = tree.transaction();
    for change in changes {
        match change {
            PlannedChange::Hold(member) => match model.insert(member.id, member.clone()) {
                Some(old) => trx.update(old.item(), member.item()).unwrap(),
                None => trx.insert(member.item()).unwrap(),
            },

            PlannedChange::Remove(id) => {
                if let Some(old) = model.remove(id) {
                    trx.remove(old.item()).unwrap();
                }
            }
        }
    }

    trx.commit().await.unwrap();
}

fn planned_change(ids: u32) -> impl Strategy<Value = PlannedChange> {
    prop_oneof![3 => planned(ids).prop_map(PlannedChange::Hold), 1 => (0..ids).prop_map(PlannedChange::Remove)]
}

/// Batches small enough to be committed node by node: phases that grow a
/// region and then mostly empty it again, between batches of any changes. A
/// phase adds members under a run of consecutive ids and a few nearby keys,
/// which often lie outside what a subtree holds, so that branches are
/// inserted above old subtrees, and its removals shrink the region's
/// subtrees to the merge threshold.
fn batches(ids: u32) -> impl Strategy<Value = Vec<Vec<PlannedChange>>> {
    let phase = (0..ids, -40i64..40, 40..100u32, proptest::option::weighted(0.75, 50..100u32)).prop_map(
        |(first_id, first_key, count, removed_percent)| {
            let hold =
                |n: u32| PlannedChange::Hold(Planned { id: first_id + n, key: Some(first_key + i64::from(n % 4)), payload: vec![n as u8] });

            let mut batches = vec![(0..count).map(hold).collect::<Vec<_>>()];
            if let Some(percent) = removed_percent {
                batches.push((0..count * percent / 100).map(|n| PlannedChange::Remove(first_id + n)).collect());
            }

            batches
        },
    );

    let any = proptest::collection::vec(planned_change(ids), 0..60).prop_map(|batch| vec![batch]);
    proptest::collection::vec(prop_oneof![phase, any], 1..5).prop_map(|groups| groups.into_iter().flatten().collect())
}

/// The members of a model, as a build takes them.
fn members(model: &BTreeMap<u32, Planned>) -> Vec<Item> { model.values().map(Planned::item).collect() }

/// The tree a sequence of transactions leaves, starting from a build of
/// `initial`, against a build over the members they leave.
async fn batches_leave_the_tree_a_build_gives<G: DigestGroup>(key_spec: Spec, initial: Vec<Planned>, batches: Vec<Vec<PlannedChange>>) {
    let mut model: BTreeMap<u32, Planned> = initial.into_iter().map(|member| (member.id, member)).collect();
    let tree = built::<G>(key_spec.clone(), members(&model)).await;

    for batch in &batches {
        commit(&tree, &mut model, batch).await;
    }

    assert_same_tree(&tree, &built(key_spec, members(&model)).await).await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, rng_seed: RngSeed::Fixed(0xa991_0001), ..ProptestConfig::default() })]

    /// The main oracle: whatever transactions a tree commits, it holds what a
    /// build over its final members holds, its member rows the same and every
    /// span's digest and count the same, through splits, merges and inserted
    /// branches. Its trie may differ in shape, since a shape depends
    /// on the order members came and went in.
    #[test]
    fn any_batches_leave_the_tree_a_build_gives(
        initial in proptest::collection::vec(planned(260), 0..220),
        batches in batches(260),
    ) {
        block_on(batches_leave_the_tree_a_build_gives::<Ristretto255>(integer_spec(), initial, batches));
    }

    /// The same oracle for a tree keyed by id alone, summed in LtHash16.
    #[test]
    fn any_batches_leave_an_lthash16_tree_keyed_by_id_a_build_gives(
        initial in proptest::collection::vec(planned(200), 0..160),
        batches in batches(200),
    ) {
        block_on(batches_leave_the_tree_a_build_gives::<LtHash16>(by_id_spec(), initial, batches));
    }

    /// One transaction that empties one side of the root while it changes
    /// the other leaves the tree a build gives: the root gives way to the side
    /// it keeps, which settles in turn, splitting if the transaction filled a
    /// bucket past its capacity, and giving way again if it emptied a side
    /// beneath.
    #[test]
    fn emptying_one_side_while_changing_the_other_leaves_the_tree_a_build_gives(
        emptied in 1..20u32,
        kept in prop_oneof![Just(64u32), 65..140u32],
        added in 0..30u32,
        removed_key in proptest::option::of(0..3i64),
    ) {
        // Key -1 parts from keys 0 to 2 at the value's first bit, so the root
        // holds the emptied members on one side and the kept ones on the
        // other.
        let kept_key = |n: u32| i64::from(n % 3);
        let initial = (0..emptied)
            .map(|n| Planned { id: n, key: Some(-1), payload: vec![] })
            .chain((0..kept).map(|n| Planned { id: 1000 + n, key: Some(kept_key(n)), payload: vec![] }))
            .collect();

        let changes = (0..emptied)
            .map(PlannedChange::Remove)
            .chain((0..added).map(|n| PlannedChange::Hold(Planned { id: 5000 + n, key: Some(kept_key(n)), payload: vec![] })))
            .chain((0..kept).filter(|&n| Some(kept_key(n)) == removed_key).map(|n| PlannedChange::Remove(1000 + n)))
            .collect();

        block_on(batches_leave_the_tree_a_build_gives::<Ristretto255>(integer_spec(), initial, vec![changes]));
    }
}

/// A transaction large against its tree lays the tree out anew, as a build
/// does, and holds what the same changes committed a few at a time, node by
/// node, hold.
async fn a_large_batch_matches_small_batches(initial: Vec<Planned>, changes: Vec<PlannedChange>) {
    let mut model: BTreeMap<u32, Planned> = initial.into_iter().map(|member| (member.id, member)).collect();
    let mut few_at_a_time_model = model.clone();

    let at_once = built::<Ristretto255>(integer_spec(), members(&model)).await;
    let few_at_a_time = built::<Ristretto255>(integer_spec(), members(&model)).await;
    commit(&at_once, &mut model, &changes).await;

    for batch in changes.chunks(7) {
        commit(&few_at_a_time, &mut few_at_a_time_model, batch).await;
    }

    assert_same_tree(&at_once, &few_at_a_time).await;

    let rebuilt = built::<Ristretto255>(integer_spec(), members(&model)).await;
    assert_same_tree(&at_once, &rebuilt).await;

    // Laid out anew, the large batch's tree has a build's very nodes.
    assert_eq!(at_once.stored_rows().await, rebuilt.stored_rows().await);
}

/// Changes to distinct ids: at least 128 of them, and at least a quarter of
/// a tree of up to 400 members.
fn large_batch() -> impl Strategy<Value = (Vec<Planned>, Vec<PlannedChange>)> {
    let initial = proptest::collection::vec(planned(400), 0..400);
    let changes = proptest::collection::vec(planned_change(600), 260..320);

    (initial, changes).prop_map(|(initial, changes)| {
        let mut seen = std::collections::HashSet::new();
        let distinct = changes
            .into_iter()
            .filter(|change| {
                seen.insert(match change {
                    PlannedChange::Hold(member) => member.id,
                    PlannedChange::Remove(id) => *id,
                })
            })
            .collect();

        (initial, distinct)
    })
}

/// How many edits `changes`, to distinct ids, make to a tree holding
/// `initial`: what the rule for laying a tree out anew counts. A new member
/// is an insertion and a removed one a removal; a member moved to another key
/// is both, one given another payload under its key a replacement, and one
/// held as it was, or removed while absent, no edit at all.
fn edits(initial: &[Planned], changes: &[PlannedChange]) -> usize {
    let held: BTreeMap<u32, &Planned> = initial.iter().map(|member| (member.id, member)).collect();
    changes
        .iter()
        .map(|change| match (change, held.get(&change_id(change))) {
            (PlannedChange::Hold(_), None) => 1,
            (PlannedChange::Hold(member), Some(old)) if member.key != old.key => 2,
            (PlannedChange::Hold(member), Some(old)) => usize::from(member.payload != old.payload),
            (PlannedChange::Remove(_), held) => usize::from(held.is_some()),
        })
        .sum()
}

fn change_id(change: &PlannedChange) -> u32 {
    match change {
        PlannedChange::Hold(member) => member.id,
        PlannedChange::Remove(id) => *id,
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, rng_seed: RngSeed::Fixed(0xb01c_0001), ..ProptestConfig::default() })]

    /// Laying a tree out anew for a large batch gives the tree that node by
    /// node maintenance gives for the same changes.
    #[test]
    fn a_large_batch_gives_the_tree_small_batches_give((initial, changes) in large_batch()) {
        let members = initial.iter().map(|member| member.id).collect::<std::collections::BTreeSet<_>>().len();
        let edits = edits(&initial, &changes);
        prop_assume!(edits >= 128 && edits * 4 >= members, "a batch the relayout rule lays out anew");
        block_on(a_large_batch_matches_small_batches(initial, changes));
    }
}
