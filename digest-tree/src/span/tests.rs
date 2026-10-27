use super::*;
use crate::fixtures::{key_part, Id, Order};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

#[test]
fn contains_honours_each_kind_of_bound() {
    let included = Span::new(Bound::Included(vec![2]), Bound::Included(vec![4]));
    let excluded = Span::new(Bound::Excluded(vec![2]), Bound::Excluded(vec![4]));

    for address in [vec![1], vec![2], vec![3], vec![4], vec![4, 0], vec![5]] {
        assert_eq!(included.contains(&address), address >= vec![2] && address <= vec![4], "{address:?} within the closed span");
        assert_eq!(excluded.contains(&address), address > vec![2] && address < vec![4], "{address:?} within the open span");
        assert!(Span::all().contains(&address));
    }

    assert!(Span::all().contains(&[]), "the empty address lies within every unbounded span");
}

#[test]
fn emptiness_subsets_and_intersections_follow_the_bounds() {
    let span = |start, end| Span::new(start, end);
    assert!(span(Bound::Included(vec![3]), Bound::Included(vec![2])).is_empty());
    assert!(span(Bound::Included(vec![2]), Bound::Excluded(vec![2])).is_empty());
    assert!(!span(Bound::Included(vec![2]), Bound::Included(vec![2])).is_empty());
    assert!(!Span::all().is_empty());

    let narrow = span(Bound::Included(vec![2]), Bound::Excluded(vec![4]));
    let wide = span(Bound::Excluded(vec![1]), Bound::Included(vec![4]));

    assert!(narrow.is_subset_of(&wide) && !wide.is_subset_of(&narrow));
    assert!(narrow.is_subset_of(&Span::all()));
    assert!(span(Bound::Included(vec![9]), Bound::Included(vec![1])).is_subset_of(&narrow), "an empty span lies within every span");
    assert_eq!(narrow.intersect(&wide), Some(narrow.clone()));
    assert_eq!(narrow.intersect(&span(Bound::Included(vec![4]), Bound::Unbounded)), None);
    assert_eq!(narrow.cmp_start(&wide), Ordering::Greater);
}

#[test]
fn normalizing_drops_empty_spans_and_merges_those_that_meet() {
    let span = |start, end| Span::new(start, end);
    let spans = vec![
        span(Bound::Included(vec![5]), Bound::Excluded(vec![7])),
        span(Bound::Included(vec![1]), Bound::Excluded(vec![3])),
        span(Bound::Included(vec![3]), Bound::Excluded(vec![4])),
        span(Bound::Included(vec![9]), Bound::Excluded(vec![8])),
        span(Bound::Excluded(vec![6]), Bound::Included(vec![7, 5])),
        span(Bound::Included(vec![5, 5]), Bound::Excluded(vec![6])),
    ];

    assert_eq!(
        Span::normalize(spans),
        vec![span(Bound::Included(vec![1]), Bound::Excluded(vec![4])), span(Bound::Included(vec![5]), Bound::Included(vec![7, 5])),]
    );
}

/// Every kind of endpoint against every other, and the gaps around any two
/// covered spans: each sample address lies in exactly one gap when the span
/// holds it and nothing covers it, and in none otherwise.
#[test]
fn gaps_partition_each_kind_of_boundary() {
    let mut endpoints = vec![Bound::Unbounded];
    for value in [vec![], vec![1], vec![1, 0], vec![2]] {
        endpoints.push(Bound::Included(value.clone()));
        endpoints.push(Bound::Excluded(value));
    }

    let spans: Vec<_> = endpoints
        .iter()
        .flat_map(|start| endpoints.iter().map(move |end| Span::new(start.clone(), end.clone())))
        .filter(|span| !span.is_empty())
        .collect();

    let samples = [vec![], vec![0], vec![1], vec![1, 0], vec![1, 0, 0], vec![1, 1], vec![2], vec![2, 0], vec![255]];
    for span in &spans {
        for a in &spans {
            for b in &spans {
                let covered = Span::normalize(vec![a.clone(), b.clone()]);
                let missing = span.gaps(&covered);

                for address in &samples {
                    let uncovered = span.contains(address) && !covered.iter().any(|part| part.contains(address));
                    assert_eq!(
                        missing.iter().filter(|part| part.contains(address)).count(),
                        usize::from(uncovered),
                        "span={span:?}, covered={covered:?}, address={address:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn a_prefix_end_follows_every_string_the_prefix_begins() {
    assert_eq!(prefix_end(&[1, 2]), Some(vec![1, 3]));
    assert_eq!(prefix_end(&[1, 255, 255]), Some(vec![2]));
    assert_eq!(prefix_end(&[255]), None);
    assert_eq!(prefix_end(&[]), None);
}

/// A key bound: up to two leading values, each of the two integer parts.
fn bound_values() -> impl Strategy<Value = Vec<i64>> { proptest::collection::vec(-3i64..3, 0..=2) }

/// A member's key values: present or missing, missing above every present.
fn key_values() -> impl Strategy<Value = [Option<i64>; 2]> { [proptest::option::of(-3i64..3), proptest::option::of(-3i64..3)] }

/// The key prefix of these leading values, each an ascending integer part.
fn prefix_of(values: &[Option<i64>]) -> Vec<u8> { values.iter().flat_map(|&value| key_part(value, Order::Ascending)).collect() }

fn bound_of(kind: u8, prefix: &[u8]) -> Bound<&[u8]> {
    match kind {
        0 => Bound::Unbounded,
        1 => Bound::Included(prefix),
        _ => Bound::Excluded(prefix),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, rng_seed: RngSeed::Fixed(0x5a4e_0001), ..ProptestConfig::default() })]

    /// A key range holds a member's address exactly when the member's leading
    /// values compare with each bound as the bound says, missing values
    /// above every present one: a bound covers every key beneath its prefix.
    #[test]
    fn a_key_range_holds_every_address_beneath_its_bounds(
        lower in (0..3u8, bound_values()),
        upper in (0..3u8, bound_values()),
        member in key_values(),
        id in any::<u32>(),
    ) {
        let as_prefix = |values: &[i64]| prefix_of(&values.iter().copied().map(Some).collect::<Vec<_>>());
        let (lower_prefix, upper_prefix) = (as_prefix(&lower.1), as_prefix(&upper.1));
        let span = Span::for_key_range(bound_of(lower.0, &lower_prefix), bound_of(upper.0, &upper_prefix));
        let address = leaf_address(&prefix_of(&member), &Id(id));

        // Missing sorts above every present value.
        let rank = |value: Option<i64>| value.map_or((1, 0), |n| (0, n));
        let compare = |bound: &[i64]| -> Ordering {
            member.iter().zip(bound).map(|(value, bound)| rank(*value).cmp(&rank(Some(*bound)))).find(|order| order.is_ne()).unwrap_or(Ordering::Equal)
        };

        let above_lower = match lower.0 { 0 => true, 1 => compare(&lower.1).is_ge(), _ => compare(&lower.1).is_gt() };
        let below_upper = match upper.0 { 0 => true, 1 => compare(&upper.1).is_le(), _ => compare(&upper.1).is_lt() };

        prop_assert_eq!(span.contains(&address), above_lower && below_upper);
    }
}

#[test]
fn empty_key_bounds_hold_everything_or_nothing() {
    assert_eq!(Span::for_key_range(Bound::Included(&[]), Bound::Included(&[])), Span::all());

    for (lower, upper) in [(Bound::Excluded(&[][..]), Bound::Unbounded), (Bound::Unbounded, Bound::Excluded(&[][..]))] {
        let span = Span::for_key_range(lower, upper);
        assert!(span.is_empty() && !span.contains(&[]) && !span.contains(&leaf_address(&[0x02], &Id(1))));
    }
}
