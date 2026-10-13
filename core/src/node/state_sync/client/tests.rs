use super::*;
use std::ops::Bound::*;

fn id(n: u8) -> proto::EntityId { proto::EntityId::from_bytes([n; 32]) }
fn address(n: u8) -> Vec<u8> { leaf_address(&[], id(n)) }
fn spec() -> proto::TreeSpec { proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True) }
fn leaf(n: u8) -> proto::KnownLeaf {
    proto::KnownLeaf { key: vec![], entity: proto::KnownEntity { entity_id: id(n), head: proto::Clock::default() } }
}

#[test]
fn authoritative_range_updates_remove_only_the_exact_interval() -> anyhow::Result<()> {
    let mut members: BTreeMap<_, _> = (0..10).map(|n| (address(n), id(n))).collect();
    let page =
        proto::LeafPage { bounds: proto::AddressRange::new(Excluded(address(2)), Included(address(7))), leaves: vec![leaf(4), leaf(6)] };
    replace_members(&mut members, &spec(), page.clone())?;
    replace_members(&mut members, &spec(), page)?;
    assert_eq!(members.values().copied().collect::<Vec<_>>(), vec![id(0), id(1), id(2), id(4), id(6), id(8), id(9)]);
    replace_members(
        &mut members,
        &spec(),
        proto::LeafPage { bounds: proto::AddressRange::new(Included(address(4)), Excluded(address(8))), leaves: vec![] },
    )?;
    assert_eq!(members.values().copied().collect::<Vec<_>>(), vec![id(0), id(1), id(2), id(8), id(9)]);
    Ok(())
}

#[test]
fn malformed_pages_cannot_partly_replace_membership_or_panic() {
    let original: BTreeMap<_, _> = (0..10).map(|n| (address(n), id(n))).collect();
    let cases = [
        proto::LeafPage { bounds: proto::AddressRange::new(Excluded(address(4)), Excluded(address(4))), leaves: vec![] },
        proto::LeafPage { bounds: proto::AddressRange::new(Included(address(8)), Included(address(2))), leaves: vec![] },
        proto::LeafPage { bounds: super::super::whole(), leaves: vec![leaf(4), leaf(3)] },
        proto::LeafPage { bounds: proto::AddressRange::new(Included(address(4)), Included(address(6))), leaves: vec![leaf(3)] },
    ];
    for page in cases {
        let mut members = original.clone();
        assert!(replace_members(&mut members, &spec(), page).is_err());
        assert_eq!(members, original);
    }
}
