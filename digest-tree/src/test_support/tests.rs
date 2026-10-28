use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;

use super::*;
use crate::common::{asc, item, Item, Property};
use crate::{DigestTree, LtHash16, MemoryStore, Ristretto255, Span};

/// The members' digests sum to the digest of a tree that holds them.
async fn member_digests_sum_to_the_trees<G: DigestGroup>() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let members: Vec<Item> = (0..150u64).map(|n| item!(n, (n % 9) as i64, &n.to_be_bytes())).collect();
    let digests = members.iter().map(|member| member_digest::<_, _, G>(&spec, member).unwrap());
    let expected = sum_digests(digests).unwrap();
    let tree = DigestTree::<_, _, _, G, _>::build(MemoryStore::new(), spec.clone(), members).await.unwrap();

    assert_eq!(tree.view().span_digest(&Span::all()).await.unwrap().digest, expected);
    assert_eq!(sum_digests::<G>([]).unwrap(), Digest::empty());
}

#[tokio::test]
async fn ristretto255_member_digests_sum_to_the_trees() { member_digests_sum_to_the_trees::<Ristretto255>().await }

#[tokio::test]
async fn lthash16_member_digests_sum_to_the_trees() { member_digests_sum_to_the_trees::<LtHash16>().await }

#[test]
fn a_member_digest_refuses_a_member_without_a_key() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);

    assert!(matches!(member_digest::<_, _, Ristretto255>(&spec, &item!(1, [], b"")), Err(Error::Key { .. })));
}
