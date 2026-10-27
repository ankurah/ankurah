use super::*;
use crate::fixtures::{asc, Item, KeySpec};
use crate::{DigestTree, LtHash16, MemoryStore, Ristretto255, Span};

/// The members' digests sum to the digest of a tree that holds them.
async fn member_digests_sum_to_the_trees<G: DigestGroup>() {
    let spec = KeySpec::new(vec![asc!("a")]);
    let members: Vec<Item> = (0..150).map(|n| Item::new(n, [Some(i64::from(n % 9))], &n.to_be_bytes())).collect();
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
    let member = Item::new(1, [], b"");
    assert!(matches!(member_digest::<_, _, Ristretto255>(&KeySpec::new(vec![asc!("a")]), &member), Err(Error::Key { .. })));
}
