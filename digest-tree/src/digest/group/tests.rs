use crate::digest::{leaf_encoding, DecodeError, DigestGroup, LtHash16, Ristretto255};

fn element<G: DigestGroup>(n: u8) -> G::Element { G::hash_leaf(&leaf_encoding(&[n], &[n; 16], &[n; 8])) }

fn sum<G: DigestGroup>(a: G::Element, b: G::Element) -> G::Element {
    let mut sum = a;
    G::add(&mut sum, &b);
    sum
}

/// What the operations promise, for one group: a leaf hashes to one
/// element and different leaves to different elements; addition has zero
/// for its identity, commutes and associates; subtraction undoes it; and
/// every element decodes from its encoding, which no other length passes
/// for.
fn the_operations_keep_their_contract<G: DigestGroup>() {
    let (a, b, c) = (element::<G>(1), element::<G>(2), element::<G>(3));
    assert_eq!(element::<G>(1), a, "a leaf hashes to one element");
    assert_ne!(a, b, "different leaves hash to different elements");

    assert_eq!(sum::<G>(a, G::zero()), a, "zero is the identity");
    assert_eq!(sum::<G>(a, b), sum::<G>(b, a), "addition commutes");
    assert_eq!(sum::<G>(sum::<G>(a, b), c), sum::<G>(a, sum::<G>(b, c)), "addition associates");

    let mut difference = sum::<G>(a, b);
    G::sub(&mut difference, &b);
    assert_eq!(difference, a, "subtraction undoes addition");

    let mut nothing = a;
    G::sub(&mut nothing, &a);
    assert_eq!(nothing, G::zero(), "an element minus itself is zero");

    for element in [G::zero(), a, sum::<G>(a, b)] {
        let mut encoding = vec![0u8; G::ELEMENT_LEN];
        G::encode(&element, &mut encoding);

        assert_eq!(G::decode(&encoding), Ok(element), "an element decodes from its encoding");
        let longer = [&encoding[..], &[0]].concat();

        for wrong in [&encoding[1..], &longer[..]] {
            assert_eq!(G::decode(wrong), Err(DecodeError::Length { expected: G::ELEMENT_LEN, found: wrong.len() }));
        }
    }
}

#[test]
fn ristretto255_keeps_the_contract() { the_operations_keep_their_contract::<Ristretto255>() }

#[test]
fn lthash16_keeps_the_contract() { the_operations_keep_their_contract::<LtHash16>() }

/// A store's tree opens only in the group it was summed in, which the
/// group's identifier tells.
#[test]
fn the_groups_have_distinct_identifiers() {
    fn identifier<G: DigestGroup>() -> u8 { G::ROW_VERSION }

    assert_ne!(identifier::<Ristretto255>(), identifier::<LtHash16>());
}
