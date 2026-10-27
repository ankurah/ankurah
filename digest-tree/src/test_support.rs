//! Helpers for other crates' tests: what the public API deliberately withholds.

use crate::digest::leaf_encoding;
use crate::member::member_key;
pub use crate::tree::Audit;
use crate::{Digest, DigestGroup, Error, Member, MemberId, Payload};

/// The digest of `member` alone under `spec`, as a tree sums it: for tests
/// that check a tree's digests against the members they expect it to hold.
/// A member that can't make its key is [`Error::Key`].
pub fn member_digest<K, M: Member<K>, G: DigestGroup>(spec: &K, member: &M) -> Result<Digest<G>, Error> {
    let key = member_key(spec, member)?;
    let element = G::hash_leaf(&leaf_encoding(&key, &member.id().to_bytes(), &member.payload().digest_bytes()));
    Ok(Digest::of_element(element))
}

/// The digest of the members of all these digests together, added to the
/// first in place as a tree adds to a node: summing two digests is one
/// addition. A count past `u64` is [`Error::Inconsistent`].
pub fn sum_digests<G: DigestGroup>(digests: impl IntoIterator<Item = Digest<G>>) -> Result<Digest<G>, Error> {
    let mut digests = digests.into_iter();
    let Some(mut sum) = digests.next() else { return Ok(Digest::empty()) };

    for digest in digests {
        sum.add(&digest)?;
    }

    Ok(sum)
}

#[cfg(test)]
mod tests;
