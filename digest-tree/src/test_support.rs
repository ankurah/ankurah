//! Helpers for other crates' tests: what the public API deliberately
//! withholds, and the store contract's conformance suite.

pub mod store_conformance;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use ankurah_core_types::key_spec::KeySpec;

use crate::digest::leaf_encoding;
use crate::key;
use crate::store::NodeRow;
pub use crate::tree::Audit;
use crate::{Digest, DigestGroup, Error, Member, MemberId, Payload, Row};

/// The key a tree makes for `member` under `spec`: for tests that place
/// members by their addresses ([`leaf_address`](crate::leaf_address)). A
/// member without a key under `spec` is refused as a tree refuses it.
pub fn member_key<K, M: Member<K>>(spec: &KeySpec<K>, member: &M) -> Result<Vec<u8>, Error> { key::member_key(spec, member) }

/// The digest of `member` alone under `spec`, as a tree sums it: for tests
/// that check a tree's digests against the members they expect it to hold.
/// A member without a key under `spec` is refused as a tree refuses it.
pub fn member_digest<K, M: Member<K>, G: DigestGroup>(spec: &KeySpec<K>, member: &M) -> Result<Digest<G>, Error> {
    let key = key::member_key(spec, member)?;
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

/// The row of a bucket whose members' digest is `digest`, as a tree keeps
/// it: for benchmarks of what a node's row costs to encode and decode, and
/// for tests that write rows behind a tree's back.
pub fn bucket_row<K, L, G: DigestGroup>(digest: Digest<G>) -> Row<K, L, G> { Row::Node(NodeRow { digest, branches_at: None }) }

/// Poll `future` once, with a waker that does nothing: whether it is ready
/// yet, without waiting for it, for tests that watch a call wait.
pub fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> { future.poll(&mut Context::from_waker(Waker::noop())) }

/// The digest a node's row holds, or `None` for a row of another kind.
pub fn node_digest<K, L, G: DigestGroup>(row: &Row<K, L, G>) -> Option<Digest<G>> {
    match row {
        Row::Node(node) => Some(node.digest),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
