//! Helpers the crate's unit tests share, beside the members they share with
//! the top-level tests (`crate::common`).

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use crate::digest::leaf_encoding;
use crate::DigestGroup;

/// The element in `G` of the leaf of this key, id and payload.
pub(crate) fn leaf_element<G: DigestGroup>(key: &[u8], id: &[u8], payload: &[u8]) -> G::Element {
    G::hash_leaf(&leaf_encoding(key, id, payload))
}

/// Run `future` to completion on the calling thread, for property tests,
/// which aren't async. Over a memory store every poll makes progress: the
/// store never waits, an uncontended lock is taken at once, and a yield wakes
/// its task before returning.
pub(crate) fn block_on<T>(future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
    }
}

/// Bytes as lowercase hexadecimal.
pub(crate) fn to_hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

/// The bytes these hexadecimal digits spell, spaces between them ignored.
pub(crate) fn from_hex(digits: &str) -> Vec<u8> {
    let digits: Vec<u8> = digits.bytes().filter(|digit| *digit != b' ').collect();
    digits.chunks(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}
