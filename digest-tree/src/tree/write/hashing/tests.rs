use super::*;
use crate::digest::leaf_encoding;
use crate::{LtHash16, Ristretto255};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Wake, Waker};

/// Counts the times its task is woken.
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::Relaxed); }
}

/// The yield is pending once, and wakes its task so that it runs again.
#[test]
fn a_yield_is_pending_once_and_wakes_its_task() {
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);

    let mut yielding = pin!(yield_now());
    assert!(yielding.as_mut().poll(&mut context).is_pending());
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1, "woken before returning pending");
    assert!(yielding.as_mut().poll(&mut context).is_ready());
}

/// Hashing across chunks, and across threads, gives each leaf the element
/// it hashes to alone, in order.
fn chunked_hashing_matches_hashing_one_by_one<G: crate::DigestGroup>() {
    let encodings: Vec<Vec<u8>> = (0..LEAVES_PER_CHUNK as u32 * 2 + 7).map(|n| leaf_encoding(&n.to_be_bytes(), b"id", b"")).collect();
    let mut hashing = pin!(hash_leaves::<G>(&encodings));

    let mut context = Context::from_waker(Waker::noop());
    let mut pending = 0;

    let elements = loop {
        match hashing.as_mut().poll(&mut context) {
            Poll::Ready(elements) => break elements,
            Poll::Pending => pending += 1,
        }
    };

    assert_eq!(pending, 2, "a yield between each two of three chunks");
    let one_by_one: Vec<G::Element> = encodings.iter().map(|encoding| G::hash_leaf(encoding)).collect();

    assert!(elements == one_by_one);
}

#[test]
fn ristretto255_chunked_hashing_matches_hashing_one_by_one() { chunked_hashing_matches_hashing_one_by_one::<Ristretto255>() }

#[test]
fn lthash16_chunked_hashing_matches_hashing_one_by_one() { chunked_hashing_matches_hashing_one_by_one::<LtHash16>() }
