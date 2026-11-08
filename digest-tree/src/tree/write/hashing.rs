//! Hashing many leaves without holding the executor: in chunks, a full chunk
//! across threads where there are threads, with a yield between chunks.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::DigestGroup;

/// How many leaves are hashed between yields to the executor: about 5 ms of
/// ristretto255 hashing on one thread.
pub(super) const LEAVES_PER_CHUNK: usize = 1024;

/// The elements of the leaves whose canonical encodings these are, in order.
/// On native targets a full chunk is hashed on rayon's global pool, which
/// the application configures; a smaller one would gain little from spreading
/// and takes the calling thread alone.
pub(super) fn hash_chunk<G: DigestGroup>(encodings: &[Vec<u8>]) -> Vec<G::Element> {
    #[cfg(not(target_arch = "wasm32"))]
    if encodings.len() >= LEAVES_PER_CHUNK {
        use rayon::prelude::*;

        return encodings.par_iter().map(|encoding| G::hash_leaf(encoding)).collect();
    }

    encodings.iter().map(|encoding| G::hash_leaf(encoding)).collect()
}

/// The elements of the leaves whose canonical encodings these are, in order,
/// hashed a chunk at a time with a yield between chunks.
pub(super) async fn hash_leaves<G: DigestGroup>(encodings: &[Vec<u8>]) -> Vec<G::Element> {
    let mut elements = Vec::with_capacity(encodings.len());
    for (index, chunk) in encodings.chunks(LEAVES_PER_CHUNK).enumerate() {
        if index > 0 {
            yield_now().await;
        }

        elements.extend(hash_chunk::<G>(chunk));
    }

    elements
}

/// Let the executor run other tasks before this one continues: a future that
/// wakes its task and is pending once. It needs no particular runtime.
pub(super) fn yield_now() -> impl Future<Output = ()> { YieldNow { yielded: false } }

struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }

        self.yielded = true;
        context.waker().wake_by_ref();

        Poll::Pending
    }
}

#[cfg(test)]
mod tests;
