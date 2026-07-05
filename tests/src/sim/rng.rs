//! The single seeded entropy source for the whole simulation.
//!
//! Every scheduling, fault, and topology decision draws from one `SimRng`
//! threaded explicitly through the scheduler. Nothing in a harness-controlled
//! path may call `rand::thread_rng`, read a wall clock, or otherwise draw
//! entropy outside this type: a second entropy source would make the
//! determinism audit (`Trace` hash equality across two runs of one seed)
//! meaningless. ChaCha8 is chosen for portable, reproducible output across
//! platforms and toolchains.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Deterministic RNG. Construct once per run from the scenario seed and thread
/// it explicitly; never clone it into a parallel path (that would fork the
/// stream and desynchronize the two would-be-identical runs).
pub struct SimRng {
    inner: ChaCha8Rng,
    seed: u64,
}

impl SimRng {
    pub fn new(seed: u64) -> Self { Self { inner: ChaCha8Rng::seed_from_u64(seed), seed } }

    pub fn seed(&self) -> u64 { self.seed }

    /// Uniform in `[0, n)`. Returns 0 for `n == 0` so callers need not special-case empty ranges.
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            self.inner.gen_range(0..n)
        }
    }

    /// Uniform in `[lo, hi]` inclusive.
    pub fn range_inclusive(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            lo
        } else {
            self.inner.gen_range(lo..=hi)
        }
    }

    /// True with probability `p` (clamped to `[0, 1]`).
    pub fn chance(&mut self, p: f64) -> bool { self.inner.gen_bool(p.clamp(0.0, 1.0)) }

    pub fn bool(&mut self) -> bool { self.inner.gen() }

    /// Pick an index into a non-empty slice; `None` if empty.
    pub fn choose_index<T>(&mut self, slice: &[T]) -> Option<usize> {
        if slice.is_empty() {
            None
        } else {
            Some(self.below(slice.len()))
        }
    }

    /// Deterministic Fisher-Yates shuffle in place. The harness uses this to
    /// permute delivery order rather than relying on any container's iteration
    /// order (HashMap order is randomized per process and would leak into the
    /// schedule).
    pub fn shuffle<T>(&mut self, slice: &mut [T]) {
        for i in (1..slice.len()).rev() {
            let j = self.below(i + 1);
            slice.swap(i, j);
        }
    }
}
