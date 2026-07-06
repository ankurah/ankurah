//! Counting global allocator for the event-DAG-shape tier memory measurements
//! (E-shapes).
//!
//! This is the sanctioned methodology for the event-DAG-shape scale tier: a
//! small test-only wrapper around the system allocator that tracks
//! currently-live bytes and the peak live-bytes high-water mark, so a scenario
//! can read peak heap use without reaching for process-level RSS (which is
//! noisy, includes the runtime and the sled page cache, and is not attributable
//! to a single scenario). The counters are process-global because a
//! `#[global_allocator]` is process-global; a scenario therefore measures a
//! delta and a peak across a `reset()` boundary, not an absolute.
//!
//! It is installed as the `#[global_allocator]` ONLY in the
//! `sim_event_dag_shapes` integration-test binary (each integration test is its
//! own binary), so the per-allocation atomic bookkeeping perturbs nothing else
//! in `cargo test -p ankurah-tests`.
//!
//! Accounting is deliberately simple and honest about its limits:
//!
//! - `live` is incremented on `alloc`/`alloc_zeroed`, adjusted by the signed
//!   size change on `realloc`, and decremented on `dealloc`. It is the number
//!   of bytes the program has requested and not yet freed, NOT the number of
//!   bytes the OS has mapped (allocator overhead, arena retention, and
//!   fragmentation are invisible here, by design).
//! - `peak` is the maximum value `live` reached. It is updated on every growth
//!   with a compare-and-swap loop, so it is exact for this allocator's view even
//!   under concurrent allocation, though the volume scenarios run single
//!   threaded.
//! - Relaxed ordering is sufficient: the counters are statistics, not a
//!   synchronization mechanism, and the values that matter are read after the
//!   allocating work has fully completed and joined.
//!
//! The E-A / 271-D caveat governs interpretation, not the mechanism: a
//! sequence-CRDT tombstone set is lower-bounded by deletion history, so a
//! scenario that accumulates deletions will show peak growth that is a property
//! of the data model (folded only by sealing), not a leak. The event-DAG-shape
//! scenarios avoid deletions for exactly this reason and their bounds are
//! documented as order-of-magnitude sanity checks, never tight thresholds.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A pass-through allocator that counts live and peak bytes.
pub struct CountingAlloc;

/// Currently-live requested bytes (allocated and not yet freed).
static LIVE: AtomicUsize = AtomicUsize::new(0);
/// High-water mark of `LIVE` since the last `reset`.
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Raise `PEAK` to at least `value` with a lock-free CAS loop.
#[inline]
fn bump_peak(value: usize) {
    let mut peak = PEAK.load(Ordering::Relaxed);
    while value > peak {
        match PEAK.compare_exchange_weak(peak, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

#[inline]
fn record_growth(bytes: usize) {
    let new_live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    bump_peak(new_live);
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            record_growth(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            record_growth(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            let old = layout.size();
            if new_size >= old {
                record_growth(new_size - old);
            } else {
                LIVE.fetch_sub(old - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

/// Currently-live requested bytes.
pub fn live_bytes() -> usize { LIVE.load(Ordering::Relaxed) }

/// Peak live-bytes high-water mark since the last `reset_peak`.
pub fn peak_bytes() -> usize { PEAK.load(Ordering::Relaxed) }

/// Reset the peak to the current live value. Call at the start of a measured
/// region so `peak_bytes()` afterward reflects that region's high-water mark
/// relative to the memory already live at entry (the fixed harness overhead of
/// built nodes, the runtime, and the sled engines).
pub fn reset_peak() { PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed); }

/// A snapshot of the live baseline at the start of a measured region and the
/// peak reached during it, so a scenario can report peak-above-baseline.
pub struct MemScope {
    baseline: usize,
}

impl MemScope {
    /// Begin measuring: capture the current live bytes as the baseline and reset
    /// the peak to it.
    pub fn begin() -> Self {
        let baseline = live_bytes();
        reset_peak();
        Self { baseline }
    }

    /// Live bytes captured at `begin`.
    pub fn baseline(&self) -> usize { self.baseline }

    /// Peak live bytes reached since `begin`.
    pub fn peak(&self) -> usize { peak_bytes() }

    /// Peak growth above the baseline (saturating; never negative).
    pub fn peak_above_baseline(&self) -> usize { peak_bytes().saturating_sub(self.baseline) }
}
