//! ReactiveGraphObserver: low-road integration with Leptos / reactive_graph.
//!
//! This module provides an `Observer` implementation that forwards
//! `CurrentObserver::track` calls from Ankurah signals into the
//! `reactive_graph` dependency-tracking system.
//!
//! Design notes:
//! - This is the **low road** integration: we introduce a small surrogate
//!   `BridgeSource` type per Ankurah `BroadcastId` that will eventually
//!   implement `reactive_graph::graph::Source`, `ToAnySource`, and
//!   `ReactiveNode`.
//! - The goal is to allow Leptos components/effects to observe Ankurah
//!   signals *without* changing Ankurah's core signal traits or `Broadcast`
//!   internals.
//! - A future **high road** implementation might:
//!   - Add explicit subscribe / unsubscribe APIs to `Broadcast` tailored
//!     to reactive_graph.
//!   - Implement `Source` / `ToAnySource` more directly on a wrapper
//!     around `Broadcast`, reducing indirection.
//!   - Coordinate with `reactive_graph` maintainers on first-class support
//!     for foreign signal systems.
//!
//! For now, `ReactiveGraphObserver` is intentionally conservative: it wires
//! into the existing `Observer` stack and records which Ankurah signals
//! have been tracked, but leaves the concrete `Source` plumbing to later
//! iterations.

use crate::{ListenerGuard, Observer, Signal, broadcast::BroadcastId};
use reactive_graph::{
    owner::Owner,
    signal::ArcRwSignal,
    traits::{Notify, Track},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Observer that forwards `CurrentObserver::track` into reactive_graph.
///
/// This is intended to be pushed onto the `CurrentObserver` stack at
/// Leptos application initialization, e.g.:
///
/// ```ignore
/// use ankurah_signals::CurrentObserver;
/// use ankurah_signals::ReactiveGraphObserver;
///
/// let rg_observer = ReactiveGraphObserver::new();
/// CurrentObserver::set(rg_observer);
/// ```
///
/// When an Ankurah signal calls `CurrentObserver::track(&self)`, the
/// active `ReactiveGraphObserver`'s `observe` method is invoked.
pub struct ReactiveGraphObserver {
    /// Per-broadcast bridge sources.
    ///
    /// Each `BridgeSource` owns:
    /// - An Arc-backed reactive_graph signal (`ArcRwSignal<()>`) used as
    ///   the trackable Source from Leptos's point of view.
    /// - A `ListenerGuard` that connects the Ankurah `Signal`'s internal
    ///   `Broadcast` to that reactive_graph signal via `notify()`.
    bridges: Mutex<HashMap<BroadcastId, Arc<BridgeSource>>>,
    observer_id: usize,
}

impl ReactiveGraphObserver {
    /// Create a new `ReactiveGraphObserver`.
    ///
    /// The `observer_id` is derived from the address of the inner map,
    /// which is stable for the lifetime of the observer and sufficient
    /// for equality comparisons used by `CurrentObserver`.
    pub fn new() -> Self {
        let bridges = Mutex::new(HashMap::new());
        // Use the address of the mutex as a crude but stable id.
        let observer_id = &bridges as *const _ as usize;
        Self { bridges, observer_id }
    }
}

impl Observer for ReactiveGraphObserver {
    fn observe(&self, signal: &dyn Signal) {
        let id = signal.broadcast_id();
        if let Ok(mut map) = self.bridges.lock() {
            // Look up or create the bridge for this broadcast.
            let bridge = map.entry(id).or_insert_with(|| BridgeSource::new(id, signal)).clone();
            // Forward tracking into reactive_graph via the bridge's trigger.
            bridge.track();
        }
    }

    fn observer_id(&self) -> usize { self.observer_id }

    fn as_any(&self) -> &dyn std::any::Any { self }
}

/// Bridge state for a single Ankurah broadcast.
///
/// Low-road behavior (current):
/// - Keeps an `ArcRwSignal<()>` that acts as the reactive_graph `Source`.
/// - Listens to the Ankurah `Signal` and calls `notify()` on that
///   `ArcRwSignal<()>` whenever the Ankurah broadcast fires.
/// - When `track()` is called, forwards to `ArcRwSignal::track()` so the
///   current reactive_graph `Observer` (effect/memo/component) registers
///   a dependency on this broadcast.
///
/// High-road behavior (future):
/// - May grow additional per-subscriber bookkeeping or move closer to
///   `Broadcast` itself via new APIs on `Broadcast` and/or reactive_graph.
struct BridgeSource {
    broadcast_id: BroadcastId,
    /// Reactive_graph source used for tracking.
    trigger: ArcRwSignal<()>,
    /// Guard that keeps the Ankurah-side subscription alive.
    _guard: ListenerGuard,
}

impl BridgeSource {
    fn new(broadcast_id: BroadcastId, signal: &dyn Signal) -> Arc<Self> {
        // Arc-backed reactive_graph signal used purely for notification.
        let trigger = ArcRwSignal::new(());

        // Subscribe to the Ankurah signal; on each notification, mark the
        // reactive_graph signal as dirty so its subscribers re-run.
        let trigger_clone = trigger.clone();
        let guard = signal.listen(Arc::new(move |_| {
            trigger_clone.notify();
        }));

        Arc::new(Self { broadcast_id, trigger, _guard: guard })
    }

    /// Track this broadcast in the current reactive_graph observer.
    fn track(&self) {
        // Only track if there's an active Owner (reactive context).
        // This avoids warnings when Ankurah internal code calls track()
        // outside of a Leptos component/effect (e.g., during transaction commits).
        if Owner::current().is_some() {
            // `ArcRwSignal` implements the `Track` blanket impl (via Source,
            // ToAnySource, DefinedAt), so this wires the current
            // `reactive_graph::Observer` to this broadcast.
            self.trigger.track();
        }
    }
}
