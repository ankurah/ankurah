## Leptos / reactive_graph Integration Questions

This document captures open questions and potential API contracts that could make the
Ankurah–Leptos integration more fluent, efficient, and idiomatic in the future.

### For ankurah-signals maintainers

- **Broadcast subscription API shape**

  - Today `Broadcast::listen` returns a `ListenerGuard` whose `Drop` removes the listener.
  - For reactive_graph integration we sometimes want explicit subscribe/unsubscribe keyed
    by an external ID (e.g., `AnySubscriber.0`) rather than purely RAII.
  - Question: would it be reasonable to add a non-RAII API like:
    - `fn listen_with_id(&self, listener: impl IntoBroadcastListener<T>) -> usize`
    - `fn remove_listener(&self, id: usize)`
    - while keeping the existing `listen`+`ListenerGuard` unchanged?

- **Exposing broadcast identity and mapping**

  - We currently use `BroadcastId` based on the internal `Arc` address.
  - For more advanced bridging, it might be useful to have a stable mapping:
    - `Signal` → underlying `Broadcast<T>` (or at least a reference type that allows
      explicit subscription management).
  - Question: do we want an internal helper that, given `&dyn Signal`, can reliably
    reach the underlying `Broadcast<()>` (for `Signal::listen`), or is the current
    `listen`/`broadcast_id` contract the intended boundary?

- **Source-like abstraction on top of Broadcast**

  - Right now, our `BridgeSource` wraps an Ankurah `Signal` by:
    - Calling `Signal::listen` and closing over a reactive_graph node (`ArcRwSignal<()>`).
    - Using `ArcRwSignal::track()` to connect into reactive_graph.
  - Question: should `ankurah-signals` grow a small, generic "Source" abstraction over
    `Broadcast` (e.g., `BroadcastSource`) that owns a `Broadcast<()>` and a map of
    opaque subscriber handles, so integrations like reactive_graph don’t have to
    re-implement this pattern?

- **Feature boundary**
  - We currently hide the reactive_graph integration behind a `reactive-graph` feature
    in `ankurah-signals`.
  - Question: is this the right boundary, or should there eventually be a dedicated
    `ankurah-signals-leptos`/`ankurah-signals-reactive` crate that depends on both
    `ankurah-signals` and `reactive_graph` and owns all bridging code?

### For Leptos / reactive_graph maintainers

- **Foreign Source integration hooks**

  - Today, the only way to make a type work with `Track::track` is to implement
    `Source + ToAnySource + DefinedAt + IsDisposed`, which requires:
    - Storing a `SubscriberSet` internally, and
    - Exposing a `Weak<dyn Source + Send + Sync>` via `AnySource`.
  - Question: would you consider a lighter-weight integration hook for _foreign_
    signal systems that already manage their own subscribers (e.g., a trait like:
    ```rust
    pub trait ExternalSource {
        fn track_external(&self, subscriber: AnySubscriber);
    }
    ```
    that `Track::track` could call instead of requiring a full `Source` impl)?

- **Type-erased Source without Weak**

  - `AnySource` currently stores `Weak<dyn Source + Send + Sync>`, which assumes the
    actual source node is Arc-backed and internally manages subscribers.
  - For external systems that already own subscriber storage, it would be convenient
    to have a "pure token" form (e.g., `AnySourceId(usize)`) that:
    - Is stable and hashable, and
    - Is only used on the subscriber side for dependency bookkeeping, without
      requiring reactive_graph to be able to call back into the Source.
  - Question: would you consider splitting `AnySource` into:
    - `AnySourceId` (for subscriber bookkeeping and diagnostics), and
    - `AnySourceNode` (with the full `Weak<dyn Source>` pointer),
      so that foreign integrations can participate in `add_source` without
      having to provide a reactive_graph-native `Source`?

- **Observer / Owner customization points**

  - `Track::track` currently calls `Observer::get()` and then:
    - `subscriber.add_source(self.to_any_source())`
    - `self.add_subscriber(subscriber)`
  - For foreign systems, we often want to:
    - Register "I depend on this external signal" with the subscriber, but
    - Have the external signal itself manage the list of subscribers and call
      `mark_dirty` when it changes.
  - Question: would you consider exposing a variant of `Track` or a helper API that
    only does `subscriber.add_source(...)` and leaves `add_subscriber` up to the
    external system?

- **Notify vs. mark_dirty semantics**

  - For our low-road integration we’re using `ArcRwSignal<()>` + `Notify::notify()`
    to mark subscribers dirty when an Ankurah broadcast fires.
  - Question: is `Notify::notify()` the intended, stable way to externally "poke"
    a reactive_graph node, or should external systems prefer `mark_dirty` /
    `mark_check` on a `ReactiveNode` interface instead?

- **Documentation for foreign signal bridging**
  - The current docs explain how Leptos/Reactive Graph works internally, but there
    isn’t a dedicated section on "how to integrate an external signal system."
  - Question: would you be open to:
    - Adding a short guide that explains the required traits (`Track`, `Source`,
      `ToAnySource`, `Notify`, etc.), and
      the minimal responsibilities for a foreign signal bridge?
    - Clarifying which internal details (like `SubscriberSet`) are stable vs.
      subject to change, so integration crates know what to rely on?
