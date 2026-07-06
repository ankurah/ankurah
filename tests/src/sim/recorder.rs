//! Synchronous notification recorder for reactor/LiveQuery coherence (C5).
//!
//! The convergence invariants ([`super::invariants`]) check the final quiescent
//! state. C5 asserts the ordered notification stream a subscriber observes over
//! the run: the session guarantees (monotonic reads, read-your-writes) phrased
//! in Jepsen terms per design-delta C5C7-A. This module records that stream.
//!
//! Determinism constraint: the shared `common::TestWatcher` waits on a
//! `tokio::sync::Notify` and sleeps a wall-clock quiesce interval. Both would
//! break the harness, whose determinism rests on a single-threaded runtime with
//! no wall-clock timers and quiescence defined by message drain. The recorder
//! here therefore never waits: its listener is a plain `Fn(ChangeSet<R>)` that
//! appends to a buffer. Because [`super::scheduler::Scheduler::run_to_quiescence`]
//! drains the network and yields to spawned reactor/relay tasks before it
//! declares quiescence, the buffer is complete and deterministic once a
//! `settle()` or the final barrier returns. Scenarios read it directly then, with
//! no polling and no races.

use ankurah::changes::{ChangeKind, ChangeSet};
use ankurah::proto;
use ankurah::signals::Subscribe;
use std::sync::{Arc, Mutex};

use super::model::SimRecordView;

/// One recorded `ItemChange`, flattened to the fields a coherence assertion
/// needs: the membership/update kind, the entity, the two LWW field values as
/// the subscriber materialized them, and the event ids that drove the change.
#[derive(Debug, Clone)]
pub struct RecordedItem {
    pub kind: ChangeKind,
    pub entity: proto::EntityId,
    /// The `title` field as read from the notified view. `None` if the field
    /// getter errored (an unset field on a partially-initialized view), which is
    /// itself recorded rather than hidden.
    pub title: Option<String>,
    /// The `body` field as read from the notified view.
    pub body: Option<String>,
    /// Ids of the events attached to this change (empty for `Initial`, and for a
    /// no-op `Update` that carries no events: the shape the #299 suppression must
    /// prevent from ever reaching an established subscription).
    pub event_ids: Vec<proto::EventId>,
    /// The entity's head clock as the notified view carried it. The causal
    /// frontier of the state the subscriber observed at this notification. Used
    /// for the monotonic-reads guarantee: within one subscription's stream the
    /// head for an entity never drops an event it previously reflected.
    pub head: Vec<proto::EventId>,
}

impl RecordedItem {
    /// True for an `Update` carrying no events. This is exactly the spurious
    /// empty-events Update the #299 suppression exists to prevent. `Initial`
    /// legitimately carries no events, so it is excluded.
    pub fn is_empty_events_update(&self) -> bool { self.kind == ChangeKind::Update && self.event_ids.is_empty() }
}

/// One recorded changeset (the unit the subscriber receives per notification),
/// preserving the order of items within it and the changeset's own arrival
/// position in the stream.
#[derive(Debug, Clone)]
pub struct RecordedChangeSet {
    pub items: Vec<RecordedItem>,
}

/// Records the ordered `ChangeSet` stream a single `LiveQuery` observes. Holds
/// the `LiveQuery` and its subscription guard alive for the run so the relay
/// context and the listener are not torn down before quiescence.
pub struct SubscriptionRecorder {
    /// The logical node index the subscription was established on.
    pub node: usize,
    /// The predicate the subscription was registered with.
    pub predicate: String,
    buffer: Arc<Mutex<Vec<RecordedChangeSet>>>,
    query: ankurah::LiveQuery<SimRecordView>,
    _guard: ankurah::signals::SubscriptionGuard,
}

impl SubscriptionRecorder {
    /// Attach a recording listener to `query`. The listener runs synchronously on
    /// the harness runtime when the reactor broadcasts; it never waits.
    pub fn attach(node: usize, predicate: String, query: ankurah::LiveQuery<SimRecordView>) -> Self {
        let buffer: Arc<Mutex<Vec<RecordedChangeSet>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = buffer.clone();
        let guard = query.subscribe(move |changeset: ChangeSet<SimRecordView>| {
            let items = changeset
                .changes
                .iter()
                .map(|change| {
                    use ankurah::model::View;
                    let view = change.entity();
                    RecordedItem {
                        kind: change.kind(),
                        entity: view.id(),
                        title: view.title().ok(),
                        body: view.body().ok(),
                        event_ids: change.events().iter().map(|e| e.payload.id()).collect(),
                        head: view.entity().head().to_vec(),
                    }
                })
                .collect();
            sink.lock().unwrap().push(RecordedChangeSet { items });
        });
        Self { node, predicate, buffer, query, _guard: guard }
    }

    /// The recorded changesets in arrival order. Read after a `settle()` or the
    /// final quiescence barrier, when the stream is complete.
    pub fn changesets(&self) -> Vec<RecordedChangeSet> { self.buffer.lock().unwrap().clone() }

    /// Every recorded item, flattened across changesets, in stream order.
    pub fn items(&self) -> Vec<RecordedItem> { self.buffer.lock().unwrap().iter().flat_map(|cs| cs.items.iter().cloned()).collect() }

    /// The recorded items for one entity, in stream order.
    pub fn items_for(&self, entity: proto::EntityId) -> Vec<RecordedItem> {
        self.items().into_iter().filter(|i| i.entity == entity).collect()
    }

    /// The current resultset membership of the underlying query as entity ids,
    /// read at call time. Used to assert the final resultset agrees with the
    /// membership the change stream reported.
    pub fn resultset_ids(&self) -> Vec<proto::EntityId> {
        use ankurah::signals::Peek;
        self.query.resultset().peek().iter().map(|v| v.id()).collect()
    }
}
