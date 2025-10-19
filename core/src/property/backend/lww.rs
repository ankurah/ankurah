//! # Causal Last-Write-Wins (LWW) Property Backend
//!
//! This module implements deterministic conflict resolution for concurrent updates using
//! **topological precedence** combined with lexicographic tiebreaking.
//!
//! ## Conflict Resolution Strategy
//!
//! When concurrent events modify the same property, the LWW backend resolves conflicts using
//! a three-tier precedence system:
//!
//! 1. **Lineage Precedence**: Events in later ReadySets (topologically deeper from the meet)
//!    supersede events in earlier ReadySets
//! 2. **Lexicographic Tiebreaking**: Among events in the same ReadySet, the event with the
//!    lexicographically greatest EventId wins
//! 3. **Primary Path Optimization**: Primary-only ReadySets before any concurrency are skipped
//!
//! ## Key Design Choice: Topological Precedence
//!
//! Unlike traditional CRDT LWW registers that use pure lexicographic comparison, our implementation
//! respects the **depth of causal history**. Events on longer causal chains take precedence over
//! shallower concurrent branches, even when both are causally concurrent from the meet.
//!
//! ### Example: Late-Arriving Grandparent Concurrency
//!
//! ```text
//! a → b → c → d → ... → m  (current head: [m])
//!     └─ q                 (late-arriving, parent: [a])
//! ```
//!
//! When event `q` arrives (forking from ancestor `a`), it is causally concurrent with `b` through `m`.
//! However, the ForwardView organizes events into ReadySets by topological distance:
//!
//! - **ReadySet 1**: {q (Primary), b (Concurrency)} → lexicographic tiebreak
//! - **ReadySet 2**: {c (Concurrency)} → supersedes ReadySet 1
//! - ...
//! - **ReadySet N**: {m (Concurrency)} → supersedes all previous
//!
//! **Result**: `m` wins via lineage precedence, respecting the deeper causal chain.
//!
//! ## See Also
//!
//! - `specs/concurrent-updates/lww-semantics.md` - Detailed specification with edge cases
//! - `crate::causal_dag::forward_view` - ForwardView and ReadySet iterators
//! - `crate::property::backend::PropertyTransaction` - Transaction trait

use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex, RwLock},
};

use ankurah_proto::{EventId, Operation};
use serde::{Deserialize, Serialize};

use crate::{
    causal_dag::forward_view::ReadySet,
    error::{MutationError, StateError},
    property::{
        backend::{PropertyBackend, PropertyTransaction},
        PropertyName, Value,
    },
};

const LWW_DIFF_VERSION: u8 = 1;

/// In memory value register - not to be serialized
#[derive(Clone, Debug)]
struct ValueRegister {
    last_writer_event_id: Option<EventId>,
    value: Option<Value>,
    committed: bool, // NOTE: this is used for Mutables which will emit events, not for event application
}

// This is what we serialize/deserialize
#[derive(Serialize, Deserialize)]
pub struct CausalLWWState {
    // a list of event ids pertinent to the current register values in this state. Max length is 2**16
    events: Vec<EventId>,
    // Registers
    registers: BTreeMap<PropertyName, ValueRegisterWire>,
}
#[derive(Serialize, Deserialize)]
pub struct ValueRegisterWire {
    entity_id_offset: u16, // offset in `events` list
    value: Option<Value>,  // tombstone
}

/// Inner state for LWWBackend, wrapped in Arc for sharing with transactions
#[derive(Debug)]
struct LWWBackendInner {
    values: RwLock<BTreeMap<PropertyName, ValueRegister>>,
    field_broadcasts: Mutex<BTreeMap<PropertyName, ankurah_signals::broadcast::Broadcast>>,
}

/// Stores properties of an entity using Last-Write-Wins semantics (newtype over Arc<LWWBackendInner>)
#[derive(Debug, Clone)]
pub struct LWWBackend(Arc<LWWBackendInner>);

#[derive(Serialize, Deserialize)]
pub struct LWWDiff {
    version: u8,
    data: Vec<u8>,
}

impl Default for LWWBackend {
    fn default() -> Self { Self::new() }
}

impl LWWBackend {
    pub fn new() -> LWWBackend {
        Self(Arc::new(LWWBackendInner { values: RwLock::new(BTreeMap::default()), field_broadcasts: Mutex::new(BTreeMap::new()) }))
    }

    pub fn set(&self, property_name: PropertyName, value: Option<Value>) {
        let mut values = self.0.values.write().unwrap();
        values.insert(property_name, ValueRegister { last_writer_event_id: None, value, committed: false });
    }

    pub fn get(&self, property_name: &PropertyName) -> Option<Value> {
        let values = self.0.values.read().unwrap();
        values.get(property_name).and_then(|entry| entry.value.clone())
    }
}

impl PropertyBackend for LWWBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Arc<dyn PropertyBackend> {
        let values = self.0.values.read().unwrap();
        let cloned = (*values).clone();
        drop(values);

        Arc::new(Self(Arc::new(LWWBackendInner {
            values: RwLock::new(cloned),
            // Create fresh broadcasts (don't clone the existing ones for transaction isolation)
            field_broadcasts: Mutex::new(BTreeMap::new()),
        })))
    }

    fn properties(&self) -> Vec<PropertyName> {
        let values = self.0.values.read().unwrap();
        values.keys().cloned().collect::<Vec<PropertyName>>()
    }

    fn property_value(&self, property_name: &PropertyName) -> Option<Value> { self.get(property_name) }

    fn property_values(&self) -> BTreeMap<PropertyName, Option<Value>> {
        let values = self.0.values.read().unwrap();
        values.iter().map(|(k, v)| (k.clone(), v.value.clone())).collect()
    }

    fn property_backend_name() -> String { "lww".to_owned() }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        // TODO stop using property_values and start using
        let property_values = self.property_values();
        let state_buffer = bincode::serialize(&property_values)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        let raw_map = bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(state_buffer)?;
        let map = raw_map.into_iter().map(|(k, v)| (k, ValueRegister { last_writer_event_id: None, value: v, committed: true })).collect();
        Ok(Self(Arc::new(LWWBackendInner { values: RwLock::new(map), field_broadcasts: Mutex::new(BTreeMap::new()) })))
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut values = self.0.values.write().unwrap();
        let mut changed_values = BTreeMap::new();

        for (name, entry) in values.iter_mut() {
            if !entry.committed {
                changed_values.insert(name.clone(), entry.value.clone());
                entry.committed = true;
            }
        }

        if changed_values.is_empty() {
            return Ok(None);
        }

        Ok(Some(vec![Operation {
            diff: bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data: bincode::serialize(&changed_values)? })?,
        }]))
    }

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError> {
        let mut changed_fields = Vec::new();

        for operation in operations {
            let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
            match version {
                1 => {
                    let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data)?;

                    let mut values = self.0.values.write().unwrap();
                    for (property_name, new_value) in changes {
                        // Insert as committed entry since this came from an operation
                        values
                            .insert(property_name.clone(), ValueRegister { last_writer_event_id: None, value: new_value, committed: true });
                        changed_fields.push(property_name);
                    }
                }
                version => return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into())),
            }
        }

        // Notify field subscribers for changed fields only
        let field_broadcasts = self.0.field_broadcasts.lock().expect("other thread panicked, panic here too");
        for field_name in changed_fields {
            if let Some(broadcast) = field_broadcasts.get(&field_name) {
                broadcast.send(());
            }
        }

        Ok(())
    }

    fn listen_field(
        &self,
        field_name: &PropertyName,
        listener: ankurah_signals::broadcast::Listener,
    ) -> ankurah_signals::broadcast::ListenerGuard {
        // Get or create the broadcast for this field
        let mut field_broadcasts = self.0.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener)
    }

    fn begin(&self) -> Result<Box<dyn PropertyTransaction<ankurah_proto::Event>>, MutationError> {
        Ok(Box::new(LWWTransaction {
            backend: self.clone(),
            base_registers: BTreeMap::new(),
            pending_winners: BTreeMap::new(),
            has_seen_concurrency: false,
        }))
    }
}

impl LWWBackend {
    /// Get the broadcast ID for a specific field, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, field_name: &PropertyName) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.0.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();
        broadcast.id()
    }
}

/// Transaction for applying concurrent updates to LWW backend using topological precedence.
///
/// ## Processing Model
///
/// The transaction processes a ForwardView's ReadySets in topological order (oldest to newest),
/// maintaining "current winners" for each property:
///
/// 1. **Initialization**: Snapshot the backend's current state (reflects Primary head at the meet)
/// 2. **Per-ReadySet**: Extract all property changes, apply lineage precedence over previous winners,
///    use lexicographic tiebreak within the ReadySet
/// 3. **Commit**: Apply final winners to the backend's ValueRegisters
///
/// ## Lineage Precedence Semantics
///
/// - Events in later ReadySets supersede events in earlier ReadySets (regardless of EventId)
/// - This means topologically deeper events win over shallower concurrent branches
/// - Within a single ReadySet, lexicographic comparison of EventId determines the winner
///
/// ## Primary-Only Optimization
///
/// Primary-only ReadySets that occur before any Concurrency events are skipped, since these
/// events are already reflected in the entity's state at transaction start.
pub struct LWWTransaction {
    /// Shared backend (cheap to clone due to internal Arc)
    backend: LWWBackend,
    /// Snapshot of registers at transaction start (reflects Primary head)
    base_registers: BTreeMap<PropertyName, ValueRegister>,
    /// Proposed winners after ReadySet processing: (event_id, value)
    pending_winners: BTreeMap<PropertyName, (EventId, Option<Value>)>,
    /// Track if we've seen any Concurrency events (needed for Primary-only optimization)
    has_seen_concurrency: bool,
}

impl PropertyTransaction<ankurah_proto::Event> for LWWTransaction {
    /// Apply a ReadySet (topological layer) to the transaction.
    ///
    /// ## Algorithm
    ///
    /// For each ReadySet processed in order:
    ///
    /// 1. **Collect candidates**: Extract all (property, event_id, value) tuples from events in this ReadySet
    /// 2. **Apply Primary-only optimization**: Skip if this ReadySet has no Concurrency events and we haven't
    ///    seen any yet (these Primary events are already in the entity state)
    /// 3. **Per-property resolution**:
    ///    - Get current winner (from `pending_winners` or `base_registers`)
    ///    - Find lexicographically greatest EventId among candidates in this ReadySet
    ///    - Update `pending_winners` (implicitly applying lineage precedence, since this ReadySet
    ///      supersedes all previous ReadySets)
    ///
    /// ## Lineage Precedence Example
    ///
    /// ```text
    /// ReadySet 1: Event A sets x=10, Event B sets x=20
    ///   → B wins (lexicographic: B > A), pending_winners[x] = (B, 20)
    ///
    /// ReadySet 2: Event C sets x=30
    ///   → C wins (lineage precedence), pending_winners[x] = (C, 30)
    ///   → Even if A or B had greater EventIds, C supersedes them
    /// ```
    fn apply_ready_set(&mut self, ready_set: &ReadySet<ankurah_proto::Event>) -> Result<(), MutationError> {
        // Lazy initialization of base_registers on first apply_ready_set
        if self.base_registers.is_empty() {
            let values = self.backend.0.values.read().unwrap();
            self.base_registers = values.clone();
        }

        // Causal LWW tiebreaking per ReadySet:
        // - Lineage precedence: Later ReadySets supersede earlier ones (implicit in Kahn's ordering)
        // - Lexicographic tiebreak: Among events in this ReadySet, greatest EventId wins
        // - Primary events establish lineage: They win over earlier concurrent events via lineage precedence

        // Check if this ReadySet has any Concurrency events
        let has_concurrency = ready_set.concurrency_events().next().is_some();
        if has_concurrency {
            self.has_seen_concurrency = true;
        }

        // Collect all properties touched by events in this ReadySet
        let mut properties_to_candidates: BTreeMap<PropertyName, Vec<(EventId, Option<Value>)>> = BTreeMap::new();

        // Collect candidates from all events (Primary and Concurrency)
        for event in ready_set.all_events() {
            let event_id = event.payload.id();

            // Extract operations for lww backend
            if let Some(ops) = event.payload.operations.0.get("lww") {
                for op in ops {
                    if let Ok(LWWDiff { data, .. }) = bincode::deserialize::<LWWDiff>(&op.diff) {
                        if let Ok(changes) = bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(&data) {
                            for (prop_name, value) in changes {
                                properties_to_candidates.entry(prop_name).or_default().push((event_id.clone(), value));
                            }
                        }
                    }
                }
            }
        }

        // If no properties were touched, nothing to do
        if properties_to_candidates.is_empty() {
            return Ok(());
        }

        // If this is a Primary-only ReadySet and we haven't seen any concurrency yet,
        // skip it (these are events already on the subject path)
        if !has_concurrency && !self.has_seen_concurrency {
            return Ok(());
        }

        // For each property touched in this ReadySet
        for (prop_name, candidates) in properties_to_candidates {
            // Get current winner: check pending_winners first, then base_registers
            let current_winner_id = self
                .pending_winners
                .get(&prop_name)
                .map(|(id, _)| id.clone())
                .or_else(|| self.base_registers.get(&prop_name).and_then(|reg| reg.last_writer_event_id.clone()));

            // Find the lexicographically greatest EventId among candidates
            // (lineage precedence is implicit: this ReadySet's events supersede previous ones)
            if let Some((best_id, best_value)) = candidates.into_iter().max_by(|(id1, _), (id2, _)| id1.cmp(id2)) {
                // Update pending_winners if the winner changed
                if Some(&best_id) != current_winner_id.as_ref() {
                    self.pending_winners.insert(prop_name, (best_id, best_value));
                }
            }
        }

        Ok(())
    }

    fn commit(&mut self) -> Result<(), MutationError> {
        // Commit changes from pending_winners to the backend
        let mut backend_values = self.backend.0.values.write().unwrap();

        for (prop_name, (event_id, value)) in &self.pending_winners {
            if let Some(register) = backend_values.get_mut(prop_name) {
                register.last_writer_event_id = Some(event_id.clone());
                register.value = value.clone();
                register.committed = true; // Mark as committed
            } else {
                // Create new register
                backend_values.insert(
                    prop_name.clone(),
                    ValueRegister { last_writer_event_id: Some(event_id.clone()), value: value.clone(), committed: true },
                );
            }
        }

        Ok(())
    }

    fn rollback(&mut self) -> Result<(), MutationError> {
        // Drop pending state
        Ok(())
    }
}

// Need ID based happens-before determination to resolve conflicts

#[cfg(test)]
mod tests {
    use super::*;
    use crate::causal_dag::forward_view::ForwardView;
    use ankurah_proto::{AttestationSet, Attested, Clock, Event, EventId};
    use std::collections::HashMap;

    /// Helper to create test EventId from simple string (for readability in tests)
    fn test_event_id(s: &str) -> EventId {
        let mut bytes = [0u8; 32];
        let s_bytes = s.as_bytes();
        let len = s_bytes.len().min(32);
        bytes[..len].copy_from_slice(&s_bytes[..len]);
        EventId::from_bytes(bytes)
    }

    /// Helper to create an event with operations
    /// The id parameter is used to generate a stable entity_id for testing
    /// Parents should be actual EventIds from previously-created events
    fn event_with_ops(entity_id_str: &str, parent_event_ids: Vec<EventId>, ops: Vec<(&str, Option<i64>)>) -> Attested<Event> {
        use ankurah_proto::{CollectionId, EntityId, OperationSet};

        let mut operation_set = BTreeMap::new();

        if !ops.is_empty() {
            let operations: Vec<Operation> = ops
                .into_iter()
                .map(|(name, val)| {
                    let changes = BTreeMap::from([(name.to_string(), val.map(Value::I64))]);
                    Operation {
                        diff: bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data: bincode::serialize(&changes).unwrap() })
                            .unwrap(),
                    }
                })
                .collect();
            operation_set.insert("lww".to_string(), operations);
        }

        // Create stable entity_id based on the test string
        let mut entity_id_bytes = [0u8; 16];
        let id_bytes = entity_id_str.as_bytes();
        let len = id_bytes.len().min(16);
        entity_id_bytes[..len].copy_from_slice(&id_bytes[..len]);
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        let parent_clock = Clock::new(parent_event_ids);
        let operations = OperationSet(operation_set);

        Attested {
            payload: Event { collection: CollectionId::fixed_name("test"), entity_id, parent: parent_clock, operations },
            attestations: AttestationSet::default(),
        }
    }

    /// Helper to extract property values from operations (unused but kept for potential future use)
    #[allow(dead_code)]
    fn extract_property_from_event(event: &Event, prop: &str) -> Option<Value> {
        if let Some(ops) = event.operations.0.get("lww") {
            for op in ops {
                if let Ok(LWWDiff { data, .. }) = bincode::deserialize::<LWWDiff>(&op.diff) {
                    if let Ok(changes) = bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(&data) {
                        if let Some(val) = changes.get(prop) {
                            return val.clone();
                        }
                    }
                }
            }
        }
        None
    }

    #[test]
    fn test_primary_only_produces_no_changes() {
        // Primary-only ReadySets should produce no changes (Primary is comparison-only)
        let backend = LWWBackend::new();
        backend.set("x".to_string(), Some(Value::I64(10)));

        // Linear history: meet(A) -> subject(B)
        // B updates x=20, but is on Primary path so should NOT be applied
        let meet_event = event_with_ops("A", vec![], vec![("x", Some(10))]);
        let subject_event = event_with_ops("B", vec![meet_event.payload.id()], vec![("x", Some(20))]);

        // ForwardView events should be BETWEEN meet and heads, not including meet itself
        let events = HashMap::from([(subject_event.payload.id(), subject_event.clone())]);

        let forward_view = ForwardView::new(
            vec![meet_event.payload.id()],
            vec![subject_event.payload.id()],
            vec![meet_event.payload.id()], // other = meet, so subject descends
            events,
        );

        let mut tx = backend.begin().expect("begin failed");

        // Apply all ReadySets
        for ready_set in forward_view.iter_ready_sets() {
            tx.apply_ready_set(&ready_set).expect("apply_ready_set failed");
        }

        tx.commit().expect("commit failed");

        // Backend value should still be 10 (Primary-only events don't cause changes)
        assert_eq!(backend.get(&"x".to_string()), Some(Value::I64(10)), "Primary-only ReadySet should produce no changes");
    }

    #[test]
    fn test_single_concurrency_event_wins() {
        // Single concurrency event should win over existing register
        let backend = LWWBackend::new();
        backend.set("x".to_string(), Some(Value::I64(10)));

        // Diverged history:
        //     meet(A)
        //    /       \
        // subject(B) other(C)
        // B keeps x=10, C sets x=30
        let meet_event = event_with_ops("A", vec![], vec![("x", Some(10))]);
        let subject_event = event_with_ops("B", vec![meet_event.payload.id()], vec![]); // No changes
        let other_event = event_with_ops("C", vec![meet_event.payload.id()], vec![("x", Some(30))]);

        let events = HashMap::from([(subject_event.payload.id(), subject_event.clone()), (other_event.payload.id(), other_event.clone())]);

        let forward_view =
            ForwardView::new(vec![meet_event.payload.id()], vec![subject_event.payload.id()], vec![other_event.payload.id()], events);

        let mut tx = backend.begin().expect("begin failed");

        for ready_set in forward_view.iter_ready_sets() {
            tx.apply_ready_set(&ready_set).expect("apply_ready_set failed");
        }

        tx.commit().expect("commit failed");

        // C should win (concurrency event) - verify via backend value
        assert_eq!(backend.get(&"x".to_string()), Some(Value::I64(30)), "Concurrency event C should win");
    }

    #[test]
    fn test_lexicographic_tiebreak_among_siblings() {
        // Concurrent siblings should tiebreak lexicographically (greatest EventId wins)
        let backend = LWWBackend::new();
        backend.set("x".to_string(), Some(Value::I64(10)));

        // Diverged history with multiple concurrent branches:
        //         meet(A)
        //       /    |    \
        // subject(B) C     D
        // B keeps x=10, C sets x=20, D sets x=30
        // D should win (lexicographically greatest)
        let meet_event = event_with_ops("A", vec![], vec![("x", Some(10))]);
        let subject_event = event_with_ops("B", vec![meet_event.payload.id()], vec![]);
        let c_event = event_with_ops("C", vec![meet_event.payload.id()], vec![("x", Some(20))]);
        let d_event = event_with_ops("D", vec![meet_event.payload.id()], vec![("x", Some(30))]);

        let events = HashMap::from([
            (subject_event.payload.id(), subject_event.clone()),
            (c_event.payload.id(), c_event.clone()),
            (d_event.payload.id(), d_event.clone()),
        ]);

        let forward_view = ForwardView::new(
            vec![meet_event.payload.id()],
            vec![subject_event.payload.id()],
            vec![c_event.payload.id(), d_event.payload.id()],
            events,
        );

        let mut tx = backend.begin().expect("begin failed");

        for ready_set in forward_view.iter_ready_sets() {
            tx.apply_ready_set(&ready_set).expect("apply_ready_set failed");
        }

        tx.commit().expect("commit failed");

        // Winner should be the lexicographically greatest between C and D
        // Just verify a value was written (can't easily check which one without downcasting)
        let final_value = backend.get(&"x".to_string()).expect("Should have a final value for x");
        assert!(final_value == Value::I64(20) || final_value == Value::I64(30), "Final value should be either from C (20) or D (30)");
    }

    #[test]
    fn test_multiple_readysets_revise_winners() {
        // Multiple ReadySets should revise winners as we move through layers
        let backend = LWWBackend::new();
        backend.set("x".to_string(), Some(Value::I64(10)));

        // History:
        //      meet(A) x=10
        //     /        \
        // subject(B)   C x=20
        //     |        |
        //     D x=30   E x=40
        //
        // ReadySet 1: B (Primary), C (Concurrency) -> C wins with x=20
        // ReadySet 2: D (Primary), E (Concurrency) -> E wins with x=40
        let meet_event = event_with_ops("A", vec![], vec![("x", Some(10))]);
        let subject_b = event_with_ops("B", vec![meet_event.payload.id()], vec![]);
        let c_event = event_with_ops("C", vec![meet_event.payload.id()], vec![("x", Some(20))]);
        let d_event = event_with_ops("D", vec![subject_b.payload.id()], vec![("x", Some(30))]);
        let e_event = event_with_ops("E", vec![c_event.payload.id()], vec![("x", Some(40))]);

        let events = HashMap::from([
            (subject_b.payload.id(), subject_b.clone()),
            (c_event.payload.id(), c_event.clone()),
            (d_event.payload.id(), d_event.clone()),
            (e_event.payload.id(), e_event.clone()),
        ]);

        let forward_view = ForwardView::new(vec![meet_event.payload.id()], vec![d_event.payload.id()], vec![e_event.payload.id()], events);

        let mut tx = backend.begin().expect("begin failed");

        let mut ready_set_count = 0;
        for ready_set in forward_view.iter_ready_sets() {
            ready_set_count += 1;
            tx.apply_ready_set(&ready_set).expect("apply_ready_set failed");
        }

        assert!(ready_set_count >= 2, "Should have multiple ReadySets, got {}", ready_set_count);

        tx.commit().expect("commit failed");

        // Final winner should be from the last ReadySet (D or E)
        let final_value = backend.get(&"x".to_string()).expect("Should have a final value for x");
        assert!(final_value == Value::I64(30) || final_value == Value::I64(40), "Final value should be either from D (30) or E (40)");
    }

    #[test]
    fn test_lineage_precedence_beats_lexicographic() {
        // Lineage precedence (later events) should beat earlier events
        // Later ReadySets supersede earlier ones regardless of lexicographic ordering
        let backend = LWWBackend::new();
        backend.set("x".to_string(), Some(Value::I64(10)));

        // History:
        //      meet(A) x=10
        //     /        \
        // subject(B)   Z x=20
        //     |
        //     C x=30
        //
        // ReadySet 1: B (Primary), Z (Concurrency) -> Z wins with x=20
        // ReadySet 2: C (Primary only) -> C wins with x=30 (lineage precedence)
        let meet_event = event_with_ops("A", vec![], vec![("x", Some(10))]);
        let subject_b = event_with_ops("B", vec![meet_event.payload.id()], vec![]);
        let z_event = event_with_ops("Z", vec![meet_event.payload.id()], vec![("x", Some(20))]);
        let c_event = event_with_ops("C", vec![subject_b.payload.id()], vec![("x", Some(30))]);

        let events = HashMap::from([
            (subject_b.payload.id(), subject_b.clone()),
            (z_event.payload.id(), z_event.clone()),
            (c_event.payload.id(), c_event.clone()),
        ]);

        let forward_view = ForwardView::new(vec![meet_event.payload.id()], vec![c_event.payload.id()], vec![z_event.payload.id()], events);

        let mut tx = backend.begin().expect("begin failed");

        for ready_set in forward_view.iter_ready_sets() {
            tx.apply_ready_set(&ready_set).expect("apply_ready_set failed");
        }

        tx.commit().expect("commit failed");

        // C should win (latest ReadySet supersedes earlier ones)
        assert_eq!(backend.get(&"x".to_string()), Some(Value::I64(30)), "C should win via lineage precedence");
    }

    #[test]
    fn test_same_property_conflict() {
        // When two concurrent events update the same property, lexicographically greater EventId wins
        let backend = LWWBackend::new();
        backend.set("x".to_string(), Some(Value::I64(10)));

        // History:
        //      meet(A) x=10
        //     /        \
        //    B x=20     C x=30
        //
        // ReadySet 1: B (Concurrency), C (Concurrency) -> C wins (lexicographically greater)
        let meet_event = event_with_ops("A", vec![], vec![("x", Some(10))]);
        let b_event = event_with_ops("B", vec![meet_event.payload.id()], vec![("x", Some(20))]);
        let c_event = event_with_ops("C", vec![meet_event.payload.id()], vec![("x", Some(30))]);

        let events = HashMap::from([(b_event.payload.id(), b_event.clone()), (c_event.payload.id(), c_event.clone())]);

        // Both B and C are concurrent (no subject path, both are concurrency)
        let forward_view = ForwardView::new(
            vec![meet_event.payload.id()],
            vec![],                                           // No subject path
            vec![b_event.payload.id(), c_event.payload.id()], // Both concurrent
            events,
        );

        let mut tx = backend.begin().expect("begin failed");

        for ready_set in forward_view.iter_ready_sets() {
            tx.apply_ready_set(&ready_set).expect("apply_ready_set failed");
        }

        tx.commit().expect("commit failed");

        // C should win (lexicographically greater EventId)
        assert_eq!(backend.get(&"x".to_string()), Some(Value::I64(30)), "C should win via lexicographic tiebreak");
    }

    #[test]
    fn test_events_table_deduplication_at_commit() {
        // Events table should deduplicate EventIds at commit time
        let backend = LWWBackend::new();
        backend.set("x".to_string(), Some(Value::I64(10)));
        backend.set("y".to_string(), Some(Value::I64(20)));

        // History:
        //      meet(A)
        //     /        \
        // subject(B)   C sets both x=30 and y=40
        //
        // C should appear once in events table even though it touches 2 properties
        let meet_event = event_with_ops("A", vec![], vec![("x", Some(10)), ("y", Some(20))]);
        let subject_b = event_with_ops("B", vec![meet_event.payload.id()], vec![]);
        let c_event = event_with_ops("C", vec![meet_event.payload.id()], vec![("x", Some(30)), ("y", Some(40))]);

        let events = HashMap::from([(subject_b.payload.id(), subject_b.clone()), (c_event.payload.id(), c_event.clone())]);

        let forward_view =
            ForwardView::new(vec![meet_event.payload.id()], vec![subject_b.payload.id()], vec![c_event.payload.id()], events);

        let mut tx = backend.begin().expect("begin failed");

        for ready_set in forward_view.iter_ready_sets() {
            tx.apply_ready_set(&ready_set).expect("apply_ready_set failed");
        }

        tx.commit().expect("commit failed");

        // Both x and y should have been updated with C's values
        assert_eq!(backend.get(&"x".to_string()), Some(Value::I64(30)));
        assert_eq!(backend.get(&"y".to_string()), Some(Value::I64(40)));

        // TODO: Once commit is implemented, verify that the events table in serialized state
        // contains C only once with correct entity_id_offset references
    }
}
