use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::{Arc, Mutex, RwLock},
};

use ankurah_proto::{Event, EventId, Operation};
use ankurah_signals::signal::Listener;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MutationError, StateError},
    event_dag::CausalContext,
    property::{backend::PropertyBackend, PropertyName, Value},
};

const LWW_DIFF_VERSION: u8 = 1;

#[derive(Clone, Debug)]
struct ValueEntry {
    value: Option<Value>,
    committed: bool,
    /// The event that wrote this value (None for uncommitted local changes)
    event_id: Option<EventId>,
}

#[derive(Debug)]
pub struct LWWBackend {
    // TODO - can this be safely combined with the values map?
    values: RwLock<BTreeMap<PropertyName, ValueEntry>>,
    field_broadcasts: Mutex<BTreeMap<PropertyName, ankurah_signals::broadcast::Broadcast>>,
}

#[derive(Serialize, Deserialize)]
pub struct LWWDiff {
    version: u8,
    data: Vec<u8>,
}

impl Default for LWWBackend {
    fn default() -> Self { Self::new() }
}

impl LWWBackend {
    pub fn new() -> LWWBackend { Self { values: RwLock::new(BTreeMap::default()), field_broadcasts: Mutex::new(BTreeMap::new()) } }

    pub fn set(&self, property_name: PropertyName, value: Option<Value>) {
        let mut values = self.values.write().unwrap();
        values.insert(property_name, ValueEntry { value, committed: false, event_id: None });
    }

    pub fn get(&self, property_name: &PropertyName) -> Option<Value> {
        let values = self.values.read().unwrap();
        values.get(property_name).and_then(|entry| entry.value.clone())
    }
}

impl PropertyBackend for LWWBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Arc<dyn PropertyBackend> {
        let values = self.values.read().unwrap();
        let cloned = (*values).clone();
        drop(values);

        Arc::new(Self {
            values: RwLock::new(cloned),
            // Create fresh broadcasts (don't clone the existing ones for transaction isolation)
            field_broadcasts: Mutex::new(BTreeMap::new()),
        })
    }

    fn properties(&self) -> Vec<PropertyName> {
        let values = self.values.read().unwrap();
        values.keys().cloned().collect::<Vec<PropertyName>>()
    }

    fn property_value(&self, property_name: &PropertyName) -> Option<Value> { self.get(property_name) }

    fn property_values(&self) -> BTreeMap<PropertyName, Option<Value>> {
        let values = self.values.read().unwrap();
        values.iter().map(|(k, v)| (k.clone(), v.value.clone())).collect()
    }

    fn property_backend_name() -> &'static str { "lww" }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        // Serialize with event_id for per-property conflict resolution after loading
        let values = self.values.read().unwrap();
        let serializable: BTreeMap<PropertyName, (Option<Value>, Option<EventId>)> =
            values.iter().map(|(k, v)| (k.clone(), (v.value.clone(), v.event_id.clone()))).collect();
        let state_buffer = bincode::serialize(&serializable)?;
        Ok(state_buffer)
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        // Try new format first (with event_id), fall back to legacy format
        if let Ok(raw_map) = bincode::deserialize::<BTreeMap<PropertyName, (Option<Value>, Option<EventId>)>>(state_buffer) {
            let map = raw_map.into_iter().map(|(k, (v, eid))| (k, ValueEntry { value: v, committed: true, event_id: eid })).collect();
            return Ok(Self { values: RwLock::new(map), field_broadcasts: Mutex::new(BTreeMap::new()) });
        }
        // Fall back to legacy format (without event_id)
        let raw_map = bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(state_buffer)?;
        let map = raw_map.into_iter().map(|(k, v)| (k, ValueEntry { value: v, committed: true, event_id: None })).collect();
        Ok(Self { values: RwLock::new(map), field_broadcasts: Mutex::new(BTreeMap::new()) })
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut values = self.values.write().unwrap();
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

    fn apply_operations(&self, operations: &[Operation]) -> Result<(), MutationError> {
        // No event tracking - used when loading from state buffer
        self.apply_operations_internal(operations, None)
    }

    fn apply_operations_with_event(&self, operations: &[Operation], event_id: EventId) -> Result<(), MutationError> {
        // Track which event set each property - used for all event applications
        self.apply_operations_internal(operations, Some(event_id))
    }

    fn apply_layer(&self, already_applied: &[&Event], to_apply: &[&Event], current_head: &[EventId]) -> Result<(), MutationError> {
        // All events in this layer are concurrent (same causal depth from meet).
        // For each property, determine winner by lexicographic EventId.
        // Only apply values where winner is from to_apply.

        use std::collections::BTreeSet;

        // Build sets of event IDs for quick lookup
        let to_apply_ids: BTreeSet<EventId> = to_apply.iter().map(|e| e.id()).collect();
        let already_applied_ids: BTreeSet<EventId> = already_applied.iter().map(|e| e.id()).collect();
        let current_head_ids: BTreeSet<EventId> = current_head.iter().cloned().collect();

        // Start with empty winners - layer events compete with each other first
        let mut winners: BTreeMap<PropertyName, (Option<Value>, EventId)> = BTreeMap::new();

        // Process all events (both already_applied and to_apply)
        for event in already_applied.iter().chain(to_apply.iter()) {
            // Extract LWW operations from this event
            if let Some(operations) = event.operations.get(&Self::property_backend_name().to_string()) {
                for operation in operations {
                    let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
                    match version {
                        1 => {
                            let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data)?;
                            for (prop, value) in changes {
                                // Check if this event wins for this property
                                let dominated = winners.get(&prop).map(|(_, existing_id)| event.id() > *existing_id).unwrap_or(true);
                                if dominated {
                                    winners.insert(prop, (value, event.id()));
                                }
                            }
                        }
                        version => {
                            return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into()))
                        }
                    }
                }
            }
        }

        // After processing layer events, check for values from concurrent tips
        // not in this layer. Example: head=[B,C], event E extends B only.
        // C's values are not in any layer but must still compete with E's values.
        //
        // We only consider values whose event_id is in the CURRENT HEAD but not
        // in this layer. Ancestor values (like genesis) should not compete.
        {
            let values = self.values.read().unwrap();
            for (prop, entry) in values.iter() {
                if let Some(eid) = &entry.event_id {
                    // Skip if this event is in the layer (already processed above)
                    if already_applied_ids.contains(eid) || to_apply_ids.contains(eid) {
                        continue;
                    }
                    // Only consider values from events in the current head
                    // These are true concurrent tips, not ancestors
                    if !current_head_ids.contains(eid) {
                        continue;
                    }
                    // This value is from a concurrent tip not in this layer
                    // It should compete with the layer winner for this property
                    match winners.get(prop) {
                        Some((_, winner_eid)) if eid > winner_eid => {
                            // Concurrent tip wins - update winner
                            winners.insert(prop.clone(), (entry.value.clone(), eid.clone()));
                        }
                        None => {
                            // No layer event touched this property - concurrent tip wins by default
                            winners.insert(prop.clone(), (entry.value.clone(), eid.clone()));
                        }
                        _ => {
                            // Layer winner has higher EventId - keep it
                        }
                    }
                }
            }
        }

        // Apply winning values that come from to_apply events
        let mut changed_fields = Vec::new();
        {
            let mut values = self.values.write().unwrap();
            for (prop, (value, winner_id)) in winners {
                if to_apply_ids.contains(&winner_id) {
                    values.insert(prop.clone(), ValueEntry { value, committed: true, event_id: Some(winner_id) });
                    changed_fields.push(prop);
                }
                // Winner from already_applied or concurrent tip → already in state, no mutation needed
            }
        }

        // Notify subscribers for changed fields
        let field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        for field_name in changed_fields {
            if let Some(broadcast) = field_broadcasts.get(&field_name) {
                broadcast.send(());
            }
        }

        Ok(())
    }

    fn apply_layer_with_context(
        &self,
        already_applied: &[&Event],
        to_apply: &[&Event],
        _current_head: &[EventId],
        context: &dyn CausalContext,
    ) -> Result<(), MutationError> {
        // Build candidate set per property: event_id -> (value, event_id)
        // Candidates include:
        // 1. All layer events that touch each property
        // 2. Stored per-property last-write values (if not already in layer)

        let to_apply_ids: BTreeSet<EventId> = to_apply.iter().map(|e| e.id()).collect();

        // Collect candidates from layer events
        let mut candidates: BTreeMap<PropertyName, Vec<(Option<Value>, EventId)>> = BTreeMap::new();

        for event in already_applied.iter().chain(to_apply.iter()) {
            if let Some(operations) = event.operations.get(&Self::property_backend_name().to_string()) {
                for operation in operations {
                    let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
                    match version {
                        1 => {
                            let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data)?;
                            for (prop, value) in changes {
                                candidates.entry(prop).or_default().push((value, event.id()));
                            }
                        }
                        version => {
                            return Err(MutationError::UpdateFailed(
                                anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into(),
                            ))
                        }
                    }
                }
            }
        }

        // Add stored per-property last-write values as candidates
        {
            let values = self.values.read().unwrap();
            for (prop, entry) in values.iter() {
                if let Some(stored_eid) = &entry.event_id {
                    // Skip if this event is already in layer candidates for this property
                    let already_candidate = candidates
                        .get(prop)
                        .map(|c| c.iter().any(|(_, id)| id == stored_eid))
                        .unwrap_or(false);

                    if !already_candidate {
                        // Add stored value as candidate
                        candidates
                            .entry(prop.clone())
                            .or_default()
                            .push((entry.value.clone(), stored_eid.clone()));
                    }
                }
            }
        }

        // Determine winners using causal context
        let mut changed_fields = Vec::new();
        {
            let mut values = self.values.write().unwrap();

            for (prop, prop_candidates) in candidates {
                let winner = self.select_winner_with_context(&prop_candidates, context)?;

                if let Some((value, winner_id)) = winner {
                    // Only apply if winner is from to_apply
                    if to_apply_ids.contains(&winner_id) {
                        values.insert(
                            prop.clone(),
                            ValueEntry { value, committed: true, event_id: Some(winner_id) },
                        );
                        changed_fields.push(prop);
                    }
                }
            }
        }

        // Notify subscribers
        let field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        for field_name in changed_fields {
            if let Some(broadcast) = field_broadcasts.get(&field_name) {
                broadcast.send(());
            }
        }

        Ok(())
    }

    fn listen_field(&self, field_name: &PropertyName, listener: Listener) -> ankurah_signals::signal::ListenerGuard {
        // Get or create the broadcast for this field
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();

        // Subscribe to the broadcast and return the guard
        broadcast.reference().listen(listener).into()
    }
}

impl LWWBackend {
    /// Select winner from candidates using causal context.
    ///
    /// Algorithm:
    /// 1. Filter dominated: Remove any candidate that is an ancestor of another candidate
    /// 2. Remaining are concurrent: All survivors are mutually concurrent
    /// 3. Lexicographic tiebreak: Pick max EventId among survivors
    ///
    /// Returns Err if causal relationship cannot be determined.
    fn select_winner_with_context(
        &self,
        candidates: &[(Option<Value>, EventId)],
        context: &dyn CausalContext,
    ) -> Result<Option<(Option<Value>, EventId)>, MutationError> {
        if candidates.is_empty() {
            return Ok(None);
        }
        if candidates.len() == 1 {
            return Ok(Some(candidates[0].clone()));
        }

        // Phase 1: Find dominated candidates
        // A candidate is dominated if another candidate descends from it
        let mut dominated = BTreeSet::new();
        // Track discovered descent relationships to avoid redundant checks
        // (i, j) in descends means candidate[i] descends from candidate[j]
        let mut descends: BTreeSet<(usize, usize)> = BTreeSet::new();
        // Track pairs that couldn't be determined from either direction
        let mut undetermined: Vec<(usize, usize)> = Vec::new();

        for (i, (_, id_a)) in candidates.iter().enumerate() {
            for (j, (_, id_b)) in candidates.iter().enumerate() {
                if i == j {
                    continue;
                }
                // Skip if we've already determined the relationship from the reverse check
                if descends.contains(&(i, j)) || descends.contains(&(j, i)) {
                    continue;
                }

                // We're checking: does id_b descend from id_a? (does candidate[j] > candidate[i]?)
                match context.is_descendant(id_b, id_a) {
                    Some(true) => {
                        // id_b > id_a causally, so id_a is dominated
                        dominated.insert(i);
                        descends.insert((j, i)); // Record: j descends from i
                    }
                    Some(false) => {
                        // id_b does not descend from id_a
                        // They might be concurrent, or id_a might descend from id_b
                        // Try the reverse direction
                        match context.is_descendant(id_a, id_b) {
                            Some(true) => {
                                // id_a > id_b causally, so id_b is dominated
                                dominated.insert(j);
                                descends.insert((i, j)); // Record: i descends from j
                            }
                            Some(false) => {
                                // Neither descends from the other - they're concurrent
                                // No action needed, lexicographic tiebreak will handle it
                            }
                            None => {
                                // Couldn't determine reverse either
                                // But since forward was Some(false), we know id_b does NOT
                                // descend from id_a. That's enough info for this pair.
                            }
                        }
                    }
                    None => {
                        // Couldn't determine from this direction - try reverse
                        match context.is_descendant(id_a, id_b) {
                            Some(true) => {
                                // id_a > id_b causally, so id_b is dominated
                                dominated.insert(j);
                                descends.insert((i, j));
                            }
                            Some(false) => {
                                // We know id_a does NOT descend from id_b
                                // But forward was None - we don't know if id_b descends from id_a
                                // This is ambiguous: could be concurrent (neither descends)
                                // or id_b > id_a (but we couldn't prove it)
                                // Mark as undetermined - require complete DAG info
                                undetermined.push((i, j));
                            }
                            None => {
                                // Truly cannot determine from either direction
                                undetermined.push((i, j));
                            }
                        }
                    }
                }
            }
        }

        // Check if any undetermined pairs can now be resolved via transitivity
        for (i, j) in undetermined {
            // If we've already determined one is dominated, skip
            if dominated.contains(&i) || dominated.contains(&j) {
                continue;
            }
            // If we still can't determine the relationship, bail out
            let id_a = &candidates[i].1;
            let id_b = &candidates[j].1;
            return Err(MutationError::InsufficientCausalInfo {
                event_a: id_a.clone(),
                event_b: id_b.clone(),
            });
        }

        // Phase 2: Collect non-dominated (concurrent) candidates
        let concurrent: Vec<_> = candidates
            .iter()
            .enumerate()
            .filter(|(i, _)| !dominated.contains(i))
            .map(|(_, c)| c.clone())
            .collect();

        // Phase 3: Lexicographic tiebreak among concurrent candidates
        let winner = concurrent.into_iter().max_by_key(|(_, id)| id.clone());

        Ok(winner)
    }

    /// Get the broadcast ID for a specific field, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, field_name: &PropertyName) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();
        broadcast.id()
    }

    /// Get the event_id that last wrote a property value (if tracked).
    pub fn get_event_id(&self, property_name: &PropertyName) -> Option<EventId> {
        let values = self.values.read().unwrap();
        values.get(property_name).and_then(|entry| entry.event_id.clone())
    }
    /// Internal implementation that handles both tracked and untracked operations.
    fn apply_operations_internal(&self, operations: &[Operation], event_id: Option<EventId>) -> Result<(), MutationError> {
        let mut changed_fields = Vec::new();

        for operation in operations {
            let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
            match version {
                1 => {
                    let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data)?;

                    let mut values = self.values.write().unwrap();
                    for (property_name, new_value) in changes {
                        // Insert as committed entry since this came from an operation
                        values.insert(property_name.clone(), ValueEntry { value: new_value, committed: true, event_id: event_id.clone() });
                        changed_fields.push(property_name);
                    }
                }
                version => return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into())),
            }
        }

        // Notify field subscribers for changed fields only
        let field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        for field_name in changed_fields {
            if let Some(broadcast) = field_broadcasts.get(&field_name) {
                broadcast.send(());
            }
        }

        Ok(())
    }
}

// Need ID based happens-before determination to resolve conflicts
