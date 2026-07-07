use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, Mutex, RwLock},
};

use ankurah_proto::{Clock, EntityId, EventId, Operation};
use ankurah_signals::signal::Listener;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MutationError, StateError},
    event_dag::{CausalRelation, EventLayer},
    property::{backend::PropertyBackend, traits::PropertyError, PropertyName, Value},
};

const LWW_DIFF_VERSION: u8 = 1;
/// Diff version for the id-keyed (v2) encoding (RFC 5.5 in specs/model-property-metadata/rfc.md, plan.md A8). Payload
/// is a [`V2Diff`]: an id-keyed map plus a name-keyed residue.
const LWW_DIFF_VERSION_2: u8 = 2;

/// Version header for serialized LWW state buffers: the first byte of every
/// buffer identifies its encoding, and deserialization refuses buffers whose
/// version it does not know rather than guessing.
///
/// Versions are offset high because unversioned pre-0.9 buffers were raw
/// bincode maps whose first byte is the low byte of the property count -- a
/// small number. Keeping versions at 0xA1+ makes the two ranges disjoint, so
/// one byte classifies any buffer without parse-probing. (A legacy entity
/// would need 160+ properties to be misclassified, and would then fail
/// loudly during deserialization, never silently misparse.)
const LWW_STATE_VERSION_BASE: u8 = 0xA0;
const LWW_STATE_VERSION_1: u8 = LWW_STATE_VERSION_BASE + 1;
/// State buffer header byte for the id-keyed (v2) encoding (RFC 5.5, plan.md
/// A8). Payload is a [`V2State`]: an id-keyed map plus a name-keyed residue.
const LWW_STATE_VERSION_2: u8 = LWW_STATE_VERSION_BASE + 2;

/// Provenance stamp for values loaded from unversioned (pre-0.9) state
/// buffers, which recorded no per-property event id. All-zeros is not a
/// reachable content hash, so it is never found in an accumulated DAG and
/// merge resolution treats such values as older-than-meet: any event that
/// writes the property wins. For pre-0.9 stores this reproduces the true
/// outcome exactly -- their histories are linear, so a real per-property
/// stamp would also lose to every later write -- and unlike stamping with
/// the local head it yields the same election result on every replica,
/// including replicas that rebuilt provenance by replaying events.
const LEGACY_EVENT_ID: [u8; 32] = [0; 32];

/// How a property is keyed inside the backend's `values` map.
///
/// Under the id-keyed contract (RFC 5.5) a property known to the schema is
/// keyed by its defining property entity id; a name that the binding cannot
/// resolve (legacy residue not yet migrated, or a catalog/system collection
/// that is permanently name-keyed) is keyed by its display name. Variant
/// order is Id-then-Name; it is only used for deterministic `BTreeMap`
/// ordering and carries no semantics.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PropertyKey {
    Id(EntityId),
    Name(String),
}

/// Result of the decode-time-hint claimant scan (see
/// [`LWWBackend::hint_claimant`]).
enum HintClaim {
    None,
    Unique(Value),
    Ambiguous(EntityId, EntityId),
}

/// A name<->id translation table for one collection's properties, supplied by
/// the schema layer (local compiled schema and/or the replicated catalog).
///
/// Invariant: every `(name, id)` in `to_id` has its `(id, name)` in
/// `to_name`. The reverse is NOT required: `to_name` may additionally carry
/// ids whose display name forward-maps to a DIFFERENT id -- retype-lineage
/// losers (same name, two ids in one contract; no tombstones until #303) --
/// kept so projection and the RFC 5.4 sibling gate can still name them.
/// Builders pick the forward winner consistently with query resolution
/// (first membership in id order; see `CatalogMapInner::binding_for`).
#[derive(Debug, Clone, Default)]
pub struct SchemaBinding {
    pub to_id: BTreeMap<String, EntityId>,
    pub to_name: BTreeMap<EntityId, String>,
}

/// Per-backend-instance wire encoding selector.
///
/// Defaults to [`WireMode::NameKeyedV1`]. Catalog and system collections stay
/// `NameKeyedV1` permanently (the RFC 4 bootstrap exemption); user
/// collections are flipped to [`WireMode::IdKeyedV2`] by later integration
/// once a schema binding is attached. The mode only governs what this
/// instance EMITS (`to_state_buffer` / `to_operations`); decode always
/// accepts every version it knows regardless of mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireMode {
    NameKeyedV1,
    IdKeyedV2,
}

impl Default for WireMode {
    fn default() -> Self { WireMode::NameKeyedV1 }
}

/// Wire payload for a version-2 (id-keyed) LWW diff. `by_id` carries the
/// normative id-keyed changes; `residue` carries name-keyed changes for
/// properties a writer could not resolve to an id (so legacy names survive a
/// rewrite without data loss). A writer with a full binding produces an empty
/// `residue`.
#[derive(Serialize, Deserialize)]
struct V2Diff {
    by_id: BTreeMap<EntityId, Option<Value>>,
    residue: BTreeMap<String, Option<Value>>,
}

/// Wire payload for a version-2 (0xA2) LWW state buffer. Same split as
/// [`V2Diff`] but over committed entries (value + provenance event id +
/// optional display-name hint). Entries are [`V2CommittedEntry`], NOT the
/// v1 [`CommittedEntry`]: the 0xA1 payload is frozen and must never gain a
/// field, so the hint lives only in the v2 entry type.
#[derive(Serialize, Deserialize)]
struct V2State {
    by_id: BTreeMap<EntityId, V2CommittedEntry>,
    residue: BTreeMap<String, V2CommittedEntry>,
}

/// A committed entry in a 0xA2 state buffer: value + provenance, plus an
/// optional display-name HINT (RFC 5.5 engine-materialization decision,
/// #289). A bound writer fills `name` from its binding's reverse map (or the
/// hints side-map) at write time; an unknown-id entry carries `None`
/// (unprojectable, correct). The hint lets a backend parsed WITHOUT a
/// binding -- the storage engines' `from_state_buffer` materialization path
/// -- still project id-keyed entries to names, so SQL/sled indexes keep
/// working across the Phase A->C window. Hints never enter DIFF bytes, so
/// identity and convergence are untouched; state buffers are not
/// identity-hashed. Renames leave stale hints until rewrite-on-save.
#[derive(Serialize, Deserialize, Clone)]
struct V2CommittedEntry {
    value: Option<Value>,
    event_id: EventId,
    name: Option<String>,
}

#[derive(Clone, Debug)]
enum ValueEntry {
    Uncommitted { value: Option<Value> },
    Pending { value: Option<Value> },
    Committed { value: Option<Value>, event_id: EventId },
}

impl ValueEntry {
    fn value(&self) -> Option<Value> {
        match self {
            ValueEntry::Uncommitted { value } => value.clone(),
            ValueEntry::Pending { value } => value.clone(),
            ValueEntry::Committed { value, .. } => value.clone(),
        }
    }

    fn event_id(&self) -> Option<EventId> {
        match self {
            ValueEntry::Committed { event_id, .. } => Some(event_id.clone()),
            ValueEntry::Uncommitted { .. } | ValueEntry::Pending { .. } => None,
        }
    }
}

#[derive(Debug)]
pub struct LWWBackend {
    // TODO - can this be safely combined with the values map?
    values: RwLock<BTreeMap<PropertyKey, ValueEntry>>,
    // Field-change broadcasts stay keyed by display NAME (String): per-field
    // signal consumers address by name, and the id-keyed contract is a wire
    // concern, not a signals concern (plan.md decision 7).
    field_broadcasts: Mutex<BTreeMap<PropertyName, ankurah_signals::broadcast::Broadcast>>,
    /// Name<->id translation for this collection's properties. Set once, when
    /// integration attaches a schema; absent for catalog/system collections
    /// and for any backend before binding.
    binding: RwLock<Option<Arc<SchemaBinding>>>,
    /// What this instance emits. Set-once alongside `binding` for user
    /// collections; permanently `NameKeyedV1` for catalog/system collections.
    wire_mode: RwLock<WireMode>,
    /// Display-name hints for id-keyed entries, carried by 0xA2 state buffers
    /// (RFC 5.5 engine-materialization decision). Populated on 0xA2 decode
    /// from each entry's `name` hint, and consulted by `property_values`,
    /// `properties`, and broadcast reverse-translation when the binding lacks
    /// the id -- so a backend parsed WITHOUT a binding (the storage engines'
    /// `from_state_buffer` path) still projects id-keyed entries to names for
    /// materialization. The binding, when present, always wins over a hint.
    hints: RwLock<BTreeMap<EntityId, String>>,
}

#[derive(Serialize, Deserialize)]
pub struct LWWDiff {
    version: u8,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone)]
struct CommittedEntry {
    value: Option<Value>,
    event_id: EventId,
}

impl Default for LWWBackend {
    fn default() -> Self { Self::new() }
}

impl LWWBackend {
    pub fn new() -> LWWBackend {
        Self {
            values: RwLock::new(BTreeMap::default()),
            field_broadcasts: Mutex::new(BTreeMap::new()),
            binding: RwLock::new(None),
            wire_mode: RwLock::new(WireMode::default()),
            hints: RwLock::new(BTreeMap::new()),
        }
    }

    /// Attach a name<->id translation table with no head knowledge: name/id
    /// collisions fall back to the concurrent event-id tiebreak. Prefer
    /// [`bind_schema_at`] wherever the entity's head clock is available.
    pub fn bind_schema(&self, binding: Arc<SchemaBinding>) { self.bind_schema_at(binding, &Clock::default()) }

    /// Attach a name<->id translation table. Migrates existing name-keyed
    /// entries whose names the binding resolves onto id keys. Idempotent-ish:
    /// re-binding re-runs the migration against the current entries.
    ///
    /// COLLISION ELECTION: when a name entry migrates onto an id key that
    /// already holds a value (e.g. a 0xA2-loaded id entry plus a residue
    /// write made while this backend was unbound), the winner is chosen by
    /// `head`: an entry whose provenance event is a CURRENT HEAD TIP is
    /// causally maximal and wins outright; otherwise the standard concurrent
    /// event-id tiebreak applies (deterministic on every replica). A full
    /// DAG election needs the apply path's layer machinery; the tip check is
    /// exact for the dominant collision -- a local unbound write descending
    /// a loaded id entry -- and never prefers an entry merely for being
    /// id-keyed (the previous rule, which could regress a newer write).
    pub fn bind_schema_at(&self, binding: Arc<SchemaBinding>, head: &Clock) {
        {
            let mut values = self.values.write().unwrap();
            let existing = std::mem::take(&mut *values);
            for (key, entry) in existing {
                match key {
                    PropertyKey::Name(name) => {
                        if let Some(id) = binding.to_id.get(&name) {
                            match values.entry(PropertyKey::Id(*id)) {
                                std::collections::btree_map::Entry::Vacant(slot) => {
                                    slot.insert(entry);
                                }
                                std::collections::btree_map::Entry::Occupied(mut slot) => {
                                    if Self::collision_prefers_incoming(slot.get(), &entry, head) {
                                        slot.insert(entry);
                                    }
                                }
                            }
                        } else {
                            values.insert(PropertyKey::Name(name), entry);
                        }
                    }
                    PropertyKey::Id(id) => {
                        values.insert(PropertyKey::Id(id), entry);
                    }
                }
            }
        }
        *self.binding.write().unwrap() = Some(binding);
    }

    /// The bind-time collision rule (see [`bind_schema_at`]): head tips win;
    /// an uncommitted incoming entry (a mid-transaction residue write, which
    /// necessarily postdates any loaded id entry) wins; otherwise the
    /// concurrent event-id tiebreak.
    fn collision_prefers_incoming(current: &ValueEntry, incoming: &ValueEntry, head: &Clock) -> bool {
        match (current.event_id(), incoming.event_id()) {
            (Some(current_event), Some(incoming_event)) => match (head.contains(&current_event), head.contains(&incoming_event)) {
                (false, true) => true,
                (true, false) => false,
                _ => incoming_event > current_event,
            },
            (_, None) => true,
            (None, _) => false,
        }
    }

    /// Choose the wire encoding this instance emits.
    pub fn set_wire_mode(&self, mode: WireMode) { *self.wire_mode.write().unwrap() = mode; }

    /// Resolve a display name to the key it should be stored/looked-up under:
    /// the binding's `Id` when it knows the name, else the bare `Name` key.
    ///
    /// Hints do NOT route here (RFC 5.4 ruling, 2026-07-05): reads/writes of a
    /// name land on the binding id or the bare Name key, nothing else. A hint
    /// is a PROJECTION aid only (it lets `property_values`/`properties`/
    /// broadcasts on an UNBOUND backend name id-keyed entries loaded from a
    /// 0xA2 buffer); it never addresses a name onto a foreign id, because
    /// keying a write off a decode-time hint would let one system's raw state
    /// masquerade as another's ("different roots means different systems").
    fn key_for_name(&self, name: &str) -> PropertyKey {
        if let Some(id) = self.binding.read().unwrap().as_ref().and_then(|b| b.to_id.get(name).copied()) {
            return PropertyKey::Id(id);
        }
        PropertyKey::Name(name.to_string())
    }

    pub fn set(&self, property_name: PropertyName, value: Option<Value>) {
        let key = self.key_for_name(&property_name);
        let mut values = self.values.write().unwrap();
        values.insert(key, ValueEntry::Uncommitted { value });
    }

    pub fn get(&self, property_name: &PropertyName) -> Option<Value> { self.lookup_entry(property_name).and_then(|e| e.value()) }

    /// Read a property under the RFC 5.4 sibling gate (rule 4). Unlike the
    /// lenient [`get`], this fails VISIBLE when a same-display-name retype
    /// lineage holds data that the binding-resolved id does not, so the
    /// checked read never silently substitutes a sibling's value (or, upstream
    /// in the View getter / predicate evaluation, fabricates a default over
    /// it). Used by the compiled View getters and by filter evaluation.
    ///
    /// The gate is answerable inside the backend because id-keyed writes from
    /// ANY contract stamp a display-name hint, so a foreign lineage's value is
    /// visible here (RFC 5.4 rule 4; the A10 spec's gate section).
    ///
    /// Three regimes, after the cross-root-transplant ruling (2026-07-05):
    /// - The binding RESOLVES `name` to id A (contract semantics).
    ///   - A holds a value -> `Ok(Some)`.
    ///   - A is absent: scan for ANY OTHER `Id` key that holds data and whose
    ///     display name (binding reverse map, else decode-time hint) equals
    ///     `name`. A match is a retype lineage sitting on this entity (or a
    ///     foreign-root state copied in) -> `Err(TypeSkew)` naming both ids.
    ///     This is where a cross-root state copy fails visible: the foreign id
    ///     does not equal A, but its hint names the same display name, so the
    ///     read refuses rather than substituting the foreign value.
    ///   - No skewing sibling: check the bare `Name` key (legacy residue not
    ///     yet migrated onto A) -> `Ok(Some/None)`.
    /// - A binding EXISTS but does not resolve `name` (the contract does not
    ///   contain it): read ONLY the bare `Name` key -> `Ok(Some/None)`. There
    ///   is deliberately no cross-id scan here: a bound backend never routes a
    ///   name onto an id outside its contract (RFC 5.4 ruling).
    /// - NO binding at all (the schema-blind projection tier: the storage
    ///   engines' `from_state_buffer` parse and `TemporaryEntity`, which have
    ///   no contract by construction): bare `Name` residue first, then the
    ///   decode-time hint projection -- exactly ONE id-keyed entry with data
    ///   claiming this display name reads as that entry (the checked
    ///   counterpart of the `property_values` materialization projection, so
    ///   engine post-filtering and policy state inspection see the same
    ///   fields the engine materializes); TWO OR MORE claimants (a retype
    ///   lineage) is a same-name skew -> `Err(TypeSkew)` rather than a guess.
    pub fn get_checked(&self, property_name: &PropertyName) -> Result<Option<Value>, PropertyError> {
        let values = self.values.read().unwrap();
        let binding = self.binding.read().unwrap();

        let Some(resolved_id) = binding.as_ref().and_then(|b| b.to_id.get(property_name).copied()) else {
            // Not contract-resolvable. Bare Name residue is the primary
            // address in both remaining regimes (it is also the winner the
            // `property_values` projection materializes on a name collision).
            if let Some(value) = values.get(&PropertyKey::Name(property_name.clone())).and_then(|e| e.value()) {
                return Ok(Some(value));
            }
            if binding.is_some() {
                // Bound, but the contract does not contain this name: no
                // cross-id scan outside the contract (RFC 5.4 ruling).
                return Ok(None);
            }
            // Unbound: project through the decode-time hints, refusing to
            // guess between same-name claimants.
            return match Self::hint_claimant(&values, &self.hints.read().unwrap(), property_name) {
                HintClaim::None => Ok(None),
                HintClaim::Unique(value) => Ok(Some(value)),
                HintClaim::Ambiguous(a, b) => {
                    Err(PropertyError::TypeSkew { name: property_name.clone(), a: a.to_base64(), b: b.to_base64() })
                }
            };
        };

        // The binding-resolved id holds a value -> return it directly.
        if let Some(value) = values.get(&PropertyKey::Id(resolved_id)).and_then(|e| e.value()) {
            return Ok(Some(value));
        }

        // Absent under the resolved id: scan for a same-display-name sibling
        // (a DIFFERENT id key that holds data). Its display name comes from the
        // binding reverse map first, else the decode-time hint.
        let hints = self.hints.read().unwrap();
        for (key, entry) in values.iter() {
            let PropertyKey::Id(id) = key else { continue };
            if *id == resolved_id {
                continue;
            }
            if entry.value().is_none() {
                continue;
            }
            let sibling_name = binding.as_ref().and_then(|b| b.to_name.get(id).cloned()).or_else(|| hints.get(id).cloned());
            if sibling_name.as_deref() == Some(property_name.as_str()) {
                return Err(PropertyError::TypeSkew { name: property_name.clone(), a: resolved_id.to_base64(), b: id.to_base64() });
            }
        }

        // No skewing sibling: the value may still live under the bare Name key
        // (legacy residue the binding has not migrated onto the id yet).
        Ok(values.get(&PropertyKey::Name(property_name.clone())).and_then(|e| e.value()))
    }

    /// Lenient projection read for schema-blind consumers ([`crate::entity::
    /// TemporaryEntity`]'s `Filterable::value`, and through it engine
    /// post-filtering of `Path` expressions and policy-agent state
    /// inspection): [`get`] first (binding id, then bare Name residue), then
    /// -- only when this backend has NO binding -- the decode-time hint
    /// projection: a UNIQUE id-keyed entry with data claiming this display
    /// name reads as that entry, and an ambiguous claim (a retype lineage)
    /// reads as `None` rather than a guess (`get_checked` is the variant that
    /// surfaces the ambiguity as `TypeSkew`). Never consulted for writes, and
    /// never applies to a bound backend: hints are projection only (RFC 5.4
    /// ruling, 2026-07-05).
    pub fn get_projected(&self, property_name: &PropertyName) -> Option<Value> {
        if let Some(value) = self.get(property_name) {
            return Some(value);
        }
        if self.binding.read().unwrap().is_some() {
            return None;
        }
        match Self::hint_claimant(&self.values.read().unwrap(), &self.hints.read().unwrap(), property_name) {
            HintClaim::Unique(value) => Some(value),
            // Ambiguous projection: refuse to guess (the checked read is the
            // variant that surfaces it as TypeSkew).
            HintClaim::None | HintClaim::Ambiguous(..) => None,
        }
    }

    /// Scan id-keyed entries for decode-time-hint claimants of `name`:
    /// the ONE shared projection primitive behind `get_checked`'s no-binding
    /// regime and [`get_projected`], so the "exactly one claimant reads,
    /// two claimants are a skew" rule cannot drift between them.
    fn hint_claimant(values: &BTreeMap<PropertyKey, ValueEntry>, hints: &BTreeMap<EntityId, String>, name: &PropertyName) -> HintClaim {
        let mut matched: Option<(EntityId, Value)> = None;
        for (key, entry) in values.iter() {
            let PropertyKey::Id(id) = key else { continue };
            if hints.get(id).map(|h| h.as_str()) != Some(name.as_str()) {
                continue;
            }
            let Some(value) = entry.value() else { continue };
            if let Some((first, _)) = matched {
                return HintClaim::Ambiguous(first, *id);
            }
            matched = Some((*id, value));
        }
        match matched {
            Some((_, value)) => HintClaim::Unique(value),
            None => HintClaim::None,
        }
    }

    /// Find the stored entry for a display name, in priority order:
    ///   1. the id the BINDING maps the name to ([`key_for_name`]);
    ///   2. the bare Name key (legacy residue not yet migrated).
    /// Shared by `get` and `get_event_id` so both resolve identically.
    ///
    /// There is deliberately NO foreign-id-by-hint fallback (RFC 5.4 ruling,
    /// 2026-07-05): a value stored under an id the binding does not resolve is
    /// NOT readable by this name. Copying raw state buffers between systems
    /// with different roots yields foreign ids that never match a local
    /// binding, and the ruling makes that fail-visible (via `get_checked`'s
    /// sibling gate) rather than silently substituting the foreign value here.
    /// The schema-blind projection tier is separate: [`get_projected`] /
    /// `get_checked`'s no-binding regime, which only ever run UNBOUND.
    fn lookup_entry(&self, property_name: &PropertyName) -> Option<ValueEntry> {
        let values = self.values.read().unwrap();
        let key = self.key_for_name(property_name);
        if let Some(entry) = values.get(&key) {
            return Some(entry.clone());
        }
        // key_for_name may have returned a binding id that has no entry; try
        // the bare name next (legacy residue).
        if !matches!(key, PropertyKey::Name(_)) {
            if let Some(entry) = values.get(&PropertyKey::Name(property_name.clone())) {
                return Some(entry.clone());
            }
        }
        None
    }

    /// Reverse-translate a stored key to a display name via the binding ONLY.
    /// Name keys pass through; Id keys resolve through `to_name`; an Id key
    /// with no known name is unprojectable and yields `None` (RFC 5.6 catalog
    /// -lag opacity). Used by the v1 EMISSION paths, where an unresolvable id
    /// key is a hard encoding error and must NOT be masked by a decode-time
    /// hint (a bound v1/system backend never legitimately holds id keys).
    fn key_to_name(binding: Option<&Arc<SchemaBinding>>, key: &PropertyKey) -> Option<String> {
        match key {
            PropertyKey::Name(name) => Some(name.clone()),
            PropertyKey::Id(id) => binding.and_then(|b| b.to_name.get(id).cloned()),
        }
    }

    /// Reverse-translate a stored key to a display name for PROJECTION,
    /// consulting the binding FIRST and the decode-time hints side-map second
    /// (RFC 5.5 engine-materialization decision). Name keys pass through; an
    /// Id key resolves through the binding, else through the hint captured on
    /// 0xA2 decode, else is unprojectable (`None`). This is the path
    /// `property_values`/`properties`/broadcasts use, so an UNBOUND backend
    /// (the engines' `from_state_buffer` materialization) still projects
    /// id-keyed entries to names via their hints.
    fn key_to_name_projected(
        binding: Option<&Arc<SchemaBinding>>,
        hints: &BTreeMap<EntityId, String>,
        key: &PropertyKey,
    ) -> Option<String> {
        match key {
            PropertyKey::Name(name) => Some(name.clone()),
            PropertyKey::Id(id) => binding.and_then(|b| b.to_name.get(id).cloned()).or_else(|| hints.get(id).cloned()),
        }
    }
}

impl PropertyBackend for LWWBackend {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> { self as Arc<dyn Any + Send + Sync + 'static> }

    fn as_debug(&self) -> &dyn Debug { self as &dyn Debug }

    fn fork(&self) -> Arc<dyn PropertyBackend> {
        let values = self.values.read().unwrap();
        let cloned = (*values).clone();
        drop(values);

        // Carry the schema binding and wire mode across the fork: a
        // transaction fork of a user-collection backend must keep keying and
        // emitting id-keyed, or writes made inside the transaction would be
        // name-keyed and mismatch on commit. Carry the hints side-map too, so
        // a fork of an UNBOUND backend loaded from a 0xA2 buffer keeps
        // projecting id-keyed entries (and re-emits their hints on save).
        let binding = self.binding.read().unwrap().clone();
        let wire_mode = *self.wire_mode.read().unwrap();
        let hints = self.hints.read().unwrap().clone();

        Arc::new(Self {
            values: RwLock::new(cloned),
            // Create fresh broadcasts (don't clone the existing ones for transaction isolation)
            field_broadcasts: Mutex::new(BTreeMap::new()),
            binding: RwLock::new(binding),
            wire_mode: RwLock::new(wire_mode),
            hints: RwLock::new(hints),
        })
    }

    fn properties(&self) -> Vec<PropertyName> {
        let values = self.values.read().unwrap();
        let binding = self.binding.read().unwrap();
        let hints = self.hints.read().unwrap();
        // Project to display names via binding-then-hint; id keys with no
        // known name are omitted (unprojectable by design, RFC 5.6).
        values.keys().filter_map(|k| Self::key_to_name_projected(binding.as_ref(), &hints, k)).collect::<Vec<PropertyName>>()
    }

    fn property_value(&self, property_name: &PropertyName) -> Option<Value> { self.get(property_name) }

    fn property_values(&self) -> BTreeMap<PropertyName, Option<Value>> {
        let values = self.values.read().unwrap();
        let binding = self.binding.read().unwrap();
        let hints = self.hints.read().unwrap();
        // Name keys pass through as-is; id keys are reverse-translated through
        // the binding, then the decode-time hints (so an UNBOUND backend still
        // projects); id keys with no known name are OMITTED (RFC 5.6).
        values.iter().filter_map(|(k, v)| Self::key_to_name_projected(binding.as_ref(), &hints, k).map(|name| (name, v.value()))).collect()
    }

    fn property_backend_name() -> &'static str { "lww" }

    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError> {
        // Serialize with required event_id for per-property conflict resolution after loading.
        let values = self.values.read().unwrap();
        let binding = self.binding.read().unwrap();
        let wire_mode = *self.wire_mode.read().unwrap();

        // Committed provenance is required for every property in either mode.
        let require_event_id = |key: &PropertyKey, entry: &ValueEntry| -> Result<EventId, StateError> {
            entry.event_id().ok_or_else(|| {
                StateError::SerializationError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("LWW state requires event_id for property {:?}", key),
                )))
            })
        };

        match wire_mode {
            WireMode::NameKeyedV1 => {
                let mut serializable: BTreeMap<PropertyName, CommittedEntry> = BTreeMap::new();
                for (key, entry) in values.iter() {
                    let event_id = require_event_id(key, entry)?;
                    // Reverse-translate id keys to names. An id key with no
                    // known name is an encoding error in v1 mode; it should be
                    // unreachable because catalog/system entities never see id
                    // keys.
                    let Some(name) = Self::key_to_name(binding.as_ref(), key) else {
                        return Err(StateError::SerializationError(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("LWW v1 (0xA1) state cannot encode unresolvable id key {:?}", key),
                        ))));
                    };
                    serializable.insert(name, CommittedEntry { value: entry.value(), event_id });
                }
                let mut state_buffer = vec![LWW_STATE_VERSION_1];
                bincode::serialize_into(&mut state_buffer, &serializable)?;
                Ok(state_buffer)
            }
            WireMode::IdKeyedV2 => {
                let hints = self.hints.read().unwrap();
                let mut state = V2State { by_id: BTreeMap::new(), residue: BTreeMap::new() };
                for (key, entry) in values.iter() {
                    let event_id = require_event_id(key, entry)?;
                    match key {
                        PropertyKey::Id(id) => {
                            // Fill the display-name hint: binding reverse map
                            // first, else the decode-time side-map, else None
                            // (unknown id stays unprojectable, correct).
                            let name = binding.as_ref().and_then(|b| b.to_name.get(id).cloned()).or_else(|| hints.get(id).cloned());
                            state.by_id.insert(*id, V2CommittedEntry { value: entry.value(), event_id, name });
                        }
                        PropertyKey::Name(name) => {
                            // Residue is already name-keyed; the hint just
                            // echoes the key so an unbound reload keeps it.
                            state
                                .residue
                                .insert(name.clone(), V2CommittedEntry { value: entry.value(), event_id, name: Some(name.clone()) });
                        }
                    }
                }
                let mut state_buffer = vec![LWW_STATE_VERSION_2];
                bincode::serialize_into(&mut state_buffer, &state)?;
                Ok(state_buffer)
            }
        }
    }

    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized {
        // Decode is schema-BLIND: it never consults a binding. Legacy/0xA1
        // produce Name keys; 0xA2 produces Id keys (by_id) and Name keys
        // (residue). Any name<->id migration happens later, only via
        // bind_schema. The freshly-decoded backend defaults to no binding and
        // NameKeyedV1 mode; integration flips those afterwards.
        let (version, payload) = match state_buffer.split_first() {
            Some((version, payload)) => (*version, payload),
            None => return Err(crate::error::RetrievalError::Other("empty LWW state buffer".to_string())),
        };
        if version < LWW_STATE_VERSION_BASE {
            // Unversioned pre-0.9 buffer: raw bincode of property -> value,
            // no header byte and no per-property provenance. The version
            // ranges are disjoint by construction, so this is a dispatch on
            // the same single byte, not a parse probe: a failure below means
            // a corrupt buffer, not format ambiguity. Loaded values carry
            // LEGACY_EVENT_ID, and the next to_state_buffer rewrites the
            // entity in the current versioned format.
            let legacy_map = bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(state_buffer)
                .map_err(|e| crate::error::RetrievalError::Other(format!("failed to parse pre-0.9 legacy LWW state buffer: {e}")))?;
            let map = legacy_map
                .into_iter()
                .map(|(k, value)| (PropertyKey::Name(k), ValueEntry::Committed { value, event_id: EventId::from_bytes(LEGACY_EVENT_ID) }))
                .collect();
            return Ok(Self::from_decoded(map));
        }
        match version {
            LWW_STATE_VERSION_1 => {
                let raw_map = bincode::deserialize::<BTreeMap<PropertyName, CommittedEntry>>(payload)?;
                let map = raw_map
                    .into_iter()
                    .map(|(k, entry)| (PropertyKey::Name(k), ValueEntry::Committed { value: entry.value, event_id: entry.event_id }))
                    .collect();
                Ok(Self::from_decoded(map))
            }
            LWW_STATE_VERSION_2 => {
                let state = bincode::deserialize::<V2State>(payload)?;
                let mut map: BTreeMap<PropertyKey, ValueEntry> = BTreeMap::new();
                // Retain each id entry's display-name hint so an UNBOUND
                // reload (the storage engines' materialization path) can still
                // project it. Residue names carry no hint: they are already
                // their own key.
                let mut hints: BTreeMap<EntityId, String> = BTreeMap::new();
                for (id, entry) in state.by_id {
                    if let Some(name) = entry.name {
                        hints.insert(id, name);
                    }
                    map.insert(PropertyKey::Id(id), ValueEntry::Committed { value: entry.value, event_id: entry.event_id });
                }
                for (name, entry) in state.residue {
                    map.insert(PropertyKey::Name(name), ValueEntry::Committed { value: entry.value, event_id: entry.event_id });
                }
                Ok(Self::from_decoded_with_hints(map, hints))
            }
            // Unknown versions are refused loudly rather than misread. A 0.9
            // binary refuses 0xA2 (and anything above 0xA1) via this identical
            // arm it shipped with; #294's version negotiation turns that
            // refusal into a negotiated capability rather than an error.
            _ => Err(crate::error::RetrievalError::Other(format!(
                "unknown LWW state buffer version {version:#04x} (this binary supports {LWW_STATE_VERSION_1:#04x} and {LWW_STATE_VERSION_2:#04x})"
            ))),
        }
    }

    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError> {
        let mut values = self.values.write().unwrap();
        let binding = self.binding.read().unwrap();
        let wire_mode = *self.wire_mode.read().unwrap();

        // Collect the keys+values that transitioned Uncommitted -> Pending.
        let mut changed: Vec<(PropertyKey, Option<Value>)> = Vec::new();
        for (key, entry) in values.iter_mut() {
            let ValueEntry::Uncommitted { value } = entry else {
                continue;
            };
            let value = value.clone();
            changed.push((key.clone(), value.clone()));
            *entry = ValueEntry::Pending { value };
        }

        if changed.is_empty() {
            return Ok(None);
        }

        let diff = match wire_mode {
            WireMode::NameKeyedV1 => {
                let mut changed_values: BTreeMap<PropertyName, Option<Value>> = BTreeMap::new();
                for (key, value) in changed {
                    // Reverse-translate id keys to names; an unresolvable id
                    // key in v1 mode is an encoding error (unreachable for
                    // catalog/system entities, which never hold id keys).
                    let Some(name) = Self::key_to_name(binding.as_ref(), &key) else {
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!("LWW v1 diff cannot encode unresolvable id key {:?}", key).into(),
                        ));
                    };
                    changed_values.insert(name, value);
                }
                bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION, data: bincode::serialize(&changed_values)? })?
            }
            WireMode::IdKeyedV2 => {
                let mut v2 = V2Diff { by_id: BTreeMap::new(), residue: BTreeMap::new() };
                for (key, value) in changed {
                    match key {
                        PropertyKey::Id(id) => {
                            v2.by_id.insert(id, value);
                        }
                        PropertyKey::Name(name) => {
                            v2.residue.insert(name, value);
                        }
                    }
                }
                bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION_2, data: bincode::serialize(&v2)? })?
            }
        };

        Ok(Some(vec![Operation { diff }]))
    }

    fn apply_operations(&self, operations: &[Operation]) -> Result<(), MutationError> {
        // No event tracking - used when loading from state buffer
        self.apply_operations_internal(operations, None)
    }

    fn apply_operations_with_event(&self, operations: &[Operation], event_id: EventId) -> Result<(), MutationError> {
        // Track which event set each property - used for all event applications
        self.apply_operations_internal(operations, Some(event_id))
    }

    fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError> {
        #[derive(Clone)]
        struct Candidate {
            value: Option<Value>,
            event_id: EventId,
            from_to_apply: bool,
            older_than_meet: bool,
        }

        // Keyed by PropertyKey so id-keyed (v2) and name-keyed (v1/legacy
        // residue) candidates elect independently; the per-key LWW logic is
        // identical for both kinds.
        let mut winners: BTreeMap<PropertyKey, Candidate> = BTreeMap::new();

        // Seed with stored last-write candidates (required event_id).
        {
            let values = self.values.read().unwrap();
            for (prop, entry) in values.iter() {
                let Some(event_id) = entry.event_id() else {
                    return Err(MutationError::UpdateFailed(
                        anyhow::anyhow!("LWW candidate missing event_id for property {:?}", prop).into(),
                    ));
                };

                // KEY RULE: If stored event_id is NOT in the accumulated DAG,
                // it is strictly older than the meet. Any layer candidate wins.
                // We still seed it so it participates if no layer event touches
                // this property, but mark it as auto-losable.
                let known_in_dag = layer.dag_contains(&event_id);
                winners.insert(
                    prop.clone(),
                    Candidate { value: entry.value(), event_id, from_to_apply: false, older_than_meet: !known_in_dag },
                );
            }
        }

        // Add candidates from events in this layer.
        for (event, from_to_apply) in layer.already_applied.iter().map(|e| (e, false)).chain(layer.to_apply.iter().map(|e| (e, true))) {
            if let Some(operations) = event.operations.get(&Self::property_backend_name().to_string()) {
                for operation in operations {
                    let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
                    // Normalize both wire versions into (PropertyKey, value)
                    // candidates; election below is version-agnostic.
                    let changes: Vec<(PropertyKey, Option<Value>)> = match version {
                        1 => bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(&data)?
                            .into_iter()
                            .map(|(name, value)| (PropertyKey::Name(name), value))
                            .collect(),
                        2 => {
                            let v2: V2Diff = bincode::deserialize(&data)?;
                            v2.by_id
                                .into_iter()
                                .map(|(id, value)| (PropertyKey::Id(id), value))
                                .chain(v2.residue.into_iter().map(|(name, value)| (PropertyKey::Name(name), value)))
                                .collect()
                        }
                        version => {
                            return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into()))
                        }
                    };
                    for (prop, value) in changes {
                        // Normalize an incoming name key onto its id key (if
                        // bound/hinted) so a v1/residue write competes with the
                        // id-keyed value for the same property in election,
                        // matching how stored values were seeded (RFC 5.5).
                        let prop = self.normalize_key(prop);
                        let candidate = Candidate { value, event_id: event.id(), from_to_apply, older_than_meet: false };
                        if let Some(current) = winners.get_mut(&prop) {
                            if current.older_than_meet {
                                // Stored value is below meet -- any layer candidate wins
                                *current = candidate;
                            } else {
                                // Both in accumulated set -- use causal comparison (infallible)
                                let relation = layer.compare(&candidate.event_id, &current.event_id);
                                match relation {
                                    CausalRelation::Descends => {
                                        *current = candidate;
                                    }
                                    CausalRelation::Ascends => {}
                                    CausalRelation::Concurrent => {
                                        if candidate.event_id > current.event_id {
                                            *current = candidate;
                                        }
                                    }
                                }
                            }
                        } else {
                            winners.insert(prop, candidate);
                        }
                    }
                }
            }
        }

        // Apply winning values that come from to_apply events.
        let mut changed_fields = Vec::new();
        {
            let mut values = self.values.write().unwrap();
            let binding = self.binding.read().unwrap();
            let hints = self.hints.read().unwrap();
            for (prop, candidate) in winners {
                if candidate.from_to_apply {
                    // Reverse-translate to a display name for the broadcast
                    // (binding then hint); an id-keyed change with no known
                    // name fires no broadcast (RFC 5.6 opacity).
                    if let Some(name) = Self::key_to_name_projected(binding.as_ref(), &hints, &prop) {
                        changed_fields.push(name);
                    }
                    values.insert(prop, ValueEntry::Committed { value: candidate.value, event_id: candidate.event_id });
                }
            }
        }

        // Notify subscribers for changed fields
        super::notify_changed_fields(&self.field_broadcasts, changed_fields.iter());

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
    /// Build a decoded backend from a key map. Decode is schema-blind, so the
    /// result carries no binding and defaults to NameKeyedV1; integration
    /// flips those afterwards via bind_schema / set_wire_mode.
    fn from_decoded(map: BTreeMap<PropertyKey, ValueEntry>) -> Self { Self::from_decoded_with_hints(map, BTreeMap::new()) }

    /// As [`from_decoded`], but also seed the display-name hints side-map
    /// (0xA2 decode only). The hints let an UNBOUND backend project id-keyed
    /// entries for engine materialization; a later bind_schema still wins.
    fn from_decoded_with_hints(map: BTreeMap<PropertyKey, ValueEntry>, hints: BTreeMap<EntityId, String>) -> Self {
        Self {
            values: RwLock::new(map),
            field_broadcasts: Mutex::new(BTreeMap::new()),
            binding: RwLock::new(None),
            wire_mode: RwLock::new(WireMode::default()),
            hints: RwLock::new(hints),
        }
    }

    /// Get the broadcast ID for a specific field, creating the broadcast if necessary
    pub fn field_broadcast_id(&self, field_name: &PropertyName) -> ankurah_signals::broadcast::BroadcastId {
        let mut field_broadcasts = self.field_broadcasts.lock().expect("other thread panicked, panic here too");
        let broadcast = field_broadcasts.entry(field_name.clone()).or_default();
        broadcast.id()
    }

    /// Normalize a just-decoded key against the current BINDING so that an
    /// incoming NAME-keyed change (a v1 diff, or v2 residue) for a property
    /// this backend knows by id lands on the SAME id key as its existing
    /// id-keyed value, and therefore COMPETES in LWW election instead of
    /// splitting into a shadow name entry (RFC 5.5 mixed-version convergence).
    /// Id keys pass through unchanged. Uses [`key_for_name`], which consults
    /// the binding ONLY: hints never route keys (RFC 5.4 ruling, 2026-07-05),
    /// so on an unbound backend a name-keyed change stays name-keyed residue.
    fn normalize_key(&self, key: PropertyKey) -> PropertyKey {
        match key {
            PropertyKey::Id(_) => key,
            PropertyKey::Name(name) => self.key_for_name(&name),
        }
    }

    /// Get the event_id that last wrote a property value (if tracked).
    /// Resolves the name to its key the same way `get` does (binding id, then
    /// bare name).
    pub fn get_event_id(&self, property_name: &PropertyName) -> Option<EventId> {
        self.lookup_entry(property_name).and_then(|e| e.event_id())
    }
    /// Internal implementation that handles both tracked and untracked operations.
    fn apply_operations_internal(&self, operations: &[Operation], event_id: Option<EventId>) -> Result<(), MutationError> {
        let mut changed_fields = Vec::new();

        for operation in operations {
            let LWWDiff { version, data } = bincode::deserialize(&operation.diff)?;
            // Normalize both wire versions into (PropertyKey, value) changes;
            // storage and stamping below are version-agnostic. Decode stays
            // schema-blind: v1 names stay Name keys (migration is bind_schema's
            // job), v2 by_id becomes Id keys and residue stays Name keys.
            let changes: Vec<(PropertyKey, Option<Value>)> = match version {
                1 => bincode::deserialize::<BTreeMap<PropertyName, Option<Value>>>(&data)?
                    .into_iter()
                    .map(|(name, value)| (PropertyKey::Name(name), value))
                    .collect(),
                2 => {
                    let v2: V2Diff = bincode::deserialize(&data)?;
                    v2.by_id
                        .into_iter()
                        .map(|(id, value)| (PropertyKey::Id(id), value))
                        .chain(v2.residue.into_iter().map(|(name, value)| (PropertyKey::Name(name), value)))
                        .collect()
                }
                version => return Err(MutationError::UpdateFailed(anyhow::anyhow!("Unknown LWW operation version: {:?}", version).into())),
            };

            // Normalize incoming name keys onto their id keys (if bound/hinted)
            // BEFORE taking the values lock, so a v1/residue write competes with
            // the id-keyed value for the same property rather than shadowing it.
            let changes: Vec<(PropertyKey, Option<Value>)> = changes.into_iter().map(|(k, v)| (self.normalize_key(k), v)).collect();

            let mut values = self.values.write().unwrap();
            let binding = self.binding.read().unwrap();
            let hints = self.hints.read().unwrap();
            for (key, new_value) in changes {
                let entry = match event_id.clone() {
                    Some(event_id) => ValueEntry::Committed { value: new_value, event_id },
                    None => ValueEntry::Pending { value: new_value },
                };
                // Reverse-translate for the broadcast (binding then hint); an
                // id-keyed change with no known name fires no broadcast (RFC
                // 5.6 opacity).
                if let Some(name) = Self::key_to_name_projected(binding.as_ref(), &hints, &key) {
                    changed_fields.push(name);
                }
                values.insert(key, entry);
            }
        }

        // Notify field subscribers for changed fields only
        super::notify_changed_fields(&self.field_broadcasts, changed_fields.iter());

        Ok(())
    }
}

// Need ID based happens-before determination to resolve conflicts

#[cfg(test)]
mod tests {
    use super::*;

    fn committed_backend(event_id: EventId) -> LWWBackend {
        let backend = LWWBackend::new();
        backend.set("title".into(), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().expect("pending write should yield operations");
        backend.apply_operations_with_event(&ops, event_id).unwrap();
        backend
    }

    #[test]
    fn state_buffer_round_trips_with_version_header() {
        let event_id = EventId::from_bytes([7; 32]);
        let backend = committed_backend(event_id.clone());

        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION_1, "first byte is the state version header");

        let restored = LWWBackend::from_state_buffer(&buffer).unwrap();
        assert_eq!(restored.get(&"title".to_string()), Some(Value::String("alpha".into())));
        assert_eq!(restored.get_event_id(&"title".to_string()), Some(event_id));
    }

    #[test]
    fn unversioned_pre_09_buffer_loads_via_fallback_and_upgrades_on_save() {
        // Simulate a pre-0.9 buffer: raw bincode of property -> Option<Value>,
        // no header. Its first byte is the map length's low byte (here 0x01).
        let legacy: BTreeMap<PropertyName, Option<Value>> =
            [("title".to_string(), Some(Value::String("alpha".into())))].into_iter().collect();
        let legacy_buffer = bincode::serialize(&legacy).unwrap();
        assert!(legacy_buffer[0] < LWW_STATE_VERSION_BASE);

        let restored = LWWBackend::from_state_buffer(&legacy_buffer).unwrap();
        assert_eq!(restored.get(&"title".to_string()), Some(Value::String("alpha".into())));
        assert_eq!(restored.get_event_id(&"title".to_string()), Some(EventId::from_bytes(LEGACY_EVENT_ID)));

        // Re-serialization writes the current versioned format: lazy upgrade.
        let upgraded = restored.to_state_buffer().unwrap();
        assert_eq!(upgraded[0], LWW_STATE_VERSION_1);
        let round_tripped = LWWBackend::from_state_buffer(&upgraded).unwrap();
        assert_eq!(round_tripped.get(&"title".to_string()), Some(Value::String("alpha".into())));
        assert_eq!(round_tripped.get_event_id(&"title".to_string()), Some(EventId::from_bytes(LEGACY_EVENT_ID)));
    }

    #[test]
    fn empty_legacy_buffer_loads_as_empty() {
        // A pre-0.9 zero-property buffer is eight zero bytes; first byte 0x00.
        let legacy: BTreeMap<PropertyName, Option<Value>> = BTreeMap::new();
        let legacy_buffer = bincode::serialize(&legacy).unwrap();
        assert_eq!(legacy_buffer[0], 0x00);

        let restored = LWWBackend::from_state_buffer(&legacy_buffer).unwrap();
        assert!(restored.properties().is_empty());
    }

    #[test]
    fn corrupt_legacy_buffer_errors_clearly() {
        // Low first byte dispatches to the legacy parser; garbage after that
        // is corruption, and the error should say which parser rejected it.
        let garbage = vec![0x03, 0xde, 0xad, 0xbe, 0xef];
        let err = LWWBackend::from_state_buffer(&garbage).unwrap_err();
        assert!(err.to_string().contains("legacy LWW state buffer"), "unexpected error: {err}");
    }

    #[test]
    fn unknown_future_version_is_refused() {
        let backend = committed_backend(EventId::from_bytes([7; 32]));
        let mut buffer = backend.to_state_buffer().unwrap();
        buffer[0] = LWW_STATE_VERSION_BASE + 9;

        let err = LWWBackend::from_state_buffer(&buffer).unwrap_err();
        assert!(err.to_string().contains("unknown LWW state buffer version"), "unexpected error: {err}");
    }

    #[test]
    fn empty_buffer_is_refused() {
        let err = LWWBackend::from_state_buffer(&Vec::new()).unwrap_err();
        assert!(err.to_string().contains("empty LWW state buffer"), "unexpected error: {err}");
    }

    // ---- id-keyed (v2) tests ----------------------------------------------

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 16];
        bytes[0] = byte;
        EntityId::from_bytes(bytes)
    }

    /// A binding mapping "title" -> id(0x11) and "author" -> id(0x22).
    fn title_author_binding() -> Arc<SchemaBinding> {
        let mut to_id = BTreeMap::new();
        let mut to_name = BTreeMap::new();
        for (name, b) in [("title", 0x11u8), ("author", 0x22u8)] {
            to_id.insert(name.to_string(), id(b));
            to_name.insert(id(b), name.to_string());
        }
        Arc::new(SchemaBinding { to_id, to_name })
    }

    /// Build a committed, id-keyed backend: bind, flip to v2, set + commit.
    fn committed_v2_backend(event_id: EventId) -> LWWBackend {
        let backend = LWWBackend::new();
        backend.bind_schema(title_author_binding());
        backend.set_wire_mode(WireMode::IdKeyedV2);
        backend.set("title".into(), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().expect("pending write should yield operations");
        backend.apply_operations_with_event(&ops, event_id).unwrap();
        backend
    }

    // (a) v2 diff round-trip and 0xA2 state round-trip (Id and residue).
    #[test]
    fn v2_diff_round_trips_id_and_residue() {
        let source = LWWBackend::new();
        source.bind_schema(title_author_binding());
        source.set_wire_mode(WireMode::IdKeyedV2);
        source.set("title".into(), Some(Value::String("alpha".into()))); // -> by_id (id 0x11)
                                                                         // A name the binding does not know becomes residue.
        source.set("legacy".into(), Some(Value::I64(7)));

        let ops = source.to_operations().unwrap().expect("writes yield operations");
        // The diff must be version 2.
        let LWWDiff { version, data } = bincode::deserialize(&ops[0].diff).unwrap();
        assert_eq!(version, LWW_DIFF_VERSION_2);
        let decoded: V2Diff = bincode::deserialize(&data).unwrap();
        assert_eq!(decoded.by_id.get(&id(0x11)), Some(&Some(Value::String("alpha".into()))));
        assert_eq!(decoded.residue.get("legacy"), Some(&Some(Value::I64(7))));

        // Apply into a fresh v2 backend with the same binding: values project.
        let sink = LWWBackend::new();
        sink.bind_schema(title_author_binding());
        sink.set_wire_mode(WireMode::IdKeyedV2);
        sink.apply_operations_with_event(&ops, EventId::from_bytes([9; 32])).unwrap();
        assert_eq!(sink.get(&"title".to_string()), Some(Value::String("alpha".into())));
        assert_eq!(sink.get(&"legacy".to_string()), Some(Value::I64(7)));
    }

    #[test]
    fn v2_state_buffer_round_trips_id_and_residue() {
        let event_id = EventId::from_bytes([7; 32]);
        let backend = LWWBackend::new();
        backend.bind_schema(title_author_binding());
        backend.set_wire_mode(WireMode::IdKeyedV2);
        backend.set("title".into(), Some(Value::String("alpha".into())));
        backend.set("legacy".into(), Some(Value::I64(7)));
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, event_id.clone()).unwrap();

        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION_2, "first byte is the 0xA2 header");
        let state: V2State = bincode::deserialize(&buffer[1..]).unwrap();
        assert!(state.by_id.contains_key(&id(0x11)));
        assert!(state.residue.contains_key("legacy"));
        // The bound writer stamped the display-name HINT onto the id entry
        // (RFC 5.5 engine-materialization decision).
        assert_eq!(state.by_id.get(&id(0x11)).and_then(|e| e.name.clone()), Some("title".to_string()));

        // Decode is schema-blind, but the hint travels with the buffer: an
        // UNBOUND reload STILL projects the id key by name (this is exactly
        // the storage engines' from_state_buffer materialization path).
        let restored = LWWBackend::from_state_buffer(&buffer).unwrap();
        assert_eq!(restored.get(&"legacy".to_string()), Some(Value::I64(7)));
        assert_eq!(
            restored.property_values().get("title"),
            Some(&Some(Value::String("alpha".into()))),
            "hint projects the id key even without a binding"
        );
        // A binding still resolves the id key the same way.
        restored.bind_schema(title_author_binding());
        assert_eq!(restored.get(&"title".to_string()), Some(Value::String("alpha".into())));
        assert_eq!(restored.get_event_id(&"title".to_string()), Some(event_id));
    }

    // (a2) The engine-materialization case, end to end: a bound writer emits a
    // 0xA2 buffer; a completely UNBOUND backend decodes it (as the storage
    // engines do inside set_state) and projects EVERY id-keyed property to its
    // name via the decode-time hints -- so SQL/sled materialization and
    // TemporaryEntity filtering keep working across the Phase A->C window.
    #[test]
    fn unbound_decode_of_bound_v2_buffer_projects_all_names_via_hints() {
        let event_id = EventId::from_bytes([4; 32]);
        let writer = LWWBackend::new();
        writer.bind_schema(title_author_binding());
        writer.set_wire_mode(WireMode::IdKeyedV2);
        writer.set("title".into(), Some(Value::String("alpha".into())));
        writer.set("author".into(), Some(Value::String("bob".into())));
        let ops = writer.to_operations().unwrap().unwrap();
        writer.apply_operations_with_event(&ops, event_id).unwrap();
        let buffer = writer.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION_2);

        // Decode with NO binding, exactly like a storage engine's
        // from_state_buffer materialization path.
        let unbound = LWWBackend::from_state_buffer(&buffer).unwrap();
        let projected = unbound.property_values();
        assert_eq!(projected.get("title"), Some(&Some(Value::String("alpha".into()))));
        assert_eq!(projected.get("author"), Some(&Some(Value::String("bob".into()))));
        // properties() is likewise fully projected via hints.
        let mut names = unbound.properties();
        names.sort();
        assert_eq!(names, vec!["author".to_string(), "title".to_string()]);

        // And the hints survive a load->save cycle on the UNBOUND backend: it
        // re-emits 0xA2 with the same names, so a later reload still projects.
        unbound.set_wire_mode(WireMode::IdKeyedV2);
        let re = unbound.to_state_buffer().unwrap();
        let restate: V2State = bincode::deserialize(&re[1..]).unwrap();
        assert_eq!(restate.by_id.get(&id(0x11)).and_then(|e| e.name.clone()), Some("title".to_string()));
        assert_eq!(restate.by_id.get(&id(0x22)).and_then(|e| e.name.clone()), Some("author".to_string()));
    }

    // (b) v1 buffer decode -> bind_schema -> to_state_buffer in IdKeyedV2 mode
    // emits 0xA2 with translated ids and residue preserved for unbound names.
    #[test]
    fn v1_buffer_migrates_to_v2_on_save_with_residue_preserved() {
        // Produce a legitimate 0xA1 buffer with two names, one of which the
        // binding will know ("title") and one it will not ("legacy").
        let event_id = EventId::from_bytes([5; 32]);
        let v1 = LWWBackend::new(); // default NameKeyedV1
        v1.set("title".into(), Some(Value::String("alpha".into())));
        v1.set("legacy".into(), Some(Value::I64(7)));
        let ops = v1.to_operations().unwrap().unwrap();
        v1.apply_operations_with_event(&ops, event_id.clone()).unwrap();
        let v1_buffer = v1.to_state_buffer().unwrap();
        assert_eq!(v1_buffer[0], LWW_STATE_VERSION_1);

        // Decode (schema-blind -> Name keys), then bind + flip to v2.
        let migrated = LWWBackend::from_state_buffer(&v1_buffer).unwrap();
        migrated.bind_schema(title_author_binding());
        migrated.set_wire_mode(WireMode::IdKeyedV2);

        let v2_buffer = migrated.to_state_buffer().unwrap();
        assert_eq!(v2_buffer[0], LWW_STATE_VERSION_2, "lazy rewrite-on-save emits 0xA2");
        let state: V2State = bincode::deserialize(&v2_buffer[1..]).unwrap();
        // "title" translated onto its id; provenance preserved.
        let title_entry = state.by_id.get(&id(0x11)).expect("title migrated to id key");
        assert_eq!(title_entry.value, Some(Value::String("alpha".into())));
        assert_eq!(title_entry.event_id, event_id);
        // "legacy" (unbound) preserved verbatim in residue.
        assert_eq!(state.residue.get("legacy").map(|e| e.value.clone()), Some(Some(Value::I64(7))));
    }

    // (c) Pre-0.9 legacy buffer decodes with LEGACY_EVENT_ID provenance and
    // migrates the same way.
    #[test]
    fn pre_09_legacy_buffer_migrates_to_v2() {
        let legacy: BTreeMap<PropertyName, Option<Value>> =
            [("title".to_string(), Some(Value::String("alpha".into()))), ("legacy".to_string(), Some(Value::I64(7)))].into_iter().collect();
        let legacy_buffer = bincode::serialize(&legacy).unwrap();
        assert!(legacy_buffer[0] < LWW_STATE_VERSION_BASE);

        let restored = LWWBackend::from_state_buffer(&legacy_buffer).unwrap();
        // Legacy provenance stamp preserved.
        assert_eq!(restored.get_event_id(&"title".to_string()), Some(EventId::from_bytes(LEGACY_EVENT_ID)));

        restored.bind_schema(title_author_binding());
        restored.set_wire_mode(WireMode::IdKeyedV2);
        let v2_buffer = restored.to_state_buffer().unwrap();
        assert_eq!(v2_buffer[0], LWW_STATE_VERSION_2);
        let state: V2State = bincode::deserialize(&v2_buffer[1..]).unwrap();
        assert_eq!(state.by_id.get(&id(0x11)).unwrap().event_id, EventId::from_bytes(LEGACY_EVENT_ID));
        assert!(state.residue.contains_key("legacy"));
    }

    // (d) Refusal: 0xA3 state buffer and diff version 3 refused with the
    // existing error shape; also pin that a 0.9 decoder refuses 0xA2 via the
    // identical unknown-version arm it shipped with.
    #[test]
    fn future_state_version_and_diff_version_are_refused() {
        // 0xA3 (one past the versions this binary knows) refused.
        let backend = committed_v2_backend(EventId::from_bytes([7; 32]));
        let mut buffer = backend.to_state_buffer().unwrap();
        buffer[0] = LWW_STATE_VERSION_BASE + 3; // 0xA3
        let err = LWWBackend::from_state_buffer(&buffer).unwrap_err();
        assert!(err.to_string().contains("unknown LWW state buffer version"), "unexpected error: {err}");
        // The supported-version text now lists both 0xA1 and 0xA2.
        assert!(err.to_string().contains("0xa1") && err.to_string().contains("0xa2"), "supported list should name both: {err}");

        // A diff carrying version 3 is refused with the existing operation
        // error shape via apply.
        let bad_diff =
            bincode::serialize(&LWWDiff { version: 3, data: bincode::serialize(&BTreeMap::<PropertyName, Option<Value>>::new()).unwrap() })
                .unwrap();
        let err = backend.apply_operations(&[Operation { diff: bad_diff }]).unwrap_err();
        assert!(err.to_string().contains("Unknown LWW operation version"), "unexpected error: {err}");

        // 0.9-decoder refusal of 0xA2: a 0.9 binary shipped an
        // unknown-version arm that refuses everything above 0xA1, so it
        // refuses 0xA2 by the SAME arm this binary uses to refuse >0xA2. We
        // assert our decoder refuses an unknown version above 0xA2; 0.9's
        // decoder refuses 0xA2 identically (it just had a lower ceiling).
        let mut a4 = backend.to_state_buffer().unwrap();
        a4[0] = LWW_STATE_VERSION_BASE + 4;
        assert!(LWWBackend::from_state_buffer(&a4).is_err());
    }

    // (e) Default-mode invariance: no binding, default mode -> state buffer
    // first byte 0xA1, diff version 1, decodable as the legacy name-keyed
    // shape (byte-compatible with today).
    #[test]
    fn default_mode_is_byte_compatible_with_v1() {
        let event_id = EventId::from_bytes([7; 32]);
        let backend = committed_backend(event_id.clone());

        // State: first byte 0xA1, payload a BTreeMap<PropertyName, CommittedEntry>.
        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION_1);
        let decoded: BTreeMap<PropertyName, CommittedEntry> = bincode::deserialize(&buffer[1..]).unwrap();
        assert_eq!(decoded.get("title").map(|e| e.value.clone()), Some(Some(Value::String("alpha".into()))));
        assert_eq!(decoded.get("title").map(|e| e.event_id.clone()), Some(event_id));

        // Diff: version byte 1, payload a BTreeMap<PropertyName, Option<Value>>.
        let fresh = LWWBackend::new();
        fresh.set("title".into(), Some(Value::String("alpha".into())));
        let ops = fresh.to_operations().unwrap().unwrap();
        let LWWDiff { version, data } = bincode::deserialize(&ops[0].diff).unwrap();
        assert_eq!(version, LWW_DIFF_VERSION);
        let changes: BTreeMap<PropertyName, Option<Value>> = bincode::deserialize(&data).unwrap();
        assert_eq!(changes.get("title"), Some(&Some(Value::String("alpha".into()))));
    }

    // (f) Opacity: apply a v2 diff carrying an unknown property id (no
    // binding); it persists through to_state_buffer (0xA2) and does NOT appear
    // in property_values().
    #[test]
    fn unknown_id_persists_but_is_unprojectable() {
        let backend = LWWBackend::new();
        backend.set_wire_mode(WireMode::IdKeyedV2); // v2 emit, but NO binding

        // Craft a v2 diff carrying a property id the backend cannot project.
        let unknown = id(0xEE);
        let v2 = V2Diff { by_id: [(unknown, Some(Value::String("opaque".into())))].into_iter().collect(), residue: BTreeMap::new() };
        let diff = bincode::serialize(&LWWDiff { version: LWW_DIFF_VERSION_2, data: bincode::serialize(&v2).unwrap() }).unwrap();
        backend.apply_operations_with_event(&[Operation { diff }], EventId::from_bytes([3; 32])).unwrap();

        // Not projectable (no binding to reverse-translate the id).
        assert!(!backend.property_values().contains_key("opaque"));
        assert!(backend.properties().is_empty(), "unprojectable id omitted from properties()");

        // But it persists: 0xA2 buffer carries it under by_id.
        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_VERSION_2);
        let state: V2State = bincode::deserialize(&buffer[1..]).unwrap();
        assert_eq!(state.by_id.get(&unknown).map(|e| e.value.clone()), Some(Some(Value::String("opaque".into()))));
    }

    // (g) Broadcasts: a named-field change still fires its field broadcast
    // after the rekey.
    #[test]
    fn named_field_change_fires_broadcast_after_rekey() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let backend = LWWBackend::new();
        backend.bind_schema(title_author_binding());
        backend.set_wire_mode(WireMode::IdKeyedV2);

        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();
        // Listen by NAME even though the value keys by id.
        let _guard = backend.listen_field(
            &"title".to_string(),
            Arc::new(move |_| {
                fired_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        backend.set("title".into(), Some(Value::String("alpha".into())));
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, EventId::from_bytes([1; 32])).unwrap();

        assert_eq!(fired.load(Ordering::SeqCst), 1, "id-keyed change must still fire the name-keyed broadcast");
    }

    /// Serialize a 0xA2 buffer directly from V2State parts (bypassing a
    /// writer) so tests can shape by_id/residue/hint combinations exactly.
    fn v2_buffer(by_id: Vec<(EntityId, Option<Value>, Option<&str>)>, residue: Vec<(&str, Option<Value>)>) -> Vec<u8> {
        let state = V2State {
            by_id: by_id
                .into_iter()
                .map(|(id, value, name)| {
                    (id, V2CommittedEntry { value, event_id: EventId::from_bytes([6; 32]), name: name.map(|n| n.to_string()) })
                })
                .collect(),
            residue: residue
                .into_iter()
                .map(|(name, value)| {
                    (name.to_string(), V2CommittedEntry { value, event_id: EventId::from_bytes([6; 32]), name: Some(name.to_string()) })
                })
                .collect(),
        };
        let mut buffer = vec![LWW_STATE_VERSION_2];
        bincode::serialize_into(&mut buffer, &state).unwrap();
        buffer
    }

    /// The schema-blind projection regime of the checked read (RFC 5.4): an
    /// UNBOUND backend -- the storage engines' post-filter parse and
    /// TemporaryEntity -- reads an id-keyed 0xA2 entry by its display-name
    /// hint when exactly one entry claims the name, and fails visible
    /// (TypeSkew) when two claim it (a retype lineage). Without this, engine
    /// post-filtering and policy state inspection would silently see every
    /// id-keyed property as absent.
    #[test]
    fn unbound_checked_read_projects_unique_hint_and_skews_ambiguity() {
        // One claimant: reads through the hint.
        let unbound =
            LWWBackend::from_state_buffer(&v2_buffer(vec![(id(0x11), Some(Value::String("alpha".into())), Some("title"))], vec![]))
                .unwrap();
        assert_eq!(unbound.get_checked(&"title".to_string()).unwrap(), Some(Value::String("alpha".into())));
        assert_eq!(unbound.get_projected(&"title".to_string()), Some(Value::String("alpha".into())));
        // The plain contract-scoped read still does NOT hint-route.
        assert_eq!(unbound.get(&"title".to_string()), None);
        // A name nothing claims is absent, not an error.
        assert_eq!(unbound.get_checked(&"author".to_string()).unwrap(), None);

        // Bare Name residue outranks a hinted id entry (matches the winner
        // the property_values projection materializes on a collision).
        let collided = LWWBackend::from_state_buffer(&v2_buffer(
            vec![(id(0x11), Some(Value::String("old".into())), Some("title"))],
            vec![("title", Some(Value::String("new".into())))],
        ))
        .unwrap();
        assert_eq!(collided.get_checked(&"title".to_string()).unwrap(), Some(Value::String("new".into())));

        // Two claimants with data: ambiguous projection fails visible.
        let skewed = LWWBackend::from_state_buffer(&v2_buffer(
            vec![(id(0x33), Some(Value::String("as-string".into())), Some("title")), (id(0x44), Some(Value::I64(7)), Some("title"))],
            vec![],
        ))
        .unwrap();
        assert!(matches!(skewed.get_checked(&"title".to_string()), Err(PropertyError::TypeSkew { .. })));
        // The lenient projection refuses to guess instead of erroring.
        assert_eq!(skewed.get_projected(&"title".to_string()), None);

        // A claimant with a None value does not count (deletion tombstone).
        let deleted = LWWBackend::from_state_buffer(&v2_buffer(
            vec![(id(0x33), None, Some("title")), (id(0x44), Some(Value::I64(7)), Some("title"))],
            vec![],
        ))
        .unwrap();
        assert_eq!(deleted.get_checked(&"title".to_string()).unwrap(), Some(Value::I64(7)));
    }

    /// Bind-time collision election (codex review finding): a residue write
    /// made while the backend was unbound must not be discarded in favor of
    /// an OLDER loaded id entry just because the id key was occupied. The
    /// entity's head clock decides: a tip is causally maximal.
    #[test]
    fn bind_collision_elects_by_head_tip() {
        let (e_old, e_new) = (EventId::from_bytes([1; 32]), EventId::from_bytes([2; 32]));

        let build = || {
            let backend =
                LWWBackend::from_state_buffer(&v2_buffer(vec![(id(0x11), Some(Value::String("old".into())), Some("title"))], vec![]))
                    .unwrap();
            // Overwrite the decode-stamped provenance with e_old, then lay a
            // residue write committed as e_new (an unbound v2-mode edit).
            backend.values.write().unwrap().insert(
                PropertyKey::Id(id(0x11)),
                ValueEntry::Committed { value: Some(Value::String("old".into())), event_id: e_old.clone() },
            );
            backend.values.write().unwrap().insert(
                PropertyKey::Name("title".into()),
                ValueEntry::Committed { value: Some(Value::String("new".into())), event_id: e_new.clone() },
            );
            backend
        };

        // The residue's event is the head tip: it wins the id slot.
        let backend = build();
        backend.bind_schema_at(title_author_binding(), &Clock::new([e_new.clone()]));
        assert_eq!(backend.get(&"title".to_string()), Some(Value::String("new".into())), "head-tip residue must survive bind");

        // The id entry's event is the head tip (the residue is interior /
        // foreign): the id entry is kept.
        let backend = build();
        backend.bind_schema_at(title_author_binding(), &Clock::new([e_old.clone()]));
        assert_eq!(backend.get(&"title".to_string()), Some(Value::String("old".into())), "head-tip id entry must be kept");

        // No head knowledge: deterministic event-id tiebreak (e_new > e_old).
        let backend = build();
        backend.bind_schema(title_author_binding());
        assert_eq!(backend.get(&"title".to_string()), Some(Value::String("new".into())));
    }

    /// A BOUND backend never projects names outside its contract (RFC 5.4
    /// ruling): a hinted id-keyed entry for a name the binding does not
    /// contain stays unreadable by name, and the sibling gate still fires
    /// for names the binding DOES contain.
    #[test]
    fn bound_backend_does_not_hint_route_names_outside_its_contract() {
        let backend = LWWBackend::from_state_buffer(&v2_buffer(
            vec![
                // A foreign/hinted entry whose name is NOT in the binding.
                (id(0x55), Some(Value::String("mystery".into())), Some("legacy_hinted")),
                // A retype sibling for a name that IS in the binding: id 0x33
                // is not the binding's "title" id (0x11), but hints "title".
                (id(0x33), Some(Value::I64(7)), Some("title")),
            ],
            vec![],
        ))
        .unwrap();
        backend.bind_schema(title_author_binding());

        // Outside the contract: bare Name only -> absent, no hint routing,
        // lenient projection likewise refuses once a binding exists.
        assert_eq!(backend.get_checked(&"legacy_hinted".to_string()).unwrap(), None);
        assert_eq!(backend.get_projected(&"legacy_hinted".to_string()), None);

        // Inside the contract: the resolved id (0x11) is absent and the
        // hinted sibling (0x33) holds data -> the gate fails visible.
        assert!(matches!(backend.get_checked(&"title".to_string()), Err(PropertyError::TypeSkew { .. })));

        // An unclaimed in-contract name is absent, not an error.
        assert_eq!(backend.get_checked(&"author".to_string()).unwrap(), None);
    }
}
