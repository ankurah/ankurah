# Storage Migration: 0.8.1 -> concurrent-updates-event-dag (PR #201)

Status: proposal / decision document. Author: investigation for the #201 merge
gate. Do not treat code snippets here as final; they are illustrative.

The primary deliverable is section 1: **what ships in #201**. Everything after
is the evidence and the follow-up design.

## Decision (2026-07-04, maintainer)

The versioning ships in #201 at the artifact that actually broke, not at the
engine or system level: **every serialized LWW state buffer is prefixed with a
one-byte version header** (`core/src/property/backend/lww.rs`). Versions are
offset high (`0xA1` = version 1) so that unversioned pre-0.9 buffers, whose
first byte is a small bincode property count, are classifiable from a single
byte with no parse-probing. Deserialization refuses unversioned and
unknown-version buffers with explicit errors. The migrator itself is deferred
to a follow-up PR (0.9.x), which will use the header (absence of a valid
version = pre-0.9 buffer needing the LWW event_id synthesis below). The
per-engine meta-marker design in section 3 is therefore superseded; the rest
of this document (compatibility inventory, synthesis rule, per-engine sweep
mechanics, wire-compat caveat) remains the plan of record for the follow-up.

**Amendment (same day):** rather than refusing unversioned buffers,
`from_state_buffer` now falls back to parsing them as the legacy map and
stamps loaded values with an all-zeros sentinel event id. The sentinel
supersedes section 5's head-stamp recommendation: because pre-0.9 histories
are linear, older-than-meet auto-lose reproduces true-provenance election
outcomes exactly, and does so identically on every replica -- head stamping
does not (a replica that rebuilds provenance by replaying events derives the
true per-property stamps and can elect a different winner than a replica
stamped with its local head). Stores upgrade lazily on the next state save;
the follow-up migrator (ankurah#284) reduces to an optional bulk sweep for
stores that want eager rewriting.

---

## 0. TL;DR

- The **only** on-disk format break in #201 is inside the opaque `"lww"` state
  buffer. Everything else -- events, `EntityState`, the `head`/`parent` clocks,
  Yrs buffers, attestations, all per-engine table/tree schemas -- is
  byte-identical to 0.8.1. Verified by empty `git diff main...HEAD` on
  `proto/src/data.rs`, `proto/src/id.rs`, all of `storage/*/src/`, and
  `core/src/value/`.
- Old LWW buffers **fail loud, not silent**: feeding a 0.8.1 LWW buffer (any
  entity with >=1 property) to the new `LWWBackend::from_state_buffer` yields a
  clean `bincode` "unexpected end of file" error that becomes
  `RetrievalError::DeserializationError` at entity-load time. It is a `Result`,
  never a panic, never a misparse-with-data. Proven empirically (section 8).
  The single ambiguous case is an empty property map (0 properties), which is
  byte-identical in both formats and harmless.
- Adopting the maintainer's directive ("do not deserialize until we know the
  encoding-scheme version"): **version-gated deserialization is the
  architecture.** The only detection that ever exists is a one-time bootstrap
  classification: *absence of a version marker = pre-versioning era = 0.8.1
  format.*

**Recommendation: ship option (c+) in #201 -- the store-level encoding-version
marker plus version-gated refuse -- and land the actual LWW migrator as an
immediate follow-up before the next published release.** The marker is close to
mandatory in #201 regardless of the loud-failure finding, because #201 changes
the format anyway; landing the new format unversioned would force us to
disambiguate *two* unversioned eras at the next format change. The migrator is
separable because the format break is small, well-isolated, and already
fails safely.

---

## 1. Recommendation: what ships in #201

### The three options, evaluated concretely

**(a) Full migration in #201.**
Code required: the version marker (below), the read-path gate, plus a working
LWW migrator for all four engines (sled shared `entities` tree rewrite;
postgres/sqlite `state_buffer` column rewrite per collection table; indexeddb
in-browser object-store rewrite), plus the event-id synthesis rule (section 5),
plus a backup/rollback story, plus migration tests per engine.
Rough size: 600-1000 LOC across `core/` and all four `storage/` crates, with
the indexeddb path being WASM-only and the hardest to test in CI.
Risk to the merge: **high.** #201 is already a large, carefully-reviewed
concurrency change (+11.8k/-2.1k). Bolting a four-engine migrator onto it
widens the review surface into storage code that #201 otherwise does not touch
(the `storage/*/src/` diff today is *only* Cargo version bumps). Migration bugs
are silent-data-loss bugs; they deserve their own PR and their own review
attention, not to ride in on a concurrency merge.

**(b) Nothing in #201.**
Merge as-is; migrator as a follow-up. Anyone on git `main` with 0.8.1 data
breaks in the interim.
Risk: the break is loud (a load-time `RetrievalError`, section 8), so this is
not a silent-corruption risk. But it leaves the **new** format unversioned. At
the *next* format change we will face the same "which unversioned era is this
store?" problem, now with two overlapping unversioned eras (0.8.1 and #201-new)
that are only distinguishable by attempting a parse -- exactly the heuristic the
maintainer has ruled out. (b) borrows against the next migration.

**(c) Minimum-viable insurance in #201: version marker + version-gated refuse.**
Ship a store-level encoding-scheme version record written by the new code, and a
read path that checks it *before* deserializing. On opening a store whose marker
is absent (pre-versioning = 0.8.1) or whose value is a version this build does
not migrate, refuse with a clear, named error pointing at the migration
path/issue. The actual data rewrite is the follow-up.
Risk to the merge: **low.** It touches each engine's open/bootstrap path with a
small, self-contained record read/write plus one guard. No data is rewritten. It
does not alter the concurrency logic under review.

### Recommendation: (c), extended to (c+)

Ship in #201: **the encoding-scheme version marker + version-gated refuse.** Do
**not** ship the data migrator in #201; land it as the immediate next PR, gating
the next published release on it.

I label this **(c+)** rather than plain (c) for one reason: the marker written
by the new code must stamp the *new* encoding version, so that the follow-up
migrator (and every future migrator) has an unambiguous version to switch on.
The refuse behavior for existing (unmarked) stores is the same as (c). The
"insurance" and the "version everything going forward" work are the same code,
so there is no reason to do less than (c+).

### The two or three facts that drive this

1. **The format break is tiny and isolated.** Verified: the *only* changed
   serialization is the inner `"lww"` buffer. Events and the `State` envelope
   are unchanged (section 2, section 4). This is what makes the migrator
   separable from #201 -- it is not entangled with the concurrency change.

2. **Old data already fails loud, not silent** (section 8, empirical). This
   *weakens* the urgency of shipping the *migrator* in #201 (nobody silently
   corrupts), but it does **not** weaken the case for the *marker*. A loud
   `bincode` EOF error names nothing and points nowhere; a version gate can say
   "0.8.1-format store detected, run migration X / see issue Y." The marker
   upgrades a confusing-but-safe failure into an actionable one, cheaply.

3. **#201 changes the format, so #201 is the correct place to introduce
   versioning.** If the new format lands unversioned, we create a *second*
   unversioned era. The whole "absence of marker = 0.8.1" bootstrap trick only
   works cleanly if there is exactly one unversioned era. Miss the window and
   every future migration pays for it. This is the decisive fact per the
   maintainer's directive.

### Effort estimate for the recommended #201 scope (marker + refuse)

- `core/`: a small `EncodingVersion` newtype/const and a shared "current
  encoding version" constant. ~30 LOC.
- Sled: add a `meta` tree; on `Database::open`, read `meta["encoding_version"]`;
  if absent AND any of `entities`/`events` is non-empty -> refuse; if absent and
  store is empty -> stamp current; if present and < current -> refuse with
  migration message. ~60-80 LOC in `storage/sled/src/database.rs` +
  `engine.rs`.
- Postgres/sqlite: a one-row `_ankurah_meta(key text primary key, value ...)`
  table created alongside the first collection's tables; same absent/empty/stale
  logic. ~80-100 LOC each, mostly SQL boilerplate, largely shared via
  `storage/common`.
- IndexedDB: reuse the **native** IDB schema version already present
  (`open_with_u32`, `db.version()`; section 3) or a row in a `meta` object
  store; refuse in the async open path. ~60-100 LOC WASM.
- Tests: an "open a seeded 0.8.1 store -> get the refuse error, not an EOF" test
  per engine. sled and postgres/sqlite are straightforward; indexeddb is
  WASM-only.

Total: roughly **300-450 LOC**, dominated by four near-parallel copies of the
same small open-path guard, plus tests. Reviewable in isolation. No change to
the concurrency logic. This is the number I would quote the maintainer for the
#201 addition.

The follow-up migrator (separate PR) is the larger, riskier piece and is scoped
in sections 4-6.

---

## 2. Format delta inventory

Verified against `git diff main...HEAD` and engine code. "Deserializes?" means:
does 0.8.1-written bytes load under the new code. "Semantically valid?" means:
is the loaded value correct/usable. Actions assume the recommended path (marker
now, migrator follow-up).

| Artifact | Deserializes under new code? | Semantically valid? | Action needed |
|---|---|---|---|
| **LWW state buffer** (`"lww"` entry) | **No** -- clean `bincode` EOF error for any entity with >=1 property (empty map is the only exception, and is byte-identical). Source: 0.8.1 wrote `bincode(BTreeMap<PropertyName, Option<Value>>)`; branch reads `bincode(BTreeMap<PropertyName, CommittedEntry{value, event_id}>)`. | No | **Migrate**: re-encode adding a synthesized `event_id` per property (section 5). This is the whole migration. |
| **Yrs state buffer** (`"yrs"` entry) | **Yes** -- `to_state_buffer` is byte-identical (yrs doc encoding). Only `apply_operations` signature changed and `apply_layer` was added; neither touches on-disk bytes. | Yes | **None.** Confirmed by `git diff` of `yrs.rs`: no `to_state_buffer`/`from_state_buffer` change. |
| **PN counter buffer** | N/A -- backend is commented out of `backend_from_string` on both branches (dead). | N/A | None. |
| **`State` envelope** (`{ state_buffers: BTreeMap<String,Vec<u8>>, head: Clock }`) | **Yes** -- `proto/src/data.rs` byte-identical between main and HEAD. | Yes (outer envelope); inner `"lww"` bytes invalid until migrated. | None for the envelope. Migration operates on the inner `"lww"` value only. |
| **`head` clock** (embedded in `State`) | **Yes** -- `Clock` wire shape is still `Vec<EventId>`. The branch added only `#[serde(from = "Vec<EventId>")]` (deserialize-side normalization: sort+dedup) and a `remove()` method. Serialize side is unchanged. Reading old clocks just re-sorts them (a no-op for already-sorted 0.8.1 clocks; `from_strings` already sorted in 0.8.1). | Yes | **None.** See section 6. |
| **Stored events** (`Attested<Event>` / `EventFragment`: `operations`, `parent` clock, attestations) | **Yes** -- `Event`, `OperationSet`, `Operation`, `Clock`, `Attested`, `AttestationSet` all byte-identical. `parent` is a `Clock`, same normalization-on-read as above. | Yes | **None.** Events fully compatible. This is the single most important compatibility fact (section 6). |
| **`EventId` + hashing** | **Yes** -- `EventId` type and `EventId::from_parts` (sha256 over `entity_id`+`operations`+`parent`) byte-identical. No read/ingest path recomputes-and-verifies id == hash (grep found only `policy_agent.check_read`, no hash re-verification). | Yes | **None.** Migrating state buffers does not touch events, so no event id is invalidated. |
| **Attestations** (`AttestationSet`) | **Yes** -- unchanged; stored alongside state (sled `StateFragment.attestations`, postgres `attestations` column) and events. | Yes | None. |
| **Sled projection tree** (`collection_{id}`: `Vec<(u32, Value)>` per entity) | **Yes** -- derived from `entity.values()` = bare `Value`s; format unchanged (`Value` enum unchanged). | Yes | **None; survives.** Values are provenance-free, so adding `event_id` to the source buffer does not change them. Regeneration optional, not required (property ids unchanged). |
| **Sled `property_config` / `index_config` trees** | **Yes** -- untouched by #201. | Yes | None. |
| **Per-engine table schemas** (postgres `{c}`/`{c}_event`, sqlite same, indexeddb object stores) | **Yes** -- `storage/*/src/` diff is Cargo bumps only; no DDL change. | Yes | None. Only the `state_buffer` *column value* (the `"lww"` sub-bytes) needs rewriting. |
| **Per-engine metadata / version marker** | **Absent today** (grep for `*version*`/`*meta*`/`migration` in `storage/`+`core/src` = empty). | -- | **Introduce** (section 3). This is the #201 deliverable. |

Bottom line: **one artifact needs migrating (LWW buffer); one artifact needs
introducing (version marker); everything else is compatible.**

---

## 3. Detection = version-gated refuse (per maintainer directive)

There is no runtime format sniffing. bincode is positional and not
self-describing, and the maintainer's directive is explicit: **do not attempt
deserialization until the encoding-scheme version is known.** So "detection"
collapses to a single, one-time bootstrap classification, plus a version check
forever after:

> **Bootstrap rule (used exactly once per store, ever):** a store with **no
> encoding-version marker** is, by definition, from the pre-versioning era =
> **0.8.1 format.** A store *with* a marker is whatever the marker says.

No try-parse-old-then-new heuristic is used, even as a fallback. The empirical
misparse/error experiment (section 8) is *not* a detection mechanism under this
architecture; it only quantifies how bad an ungated upgrade is (the urgency
argument in section 1), which is why the marker is worth shipping now.

### The read path refuses ahead of deserialization

At store open (or first collection access), before any `from_state_buffer`
call:

1. Read the encoding-version marker.
2. If **absent**: the store is either empty (fresh) or 0.8.1.
   - Empty store -> stamp `current` and proceed.
   - Non-empty store -> **this is the 0.8.1 case.** In #201 (marker+refuse),
     return a clear error naming the migration path. In the follow-up
     (migrator present), run the migration, then stamp `current`.
3. If **present and == current** -> proceed normally.
4. If **present and < current** and this build has a migrator for that
   version -> migrate, then stamp. Otherwise -> refuse with a named error.
5. If **present and > current** (store written by a newer build) -> refuse:
   "store written by a newer version; upgrade the binary."

The refusal must be a distinct, greppable error (e.g.
`RetrievalError::IncompatibleStoreVersion { found, expected, migration }`), not a
bincode error.

### Where the marker lives, per engine

| Engine | Marker home | Notes |
|---|---|---|
| **Sled** | New `meta` tree, key `"encoding_version"` -> `u32` (or a small struct). Read in `Database::open` (`storage/sled/src/database.rs`) right after opening the existing `entities`/`events`/`property_config`/`index_config` trees. | Cheapest possible: sled trees are created on demand. Absence of the key in a store that already has a populated `entities` tree = 0.8.1. |
| **Postgres** | One-row table `"_ankurah_meta"(key text primary key, value bytea|int)`, created in the collection-bootstrap path alongside `create_state_table`/`create_event_table` (`storage/postgres/src/lib.rs`, ~line 124). Existence of any collection table without the meta row = 0.8.1. | Schema detection already uses `information_schema`; reuse it to tell "empty database" from "existing 0.8.1 database." |
| **SQLite** | Same `_ankurah_meta` table, OR SQLite's built-in `PRAGMA user_version` (a free 32-bit int in the DB header, purpose-built for exactly this). `user_version` is the idiomatic choice and needs no table. | Recommend `PRAGMA user_version` for sqlite specifically -- zero schema footprint. |
| **IndexedDB** | Prefer the **native IDB schema version** (`open_with_u32`, `db.version()`; already used in `storage/indexeddb-wasm/src/database.rs`). Alternatively a row in a `meta` object store. | Caveat: the native version is *currently* bumped for index-schema changes (`open_with_index`, `current_version + 1`). Encoding-scheme version and index-schema version are therefore **entangled** on this engine. See "version everything separately" below -- for indexeddb a dedicated `meta` object-store key is cleaner and keeps encoding-version independent of index bumps. |

### Version state buffers and events separately (recommended)

The maintainer flagged this and the format delta confirms its value: **events
are compatible while state buffers are not.** If we version them together, a
future change that touches only events forces a state-buffer "migration" that is
a no-op, and vice versa. Recommendation:

- Keep a **store-level** marker as the coarse gate (cheap, answers "can this
  binary open this store at all?").
- Additionally reserve room for **per-artifact-class versions** -- at minimum
  distinguish `state_encoding_version` from `event_encoding_version`. In #201
  these can be two fields in the same meta record; the store-level gate reads
  both. This lets the #201-follow-up migrator rewrite only state buffers and
  bump only `state_encoding_version`, leaving events untouched and un-migrated.

Per-record envelopes (versioning each individual buffer) are the most robust
against partial migrations but the most invasive (they change the stored bytes
of every artifact and re-introduce a format change). Given the break is confined
to `"lww"` and migration is a full rewrite (not partial), a **store-level marker
with separate state/event version fields is the right cost/robustness point for
#201.** Per-record envelopes can be revisited if a future change genuinely needs
independent per-artifact migration.

---

## 4. Migration mechanics (follow-up PR)

One-shot at engine open, gated by the version marker, is recommended over lazy
per-entity migration. Rationale: the write path (`Entity::to_state` ->
`LWWBackend::to_state_buffer`) will **error** the moment a migrated-but-unstamped
entity is re-persisted (section 5), so half-migrated stores are hazardous; a
one-shot pass under the open guard keeps the store in exactly one of two states.

### Per engine

- **Sled.** Iterate the shared `entities` tree. For each value, deserialize
  `StateFragment`, take `state.state_buffers["lww"]` if present, run the LWW
  buffer transform (section 5), re-serialize `StateFragment`, write back. The
  `events` tree is untouched. The `collection_{id}` projection trees are
  **not** rewritten -- their `(property_id, Value)` tuples are unchanged because
  the migration adds only provenance (`event_id`) to the buffer, not new
  `Value`s. (Confirmed: projection is built from `entity.values()`, which are
  bare `Value`s.) Stamp `meta["encoding_version"]` at the end, inside the same
  logical pass.

- **Postgres.** Per collection state table `"{collection}"`: `SELECT id,
  state_buffer`, deserialize the `StateBuffers` envelope, transform the inner
  `"lww"` bytes, re-serialize, `UPDATE ... SET state_buffer = $1 WHERE id = $2`.
  Do it inside a transaction per table (or one transaction total). Event tables
  (`"{collection}_event"`) untouched. Property columns (materialized) are
  derived and unchanged. Insert the `_ankurah_meta` row at the end.

- **SQLite.** Identical to postgres against the bare `{collection}` table; bump
  `PRAGMA user_version` at the end.

- **IndexedDB.** In-browser, in an upgrade/versionchange transaction: open a
  cursor over the shared `entities` object store, transform each record's
  `"lww"` buffer, `put` it back. `events` object store untouched. Bump the
  encoding marker (dedicated meta key recommended, per section 3). This is the
  only engine where migration runs in the user's browser on first load of the
  new app build; it must be idempotent and resumable-ish (the version gate makes
  it run-once).

### Backup / rollback

- **Sled / IndexedDB (embedded, no server DBA):** because migration mutates in
  place, take a copy first. Sled: copy the DB directory before the pass (or
  export the `entities` tree). IndexedDB: this is harder (no cheap file copy);
  mitigate by making the transform provably reversible-free (it is not
  reversible -- see section 5 -- so the honest story is "back up via app-level
  export if available, otherwise forward-only"). Document forward-only for
  indexeddb.
- **Postgres:** rely on the operator's normal backup (`pg_dump`) before
  upgrading. The migrator should refuse to run without an explicit opt-in flag
  or at least log loudly.
- **Rollback:** the transform is **not** losslessly reversible (it *adds*
  synthesized `event_id`s that were never in the old buffer; dropping them
  returns to the old format bit-for-bit, so a *downgrade* migrator is trivial IF
  needed). Practical rollback = restore the pre-migration backup. Provide a
  down-migration only if a released build ever needs to interoperate backward.

---

## 5. The LWW `event_id` synthesis rule

Old buffers have no per-property `event_id`; the new format requires one per
property, and -- critically -- **the write path enforces it**:
`LWWBackend::to_state_buffer` returns `StateError::SerializationError("LWW state
requires event_id for property {name}")` if any property lacks an `event_id`.
So migration cannot leave `event_id` absent; it must synthesize one. And
`Entity::to_state` calls `to_state_buffer` on every persist, so an entity whose
buffer we migrated but whose `event_id`s are wrong will still *save* -- the
question is purely about *merge semantics*, not loadability.

### How the id is consumed (from `lww.rs::apply_layer`)

When a concurrent merge runs, each stored property is seeded as a candidate with
its `event_id`. Then:
- If `layer.dag_contains(&event_id)` is **false**, the stored value is marked
  `older_than_meet` and **any** layer candidate beats it unconditionally
  (lww-merge rule 1).
- If **true**, normal causal comparison applies (rule 2: descendant wins; rule
  3: concurrent -> higher `EventId` wins).

`dag_contains(id)` is true iff `id` is a key in the accumulated DAG, which is
built by walking parent pointers back from the events being merged. A stored
`event_id` that is a real ancestor of the incoming events *will* be in that DAG.

### Option (a): stamp every property with the entity's head event id

0.8.1 sets `state.head = event.id().into()` after each local commit, so a
single-writer store's head is a **single tip** whose event exists in the
`events` store (verified in `git show main:core/src/entity.rs`). Multi-tip heads
arise only from prior concurrent merges; for those, stamp each property with
**one** representative tip (e.g. the max `EventId` in the head, deterministic).

Consequence: the synthesized id is a **real event in the DAG**. In any future
merge, `dag_contains` returns true, and the migrated value participates in
normal causal comparison at its true DAG position -- it behaves exactly as if
the last writer had stamped it (which, semantically, they did: that head event
*is* the last write the entity saw). This is **correct** and makes migrated data
first-class.

Caveat to verify in the migrator: a property might have last been written by an
*ancestor* of the head, not the head event itself. Stamping it with the head
event id makes it look *newer* than it really was. In practice this is benign:
the head event causally dominates every prior write to the entity, so treating
all current values as "written at the head" preserves the invariant that the
current state is the state as of the head. The only observable difference would
be in an exotic concurrent merge that reaches *between* the true write and the
head -- impossible, because nothing can be concurrent with an event that is an
ancestor of the current head. **(a) is safe.**

### Option (b): sentinel id absent from the DAG

Stamp with an id guaranteed **not** in any DAG. Then `dag_contains` is always
false -> `older_than_meet` -> the migrated value **auto-loses to any later
concurrent write.** Rationale would be "treat pre-migration values as maximally
stale." But this is wrong in the common, non-concurrent case: a migrated value
should not be pre-emptively second-class. If a user migrates and then makes a
*non-concurrent* edit to a *different* property, nothing is concurrent and (b)
never bites -- fine. But the first time two replicas diverge across the
migration boundary, (b) silently discards the migrated side even when it was the
genuinely later write. (b) also corrupts the "current state = state as of head"
invariant the moment any concurrent event lands.

### Recommendation: (a)

Stamp every property with the entity's head event id (single tip; for multi-tip
heads, the deterministic max tip). It yields correct merge semantics, keeps
migrated data first-class, and relies only on facts we verified: 0.8.1 heads are
single-tip for single-writer stores, and the head event is present in the
`events` store so `dag_contains` resolves true in future merges.

Do **not** use (b) unless a deliberate product decision is made that all
pre-migration data should lose to post-migration concurrent writes -- there is
no correctness reason to want that.

---

## 6. Events: compatible, no transform needed

The feared dominant risk -- that `Clock`'s shape changed and stored events no
longer deserialize -- **did not materialize.**

- `proto/src/clock.rs` changed +92 lines, but the change is: `#[serde(from =
  "Vec<EventId>")]` (deserialize-side normalization), a `normalized()` helper
  (sort+dedup), a `remove()` method, and tests. **The serialized shape is still
  `Vec<EventId>` and the serialize path is unchanged.** Old clocks read back
  fine; they are simply re-sorted on load (a no-op for 0.8.1 clocks, which were
  already sorted by `from_strings`/`TryInto`).
- `Event`, `EventFragment`, `OperationSet`, `Operation`, `EventId`, `Attested`,
  `EntityState`, `State`, `StateBuffers`: **byte-identical** (`proto/src/data.rs`
  diff empty).
- `EventId::from_parts` hashing unchanged; no ingest path recomputes and
  verifies id == hash(content), so nothing rejects old events on a hash
  mismatch (there is none anyway).

Therefore the migration is **state-only**, and there is no loss of event
history: `DivergedSince`/deep-merge logic that needs events keeps working
against the untouched `events` store. The honest "if events were incompatible"
fallback (state-only migration with opaque pre-migration history) is **not
needed** -- events are fully compatible.

One nuance to note for the migrator author: because events are untouched and
`event_id`s are unchanged, the head-event ids that option (a) stamps into
migrated buffers **match real event ids in the store**, which is exactly what
makes `dag_contains` resolve correctly. The event compatibility and the LWW
synthesis rule reinforce each other.

---

## 7. Wire compatibility during a rolling upgrade (flagged, not solved)

Out of scope to solve, but flagged: **can a 0.8.1 node and a #201 node
interoperate live?**

- Events on the wire (`Attested<Event>`, `EventFragment`) are shape-compatible
  (section 6), so event gossip *decodes* both ways.
- But a #201 node that *ingests* a 0.8.1-authored event and then persists the
  resulting entity must produce a valid new-format LWW buffer, which needs
  `event_id`s. The apply path (`apply_operations_with_event`) stamps the
  *incoming* event's id, so freshly-ingested events are fine. The hazard is a
  #201 node loading a *locally-stored* 0.8.1 buffer (covered by migration) vs.
  events arriving over the wire (fine).
- A 0.8.1 node receiving a #201-authored **state** snapshot
  (`Attested<EntityState>`, e.g. via state sync rather than event replay) would
  get an LWW buffer it cannot parse -- the mirror of our load problem, in the
  *old* binary, which has no version gate and will bincode-EOF.
- There is a `Presence` handshake (`proto/src/peering.rs`) carrying
  `node_id`/`durable`/`system_root`, but **no protocol/encoding version field**
  was found. Adding one is the clean fix for rolling upgrades but is a separate
  concern from on-disk migration.

Recommendation: treat 0.8.x <-> #201 as **not wire-compatible for state
transfer** and document "upgrade all nodes together, or drain before upgrade"
until a protocol-version handshake exists. Do not block #201 on this.

---

## 8. Empirical: does old LWW data misparse or fail loud?

This is the one fact worth an experiment, because it calibrates the *urgency* of
shipping the migrator (not the mechanism -- the mechanism is version-gating
regardless). Throwaway crate in `/private/tmp/lww-repro`, bincode 1.3.3 (the
workspace version), modeling the exact shapes: 0.8.1 =
`BTreeMap<String, Option<Value>>`, branch =
`BTreeMap<String, CommittedEntry{value: Option<Value>, event_id: [u8;32]}>`.

Serialize old, attempt to deserialize as new:

```
[1 single-string] old len=38  -> Err (CLEAN FAIL): unexpected end of file
[2 two-props]     old len=58  -> Err (CLEAN FAIL): unexpected end of file
[3 none-value]    old len=24  -> Err (CLEAN FAIL): unexpected end of file
[4 empty-map]     old len=8   -> Ok  (0 entries; bytes identical in both formats)
[5 large-binary]  old len=73  -> Err (CLEAN FAIL): unexpected end of file
[6 bool]          old len=26  -> Err (CLEAN FAIL): unexpected end of file
[7 six-i16]       old len=110 -> Err (CLEAN FAIL): unexpected end of file
```

Then a brute-force sweep (0-4 properties x 7 value kinds x sizes {0,1,31,32,33,64}):

```
=== Any misparse that produced NON-EMPTY data? false ===
empty old bytes == empty new bytes ? true  (both = [0,0,0,0,0,0,0,0])
```

**Findings:**
- Every old buffer with >=1 property fails with a clean `bincode` "unexpected
  end of file." No configuration in the swept space produced a misparse that
  yielded non-empty data. The `event_id` field (32 trailing bytes per entry
  that the old format never wrote) reliably triggers EOF, and the map's u64
  length prefix pins the entry count so trailing bytes cannot be silently
  re-interpreted as a smaller map.
- The **only** non-error case is the empty property map (0 properties), which is
  byte-identical across formats and decodes to 0 entries -- harmless and
  genuinely ambiguous (there is nothing to migrate).

**In-tree path this maps to:** `LWWBackend::from_state_buffer` does
`bincode::deserialize::<BTreeMap<PropertyName, CommittedEntry>>(state_buffer)?`;
the `?` converts `bincode::Error` via `From<bincode::Error> for RetrievalError`
into `RetrievalError::DeserializationError`, propagated up through
`backend_from_string` and `Entity::from_state`. It is a `Result`, never a panic.

**Implication (urgency, not mechanism):** an ungated upgrade does not silently
corrupt -- it fails at entity-load with a typed error. That is why the *migrator*
can be a fast-follow rather than a merge blocker. But the error names nothing
useful; the **version marker** (shipped in #201) is what turns "bincode EOF at
load" into "0.8.1 store, run migration X," which is the actionable behavior the
maintainer wants. The experiment therefore *supports shipping the marker now and
the migrator immediately after*, and argues against needing runtime parse-probe
detection at all.

---

## 9. Riskiest assumptions (re-verify before implementing)

1. **Single representative tip is safe for multi-tip 0.8.1 heads.** Verified
   that single-writer heads are single-tip and that stamping with the head event
   yields correct `dag_contains` behavior. **Not** exhaustively verified: that
   picking one tip (max `EventId`) for a genuinely multi-tip migrated entity
   never produces a worse merge outcome than the "true" per-property writer id.
   Argued benign in section 5; worth a targeted test with a multi-tip fixture.

2. **The empty-map ambiguity is truly harmless in situ.** Verified the bytes are
   identical and decode to 0 entries. Assumed: a real entity never legitimately
   has an empty LWW map that must be distinguished from "fresh." Since an empty
   map migrates to an empty map (nothing to stamp), this is almost certainly
   fine, but the migrator should treat empty `"lww"` as a no-op explicitly.

3. **IndexedDB native version vs. encoding version can be cleanly separated.**
   The engine already bumps the native IDB version for index-schema changes.
   Assumed we can carve out an independent encoding-version marker (dedicated
   meta key) without colliding with index bumps. Needs a WASM spike; it is the
   least-tested engine and the one that migrates in users' browsers.

Secondary: that no *other* consumer writes/reads the `"lww"` buffer format
outside `LWWBackend` (grep supports this -- only `backend_from_string` dispatches
to it), and that postgres/sqlite never stored an LWW buffer anywhere but the
`state_buffer` column (confirmed: single write site).

---

## 10. Open questions (could not fully pin down in timebox)

- **Exact meta-record schema and shared plumbing.** Whether the version
  marker should be a bare `u32`, a `{state_version, event_version}` struct, or a
  general `BTreeMap<String, Vec<u8>>` meta bag differs slightly per engine
  (sqlite `user_version` is a single int; sled/postgres/idb can hold a struct).
  Recommend a small struct with room for both versions; final shape is an
  implementation decision.
- **Does any existing test fixture or template embed a 0.8.1 sled store** that
  would let us test the refuse path against *real* bytes rather than
  synthesized ones? `~/ak/ankurah-react-sled-template-0.8.1` pins 0.8.1 crates
  but I did not confirm it contains a populated on-disk store (its `.gitignore`
  likely excludes the data dir). Generating a fixture with the 0.8.1 binary is
  the reliable route for the migrator's tests.
- **Rolling-upgrade / protocol-version handshake** (section 7) -- deliberately
  left unsolved; needs a proto-level decision independent of on-disk migration.
- **Whether the follow-up migrator should be forward-only or offer a
  down-migration.** The transform is trivially invertible (drop the `event_id`s),
  so a down-migrator is cheap if any released build ever needs backward interop;
  not needed for the immediate release.
