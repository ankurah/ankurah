## Clarifying Questions & Design Decisions

### Resolution Status (Phase 1)

| Question | Status | Resolution |
|----------|--------|------------|
| Q4: Json builtin type | ✅ Resolved | `Json` wraps `serde_json::Value`, stored as `Value::Binary` |
| Q5: Json query integration | ✅ Resolved | Option A implemented: deserialize at eval time, no schema validation |
| Q6: First milestone | ✅ Resolved | Structured property navigation (Option B) delivered |
| Q1-Q3: Ref type design | ⏳ Pending | Deferred to Phase 2 |
| Q7-Q10: Schema registry | ⏳ Pending | Deferred to Phase 3 |

---

### Critical Prerequisites

#### Q1: How do we know the target collection for a Ref field?

**Problem**: To traverse `artist.name` from an Album, we need to:
1. Get `album.artist` → EntityId  
2. Know which collection to fetch from (e.g., "artist")
3. Fetch that entity and continue

Currently, **EntityId does not encode collection**. `get_entity()` requires both:
```rust
async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool)
```

**Options**:

| Option | Pros | Cons |
|--------|------|------|
| A. Convention: field name = collection | Zero infrastructure | Fragile, inflexible |
| B. Field annotation: `#[model(ref_to = "artist")]` | Minimal infrastructure, derive-time | Still need runtime access |
| C. Minimal ref registry (field→collection map per model) | Lighter than full schema | Another registry to maintain |
| D. Full schema registry (Phase 2) | Complete solution | Delays first milestone |

**Recommendation**: Option B with a minimal runtime map. The derive macro emits:
```rust
impl AlbumView {
    pub fn ref_targets() -> &'static [(&'static str, &'static str)] {
        &[("artist", "artist")]  // (field_name, collection_id)
    }
}
```

This can be used at query evaluation time without a full schema registry.

**Decision needed**: Which option to pursue?

---

#### Q2: What is the `Ref<T>` type?

**Current state**: Models use raw `EntityId` for references:
```rust
pub struct Album {
    pub name: String,
    pub artist: EntityId,  // Is this the intended pattern?
}
```

**Options**:

| Option | Syntax | Notes |
|--------|--------|-------|
| A. Keep `EntityId`, annotate with attribute | `#[model(ref_to = "Artist")] artist: EntityId` | Explicit, no new type |
| B. New `Ref<T>` wrapper | `artist: Ref<Artist>` | Type-safe, self-documenting |
| C. Both (Ref is sugar for annotated EntityId) | Either works | Flexibility |

**Decision needed**: Define the canonical ref representation.

---

#### Q3: What about `Vec<Ref<T>>` / MultiRef?

Same question for multi-valued references. Is it:
- `Vec<EntityId>` with annotation?
- `Vec<Ref<Artist>>`?
- `Refs<Artist>` (custom collection type)?

**Decision needed**: Define multi-ref representation.

---

### Json Type

#### Q4: Should `Json` be a first-class builtin type?

**Current state**: JSON is used ad-hoc via `serde_json::Value` in enums:
```rust
pub enum Payload {
    Text(String),
    Json(serde_json::Value),
}
```

**Proposal**: Introduce `ankurah::Json` as a property type:
```rust
pub struct Track {
    pub name: String,
    pub licensing: Json,  // or Json<LicensingSchema>
}
```

**Open questions**:
- Does `Json` need a schema type parameter? `Json<T>` vs `Json`
- Which backend? LWW? Or Yrs for collaborative editing?
- Should it support `Any` (schemaless) and typed variants?

**Decision needed**: Define the Json type design.

---

#### Q5: How does Json integrate with queries?

For `licensing.territory = ?`:

**Option A**: Json stores as `Value::Object(Vec<u8>)`, deserialize during query evaluation
- Simple implementation
- No schema validation at query time

**Option B**: Json has typed schema, validate paths at query time
- Better error messages
- More complex

**Option C**: `StructuredProperty` trait abstraction
```rust
pub trait StructuredProperty {
    fn get_path(&self, path: &[&str]) -> Option<Value>;
}
```
- Extensible to other nested types
- Deferred complexity

**Recommendation**: Start with Option A, evolve to C later.

**Decision needed**: Confirm approach.

---

### Phasing Questions

#### Q6: What is the minimal viable first milestone?

**Original assumption**: Forward ref traversal (`artist.name = ?`)

**Revised options**:

| Milestone | Prerequisites | Effort |
|-----------|---------------|--------|
| A. Parser only (syntax support, no evaluation) | None | 1-2 days |
| B. Structured property navigation (`licensing.territory`) | Json type, Value path extraction | 2-3 days |
| C. Forward ref traversal (`artist.name`) | Ref metadata (Q1), entity fetcher integration | 3-4 days |

**Observation**: Structured navigation (B) might be simpler than ref traversal (C) because it doesn't cross entity boundaries.

**Decision needed**: Which milestone first?

---

#### Q7: Is full schema registry needed for basic ref traversal?

**No**, if we use the minimal ref registry approach (Q1 Option B/C).

The full schema registry (Phase 2 in current plan) becomes needed for:
- Structured property schema validation
- Query-time path validation
- Compile-time ID injection
- Inbound navigation validation

**Recommendation**: Defer full schema registry; start with minimal ref metadata.

---

### Architecture Questions

#### Q8: Async in filter evaluation?

Current `Filterable::value()` is sync. Ref traversal requires entity fetching, which is async.

**Options**:
- Make filter evaluation async throughout
- Prefetch referenced entities before evaluation
- Cache fetched entities during a single query evaluation

**Decision needed**: Async strategy for filter evaluation.

---

#### Q9: Should path resolution happen at parse time or evaluation time?

**Parse time**: Requires schema access in parser (or a separate resolve pass)
**Evaluation time**: Simpler, but repeated work and late errors

**Recommendation**: Separate resolve pass after parsing, before evaluation. This is the "Resolved AST" approach in the current plan.

---

---

### Resolved Questions from Draft Audit

#### Q10: `[:Type]` Relation Type Selector

**Decision**: NOT INCLUDED. Relation types should not be treated specially from any other predicate. Use a `kind`/`type` field and filter like anything else:
```
album.contribution[kind = "ProducerCredit" AND role = "executive"]->person.name = ?
```

---

#### Q11: ALL Semantics for Multi-valued Traversals

**Decision**: ANY is the default. ALL semantics are only relevant for:
- Inbound traversals: `^Album.artist[ALL: year > 2000]`
- Multi-refs: `genres[ALL: name = "Rock"]`

If needed in future, ALL should be a **modifier on the traversal filter**, not a separate construct. Out of scope for this project.

---

#### Q12: Compile-time ID Injection

**Decision**: Non-goal for this project. This was one argument against making the AST immutable (to allow future compile-time ID injection), but implementing it is not part of this scope.

---

#### Q13: Stale ID / Schema Evolution

**Decision**: Omitted. Not relevant since we're not implementing PropertyId-based resolution in Phase 1.

---

### Summary: Open vs Decided

**Phase 1 — All Decided:**
- ✓ Json type: Yes, create builtin `Json` property type
- ✓ Json storage: `Value::Binary(json_bytes)` + LWW backend
- ✓ Json queries: Runtime path extraction, no schema validation needed

**Phase 2 — Still Open:**
- Q1: **Target collection metadata** — How does query know where to fetch referenced entity?
- Q2: **Ref representation** — `Ref<T>` wrapper vs annotated `EntityId`?
- Q3: **MultiRef representation** — `Vec<Ref<T>>` vs `Vec<EntityId>` with annotation?
- Q8: **Async filter evaluation** — How to fetch referenced entities during filtering?

**Resolved/Out of Scope:**
- Q10: `[:Type]` syntax — NOT INCLUDED (use `[predicate]` filter)
- Q11: ALL semantics — ANY is default; ALL would be a traversal filter modifier if ever added
- Q12: Compile-time IDs — Non-goal for this project
- Q13: Schema evolution — Omitted

