## Implementation Plan: Unified Navigation

### Overview

This plan prioritizes delivering **Json property + structured queries** as the first milestone, deferring schema registry, ref traversal, and edge navigation to follow-on phases.

### Phase Summary

| Phase | Goal | Depends On | Status |
|-------|------|------------|--------|
| **1a** | Parser: basic dot-path navigation | — | ✅ Complete |
| **1b** | `Json` builtin property type | — | ✅ Complete |
| **1c** | Structured property queries (`licensing.territory = ?`) | 1a, 1b | ✅ Complete |
| **1d** | PostgreSQL JSONB pushdown + predicate splitting | 1c | ✅ Complete |
| **1e** | JSON-aware type casting (numeric-only) | 1c | ✅ Complete |
| **1f** | JSON path index pushdown (Sled/IndexedDB) | 1c | ✅ Complete |
| **2a** | `Ref<T>` typed entity reference | 1 | 🚧 In Progress (WASM wrappers pending) |
| **2b** | Ref traversal in queries | 2a | Pending |
| 3 | Full schema registry / PropertyId | 2 | Pending |
| 4 | Inbound navigation, traversal filters | 2, 3 | Pending |
| 5 | Relation-entities, index pushdown | 3, 4 | Future |

---

### Phase 1: Json + Structured Queries ✅ COMPLETE

**Deliverable**: Query nested JSON properties with dot syntax

```rust
#[derive(Model)]
pub struct Track {
    pub name: String,
    pub licensing: Json,
}

ctx.fetch::<TrackView>("licensing.territory = ?", "US")
ctx.fetch::<TrackView>("licensing.rights.holder = ?", "Label")
```

**What was delivered:**

| Component | Description |
|-----------|-------------|
| `PathExpr` AST | Dot-separated paths replace `Identifier` enum |
| `Json` property type | New builtin wrapping `serde_json::Value` |
| Filter evaluation | Multi-step path → JSON traversal in Rust |
| PostgreSQL JSONB | `->` and `->>` operators for pushdown |
| Predicate splitting | Infrastructure for partial pushdown |
| JSON-aware casting | Numeric-only casting for JSON values |

**Key files changed:**
- `ankql/src/ast.rs` — `PathExpr` struct, `Expr::Path`
- `ankql/src/ankql.pest` — `PathExpr` grammar rule
- `ankql/src/parser.rs` — Parse dot-paths
- `core/src/property/value/json.rs` — New `Json` type
- `core/src/selection/filter.rs` — JSON path evaluation + casting
- `storage/postgres/src/sql_builder.rs` — JSONB SQL generation

**Known limitations (to be addressed in Phase 3):**
- JSON semantics inferred from multi-step paths (hack)
- No schema validation of JSON structure
- No `Ref<T>` traversal yet

**What this phase does NOT include:**
- `[filter]` syntax (for multi-valued traversals)
- `->role` syntax (for relation-entities)  
- `^Model.field` syntax (for inbound navigation)
- Schema registry / PropertyId
- Ref traversal across entities

---

### Phase 1f: JSON Path Index Pushdown (Sled/IndexedDB)

**Deliverable**: Index-backed JSON path queries for Sled and IndexedDB

Currently Sled and IndexedDB evaluate JSON path predicates via post-fetch filtering in Rust. Phase 1f enables these queries to use indexes:

```rust
// These should use indexes, not post-fetch filtering:
ctx.fetch::<DetectionView>("context.session_id = ?", "sess1")
ctx.fetch::<DetectionView>("context.year > ?", 2020)
```

**Key changes:**

| Component | Change |
|-----------|--------|
| `IndexKeyPart` | Add `sub_path: Option<Vec<String>>` for sub-navigation |
| `Value` | Add `extract_at_path()` method for structured value extraction |
| Planner | Generate keyparts with sub_path from multi-step `PathExpr` |
| Sled | Use `extract_at_path`, skip indexing if path missing |
| IndexedDB | Translate to native `createIndex()` with dot-path keyPath |

**Design notes:**
- `sub_path` generalizes over JSON, future Ref traversal, etc.
- Missing path → don't index (distinguishes from explicit null)
- Array offsets normalized: `items[0].name` → `["items", "name"]`
- IndexedDB natively supports `"context.session_id"` keyPath syntax

See: `phase-1f-index-pushdown.md`

---

### Phase 2a: `Ref<T>` Typed Entity Reference 🚧 IN PROGRESS

**Deliverable**: Type-safe entity references with programmatic traversal

```rust
#[derive(Model)]
pub struct Album {
    pub name: String,
    pub artist: Ref<Artist>,  // typed reference
}

// Rust-side programmatic traversal ✅
let album: AlbumView = ctx.get(album_id).await?;
let artist: ArtistView = album.artist().get(&ctx).await?;

// TypeScript-side traversal ❌ (needs wrapper generation)
const album = await Album.get(ctx, albumId);
const artistId = album.artist;  // string (EntityId base64) ✅
const artist = await ctx.get<ArtistView>(artistId);  // manual lookup required
```

**What was delivered:**

| Component | Description | Status |
|-----------|-------------|--------|
| `Ref<T>` type | Wrapper around `EntityId` with `PhantomData<T>` | ✅ |
| `Property` impl | Stores as `Value::EntityId` | ✅ |
| `.get(&ctx)` | Async method to fetch referenced entity as `T::View` | ✅ Rust only |
| Conversions | `From<EntityId>` / `Into<EntityId>` | ✅ |
| Serde | Transparent serialization | ✅ |
| WASM bindings | `Ref<T>` ↔ `JsValue` (string) | ✅ |
| LWW wrapper gen | `LWWRefFoo` for each `Ref<Foo>` usage | ❌ |

**Open issue: WASM wrapper generation for `Ref<T>`**

For WASM, we need `LWWRefArtist` wrapper types. Two problems:

1. **Manual invocation required**: Currently requires `impl_wrapper_type!(Ref<Artist>)` 
   for each concrete `Ref<T>` used in models — poor ergonomics.

2. **Duplicate generation risk**: If the Model derive macro auto-generates wrappers,
   multiple models using `Ref<Artist>` would generate duplicate `LWWRefArtist` definitions.

**Possible solutions:**
- Special-case `Ref<T>` in the derive macro to emit wrappers once per type
- Move wrapper generation to a centralized location (e.g., `impl_provided_wrapper_types!`)
- Use a registry/dedup mechanism during compilation
- Generate wrappers lazily at runtime (WASM overhead concern)

**Key files:**
- `core/src/property/value/entity_ref.rs` — `Ref<T>` implementation
- `tests/tests/sled/ref_traversal.rs` — Integration tests
- `storage/indexeddb-wasm/tests/ref_property.rs` — WASM tests (requires manual wrapper)

---

### Phase 2b: Ref Traversal in Queries (Pending)

**Deliverable**: Query across entity references

```rust
// Query-time traversal (requires resolver)
ctx.fetch::<AlbumView>("artist.name = ?", "Radiohead")
```

**Key requirements:**
- Resolver to annotate paths with target collection
- Cross-entity fetching during filter evaluation
- N+1 query considerations

See: `phase-2b-ref-traversal.md` (to be created)

---

### Phase 3: Schema Registry / PropertyId

**Deliverable**: Runtime schema metadata for validation and future optimizations

- `PropertyId` for stable field identifiers
- `ModelSchema` registry
- Structured property schemas
- Query-time path validation

See: `phase-2-schema.md` (to be revised)

---

### Phase 4: Inbound Navigation + Traversal Filters

**Deliverable**: Reverse navigation and filtered traversals

```rust
// Find artists that have albums named "OK Computer"
ctx.fetch::<ArtistView>("^Album.artist[name = ?]", "OK Computer")

// Find albums with rock genres
ctx.fetch::<AlbumView>("genres[name = ?].id = ?", "Rock", genre_id)
```

**Requires**: Schema registry (to validate inbound refs)

See: `phase-4-inbound.md` (to be revised)

---

### Phase 5: Relation-Entities + Index Pushdown (Future)

**Deliverable**: Hypergraph edge entities, optimized JSON indexing

```rust
// Traverse through contribution edge entity
ctx.fetch::<AlbumView>("contribution[role = ?]->person.name = ?", "producer", "Brian Eno")
```

---

### AST Changes (Phase 1a)

**Remove `Identifier` enum entirely** — `PathExpr` replaces it:

```rust
/// A dot-separated path like `name` or `licensing.territory`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathExpr {
    pub steps: Vec<String>,
}

pub enum Expr {
    Literal(Literal),
    Path(PathExpr),      // replaces Identifier(Identifier)
    Predicate(Predicate),
    InfixExpr { left: Box<Expr>, operator: InfixOperator, right: Box<Expr> },
    ExprList(Vec<Expr>),
    Placeholder,
}
```

**Later phases** will extend `PathExpr` to include:
- `filter: Option<Box<Predicate>>` — for `[predicate]`
- `role: Option<String>` — for `->role`
- `inbound_source: Option<String>` — for `^Model.field`
- `resolved: Option<ResolvedStep>` — for schema resolution

But Phase 1 uses the minimal form with just `steps: Vec<String>`.

---

### File Changes Summary (Phase 1)

| File | Phase | Changes |
|------|-------|---------|
| `ankql/src/ankql.pest` | 1a | `PathExpr` grammar |
| `ankql/src/ast.rs` | 1a | `PathExpr`, remove `Identifier` |
| `ankql/src/parser.rs` | 1a | Parse paths |
| `core/src/property/json.rs` | 1b | New `Json` type |
| `core/src/property/mod.rs` | 1b | Export Json |
| `core/src/selection/filter.rs` | 1c | Multi-step path evaluation |
| `storage/postgres/src/sql_builder.rs` | 1c | JSON operators |
| Various | 1a | Update `Identifier` match arms |

---

### Open Questions

See `questions.md` for detailed design questions. Key ones for Phase 1:

1. **Json storage format**: `Value::Binary(json_bytes)` or add `Value::Json`?
2. **Backend for Json**: LWW default, or allow Yrs for collaborative editing?
3. **Optional Json**: How does `Option<Json>` work? (Probably just works™)
