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
| 2 | Ref metadata + forward ref traversal | 1 | Pending |
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

### Phase 2: Forward Ref Traversal

**Deliverable**: Query across entity references

```rust
#[derive(Model)]
pub struct Album {
    pub name: String,
    #[model(ref_to = "artist")]  // metadata for target collection
    pub artist: EntityId,
}

ctx.fetch::<AlbumView>("artist.name = ?", "Radiohead")
```

**Key requirement**: Need minimal ref metadata to know target collection. Options:
- Field annotation: `#[model(ref_to = "artist")]`
- `Ref<T>` wrapper type
- Runtime registry

See: `phase-3-ref-traversal.md` (to be revised)

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
