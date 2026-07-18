# Text & Vector Indexing Specification

## Overview

This specification defines new ankQL operators and index types for text search and vector similarity search. The system encompasses:

1. **CONTAINS / ICONTAINS operators** — Substring text search with inverted index acceleration
2. **distance() expression** — Vector distance computation for kNN and radius queries
3. **IndexType trait abstraction** — Polymorphic index infrastructure supporting composite, text, and vector indexes
4. **Per-backend implementations** — PostgreSQL (native GIN/HNSW), SQLite (instr/FTS5), Sled + IndexedDB (custom inverted/vector indexes)

## Design Goals

- **Postgres-canonical semantics**: PostgreSQL is tier-1. Its operator behavior defines what all backends must match.
- **Cross-backend parity**: Every operator works on every backend. SQL backends push down to native ops; KV backends evaluate in application code with optional index acceleration.
- **Pluggable index types**: New index types plug into the existing Planner → Plan → Scanner → Stream pipeline without modifying the execution framework.
- **WASM-compatible**: All application-level code (tokenization, distance computation) must compile for wasm32 targets.
- **Incremental adoption**: Operators work without indexes (full scan + filter). Indexes are an optimization, not a requirement.
- **WHERE is boolean, ORDER BY is scoring**: All WHERE predicates produce true/false. Scoring expressions (`distance()`, `relevance()`) are ORDER BY expressions. Filters never affect scores.

## New ankQL Operators

### CONTAINS / ICONTAINS (Text Search)

**Semantics:** Boolean substring match. Returns true if the field value contains the search string.

```
name CONTAINS 'Alice'          -- case-sensitive substring
name ICONTAINS 'alice'         -- case-insensitive substring
description ICONTAINS 'urgent' AND status = 'open'
```

**Evaluation:**
- `CONTAINS`: `field_value.contains(search_string)`
- `ICONTAINS`: `field_value.to_lowercase().contains(search_string.to_lowercase())`

**Backend translation:**

| Backend | CONTAINS | ICONTAINS |
|---------|----------|-----------|
| PostgreSQL | `position($1 in "col") > 0` | `position(lower($1) in lower("col")) > 0` |
| SQLite | `instr("col", ?) > 0` | `instr(lower("col"), lower(?)) > 0` |
| Sled/IndexedDB | `value.contains(needle)` | `.to_lowercase().contains(...)` |

### distance() Expression (Vector Search)

**Semantics:** Computes distance between a vector field and a query vector. Returns a float. Becomes boolean via comparison, becomes ordering via ORDER BY.

```
-- kNN: find 10 nearest neighbors
ORDER BY distance(embedding, [0.1, 0.2, ...], 'cosine') LIMIT 10

-- Radius search: find all within threshold (boolean predicate)
WHERE distance(embedding, [0.1, 0.2, ...], 'cosine') < 0.5

-- Combined with other predicates
WHERE category = 'news'
ORDER BY distance(embedding, [0.1, 0.2, ...], 'cosine') LIMIT 10
```

**Metrics:** `'l2'` (Euclidean), `'cosine'`, `'dot'` (inner product)

**Backend translation:**

| Backend | distance() in ORDER BY | distance() in WHERE |
|---------|----------------------|---------------------|
| PostgreSQL | `embedding <=> $1` (cosine) | `(embedding <=> $1) < 0.5` |
| SQLite | Application-level compute | Application-level compute |
| Sled/IndexedDB | Application-level compute | Application-level compute |

## Scoring & Predicate Composition

### Core Principles

Informed by patterns across PostgreSQL, Elasticsearch, Lucene/Solr, and vector databases:

1. **All WHERE predicates are purely boolean** — `=`, `CONTAINS`, `distance(...) < 0.5` all produce true/false
2. **Scoring expressions live in ORDER BY** — `distance()`, `relevance()` return floats for ranking
3. **Functions work in both contexts** — same `distance()` call; WHERE adds a threshold comparison, ORDER BY uses the raw value
4. **CONTAINS is not a scoring operator** — it's boolean. For text relevance ranking, use `relevance(field, query)` in ORDER BY
5. **Filters never affect scores** — WHERE narrows the candidate set, ORDER BY ranks it. Independent concerns.

### Execution Model

```
1. WHERE phase:  Evaluate boolean predicates → candidate set
                  (indexes accelerate this: composite → range scan, text → token lookup, vector → threshold scan)
2. ORDER BY phase: Compute scoring expressions on surviving candidates
                  (distance(), relevance() evaluated per-candidate)
3. LIMIT phase:  Take top-K from scored results
```

### Compound Query Example

```
WHERE category = 'news' AND description ICONTAINS 'urgent'
ORDER BY distance(embedding, [0.1, 0.2, ...], 'cosine')
LIMIT 10
```

Execution: composite index on `category` narrows to news articles → text index on `description` further narrows to those containing "urgent" → compute cosine distance for survivors → return top 10 by distance.

### Grammar Extension: Function Expressions

Add function call syntax to ankQL (currently only has paths, literals, infix operators):

```pest
VectorLiteral = { "[" ~ Float ~ ("," ~ Float)* ~ "]" }
FunctionCall  = { Identifier ~ "(" ~ (Expr ~ ("," ~ Expr)*)? ~ ")" }
AtomicExpr    = _{ ... | FunctionCall | VectorLiteral | ... }
```

AST additions:
- `Expr::FunctionCall { name: String, args: Vec<Expr> }`
- `Literal::Vector(Vec<f64>)`
- `OrderByItem` accepts expressions (not just field paths)

### Future: Weighted Multi-Signal Scoring

```
WHERE category = 'news'
ORDER BY distance(embedding, query, 'cosine') * 0.7 + relevance(description, 'search') * 0.3
LIMIT 10
```

Requires arithmetic expressions in ORDER BY — a natural extension of function expression support.

## Index Type Abstraction

### IndexType Trait

```rust
pub trait IndexType: Send + Sync + Debug {
    fn extract_entries(
        &self, eid: &EntityId, properties: &[(u32, Value)], pm: &PropertyManager,
    ) -> Result<Vec<(Vec<u8>, EntityId)>, IndexError>;

    fn infer_bounds_from_predicate(
        &self, predicates: &[Predicate],
    ) -> Result<Option<IndexBounds>, PlannerError>;

    fn make_plan(
        &self, bounds: IndexBounds, remaining: Predicate, order_by_spill: OrderByComponents,
    ) -> Plan;
}
```

### IndexSpec Enum

```rust
pub enum IndexSpec {
    Composite(CompositeIndexType),   // Existing behavior, wrapped
    Text(TextIndexType),             // Inverted token index
    Vector(VectorIndexType),         // kNN vector index
}
```

### Plan Enum (Extended)

```rust
pub enum Plan {
    Index { index_spec, scan_direction, bounds, remaining_predicate, order_by_spill },
    TextIndex { field, tokens, remaining_predicate, order_by_spill },
    VectorSearch { field, query_vector, k_limit, radius, remaining_predicate, order_by_spill },
    TableScan { bounds, scan_direction, remaining_predicate, order_by_spill },
    EmptyScan,
}
```

### IndexBounds Polymorphism

```rust
pub enum IndexBounds {
    Composite(CompositeKeyBounds),     // Existing KeyBounds
    Text(TextBounds { tokens, match_type }),
    Vector(VectorBounds { query_vector, k, radius }),
}
```

## Text Index Design (KV Backends)

### Key Format
```
[token_bytes][0x00][entity_id_bytes]
```

Sorts lexicographically by token, then by entity. Prefix scan finds all entities for a given token.

### Tokenization (MVP)
- Split on whitespace and punctuation
- Lowercase all tokens
- No stemming initially (add via `rust-stemmers` later)
- WASM-compatible: pure Rust, no native deps

### Index Maintenance
On every entity write:
1. Extract text field value from materialized properties
2. Tokenize old value → old token set
3. Tokenize new value → new token set
4. Compute set diff: remove old-only tokens, insert new-only tokens

Uses the polymorphic `extract_entries()` → set diff pattern in `update_indexes_for_entity()`.

### Query Execution
For `description CONTAINS 'hello world'`:
1. Tokenize search string → `["hello", "world"]`
2. For each token: prefix scan index tree, collect EntityId sets
3. Intersect sets → candidate entities
4. Post-filter with actual substring match (handles false positives from tokenization)

## Vector Index Design

### Postgres
Native pgvector: `CREATE INDEX ... USING HNSW (embedding vector_cosine_ops)`

### KV Backends (Phased)
- **Phase 1:** Brute force — scan all vectors, compute distances, return top-K
- **Phase 2:** IVF — cluster centroids + per-cluster scan
- **Phase 3:** HNSW graph structure in separate trees (if scale demands it)

### Distance Computation
Pure Rust f32/f64 operations — WASM-safe, no external deps:
- L2: `sqrt(sum((a[i] - b[i])^2))`
- Cosine: `1.0 - dot(a, b) / (norm(a) * norm(b))`
- Dot product: `sum(a[i] * b[i])`

## Model Annotations

```rust
#[derive(Model)]
#[index(composite, fields = ["user_id", "created_at"])]
#[index(text, field = "description")]
#[index(vector, field = "embedding", dimensions = 768)]
pub struct Task {
    pub user_id: String,
    pub created_at: String,
    pub description: String,
    pub embedding: Vec<f64>,
}
```

## Serialization

IndexRecord evolves to hold `IndexSpecSerde` — a tagged enum:
```json
{
  "spec": {
    "type": "text",
    "config": { "field": "description", "tokenizer": "simple" }
  }
}
```

Backward-compatible: existing `KeySpec`-only records auto-upgrade to `IndexSpec::Composite` on load.
