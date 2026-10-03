# Text & Vector Indexing — Implementation Plan

## Overview

Add text search (CONTAINS) and vector search (kNN distance) operators to ankQL, backed by new index types that work across all storage backends. PostgreSQL is the tier-1 backend — its capabilities define the semantics that other backends must match.

## Phase Summary

| Phase | Focus | Deliverable |
|-------|-------|-------------|
| 1 | Index Type Abstraction Refactor | `IndexType` trait, `IndexSpec` enum wrapping existing composite indexes — non-breaking |
| 2 | CONTAINS Operator + Text Index | Grammar, AST, parser, evaluation, SQL pushdown, inverted index on KV backends |
| 3 | Vector Storage + Distance Expressions | Vector column type, `distance()` expression, kNN ORDER BY, brute-force scan |
| 4 | Vector Indexes | HNSW/IVF acceleration on Postgres (pgvector), KV backends |
| 5 | Advanced Text Search | Word-level MATCHES operator, tsvector/tsquery on Postgres, stemming |

## Phase 1: Index Type Abstraction Refactor (Non-Breaking)

### Objective
Introduce the `IndexType` trait and `IndexSpec` enum so the existing composite key indexes use the new abstraction. No behavior changes — just infrastructure for phases 2-4.

### Files
- `core/src/indexing/index_type.rs` — `IndexType` trait, `IndexSpec` enum, `IndexBounds` enum
- `core/src/indexing/composite_index.rs` — `CompositeIndexType` implementing the trait
- `core/src/indexing/key_spec.rs` — unchanged, wrapped by `CompositeIndexType`
- `storage/common/src/planner.rs` — refactor to call `index_type.infer_bounds_from_predicate()`
- `storage/common/src/types.rs` — keep `Plan::Index` as-is
- `storage/sled/src/index.rs` — `update_indexes_for_entity()` uses `extract_entries()` with set diff
- Serde migration: `IndexRecord.spec` becomes `IndexSpecSerde` enum, auto-upgrades old `KeySpec`-only records

### Acceptance Criteria
- All existing tests pass unchanged
- `IndexManager` stores `IndexSpec::Composite(...)` internally
- Index maintenance uses `extract_entries()` → set diff pattern

## Phase 2: CONTAINS Operator + Text Index

### Objective
Add `CONTAINS` / `ICONTAINS` operators for substring text search, backed by inverted indexes on KV backends and native SQL on Postgres/SQLite.

### Grammar + AST
- `ankql/src/ankql.pest` — add `Contains` / `IContains` rules to `CmpInfixOp`
- `ankql/src/ast.rs` — add to `ComparisonOperator` enum
- `ankql/src/parser.rs` — map in `create_comparison()`

### Evaluation
- `core/src/selection/filter.rs` — string matching branches for CONTAINS/ICONTAINS

### SQL Backends
- Postgres: `position(lower($1) in lower("col")) > 0` (ICONTAINS), pushdown via `sql_builder.rs`
- SQLite: `instr(lower("col"), lower(?)) > 0`, pushdown via `sql_builder.rs`

### KV Backends (Text Index)
- `core/src/indexing/text_index.rs` — `TextIndexType` implementing `IndexType`
- Tokenizer: whitespace split + lowercase (simple, WASM-compatible)
- Index key format: `[token_bytes][0x00][entity_id]`
- `storage/common/src/types.rs` — add `Plan::TextIndex { field, tokens, remaining_predicate, ... }`
- `storage/common/src/planner.rs` — recognize CONTAINS predicates, generate TextIndex plans
- Sled: token prefix scan using existing scanner + prefix guard
- IndexedDB: same pattern via IdbKeyRange

### Model Annotations
- `#[index(text, field = "description")]`

## Phase 3: Vector Storage + Distance Expressions

### Objective
Add vector column type and `distance()` expression to ankQL. Support kNN queries via ORDER BY distance LIMIT K. Brute-force evaluation initially (no vector indexes yet).

### Grammar + AST
- Add `distance(field, vector_literal, metric)` as a function expression in ankQL grammar
- `distance()` returns a float — becomes boolean via comparison (`< 0.5`), becomes ordering via ORDER BY
- Vector literal syntax: `[0.1, 0.2, 0.3, ...]`

### Storage
- Postgres: `vector(N)` column type via pgvector extension, `<=>` / `<->` operators
- SQLite: BLOB storage, application-level distance computation
- Sled/IndexedDB: binary storage, application-level distance computation

### Evaluation
- `core/src/selection/filter.rs` — distance computation for WHERE threshold predicates
- Distance metrics: L2 (Euclidean), Cosine, Dot Product — pure Rust f32/f64 ops (WASM-safe)

## Phase 4: Vector Indexes

### Objective
Add index acceleration for vector kNN queries.

### Postgres
- `CREATE INDEX ... USING HNSW (embedding vector_cosine_ops)` via pgvector
- Translates `ORDER BY distance(...)` to `ORDER BY embedding <=> $1`

### KV Backends
- IVF (Inverted File Index) for medium scale: cluster centroids + per-cluster scan
- HNSW graph structure in separate trees for large scale
- `Plan::VectorSearch` variant in planner

## Phase 5: Advanced Text Search (Future)

### Objective
Add word-level full-text search with linguistic features.

- `MATCHES` operator — word/token-level matching with AND/OR semantics
- Postgres: `to_tsvector('simple', col) @@ to_tsquery('simple', ?)` + GIN index
- Stemming via `rust-stemmers` crate (WASM-compatible)
- Relevance ranking via `relevance()` expression in ORDER BY
