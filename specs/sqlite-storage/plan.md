# SQLite Storage Engine - Implementation Plan

**Status: COMPLETED**

## Phase 1: Foundation

### Task 1.1: Extract Shared Predicate Logic
**Status: DEFERRED** - Predicate splitting implemented directly in SQLite module (`sql_builder.rs`) rather than extracting to common. Can be refactored later if Postgres needs the same pattern.

### Task 1.2: Create SQLite Crate Structure ✅
- Created `storage/sqlite/` directory
- Created `Cargo.toml` with verified dependency versions (rusqlite 0.32)
- Created module structure:
  - `src/lib.rs` (re-exports)
  - `src/engine.rs` (SqliteStorageEngine + SqliteBucket)
  - `src/sql_builder.rs` (SQL builder and predicate splitting)
  - `src/value.rs` (value type conversions)
  - `src/connection.rs` (bb8 connection manager)
  - `src/error.rs` (error types)
- Added crate to workspace `Cargo.toml`

## Phase 2: Connection Management

### Task 2.1: Implement Connection Manager ✅
- Implemented custom `SqliteConnectionManager` in `connection.rs`:
  - `PooledConnection` wrapper with `Arc<tokio::sync::Mutex<Connection>>`
  - `with_connection()` method using `spawn_blocking`
  - `bb8::ManageConnection` trait implementation
  - Connection validation via `SELECT 1`

### Task 2.2: Implement SqliteStorageEngine ✅
- Implemented `SqliteStorageEngine::open(path)` with:
  - Connection pool creation (default 10 connections)
  - Performance PRAGMAs (WAL, synchronous, cache_size, mmap_size, temp_store)
- Implemented `SqliteStorageEngine::open_in_memory()` (single connection)
- Implemented `StorageEngine` trait:
  - `collection()` method returning `SqliteBucket`
  - `delete_all_collections()` for cleanup
- Unit tests for engine creation

## Phase 3: Core Storage Operations

### Task 3.1: Implement Basic Table Operations ✅
- Implemented `SqliteBucket` struct with:
  - Pool reference
  - Collection ID + cached table names
  - Column cache (`Arc<std::sync::RwLock<Vec<SqliteColumn>>>`)
  - DDL lock (`Arc<tokio::sync::Mutex<()>>`)
- Implemented state and event table creation
- Implemented column introspection via `PRAGMA table_info`

### Task 3.2: Implement State Operations ✅
- Implemented `StorageCollection::get_state()`
- Implemented `StorageCollection::set_state()` with:
  - UPSERT pattern (`INSERT ... ON CONFLICT DO UPDATE`)
  - JSONB columns wrapped with `jsonb(?)` in SQL
  - Change detection via head comparison
- Implemented event operations:
  - `add_event()`, `get_events()`, `dump_entity_events()`

### Task 3.3: Implement Value Conversions ✅
- Created `src/value.rs` with `SqliteValue` enum:
  - Text, Integer, Real, Blob, Jsonb, Null variants
  - `sqlite_type()` for column creation
  - `is_jsonb()` for special SQL handling
  - `to_sql()` for parameter conversion

## Phase 4: Query Support

### Task 4.1: Implement SQLite SQL Builder ✅
- Created `SqlBuilder` in `src/sql_builder.rs`
- Implemented predicate conversion:
  - `?` placeholders for parameters
  - Comparison operators (=, <>, >, >=, <, <=, IN)
  - Logical operators (AND, OR, NOT)
- Implemented JSON path handling:
  - `json_extract("column", '$.path')` for nested paths
  - Direct SQL value comparison (not JSONB-to-JSONB)

### Task 4.2: Implement Fetch States ✅
- Implemented `StorageCollection::fetch_states()`:
  - `split_predicate_for_sqlite()` for pushdown analysis
  - SQL generation with ORDER BY and LIMIT
  - Post-filtering for non-pushdown predicates
  - Schema cache refresh for unknown columns

### Task 4.3: Implement Materialized Columns ✅
- Column detection during `set_state()`
- DDL locking pattern:
  1. Acquire `ddl_lock` mutex
  2. Rebuild column cache
  3. Add missing columns via `ALTER TABLE ADD COLUMN`
  4. Rebuild cache again
  5. Release lock

## Phase 5: Testing

### Task 5.1: Unit Tests ✅
- `test_open_in_memory()` - engine creation
- `test_sane_name()` - collection name validation
- `test_jsonb_function_availability()` - JSONB function verification
- `test_json_path_query()` - SQL builder JSON path generation
- `test_jsonb_storage_and_parameterized_query()` - full JSONB cycle
- SQL builder tests for predicates

### Task 5.2: Integration Tests ✅
- Created `tests/tests/sqlite/mod.rs`
- Created `tests/tests/sqlite/basic.rs`
- Created `tests/tests/sqlite/json_property.rs`

### Task 5.3: Feature Flag Integration ✅
- Added `sqlite` feature to `tests/Cargo.toml`
- SQLite tests run with feature enabled

## Phase 6: Quality Assurance

### Task 6.1: Code Quality ✅
- Code formatted with `cargo fmt`
- Clippy clean
- Proper error handling with `?` and `.expect()` for lock poisoning
- Fully qualified `std::sync::RwLock` and `tokio::sync::Mutex` paths

### Task 6.2: Documentation ✅
- Module documentation in `lib.rs` with SQLite version requirements
- Doc comments on public types and methods
- Usage examples in doc comments

### Task 6.3: Final Verification ✅
- Tests passing
- Compatible with existing storage engine interface

## Dependencies Between Tasks

```
Phase 1 ──┬── Task 1.1 (Shared Predicate) ───┐
          │                                   │
          └── Task 1.2 (Crate Structure) ─────┤
                                              ▼
Phase 2 ──┬── Task 2.1 (Connection Manager) ──┤
          │                                   │
          └── Task 2.2 (StorageEngine) ───────┤
                                              ▼
Phase 3 ──┬── Task 3.1 (Table Operations) ────┤
          │                                   │
          ├── Task 3.2 (State Operations) ────┤
          │                                   │
          └── Task 3.3 (Value Conversions) ───┤
                                              ▼
Phase 4 ──┬── Task 4.1 (Predicate Builder) ───┤
          │                                   │
          ├── Task 4.2 (Fetch States) ────────┤
          │                                   │
          └── Task 4.3 (Materialized Cols) ───┤
                                              ▼
Phase 5 ──┬── Task 5.1 (Unit Tests) ──────────┤
          │                                   │
          ├── Task 5.2 (Integration Tests) ───┤
          │                                   │
          └── Task 5.3 (Feature Flags) ───────┤
                                              ▼
Phase 6 ──┬── Task 6.1 (Code Quality) ────────┤
          │                                   │
          ├── Task 6.2 (Documentation) ───────┤
          │                                   │
          └── Task 6.3 (Final Verification) ──┘
```

## Implementation Notes

- **Shared predicate logic (Task 1.1)**: Deferred. The predicate splitting logic is currently SQLite-specific in `sql_builder.rs`. If Postgres needs similar functionality, this can be extracted to `storage/common` in a future refactor.
- **JSON path handling**: Uses `json_extract()` rather than `->` operator for more reliable comparisons when JSONB is stored as BLOB.
- **Event tables**: Added `{collection}_event` tables for event sourcing, not originally in the spec.
- **Connection pooling**: Single connection for in-memory databases (to keep database alive), configurable pool size for file databases.
