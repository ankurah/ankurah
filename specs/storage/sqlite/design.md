# SQLite Storage Engine - Design

## Interface Implementation

Implements the standard `StorageEngine` and `StorageCollection` traits from `ankurah_core::storage`.

### StorageEngine

```rust
pub struct SqliteStorageEngine {
    pool: bb8::Pool<SqliteConnectionManager>,  // Custom manager wrapping rusqlite
}

impl SqliteStorageEngine {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self>;
    pub fn open_in_memory() -> anyhow::Result<Self>;  // For testing
}
```

**Connection PRAGMAs**: Enable WAL mode and additional performance optimizations:
```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA cache_size=-64000;
PRAGMA mmap_size=268435456;
PRAGMA temp_store=MEMORY;
```

### StorageCollection

```rust
pub struct SqliteBucket {
    pool: bb8::Pool<SqliteConnectionManager>,  // Custom manager wrapping rusqlite
    collection_id: CollectionId,
    state_table_name: String,                  // Cached table name (avoids allocations)
    event_table_name: String,                  // Cached event table name
    columns: Arc<std::sync::RwLock<Vec<SqliteColumn>>>,  // Cached column metadata
    ddl_lock: Arc<tokio::sync::Mutex<()>>,    // Serialize DDL operations
}
```

## Schema Design

Follow the Postgres pattern with dynamic table creation per collection:

### State Table

```sql
CREATE TABLE IF NOT EXISTS "{collection_id}" (
    id TEXT PRIMARY KEY,          -- Base64-encoded ULID (matches Postgres)
    state_buffer BLOB NOT NULL,   -- bincode-serialized BTreeMap<String, Vec<u8>>
    head TEXT NOT NULL,           -- JSON array of ULID strings (Clock)
    attestations BLOB             -- bincode-serialized Vec<Vec<u8>>
);
```

### Event Table

```sql
CREATE TABLE IF NOT EXISTS "{collection_id}_event" (
    id TEXT PRIMARY KEY,          -- Base64-encoded EventId
    entity_id TEXT,               -- Base64-encoded EntityId
    operations BLOB,              -- bincode-serialized OperationSet
    parent TEXT,                  -- JSON serialized Clock
    attestations BLOB             -- bincode-serialized AttestationSet
);

CREATE INDEX IF NOT EXISTS "{collection_id}_event_entity_id_idx"
    ON "{collection_id}_event"("entity_id");
```

### Materialized Columns

Like Postgres, materialize CRDT values for queryable fields:

| Backend | SQLite Type |
|---------|-------------|
| Yrs (text) | TEXT |
| LWW | BLOB |
| PN (counter) | INTEGER |
| Json | BLOB (JSONB format via `jsonb()`, queried via `json_extract()`) |

Columns added dynamically via `ALTER TABLE ADD COLUMN` when first encountered.

**JSONB Implementation**: SQLite stores JSONB values as BLOB (using SQLite's native JSONB format via `jsonb()` function during INSERT). Queries use `json_extract("column", '$.path')` for path traversal, which returns SQL-native types (not JSON strings), enabling type-aware comparisons. This ensures numeric comparisons work correctly (e.g., `data.count > 10` compares as numbers, not strings).

**DDL Locking**: Use a `tokio::sync::Mutex` per collection to serialize DDL operations (similar to Postgres advisory locks). Pattern:
1. Acquire mutex lock
2. Re-check columns (another task may have added them)
3. Add missing columns
4. Rebuild column cache
5. Release lock

This prevents race conditions when multiple concurrent writes discover new properties simultaneously.

## Query Handling

**Server-side filtering** using SQL (similar to Postgres approach):

1. SQLite-specific SQL builder in `storage/sqlite/src/sql_builder.rs`
2. Convert AnkQL predicates to SQLite-compatible SQL WHERE clauses
3. Split predicates into pushdown (SQL) and post-filter (Rust) portions
4. Execute filtered queries directly on the database

### Predicate Splitting

The `split_predicate_for_sqlite()` function separates predicates:

- **Pushdown predicates**: Simple comparisons, AND/OR/NOT combinations of pushdown-capable expressions
- **Post-filter predicates**: Complex expressions that require Rust evaluation

```rust
pub struct SplitPredicate {
    pub sql_predicate: Predicate,      // Pushed to SQLite WHERE clause
    pub remaining_predicate: Predicate, // Evaluated in Rust post-fetch
}
```

### SQL Builder

The `SqlBuilder` in `storage/sqlite/src/sql_builder.rs`:

```rust
pub struct SqlBuilder {
    sql: String,
    params: Vec<rusqlite::types::Value>,
    fields: Vec<String>,
    table_name: Option<String>,
}

impl SqlBuilder {
    pub fn selection(&mut self, selection: &Selection) -> Result<(), SqlGenerationError>;
    pub fn build(self) -> Result<(String, Vec<rusqlite::types::Value>), SqlGenerationError>;
}
```

Key implementation details:
- Use `?` placeholders (rusqlite uses positional parameters)
- JSONB path handling: `json_extract("column", '$.path')` for reliable type-aware comparisons
- Simple paths: Direct column reference `"column"`
- Multi-step paths: `json_extract("column", '$.step1.step2')`
- ULID storage: TEXT (base64) matching Postgres

## Connection Manager

Custom `bb8::ManageConnection` implementation in `connection.rs`:
- Wraps `rusqlite::Connection` in `Arc<tokio::sync::Mutex>` for thread-safe access
- Uses `spawn_blocking` for all synchronous rusqlite operations
- Validates connections with `SELECT 1`

### Async Considerations

rusqlite is synchronous. The implementation uses:

1. **`PooledConnection`**: Wraps `Connection` in `Arc<tokio::sync::Mutex>` for thread-safe access
2. **`with_connection()`**: Executes closures via `spawn_blocking` with mutex lock
3. **`SqliteConnectionManager`**: Implements `bb8::ManageConnection` for pool integration

```rust
pub struct PooledConnection {
    inner: Arc<Mutex<Connection>>,
}

impl PooledConnection {
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&Connection) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static;
}
```

## File Structure

```
storage/sqlite/
├── Cargo.toml
└── src/
    ├── lib.rs          # Re-exports and module documentation
    ├── engine.rs       # SqliteStorageEngine + SqliteBucket implementation
    ├── connection.rs   # bb8 connection manager wrapping rusqlite
    ├── sql_builder.rs  # SQLite-specific SQL builder and predicate splitting
    ├── value.rs        # SQLite value type conversions
    └── error.rs        # SqliteError type definitions
```

## Design Decisions

1. **WASM support**: Deferred - not needed for initial implementation
2. **WAL mode**: Enabled by default with additional performance PRAGMAs
3. **Connection pooling**: Use `bb8` with custom manager (matches Postgres pattern)
4. **ULID storage**: TEXT (base64) matching Postgres
5. **DDL locking**: Use `tokio::sync::Mutex` per collection to serialize ALTER TABLE operations
6. **JSONB for JSON values**: Use SQLite's JSONB format (stored as BLOB via `jsonb()` function), queried via `json_extract()` for type-aware comparisons
7. **Event storage**: Separate `{collection}_event` tables with entity_id index for efficient event retrieval
8. **Table name caching**: Cache state and event table names in `SqliteBucket` to avoid repeated allocations

## References

- Existing Postgres implementation: `storage/postgres/src/lib.rs`
- Existing Sled implementation: `storage/sled/src/sled.rs`
- Storage traits: `core/src/storage.rs`
