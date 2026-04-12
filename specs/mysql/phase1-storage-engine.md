# MySQL Storage Engine Specification

## Overview

A MySQL implementation of `StorageEngine` and `StorageCollection`, equivalent to the existing Postgres implementation. Uses the standard ankurah state model: serialized StateBuffers blob, Clock (head), event tables, and auto-materialized property columns for query pushdown. Standard EntityId (ULID) primary keys.

## Crate Structure

```
storage/mysql/
  Cargo.toml
  src/
    lib.rs          -- MySQL struct, StorageEngine impl, MysqlBucket, StorageCollection impl
    sql_builder.rs  -- ankql predicate -> MySQL WHERE clause translation
    value.rs        -- MySQLValue enum (analogous to PGValue)
```

## Dependencies

```toml
[package]
name = "ankurah-storage-mysql"
edition = "2021"

[dependencies]
mysql_async  = "0.34"       # Tokio-native MySQL client with built-in connection pooling
serde_json   = "1"
bincode      = "1.3"
anyhow       = "1.0"
thiserror    = "2.0"
tokio        = { version = "1.36", features = ["full"] }
ankql        = { path = "../../ankql" }
ankurah-core = { path = "../../core" }
ankurah-proto = { path = "../../proto" }
tracing      = "0.1"
async-trait  = "0.1"

[dev-dependencies]
ankurah            = { path = "../../ankurah", features = ["derive"] }
tokio              = { version = "1", features = ["rt-multi-thread", "macros"] }
tracing-subscriber = "0.3"
ctor               = "0.2"
serde              = { version = "1.0", features = ["derive"] }
testcontainers         = { version = "0.23", features = ["reusable-containers"] }
testcontainers-modules = { version = "0.11.4", features = ["mysql"] }
```

`mysql_async` provides its own async connection pool, so `bb8` is not needed (unlike `tokio-postgres` which requires an external pool).

## Connection Management

```rust
pub struct MySQL {
    pool: mysql_async::Pool,
}

impl MySQL {
    pub fn new(pool: mysql_async::Pool) -> Self {
        Self { pool }
    }

    pub async fn open(url: &str) -> anyhow::Result<Self> {
        let opts = mysql_async::Opts::from_url(url)?;
        let pool = mysql_async::Pool::new(opts);
        Ok(Self::new(pool))
    }

    /// Validate collection name to prevent SQL injection.
    /// Only allows alphanumeric, underscore, dot, and colon characters.
    /// Same validation as the Postgres engine.
    pub fn sane_name(collection: &str) -> bool {
        // same logic as Postgres::sane_name()
    }
}
```

## Table Schemas

### State Table

Named after the collection_id (e.g., collection "album" → table `` `album` ``).

```sql
CREATE TABLE IF NOT EXISTS `{collection_id}` (
    `id`            CHAR(22) PRIMARY KEY,
    `state_buffer`  MEDIUMBLOB,
    `head`          JSON,
    `attestations`  MEDIUMBLOB
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

MySQL-specific type choices:
- `MEDIUMBLOB` (16MB max) instead of Postgres `BYTEA` — `BLOB` is only 64KB which may be insufficient for large state buffers.
- `JSON` instead of Postgres `character(43)[]` — MySQL has no native array type. Clock is stored as a JSON array of base64 EventId strings.
- `MEDIUMBLOB` for attestations instead of Postgres `BYTEA[]` — attestations are serialized as a single blob rather than an array of blobs, since MySQL has no native array type.
- Materialized property columns are added dynamically via `ALTER TABLE ADD COLUMN` (same as Postgres).

### Event Table

Named `{collection_id}_event`.

```sql
CREATE TABLE IF NOT EXISTS `{collection_id}_event` (
    `id`            CHAR(43) PRIMARY KEY,
    `entity_id`     CHAR(22),
    `operations`    MEDIUMBLOB,
    `parent`        JSON,
    `attestations`  MEDIUMBLOB,
    INDEX `idx_entity_id` (`entity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

Note: `commit_seq BIGINT UNSIGNED AUTO_INCREMENT UNIQUE` is NOT included in this phase. It belongs to the durable subscriptions feature and will be added when that is implemented for MySQL.

### Invisible Columns

MySQL 8.0.23+ supports invisible columns, which are excluded from `SELECT *` but accessible when explicitly named. The `head` column on the state table should be created as `INVISIBLE` to keep ankurah's bookkeeping out of casual table inspection:

```sql
`head` JSON INVISIBLE
```

Verify during implementation that invisible columns work correctly with the engine's explicit `SELECT` lists (they should — invisible only affects `SELECT *`).

## Clock Serialization

Postgres stores `Clock` (`Vec<EventId>`) natively as `character(43)[]`. MySQL uses a JSON array:

```rust
fn clock_to_json(clock: &Clock) -> serde_json::Value {
    serde_json::Value::Array(
        clock.iter().map(|eid| serde_json::Value::String(eid.to_base64())).collect()
    )
}

fn clock_from_json(value: serde_json::Value) -> Result<Clock, RetrievalError> {
    match value {
        serde_json::Value::Array(arr) => {
            let mut event_ids = Vec::new();
            for item in arr {
                if let serde_json::Value::String(s) = item {
                    event_ids.push(EventId::from_base64(&s)?);
                }
            }
            Ok(Clock::new(event_ids))
        }
        serde_json::Value::Null => Ok(Clock::default()),
        _ => Err(RetrievalError::StorageError("Invalid Clock JSON".into())),
    }
}
```

## Attestation Serialization

Postgres stores attestations as `BYTEA[]` (an array of individually-serialized attestation blobs). MySQL has no array type, so attestations are serialized as a single blob:

```rust
// Write: serialize the full AttestationSet as one blob
let attestations_blob: Vec<u8> = bincode::serialize(&state.attestations)?;

// Read: deserialize the full AttestationSet from one blob
let attestations: AttestationSet = bincode::deserialize(&attestations_blob)?;
```

This differs from Postgres, which serializes each `Attestation` individually into a `Vec<Vec<u8>>`. The MySQL approach is simpler. This requires that `AttestationSet` (or its contents) implements `Serialize`/`Deserialize`, which it already does.

## DDL Serialization (Advisory Locks)

MySQL uses `GET_LOCK(name, timeout)` / `RELEASE_LOCK(name)` instead of Postgres `pg_advisory_lock(key)`.

Key differences:
- Postgres advisory locks are identified by an integer hash. MySQL uses a string name.
- Postgres advisory locks are session-scoped and released on disconnect. MySQL `GET_LOCK` is the same.
- MySQL `GET_LOCK` requires explicit release (no automatic release on transaction end, only on disconnect).

```rust
async fn acquire_ddl_lock(conn: &mut mysql_async::Conn, collection_id: &str) -> Result<(), StateError> {
    let lock_name = format!("ankurah_ddl:{}", collection_id);
    // Use parameterized query — never interpolate the lock name
    let result: Option<i32> = conn
        .exec_first("SELECT GET_LOCK(?, 30)", (lock_name,))
        .await
        .map_err(|e| StateError::DDLError(Box::new(e)))?;

    match result {
        Some(1) => Ok(()),
        Some(0) => Err(StateError::DDLError("Lock acquisition timed out".into())),
        _ => Err(StateError::DDLError("Lock acquisition failed".into())),
    }
}

async fn release_ddl_lock(conn: &mut mysql_async::Conn, collection_id: &str) -> Result<(), StateError> {
    let lock_name = format!("ankurah_ddl:{}", collection_id);
    conn.exec_drop("SELECT RELEASE_LOCK(?)", (lock_name,))
        .await
        .map_err(|e| StateError::DDLError(Box::new(e)))?;
    Ok(())
}
```

## StorageEngine Implementation

```rust
#[async_trait]
impl StorageEngine for MySQL {
    type Value = MySQLValue;

    async fn collection(&self, collection_id: &CollectionId)
        -> Result<Arc<dyn StorageCollection>, RetrievalError>
    {
        if !MySQL::sane_name(collection_id.as_str()) {
            return Err(RetrievalError::InvalidBucketName);
        }

        let mut conn = self.pool.get_conn().await.map_err(RetrievalError::storage)?;

        let schema: Option<String> = conn
            .exec_first("SELECT DATABASE()", ())
            .await
            .map_err(RetrievalError::storage)?;
        let schema = schema.unwrap_or_default();

        let bucket = MysqlBucket {
            pool: self.pool.clone(),
            collection_id: collection_id.clone(),
            schema,
            columns: Arc::new(RwLock::new(Vec::new())),
        };

        acquire_ddl_lock(&mut conn, collection_id.as_str()).await?;
        let result = async {
            bucket.create_state_table(&mut conn).await?;
            bucket.create_event_table(&mut conn).await?;
            bucket.rebuild_columns_cache(&mut conn).await?;
            Ok::<_, StateError>(())
        }.await;
        release_ddl_lock(&mut conn, collection_id.as_str()).await?;

        result?;
        Ok(Arc::new(bucket))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        // Query information_schema.tables for ankurah-managed tables,
        // DROP each in a transaction. Same approach as Postgres.
    }
}
```

## MysqlBucket

```rust
pub struct MysqlBucket {
    pool: mysql_async::Pool,
    collection_id: CollectionId,
    schema: String,
    columns: Arc<RwLock<Vec<MysqlColumn>>>,
}

#[derive(Clone, Debug)]
pub struct MysqlColumn {
    pub name: String,
    pub is_nullable: bool,
    pub data_type: String,
}
```

### Column Cache

Same approach as Postgres: query `information_schema.columns` to discover existing columns, cache in memory, refresh when unknown columns are encountered.

```rust
async fn rebuild_columns_cache(&self, conn: &mut mysql_async::Conn) -> Result<(), StateError> {
    let rows: Vec<(String, String, String)> = conn.exec(
        "SELECT column_name, is_nullable, data_type \
         FROM information_schema.columns \
         WHERE table_schema = ? AND table_name = ?",
        (&self.schema, self.collection_id.as_str()),
    ).await.map_err(|e| StateError::DDLError(Box::new(e)))?;

    let new_columns = rows.into_iter().map(|(name, nullable, dtype)| {
        MysqlColumn {
            name,
            is_nullable: nullable == "YES",
            data_type: dtype,
        }
    }).collect();

    *self.columns.write().unwrap() = new_columns;
    Ok(())
}
```

### Auto-Materialized Columns

Same as Postgres: when `set_state` encounters a property value whose column doesn't exist, create it via `ALTER TABLE ADD COLUMN`. Protected by DDL advisory lock, with cache refresh after acquiring lock to handle races.

```rust
async fn add_missing_columns(
    &self,
    conn: &mut mysql_async::Conn,
    missing: Vec<(String, &'static str)>,  // (column_name, mysql_type)
) -> Result<(), StateError> {
    // Acquire DDL lock
    // Re-check columns (another connection may have added them)
    // ALTER TABLE `{table}` ADD COLUMN `{column}` {type}
    // Rebuild cache
    // Release lock
}
```

## StorageCollection Implementation

### set_state

The Postgres engine uses a CTE with `RETURNING` to atomically read the old head while upserting. MySQL doesn't have `RETURNING`. Use a transaction instead:

```sql
START TRANSACTION;

-- Read old head (if exists)
SELECT `head` FROM `{table}` WHERE `id` = ? FOR UPDATE;

-- Upsert state
INSERT INTO `{table}` (`id`, `state_buffer`, `head`, `attestations`, ...materialized...)
VALUES (?, ?, ?, ?, ...?)
ON DUPLICATE KEY UPDATE
    `state_buffer` = VALUES(`state_buffer`),
    `head` = VALUES(`head`),
    `attestations` = VALUES(`attestations`),
    ...materialized = VALUES(materialized)...;

COMMIT;
```

`SELECT ... FOR UPDATE` locks the row for the duration of the transaction, preventing concurrent modifications between the read and write. This provides the same atomicity as the Postgres CTE approach.

Returns `true` if the head changed (new entity or head differs from old), `false` otherwise.

Materialized columns are extracted from state buffers the same way as Postgres:

```rust
for (backend_name, state_buffer) in state.payload.state.state_buffers.iter() {
    let backend = backend_from_string(backend_name, Some(state_buffer))?;
    for (column_name, value) in backend.property_values() {
        // Convert value to MySQLValue, add missing columns if needed
    }
}
```

### get_state

```sql
SELECT `id`, `state_buffer`, `head`, `attestations`
FROM `{table}` WHERE `id` = ?
```

Deserialize state_buffer, head (from JSON), and attestations. Same logic as Postgres but with MySQL-specific deserialization (JSON clock, single-blob attestations).

Handle "table doesn't exist" by creating it and returning `EntityNotFound` (same pattern as Postgres).

### fetch_states

Same flow as Postgres:

1. Check referenced columns against cache; refresh if unknown columns found.
2. Treat genuinely missing columns as NULL via `selection.assume_null()`.
3. Split predicate into SQL-pushdown vs post-filter parts via `split_predicate()`.
4. Build MySQL SELECT with pushdown predicate.
5. Execute query, deserialize results.
6. Post-filter with remaining predicate if needed.
7. Apply LIMIT after post-filtering.

The predicate split logic (`split_predicate_for_postgres` in the Postgres engine) is SQL-dialect-agnostic — it classifies which predicates can be pushed to SQL vs must be post-filtered. This should be extracted to `storage/common/` or duplicated with the function renamed.

### add_event

```sql
INSERT INTO `{table}_event` (`id`, `entity_id`, `operations`, `parent`, `attestations`)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE `id` = `id`
```

`ON DUPLICATE KEY UPDATE id = id` is a MySQL idiom for "do nothing on conflict" (there's no `ON CONFLICT DO NOTHING`). It's a no-op update that prevents the error without changing anything.

Returns `true` if a new row was inserted (affected rows > 0).

Handle "table doesn't exist" by creating it and retrying once (same as Postgres).

### get_events

Postgres uses `WHERE "id" = ANY($1)` for array-based lookup. MySQL doesn't have `ANY(array)`. Use `WHERE id IN (?, ?, ?, ...)` with dynamically-generated placeholders:

```rust
let placeholders = std::iter::repeat("?").take(event_ids.len()).collect::<Vec<_>>().join(", ");
let query = format!(
    "SELECT `id`, `entity_id`, `operations`, `parent`, `attestations` \
     FROM `{}` WHERE `id` IN ({})",
    self.event_table(),
    placeholders,
);
```

### dump_entity_events

```sql
SELECT `id`, `operations`, `parent`, `attestations`
FROM `{table}_event` WHERE `entity_id` = ?
```

Straightforward, no MySQL-specific concerns.

## SQL Builder

Port of `storage/postgres/src/sql_builder.rs` with MySQL syntax differences.

### Key Changes from Postgres

| Aspect | Postgres | MySQL |
|--------|----------|-------|
| Identifier quoting | `"column"` | `` `column` `` |
| Escape for identifiers | `""` (double the quote) | ` `` ` (double the backtick) |
| Parameter placeholders | `$1`, `$2`, `$3` | `?`, `?`, `?` |
| JSON path (preserve type) | `"col"->'key'` | `col->'$.key'` |
| JSON path (extract text) | `"col"->>'key'` | `col->>'$.key'` |
| JSONB literal cast | `'value'::jsonb` | `CAST('value' AS JSON)` |
| LIMIT parameter type | i64 | u64 |

### Predicate Split

The `split_predicate_for_postgres()` function is not Postgres-specific — it classifies predicates by whether they can be expressed in SQL at all. The same logic applies to MySQL. Either:
- Extract to `storage/common/` as `split_predicate_for_sql()`, or
- Duplicate in the MySQL crate with the appropriate name.

Extracting is preferred since the logic is identical.

### SqlBuilder Structure

```rust
pub enum MySqlExpr {
    Sql(String),
    Argument(mysql_async::Value),
}

pub struct MySqlBuilder {
    expressions: Vec<MySqlExpr>,
    fields: Vec<String>,
    table_name: Option<String>,
}
```

The `build()` method produces `(String, Vec<mysql_async::Value>)` instead of Postgres's `(String, Vec<Box<dyn ToSql>>)`.

Parameter placeholders are `?` instead of `$N`, so the builder doesn't need a counter.

### JSON Path Syntax

Postgres JSONB traversal: `"column"->'key'` (returns JSONB) / `"column"->>'key'` (returns text).

MySQL JSON traversal: `` `column`->'$.key' `` (returns JSON) / `` `column`->>'$.key' `` (returns text, unquoted).

For multi-step paths like `data.nested.field`:
- Postgres: `"data"->'nested'->'field'`
- MySQL: `` `data`->'$.nested.field' `` (single JSON path expression)

## MySQLValue

Analogous to `PGValue`. Maps `ankurah_core::value::Value` variants to MySQL types.

```rust
pub enum MySQLValue {
    String(String),        // VARCHAR
    TinyInt(i8),           // TINYINT
    SmallInt(i16),         // SMALLINT
    Int(i32),              // INT
    BigInt(i64),           // BIGINT
    Double(f64),           // DOUBLE
    Boolean(bool),         // TINYINT(1)
    Blob(Vec<u8>),         // MEDIUMBLOB
    Json(serde_json::Value), // JSON
}

impl MySQLValue {
    pub fn mysql_type(&self) -> &'static str {
        match self {
            MySQLValue::String(_) => "VARCHAR(255)",
            MySQLValue::TinyInt(_) => "TINYINT",
            MySQLValue::SmallInt(_) => "SMALLINT",
            MySQLValue::Int(_) => "INT",
            MySQLValue::BigInt(_) => "BIGINT",
            MySQLValue::Double(_) => "DOUBLE",
            MySQLValue::Boolean(_) => "TINYINT(1)",
            MySQLValue::Blob(_) => "MEDIUMBLOB",
            MySQLValue::Json(_) => "JSON",
        }
    }
}

impl From<Value> for MySQLValue {
    fn from(property: Value) -> Self {
        match property {
            Value::String(s) => MySQLValue::String(s),
            Value::I16(i) => MySQLValue::SmallInt(i),
            Value::I32(i) => MySQLValue::Int(i),
            Value::I64(i) => MySQLValue::BigInt(i),
            Value::F64(f) => MySQLValue::Double(f),
            Value::Bool(b) => MySQLValue::Boolean(b),
            Value::EntityId(eid) => MySQLValue::String(eid.to_base64()),
            Value::Object(bytes) => MySQLValue::Blob(bytes),
            Value::Binary(bytes) => MySQLValue::Blob(bytes),
            Value::Json(json) => MySQLValue::Json(json),
        }
    }
}
```

## Error Handling

Map `mysql_async::Error` to ankurah error types. MySQL error codes are numeric (not named enums like Postgres `SqlState`). Key error codes:

| MySQL Error Code | Meaning | Ankurah Mapping |
|-----------------|---------|-----------------|
| 1146 (ER_NO_SUCH_TABLE) | Table doesn't exist | Create table and retry (same as Postgres `UndefinedTable`) |
| 1054 (ER_BAD_FIELD_ERROR) | Unknown column | `UndefinedColumn` equivalent |
| 1062 (ER_DUP_ENTRY) | Duplicate key | Expected for upserts, not an error |

```rust
pub enum ErrorKind {
    UndefinedTable { table: String },
    UndefinedColumn { column: String },
    DuplicateKey,
    Unknown,
}

pub fn error_kind(err: &mysql_async::Error) -> ErrorKind {
    // Extract MySQL error code and message, classify
}
```

## Testing

Port the Postgres integration tests to MySQL, using `testcontainers` with a MySQL container.

Tests should cover:
- Basic CRUD: create entity, read back, verify state
- Upsert: set_state twice, verify changed detection
- fetch_states with predicate pushdown
- Materialized column auto-creation
- Event storage and retrieval
- Multiple collections
- Concurrent access (DDL lock contention)
- Missing table auto-creation
