# MySQL CDC and Replication-Based Observation

## Overview

MySQL binary log (binlog) replication provides a stream of row-level changes committed to the database. Ankurah uses this stream for two distinct purposes:

1. **External write lifting** — Detecting writes made outside ankurah (by non-ankurah code that writes directly to MySQL), creating synthetic ankurah events from those changes, and updating the record's head to bring ankurah back into sync.

2. **Durable node observation** — Allowing durable ankurah nodes sharing the same MySQL database to observe each other's committed changes via the binlog, rather than relying solely on peer-to-peer messaging.

Both modes consume the same binlog stream. The discriminator is whether `_ankurah_head` changed in a given row event:

- **Head unchanged** → external write (non-ankurah). Handled by mode 1.
- **Head changed** → ankurah write (by another durable node). Handled by mode 2.

## MySQL Server Requirements

The binlog listener requires row-based replication with full row images:

```ini
[mysqld]
binlog_format     = ROW
binlog_row_image  = FULL
log_bin           = mysql-bin
expire_logs_days  = 7     # retain binlogs for listener catch-up
```

The listener connects as a replication client. Required MySQL user privileges:

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ankurah_repl'@'%';
```

`binlog_row_image = FULL` is essential — it ensures both before-image and after-image contain all columns, which the discriminator needs to detect whether `_ankurah_head` changed.

## Binlog Listener

A single in-process async task per database that connects to MySQL as a replication client and processes row events.

```rust
pub struct BinlogListener {
    pool: mysql_async::Pool,
    position: BinlogPosition,
    watched_tables: HashSet<String>,  // "database.table" format
    table_map: HashMap<u64, TableMapEvent>,  // binlog table_id -> metadata
}

pub struct BinlogPosition {
    pub filename: String,
    pub position: u64,
}
```

### Event Loop

The listener reads binlog events sequentially. Relevant event types:

- `TABLE_MAP_EVENT` — Maps a table_id to database + table name. Cache this for subsequent row events.
- `WRITE_ROWS_EVENT` — INSERT. After-image only.
- `UPDATE_ROWS_EVENT` — UPDATE. Before-image and after-image.
- `DELETE_ROWS_EVENT` — DELETE. Before-image only.

For each row event, check whether the table is watched. If not, skip without parsing row data. If watched, dispatch to the appropriate handler based on the head-field discriminator.

### Table Filtering

The listener only processes events for explicitly watched tables. The `TABLE_MAP_EVENT` provides database and table names for filtering. Non-watched tables are skipped without parsing row images, which is important for performance on databases with many tables.

## Mode 1: External Write Lifting

Exactly one instance of this runs per database. It is responsible for detecting non-ankurah writes and converting them into ankurah events.

### Discriminator

For `UPDATE_ROWS_EVENT`, compare the `_ankurah_head` column in the before-image and after-image:

- **Same value (or both NULL)** → external write. The writer didn't know about `_ankurah_head` and didn't modify it.
- **Different value** → ankurah write. Pass to mode 2 handler.

For `WRITE_ROWS_EVENT` (INSERT), check whether `_ankurah_head` is set in the after-image:

- **NULL** → external insert. The row was created outside ankurah.
- **Non-NULL** → ankurah insert.

For `DELETE_ROWS_EVENT`: defer to future work. Ankurah does not currently have a delete/tombstone operation.

### Synthetic Event Creation

When an external write is detected:

1. **Determine entity identity**: Read the `_ankurah_entity_id` column from the row. If NULL (row never touched by ankurah), compute a deterministic EntityId from the table name and primary key, and note that this needs to be written back.

2. **Determine parent clock**: Read `_ankurah_head` from the **binlog event's before-image** (for UPDATEs) or use an empty Clock (for INSERTs). Do NOT query the live database for the current head — the before-image in the binlog is the correct parent, and using it ensures deterministic EventId generation across crash recovery replays.

3. **Build OperationSet from column diffs**: For UPDATEs, compare before-image and after-image column by column. For INSERTs, all columns are "new." Construct LWW operations:

    ```rust
    // For each changed column, build an LWW diff
    let changed_values: BTreeMap<PropertyName, Option<Value>> = /* column diffs */;
    let lww_diff = LWWDiff { version: 1, data: bincode::serialize(&changed_values)? };
    let operation = Operation { diff: bincode::serialize(&lww_diff)? };
    ```

    The operation format must exactly match what `LWWBackend::apply_operations()` expects. This is `LWWDiff { version: u8, data: Vec<u8> }` where `data` is `bincode::serialize(&BTreeMap<PropertyName, Option<Value>>)`.

4. **Create the Event**:

    ```rust
    let event = Event {
        collection: CollectionId::from(table_name),
        entity_id,
        operations: OperationSet(btreemap!{ "lww".to_string() => vec![operation] }),
        parent: parent_clock,
    };
    ```

    The EventId is content-addressed: `SHA256(entity_id || operations || parent)`. Because the inputs are deterministic (entity_id from PK, operations from column values, parent from before-image), the same external write always produces the same EventId. This makes crash recovery replay idempotent — re-processing the same binlog event produces the same EventId, and `INSERT ... ON DUPLICATE KEY UPDATE id = id` on the event table is a no-op.

5. **Store the event** via `StorageCollection::add_event()`.

6. **Update the record's head**: Write `_ankurah_head` (and `_ankurah_entity_id` if it was NULL) back to the row:

    ```sql
    UPDATE `{table}` SET
        `_ankurah_head` = ?,
        `_ankurah_entity_id` = ?
    WHERE `{pk_column}` = ?
    ```

    This write itself appears in the binlog. The listener will see it on the next event — but the discriminator correctly classifies it as an ankurah write (head changed) and passes it to mode 2, which other nodes handle. The mode 1 listener recognizes its own writes because the head changed to a value it just computed.

7. **Notify the reactor**: The synthetic event needs to flow through the same notification path as native events. This triggers live query updates and durable subscription delivery for any subscribers watching the affected collection/entity.

### Singleton Constraint

Exactly one mode 1 process runs per database. Running multiple would produce duplicate synthetic events with potentially different EventIds (if they race on reading parent clocks). Enforcement options:

- **Configuration**: Designate one node as the external-write lifter.
- **MySQL named lock**: Acquire a persistent `GET_LOCK('ankurah_cdc_lifter', 0)` — only one connection gets it.
- **Leader election**: If multiple durable nodes exist, elect one to run mode 1.

The simplest starting point is configuration. Leader election can be added later if needed.

## Mode 2: Durable Node Observation

Every durable node sharing the same MySQL database can run a mode 2 observer. When it sees a binlog event where `_ankurah_head` changed (an ankurah write from another node), it:

1. **Identifies the entity**: Read `_ankurah_entity_id` from the after-image.

2. **Compares heads**: Read the incoming head from the after-image and compare it to the local head for this entity. If they match, the local node already has this state (it either made the write itself, or already received it via peer-to-peer). Skip — no further work.

3. **If heads differ**: The incoming head is ahead of (or divergent from) the local head. Read the new events from the event table using the EventIds in the incoming head that aren't in the local head. Alternatively, reconstruct the full state from the `state_buffer` column in the after-image.

4. **Updates local entity cache**: Apply the new events or replace the state.

5. **Notifies the reactor**: Triggers live query re-evaluation and durable subscription delivery on this node.

The head comparison in step 2 is the correctness gate. It is also the primary optimization: for a node's own writes, the local head was already updated during the normal commit path before the binlog event arrives, so the comparison is a cheap no-op that avoids touching the reactor at all.

For writes from other nodes, the head comparison correctly identifies that work needs to be done. The reactor notification in step 5 is necessary in this case — live queries may need to add, remove, or update entities.

### Reactor Traffic Optimization

Even with the head comparison gate, mode 2 introduces a new source of reactor notifications (one per observed external-node commit). For deployments with high write throughput across multiple durable nodes, this could increase reactor evaluation load.

Note: the entity may or may not be resident in the local node's entity set when the replication event arrives. And there may or may not be active subscriptions whose predicates match the changed entity. We cannot skip reactor notification based on entity residency — predicate-based subscriptions (live queries) can match entities that are not currently loaded.

Potential optimizations (not required for initial implementation):
- **Batch binlog events**: Accumulate a short window of binlog events (e.g., 10-50ms) and notify the reactor once with all changes, rather than per-event.
- **Collection-level filtering**: Only notify the reactor for entities whose collection has at least one active subscription. If no subscriptions exist for a collection, all binlog events for that collection's tables can be skipped.

### Latency Characteristics

Binlog delivery is near-real-time (typically sub-second) but not instant. Mode 2 provides eventual consistency between durable nodes via the shared database. For use cases requiring tighter consistency, the peer-to-peer protocol remains available.

## Listener Self-Write Handling

The listener writes to the database (mode 1 writes `_ankurah_head`), and those writes appear in the binlog. The listener must not create infinite loops.

This is handled naturally by the discriminator:
- Mode 1 writes change `_ankurah_head` → the event is classified as an ankurah write → mode 2 handler picks it up (if other nodes care) → mode 1 ignores it.
- Mode 2 is read-only (no database writes) → no feedback loop.

The mode 1 process should also track the EventIds it generates in the current session to quickly skip its own writes when they appear in the binlog.

## Position Persistence and Crash Recovery

The listener persists its binlog position for crash recovery:

```sql
CREATE TABLE IF NOT EXISTS `_ankurah_cdc_position` (
    `listener_id`   VARCHAR(255) PRIMARY KEY,
    `binlog_file`   VARCHAR(255) NOT NULL,
    `binlog_pos`    BIGINT UNSIGNED NOT NULL,
    `updated_at`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;
```

On startup, the listener reads its last saved position and connects to MySQL starting from that point. The position is saved periodically (e.g., every N events or every M seconds).

On crash, some events may be reprocessed from the last saved position. This is safe because:
- Mode 1: Synthetic events have deterministic content-addressed EventIds. Re-inserting the same event is a no-op (`ON DUPLICATE KEY UPDATE id = id`). The `_ankurah_head` writeback is also idempotent (writing the same head value).
- Mode 2: Applying the same state/events that the node already has is a no-op (head comparison detects no change).

The key to idempotent mode 1 replay is that the parent clock comes from the binlog before-image, not from a live query. The before-image is immutable in the binlog, so replaying the same event produces the same parent and thus the same EventId.

## Column Type Mapping for Synthetic Events

The binlog listener needs to convert MySQL column values from the binlog row images into ankurah `Value` variants for constructing LWW operations. This mapping mirrors the `MySQLValue` → `Value` conversion in the storage engine but operates on raw binlog column data rather than query results.

The `TABLE_MAP_EVENT` provides column type metadata (MySQL type codes), which the listener uses to interpret the raw bytes in row images.

## Multi-Database / Multi-Tenant Considerations

Each MySQL database has its own binlog (or shares a server-level binlog with database-prefixed events). For multi-tenant deployments:

- If tenants have **separate MySQL servers**: one binlog listener per server.
- If tenants share a **MySQL server** (different databases): one binlog stream contains events for all databases. The `TABLE_MAP_EVENT` includes the database name, so the listener can route events to the correct tenant context.

Start with one listener per database for simplicity. Multi-database listeners can be optimized later if connection count becomes a concern.

## Integration with Durable Subscriptions

If the durable subscriptions feature is implemented for MySQL (with `commit_seq` on the event table), synthetic events from mode 1 use `add_event()` which assigns the next `commit_seq`. From the perspective of durable subscriptions, synthetic events are indistinguishable from native events. They appear in the commit sequence and are delivered to subscribers normally.

## Future Work

- **DELETE handling**: Create tombstone events when external DELETEs are detected. Depends on ankurah adding a delete/tombstone operation.
- **Leader election** for mode 1 singleton: Automatic failover if the designated lifter node goes down.
- **Batched synthetic events**: For high-volume external writes, batch multiple column changes into fewer synthetic events.
- **Postgres CDC**: Logical replication for Postgres serves the same role. The mode 1 / mode 2 distinction applies equally. This spec should not paint Postgres CDC into a corner.
