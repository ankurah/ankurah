
Production-usable event-sourced ORM which uses an off the shelf database for storage
    - Creating records
    - Setting records
    - Deleting records
    - Relationships?
Rust structs for data modeling
    - Alter database based on this? Making rust structs the ground truth for data layout.
    - Derive macros to set up relationships?
Signals pattern for notifications
    - Per storage source implementation?
WASM Bindings for client-side use
React Bindings
Basic included data-types: including CRDT text and primative types
Very basic embedded KV backing store (Sled DB)
Basic, single field queries (auto indexed?)

Given:

pub struct Album {
    name: String,
}

what data do we want to persist?

// materialized state table gives you the ability to see the current state easily
// and you don't have to apply all the events ever in order to get a starting state
CREATE TABLE album (
    id ULID PRIMARY KEY,
    name TEXT NOT NULL,
    precursors ARRAY<ULID>,
);
CREATE TABLE album_events (
    id ULID
    record_id ULID,
    precursors ARRAY<ULID>,
    operation BINARY
);

op1 <- op2 <- op3   <- op4
    \- opQ <- opR /

#kvstore mechanics for record-based storage
bucket1 uses pkey as the key and contains the serialized record as the value
other buckets define indexes:
   - bucket2 uses the collated name as the key and the pkey as the value

Desires:
[ ] - Audit trail to know who did what
[ ] - 


Crude version of Text CDRT:
- INSERT OP op1 @ null,0: "helo"
- INSERT OP op2 @ op2, 2: "l" -> State is "hello"