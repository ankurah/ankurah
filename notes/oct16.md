Production-usable event-sourced ORM which uses an off the shelf database for storage - Creating entities - Setting entities - Deleting entities - Relationships?
Rust structs for data modeling - Alter database based on this? Making rust structs the ground truth for data layout. - Derive macros to set up relationships?
Signals pattern for notifications - Per storage source implementation?
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
name TEXT NOT NULL, // all of this data should be in the state binary - we're just putting it here for humans looking at the database
precursors ARRAY<ULID>,
state BINARY // this contains all state necessary to reconstitute a yrs::Doc, or other state
// representations - eg: serde serialized Vec<TypeModuleState> where one of those ops is a YrsState
);
// imagine you have 5 fields, which are driven by 2 modules: yrs for 3 of them, and LWW (last writer wins) for 2 of them
// imagine in a transaction you change all 5 fields - how many operations should have? (2)

CREATE TABLE album_events (
id ULID
entity_id ULID,
user_id ULID,
precursors ARRAY<ULID>,
operations BINARY serde serialized Vec<TypeModuleOp>
);

op1 <- op2 <- op3 <- op4
\- opQ <- opR /

#kvstore mechanics for entity-based storage
bucket1 uses pkey as the key and contains the serialized entity as the value
other buckets define indexes:

- bucket2 uses the collated name as the key and the pkey as the value

Desires:
[ ] - Audit trail to know who did what
[ ] -

Crude version of Text CDRT:

- INSERT OP op1 @ null,0: "helo"
- INSERT OP op2 @ op2, 2: "l" -> State is "hello"
