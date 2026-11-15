//! Ordered maps of bytes on the PostgreSQL engine, beside its entity records,
//! and the locks its storage transactions take.
//!
//! Every map's entries live in one table, keyed by map and key, whose keys
//! order by their bytes as maps' keys do. The map and the key together are
//! the table's primary key, so what PostgreSQL stores of them, which it
//! compresses where it can, is bounded by what one entry of its B-tree index
//! holds, about a third of a page: a put whose key doesn't fit fails its
//! transaction with the database's error.
//!
//! A storage transaction that makes a map call holds a connection of the
//! engine's maps' pool from that call to its end, and its native transaction
//! there begins with that call's request: its map writes and its entity
//! records then commit together on that connection. A lock name is an
//! advisory lock of that native transaction. A snapshot is a `REPEATABLE
//! READ` read-only transaction on a connection of its own.
//!
//! Each map call is one request, its statements sent together and answered
//! in one round trip; writes wait to go ahead of the transaction's next
//! request, and a commit's go with its `COMMIT`, after any entity records'
//! statements. So a storage transaction that takes its locks and reads in one
//! call and then writes costs two round trips, and one with entity records
//! only their statements' round trips besides.

pub(crate) mod calls;
mod connection;
mod error;
mod request;
mod snapshot;

pub(crate) use connection::schema::reset;
pub use connection::MapSettings;
pub(crate) use connection::{Lease, Pool};
pub use error::MapError;
pub use snapshot::PostgresMapSnapshot;
