//! Ordered maps on the IndexedDB engine, beside its entity records, and the
//! locks its storage transactions take.
//!
//! Every map's entries live in one object store, each under its map's name
//! and its own key, both as bytes, which IndexedDB orders as maps' keys
//! order. The two object stores of the maps come through an upgrade of the
//! database, at the first map call that finds them missing.
//!
//! Every tab is a node of its own on the database, and an IndexedDB
//! transaction ends once its requests are done and control goes back to the
//! event loop, so no lock in the database can hold a name from a storage
//! transaction's first map call to its commit. Within the engine, a writers'
//! lock per name makes a transaction asking for a name wait until the one
//! holding it ends. Across tabs, each name has a version, kept in an object
//! store of its own, where an absent name is at version 0: a transaction
//! notes each name's version as it takes the name, and its commit reads
//! every noted version again in its one readwrite transaction, before
//! anything else, fails with a write conflict if one moved, and otherwise
//! raises each beside its writes. IndexedDB runs readwrite transactions over
//! the same object stores one at a time, so no other commit comes between
//! the comparison and the writes.
//!
//! A reset empties the maps and clears the versions, and puts a new reset
//! epoch, one more record beside the versions, in the same readwrite
//! transaction. Every map transaction and snapshot notes the epoch at its
//! first map call, so a reset since fails its later reads and its commit as a
//! write conflict, even one whose names are back at the versions it noted.
//!
//! A storage transaction's map writes wait in memory until its commit, and
//! its reads lay them over what the database holds. Reads are made in
//! readonly transactions, each of which sees one committed state while it
//! lasts. A read made while the last read's transaction is still active is
//! made in it; any other opens a new one, which reads the noted versions and
//! epoch again before anything it reads means anything, and fails the read
//! as a write conflict if one moved. Later reads are made in the new
//! transaction only once that comparison has passed.
//!
//! A storage transaction that makes no map call takes none of this: its
//! commit's readwrite transaction covers only the entity records' object
//! stores, and neither reads nor writes the maps'.

mod error;
mod layout;
mod reading;
mod requests;
mod snapshot;
mod transaction;
mod versions;
mod writes;

pub use error::MapError;
pub(crate) use layout::{create_object_stores, has_object_stores, STORES};
pub use snapshot::IndexedDbMapSnapshot;
pub(super) use transaction::TransactionMaps;
pub(super) use writes::{reset, MapLanding};
