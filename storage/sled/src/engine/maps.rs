//! Ordered maps of bytes on the sled engine, beside its entity records, and
//! the locks its storage transactions take.
//!
//! Every map's entries live in one sled tree, each under its map's name and
//! then its key, so that a map's entries lie together in the order of their
//! keys. A storage transaction's map writes wait in the transaction, which
//! lays them over the stored entries for its own reads, until its commit
//! lands them with its entity records in one sled transaction.
//!
//! A sled transaction can't read a range, and reads outside one can see a
//! sled transaction landed between them, so the engine keeps in process the
//! locks a map transaction relies on. Under each lock name there are two:
//!
//! - a writers' lock, which a storage transaction holds from the call that
//!   takes the name to the transaction's end, so that one transaction at a
//!   time reads and writes under the name;
//! - a readers' lock, which a snapshot under the name holds shared while it
//!   is open, and a commit holding the name's writers' lock holds exclusively
//!   while it lands, so that a snapshot sees the maps before that commit or
//!   after it, and never waits for a transaction that is open but not yet
//!   committing.
//!
//! A commit with map writes lands while no other one does: it lists the
//! entries each map it clears holds and removes each of them, and no entry
//! arrives between the listing and the removal. A reset of the engine holds
//! the reset lock exclusively, which every map transaction and snapshot
//! shares from its first map call to its end, so a reset waits for the map
//! transactions and snapshots under way, and none lands anything in the maps
//! it empties.

mod keys;
mod landing;
mod locks;
mod read;
mod snapshot;
mod transaction;

pub(crate) use landing::MapEntries;
pub(crate) use locks::MapLocks;
pub use snapshot::SledMapSnapshot;
pub(crate) use transaction::TransactionMaps;
