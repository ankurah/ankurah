//! Digest-tree maintenance across PostgreSQL connections, failures and wire batches.

#[path = "../database/mod.rs"]
mod database;
mod faults;
mod nodes;
mod resets;
mod round_trips;
#[path = "../server/mod.rs"]
mod server;
type Tree = ankurah::core::sync::conformance::fixtures::Tree<ankurah_storage_postgres::Postgres, ankurah_digest_tree::Ristretto255>;
