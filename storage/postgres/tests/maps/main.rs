//! PostgreSQL's maps: the storage contract's map conformance suite, what a
//! reset does to them and to the map transactions open on them, and the
//! round trip a commit's map writes make.

#[path = "../database/mod.rs"]
mod database;
mod requests;
mod resets;
#[path = "../server/mod.rs"]
mod server;
mod suite;
