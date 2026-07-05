//! Crash and recovery fault injection suite (concurrency phase 2, workstream C6).
//!
//! Process-kill and reopen cycles over the sled storage engine at adversarial
//! points. Each scenario spawns the SAME test binary as a child, crashes it
//! mid-write at a deterministic storage operation, then reopens the surviving
//! sled directory and asserts the recovery invariants. See `harness.rs` for the
//! wrapper and re-exec plumbing, and `scenarios.rs` for the individual crash
//! points and the invariants each one pins.
//!
//! IndexedDB/wasm is out of scope: `wasm-pack test` has no process-kill
//! semantics (there is no OS process to abort and no on-disk directory to
//! reopen), so the child/reopen model does not apply. Postgres is structured as
//! an env-gated arm in `scenarios.rs`.

mod harness;
mod models;
mod scenarios;
