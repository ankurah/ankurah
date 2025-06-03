//! This module is a crude consistency enforcement mechanism that is used to
//! compare the results between a local fetch and a remote fetch, with the understanding
//! that items which are present in the local fetch, but missing from the remote fetch
//! are likely stale.
//!
//! This is a stopgap mechanism until we implement a more sophisticated consistency enforcement mechanism
//! based on event lineage

pub struct DiffResolver {}

// TODO move the detect_and_refresh_stale_entities logic here
