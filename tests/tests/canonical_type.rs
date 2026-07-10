//! Canonical value_type end-to-end (rfc.md 5.6 as amended 2026-07-10).
//!
//! A property's value_type is CANONICAL: fixed when the property is first
//! allocated, never changed by registration. A binary compiled with a
//! drifted-but-castable type operates through `Value::cast_to`: its commits
//! canonicalize staged values INTO the backends (so engines and indexes
//! collate one consistent type), its getters cast canonical values back out
//! to the compiled type, and comparison literals canonicalize at resolution
//! so predicate evaluation matches the stored type. A value the canonical
//! type cannot represent fails ITS OWN commit, at the writer.
//!
//! The two fleet versions are modeled as two same-name structs in sibling
//! modules: both derive collection "film"; v1 (i32) registers first and fixes
//! the canonical type, v2 (i64) is the castable drift.

mod common;

use ankurah::core::property::backend::PropertyBackend;
use ankurah::core::value::Value;
use ankurah::policy::DEFAULT_CONTEXT;
use ankurah::Model;
use anyhow::Result;
use common::*;
use serde::{Deserialize, Serialize};

mod v1 {
    use super::*;
    /// The first fleet version: `year: i32` fixes the canonical type.
    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Film {
        pub year: i32,
    }
}

mod v2 {
    use super::*;
    /// A later fleet version compiled with `year: i64`: castable drift.
    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Film {
        pub year: i64,
    }
}

#[tokio::test]
async fn castable_drift_writes_and_reads_through_the_canonical_type() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    // v1 creates first: registration allocates `year` with canonical i32.
    let v1_film_id = {
        let trx = ctx.begin();
        let film = trx.create(&v1::Film { year: 1999 }).await?;
        let id = film.id();
        trx.commit().await?;
        id
    };

    // v2 (compiled i64) creates another film: the staged I64 canonicalizes
    // to I32 at commit, before it reaches the backends or the wire.
    let v2_film_id = {
        let trx = ctx.begin();
        let film = trx.create(&v2::Film { year: 2001 }).await?;
        let id = film.id();
        trx.commit().await?;
        id
    };

    // The stored payload is canonically typed: I32, not the writer's I64.
    let storage = node.collections.get(&"film".into()).await?;
    let state = storage.get_state(v2_film_id).await?;
    let buffer = state.payload.state.state_buffers.0.get("lww").expect("film fields are LWW").clone();
    let values: Vec<Option<Value>> =
        ankurah::core::property::backend::LWWBackend::from_state_buffer(&buffer)?.property_values().into_values().collect();
    assert!(values.contains(&Some(Value::I32(2001))), "the v2 write must be stored as canonical I32, got {values:?}");

    // Each compiled type reads both films through the canonical<->compiled
    // casts: v2 (i64) over v1's I32 data, v1 (i32) over v2's canonicalized
    // write.
    let through_v2: v2::FilmView = ctx.get(v1_film_id).await?;
    assert_eq!(through_v2.year()?, 1999i64);
    let through_v1: v1::FilmView = ctx.get(v2_film_id).await?;
    assert_eq!(through_v1.year()?, 2001i32);

    // Comparison literals canonicalize at resolution: v2's i64 literal
    // collates as I32 against the stored values.
    let hits: Vec<v2::FilmView> = ctx.fetch("year = 2001").await?;
    assert_eq!(hits.len(), 1, "the i64 literal must match the canonically stored I32");
    assert_eq!(hits[0].id(), v2_film_id);

    // A value the canonical type cannot represent fails ITS commit, at the
    // writer: 5_000_000_000 does not fit i32.
    let trx = ctx.begin();
    trx.create(&v2::Film { year: 5_000_000_000 }).await?;
    let err = trx.commit().await.expect_err("an i64 value exceeding the canonical i32 must fail the commit");
    assert!(err.to_string().to_lowercase().contains("overflow"), "expected a numeric-overflow cast error, got: {err}");

    Ok(())
}
