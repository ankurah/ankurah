//! Canonical value_type end-to-end (rfc.md 5.6 as amended 2026-07-10).
//!
//! A property's value_type is CANONICAL: fixed when the property is first
//! allocated, never changed by registration. A binary compiled with a
//! drifted-but-castable type operates through `Value::cast_to`: its commits
//! canonicalize staged values INTO the backends (so engines and indexes
//! collate one consistent type), its getters cast canonical values back out
//! to the compiled type, and comparison literals canonicalize at resolution
//! so predicate evaluation matches the stored type. A value the canonical
//! type cannot represent fails at the writer: during eager `create()` when it
//! is part of the identity-bearing genesis, or at commit for later edits.
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

    // A genesis value the canonical type cannot represent fails eager
    // create, at the writer: genesis operations freeze (and determine the
    // entity id) before create returns, so this cannot be deferred to commit.
    // 5_000_000_000 does not fit i32.
    let trx = ctx.begin();
    let err = match trx.create(&v2::Film { year: 5_000_000_000 }).await {
        Ok(_) => panic!("an i64 genesis value exceeding canonical i32 must fail eager create"),
        Err(err) => err,
    };
    assert!(err.to_string().to_lowercase().contains("overflow"), "expected a numeric-overflow cast error, got: {err}");

    Ok(())
}

/// The reactor's byte-collated watcher index matches through the canonical
/// type: the drifted (i64) subscriber's literal canonicalizes at resolution,
/// the drifted writer's value canonicalizes at commit, and the entity-side
/// extraction reads canonical bytes -- so a live query notifies across the
/// drift in both directions (match arrives, match leaves).
#[tokio::test]
async fn live_query_watcher_matches_through_canonical_types() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    // v1 (i32) registers first and fixes the canonical type.
    let trx = ctx.begin();
    trx.create(&v1::Film { year: 1980 }).await?;
    trx.commit().await?;

    // Subscribe through the drifted v2 model with an i64 literal.
    let watcher = TestWatcher::changeset();
    let predicate = ankql::parser::parse_selection("year = 2020")?;
    use ankurah::signals::{Peek, Subscribe};
    let query = ctx.query_wait::<v2::FilmView>(predicate).await?;
    let _handle = query.subscribe(&watcher);
    assert_eq!(watcher.quiesce().await, 0, "no initial matches");

    // A v2 write (staged I64, canonicalized to I32 at commit) must reach the
    // watcher through the canonical index bytes.
    let trx = ctx.begin();
    let film = trx.create(&v2::Film { year: 2020 }).await?;
    let film_id = film.id();
    trx.commit().await?;
    assert!(watcher.wait().await, "the watcher must fire for the canonicalized match");
    assert_eq!(query.peek().iter().map(|f| f.id()).collect::<Vec<_>>(), vec![film_id]);

    // Move it out of the match window through the drifted setter: removal
    // notifies too.
    let trx = ctx.begin();
    let views: Vec<v2::FilmView> = ctx.fetch("year = 2020").await?;
    views[0].edit(&trx)?.year().set(&1999i64)?;
    trx.commit().await?;
    assert!(watcher.wait_for_count(2, Some(std::time::Duration::from_secs(10))).await, "the removal must notify");
    assert!(query.peek().is_empty(), "the entity left the live query");

    Ok(())
}

/// An ill-typed payload under a KNOWN property id (what a misbehaving
/// peer's schema-blind-applied event could leave behind) is contained at
/// every read boundary, each with its own posture: storage ACCEPTS it
/// schema-blind (ingest never blocks on the catalog); a direct get loads
/// it, and the typed getter fails loudly (`NonCastable`) instead of
/// fabricating a default; predicate evaluation reads it as NULL (no error,
/// no phantom match); and a predicate FETCH through sled errors loudly at
/// scan materialization (the engine tier's coarse pre-#312 posture: a junk
/// entity fails the query rather than silently vanishing from it).
#[tokio::test]
async fn ill_typed_payload_is_contained_at_every_read_boundary() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    let trx = ctx.begin();
    let film = trx.create(&v1::Film { year: 2001 }).await?;
    let film_id = film.id();
    drop(film);
    trx.commit().await?;

    // Forge year = String("abc") under the REAL property id: the
    // String<->I32 pair is castable (so nothing refuses the pair), but this
    // value is not.
    let year_id = node.catalog.siblings_by_name("year")[0];
    let storage = node.collections.get(&"film".into()).await?;
    let mut state = storage.get_state(film_id).await?;
    let forged = {
        let b = ankurah::core::property::backend::LWWBackend::new();
        b.set(ankurah::core::property::PropertyKey::Id(year_id), Some(Value::String("abc".into())));
        let ops = b.to_operations().unwrap().unwrap();
        b.apply_operations_with_event(&ops, proto::EventId::from_bytes([7u8; 32])).unwrap();
        b.to_state_buffer().unwrap()
    };
    state.payload.state.state_buffers.0.insert("lww".into(), forged);

    // Storage accepts schema-blind.
    storage.set_state(state).await?;

    // A direct get loads the entity; the typed getter fails visible.
    let view = ctx.get::<v1::FilmView>(film_id).await?;
    let err = view.year().expect_err("an uncastable stored value must fail the getter");
    assert!(err.to_string().contains("not castable"), "expected NonCastable, got: {err}");

    // Predicate evaluation reads junk as NULL: no match, and no error.
    use ankurah::core::selection::filter::evaluate_predicate;
    use ankurah::model::View;
    let resolved = node.catalog.resolve_selection(&"film".into(), &ankql::parser::parse_selection("year = 2001")?)?;
    assert!(!evaluate_predicate(view.entity(), &resolved.predicate)?, "junk must not match its pre-forge value");
    let resolved = node.catalog.resolve_selection(&"film".into(), &ankql::parser::parse_selection("year = 5")?)?;
    assert!(!evaluate_predicate(view.entity(), &resolved.predicate)?, "junk must not match anything");

    // A predicate fetch through sled refuses loudly at scan materialization.
    let err = ctx.fetch::<v1::FilmView>("year = 2001").await.expect_err("sled scan materialization must refuse the junk");
    assert!(err.to_string().contains("Type mismatch"), "expected the engine's type mismatch, got: {err}");

    Ok(())
}

/// ORDER BY collates in the canonical type across drifted writers: numeric
/// order, never the lexicographic order a string collation would produce
/// ("10" < "2" < "3").
#[tokio::test]
async fn order_by_collates_numerically_through_canonical_types() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    let trx = ctx.begin();
    trx.create(&v1::Film { year: 3 }).await?;
    trx.create(&v2::Film { year: 10 }).await?;
    trx.create(&v2::Film { year: 2 }).await?;
    trx.commit().await?;

    let films: Vec<v2::FilmView> = ctx.fetch("year > 0 ORDER BY year ASC").await?;
    let years: Vec<i64> = films.iter().map(|f| f.year().unwrap()).collect();
    assert_eq!(years, vec![2, 3, 10], "canonical numeric collation, not lexicographic");

    Ok(())
}
