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

/// The resolved catalog property id an ORDER BY key carries, or `None` for an
/// unresolved path or the `id` pseudo-property. Mirrors the pre-column-layering
/// `OrderByItem::property` field the `OrderKey` enum replaced.
fn order_property_id(item: &ankql::ast::OrderByItem) -> Option<ulid::Ulid> {
    match &item.key {
        ankql::ast::OrderKey::Property(identifier) => match identifier.id_or_systemname() {
            ankql::ast::PropertyId::EntityId(id) => Some(id),
            _ => None,
        },
        ankql::ast::OrderKey::Path(_) => None,
    }
}

/// The display name an ORDER BY key renders to (the resolved-from label, or the
/// raw path's first step when unresolved). Mirrors the old
/// `OrderByItem::path.first()`.
fn order_display(item: &ankql::ast::OrderByItem) -> String {
    match &item.key {
        ankql::ast::OrderKey::Property(identifier) => identifier.to_string(),
        ankql::ast::OrderKey::Path(path) => path.first().to_string(),
    }
}

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

mod float_model {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Metric {
        pub score: f64,
    }
}

mod before_rename {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Ledger {
        pub release_year: f64,
    }
}

mod after_rename {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Ledger {
        #[property(renamed_from = "release_year")]
        pub year: f64,
    }
}

mod canonical_backend {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Profile {
        pub name: String,
    }
}

mod incompatible_backend {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Profile {
        #[active_type(LWW)]
        pub name: String,
    }
}

fn assert_resolved_f64_comparison(selection: &ankql::ast::Selection, expected: f64) {
    use ankql::ast::{Expr, Literal, Predicate};
    match &selection.predicate {
        Predicate::Comparison { left, right, .. } => {
            assert!(matches!(left.as_ref(), Expr::PropertyPath(_)), "property reference must be catalog-resolved: {left:?}");
            assert!(
                matches!(right.as_ref(), Expr::Literal(Literal::F64(value)) if *value == expected),
                "comparison literal must be canonical f64 {expected}, got {right:?}"
            );
        }
        other => panic!("expected comparison predicate, got {other:?}"),
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

/// The first-use latch is schema-shaped, not merely collection-shaped. A
/// second compiled model for the same collection must still pass canonical
/// compatibility instead of inheriting the first model's successful latch.
#[tokio::test]
async fn distinct_schema_for_ensured_collection_still_runs_compatibility_gate() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    let trx = ctx.begin();
    trx.create(&canonical_backend::Profile { name: "canonical yrs".into() }).await?;
    trx.commit().await?;

    let fetch_error = ctx
        .fetch::<incompatible_backend::ProfileView>("name = 'canonical yrs'")
        .await
        .expect_err("fetch must admit its exact schema even when the catalog already resolves the field name");
    assert!(fetch_error.to_string().contains("not castable"), "the incompatible fetch must fail registration, got: {fetch_error}");

    use ankurah::signals::With;
    let query = ctx.query::<incompatible_backend::ProfileView>("name = 'canonical yrs'")?;
    query.wait_initialized().await;
    query.error().with(|error| {
        let message = error.as_ref().expect("live query must surface exact-schema registration failure").to_string();
        assert!(message.contains("not castable"), "the incompatible query must fail registration, got: {message}");
    });

    let trx = ctx.begin();
    let error = trx
        .create(&incompatible_backend::Profile { name: "incompatible lww".into() })
        .await
        .expect_err("a different schema shape must not reuse the collection-only registration latch");
    assert!(error.to_string().contains("unconfirmed schema"), "the incompatible backend must fail before writing, got: {error}");

    // A failed declaration must not poison a later transaction that uses the
    // already-confirmed compatible shape for this collection.
    let trx = ctx.begin();
    trx.create(&canonical_backend::Profile { name: "still canonical".into() }).await?;
    trx.commit().await?;

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
        b.set(ankql::ast::PropertyId::EntityId(year_id.to_ulid()), Some(Value::String("abc".into())));
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

/// The async live-query paths keep one catalog-resolved selection: the exact
/// form forwarded to the durable peer is also stored for local reactor
/// activation. Integer syntax against a canonical f64 property is deliberate:
/// the parser produces I64, so a raw/resolved split is directly observable.
#[tokio::test]
async fn live_query_initial_and_updated_selections_stay_resolved() -> Result<()> {
    use ankurah::signals::With;

    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context_async(DEFAULT_CONTEXT).await;
    let client_ctx = client.context_async(DEFAULT_CONTEXT).await;

    // Register the model/property before the ephemeral query warms its map.
    let trx = server_ctx.begin();
    trx.create(&float_model::Metric { score: 0.0 }).await?;
    trx.commit().await?;

    let query = client_ctx.query_wait::<float_model::MetricView>(nocache("score = 5")?).await?;
    let (selection, version) = query.selection().peek();
    assert_eq!(version, 1);
    assert_resolved_f64_comparison(&selection, 5.0);

    let watcher = TestWatcher::changeset();
    let _guard = query.subscribe(&watcher);
    assert_eq!(watcher.quiesce().await, 0);

    let trx = server_ctx.begin();
    let five_id = trx.create(&float_model::Metric { score: 5.0 }).await?.id();
    trx.commit().await?;
    assert!(watcher.wait().await, "canonical f64 update must reach the ephemeral reactor watcher");
    assert_eq!(query.ids(), vec![five_id]);
    watcher.drain();

    query.update_selection_wait("score = 6").await?;
    let (selection, version) = query.selection().peek();
    assert_eq!(version, 2);
    assert_resolved_f64_comparison(&selection, 6.0);
    assert!(watcher.wait().await, "selection update must remove the old result");
    assert!(query.ids().is_empty(), "score=5 must leave after updating the selection to score=6");
    watcher.drain();

    let trx = server_ctx.begin();
    let six_id = trx.create(&float_model::Metric { score: 6.0 }).await?.id();
    trx.commit().await?;
    assert!(watcher.wait().await, "newly matching score=6 must enter the updated live query");
    assert_eq!(query.ids(), vec![six_id]);
    watcher.drain();

    // The durable update path uses the same resolve/store/activate sequence.
    let durable_query = server_ctx.query_wait::<float_model::MetricView>("score = 5").await?;
    durable_query.update_selection_wait("score = 7").await?;
    let (selection, version) = durable_query.selection().peek();
    assert_eq!(version, 2);
    assert_resolved_f64_comparison(&selection, 7.0);

    // An update may be requested immediately after construction, while the
    // initial ephemeral resolution/registration task is still in flight.
    let eager = client_ctx.query::<float_model::MetricView>(nocache("score = 8")?)?;
    tokio::time::timeout(std::time::Duration::from_secs(5), eager.update_selection_wait("score = 9"))
        .await
        .expect("eager update must complete instead of waiting on an obsolete initial version")?;
    let (selection, version) = eager.selection().peek();
    assert_eq!(version, 2);
    assert_resolved_f64_comparison(&selection, 9.0);
    eager.error().with(|error| assert!(error.is_none(), "eager update must not race initial registration: {error:?}"));

    // Cached ephemeral queries also run an immediate local-cache activation;
    // that path must not open the update gate before the relay entry exists.
    let eager_cached = client_ctx.query::<float_model::MetricView>("score = 12")?;
    eager_cached.update_selection_wait("score = 13").await?;
    let (selection, version) = eager_cached.selection().peek();
    assert_eq!(version, 2);
    assert_resolved_f64_comparison(&selection, 13.0);
    eager_cached.error().with(|error| assert!(error.is_none(), "cached eager update must wait for relay insertion: {error:?}"));

    let eager_durable = server_ctx.query::<float_model::MetricView>("score = 10")?;
    eager_durable.update_selection_wait("score = 11").await?;
    let (selection, version) = eager_durable.selection().peek();
    assert_eq!(version, 2);
    assert_resolved_f64_comparison(&selection, 11.0);
    eager_durable.error().with(|error| assert!(error.is_none(), "eager durable update must wait for initial resolution: {error:?}"));

    // Unknown updates fail closed and leave the last good resolved selection
    // in place rather than storing an unresolved AST for later activation.
    // The waiter itself surfaces the latched error.
    let last_good = query.selection().peek();
    let error =
        query.update_selection_wait("not_a_property = 1").await.expect_err("unknown property update must surface an error from the waiter");
    assert!(error.to_string().contains("not_a_property"), "the surfaced error names the unknown property: {error}");
    query.error().with(|error| assert!(error.is_some(), "the latched error must remain observable after the waiter returns it"));
    assert_eq!(query.selection().peek(), last_good);

    Ok(())
}

/// A catalog rename preserves property identity while Sled preserves the
/// first physical column name. ORDER BY must carry the canonical type across
/// that name translation instead of trying to resolve the old column name
/// back through the current catalog.
#[tokio::test]
async fn renamed_numeric_property_keeps_canonical_sled_collation() -> Result<()> {
    let server = durable_sled_setup().await?;
    let ctx = server.context_async(DEFAULT_CONTEXT).await;

    let trx = ctx.begin();
    let ten_id = trx.create(&before_rename::Ledger { release_year: 10.0 }).await?.id();
    let two_id = trx.create(&before_rename::Ledger { release_year: 2.0 }).await?.id();
    let three_id = trx.create(&before_rename::Ledger { release_year: 3.0 }).await?.id();
    trx.commit().await?;

    // Install the query before the rename. Its predicate watcher and sort
    // extractor must retain the resolved property id after the display name
    // changes in the catalog.
    let active = ctx.query_wait::<before_rename::LedgerView>("release_year > 0 ORDER BY release_year ASC").await?;
    assert_eq!(active.ids(), vec![two_id, three_id, ten_id]);
    let resolved_property =
        order_property_id(&active.selection().peek().0.order_by.as_ref().unwrap()[0]).expect("resolved ORDER BY carries the property id");
    let watcher = TestWatcher::changeset();
    let _guard = active.subscribe(&watcher);
    assert_eq!(watcher.quiesce().await, 0);

    let client = ephemeral_sled_setup().await?;
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    let response = client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::RegisterSchema {
                models: vec![],
                properties: vec![proto::PropertyDescriptor {
                    minting_collection: "ledger".into(),
                    name: "year".into(),
                    renamed_from: Some("release_year".into()),
                    backend: "lww".into(),
                    value_type: "f64".into(),
                    target_collection: None,
                    explicit_id: None,
                }],
                memberships: vec![],
            },
        )
        .await?;
    assert!(matches!(response, proto::NodeResponseBody::SchemaRegistered { .. }));

    // A binary that registered before the rename keeps the exact property id
    // behind its old local alias. Fresh reads, queries, and staged writes must
    // not reconstruct identity from the catalog's new display name.
    let stale = ctx.get::<before_rename::LedgerView>(ten_id).await?;
    assert_eq!(stale.release_year()?, 10.0);
    let stale_results: Vec<before_rename::LedgerView> = ctx.fetch("release_year > 0 ORDER BY release_year ASC").await?;
    assert_eq!(stale_results.iter().map(|ledger| ledger.id()).collect::<Vec<_>>(), vec![two_id, three_id, ten_id]);
    let trx = ctx.begin();
    stale.edit(&trx)?.release_year().set(&12.0)?;
    trx.commit().await?;
    assert_eq!(ctx.get::<before_rename::LedgerView>(ten_id).await?.release_year()?, 12.0);
    watcher.quiesce_drain().await;

    // Rebuild the installed query from its serialized/stored resolved
    // selection, the same shape a relay reconnect re-upserts. The property id
    // remains the pre-rename identity; the ORDER BY key's Display label is a
    // resolved-at snapshot that column-layering (#307) deliberately does NOT
    // refresh on re-resolution (core/src/schema/resolve.rs, `resolve_order_by`),
    // so it keeps the pre-rename "release_year".
    let stored_selection = active.selection().peek().0;
    active.update_selection_wait(stored_selection).await?;
    let rebuilt = active.selection().peek().0;
    let rebuilt_order = &rebuilt.order_by.as_ref().unwrap()[0];
    assert_eq!(order_property_id(rebuilt_order), Some(resolved_property));
    assert_eq!(order_display(rebuilt_order), "release_year");
    assert_eq!(active.ids(), vec![two_id, three_id, ten_id]);
    watcher.quiesce_drain().await;

    let ledgers: Vec<after_rename::LedgerView> = ctx.fetch("year > 0 ORDER BY year ASC").await?;
    let years: Vec<f64> = ledgers.iter().map(|ledger| ledger.year().unwrap()).collect();
    assert_eq!(years, vec![2.0, 3.0, 12.0], "renamed physical column must retain canonical numeric collation");

    let trx = ctx.begin();
    let seven_id = trx.create(&after_rename::Ledger { year: 7.0 }).await?.id();
    let one_id = trx.create(&after_rename::Ledger { year: 1.0 }).await?.id();
    trx.commit().await?;

    assert!(watcher.wait().await, "the pre-rename predicate watcher must observe writes under the stable property id");
    assert_eq!(
        active.ids(),
        vec![one_id, two_id, three_id, seven_id, ten_id],
        "the pre-rename ORDER BY must extract new values through the stable property id"
    );

    Ok(())
}
