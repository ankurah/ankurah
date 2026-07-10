//! RFC 5.4 (specs/model-property-metadata/rfc.md) read-path rules with the sibling gate (work package A10).
//!
//! Four behaviors, pinned end to end:
//!  1. Retype lineage (the crown): a same-display-name property from a
//!     different property id holds data on an entity; reading the field through
//!     the new contract is fail-visible `TypeSkew` -- from the compiled View
//!     getter AND from a resolved-identifier predicate comparison -- never a
//!     fabricated default over the sibling's real value (rule 4).
//!  2. A required LWW `i64` that is absent, with NO sibling, reads the type
//!     default `0` (rule 3, the #175 fix generalized to LWW). A required
//!     `String` reads "", a required `bool` reads `false`.
//!  3. An absent OPTIONAL field reads `None`, `IS NULL` matches it, and a
//!     comparison against it is false (rule 2 + the absent-as-NULL predicate
//!     unification).
//!  4. A membership whose `optional` follow-up has not arrived is treated as
//!     optional at the catalog level (`MembershipDef.optional == None`);
//!     predicate-level required-defaults are deliberately OUT OF SCOPE for
//!     Phase A (documented below).
//!
//! The cross-root transplant ruling (2026-07-05) applies throughout: the
//! sibling gate makes a foreign value fail visible rather than silently
//! substituting it; there is no lenient foreign-id fallback under the checked
//! read. See `core/src/property/backend/lww.rs::get_checked`.

mod common;

use ankurah::core::entity::Entity;
use ankurah::core::property::backend::lww::LWWBackend;
use ankurah::core::property::backend::PropertyBackend;
use ankurah::core::property::value::LWW;
use ankurah::core::property::{lww_read_checked, FromEntity, PropertyError, PropertyKey};
use ankurah::core::selection::filter::evaluate_predicate;
use ankurah::model::View;
use ankurah::proto::{self, EntityId};
use ankurah::value::Value;
use common::*;
use std::collections::BTreeMap;

/// Build a 0xA2 LWW state buffer holding `value` under `foreign_id` -- exactly
/// what a prior contract's data looks like on disk: a value keyed by a property
/// id the local catalog does not resolve. The backend is now a dumb identity
/// store, so we key the value directly under `PropertyKey::Id(foreign_id)` (no
/// binding, no display-name hint -- both were withdrawn by the PropertyKey
/// amendment, #289) and serialize; the sole entry is keyed by `foreign_id`.
fn foreign_id_state_buffer(foreign_id: EntityId, value: Value, event_id: proto::EventId) -> Vec<u8> {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(foreign_id), Some(value));
    let ops = backend.to_operations().unwrap().unwrap();
    backend.apply_operations_with_event(&ops, event_id).unwrap();
    backend.to_state_buffer().unwrap()
}

// ---------------------------------------------------------------------------
// (1) Retype lineage: the sibling gate fails visible (TypeSkew) over a
//     same-display-name sibling holding data.
// ---------------------------------------------------------------------------
//
// POST-#289 SHAPE: the sibling gate moved off the backend's display-name scan
// and onto the catalog-supplied `sibling_ids` set (amendment #289 point 6;
// `ankurah_core::property::lww_read_checked(backend, resolved_id, name, sibling_ids)`,
// dispatched OUTSIDE the backend). The backend is a
// dumb identity store now, so name-collision detection is a catalog concern and
// the backend does pure presence + the supplied gate. This test pins the gate
// at that home: the compiled View getter and resolved-identifier predicate both
// funnel through `lww_read_checked`, so exercising it directly with the two id
// entries a retype lineage produces is the faithful expression of the intent.
//
// (The prior end-to-end form seeded a value under a SYNTHETIC foreign id and
// leaned on a display-name hint + on-backend scan to make the getter skew. Both
// the hint and the on-backend name scan were withdrawn by #289, and a synthetic
// id is never in the catalog's `names_global`, so the resolver cannot surface it
// as a sibling -- an end-to-end retype now requires two real same-name property
// registrations, out of scope for this unit. The gate logic itself is unchanged
// and is what this pins.)

#[tokio::test]
async fn retype_lineage_type_skews_through_checked_dispatch() -> anyhow::Result<()> {
    // Two property-definition ids sharing the display name "title": A is the
    // locally-resolved (string) lineage, B a sibling (older i64) lineage whose
    // data is what sits on the entity -- the mid-retype / cross-root on-disk
    // state. The backend holds the value ONLY under B (id-keyed 0xA2), nothing
    // under A.
    let a_string = EntityId::new();
    let b_i64 = EntityId::new();
    assert_ne!(a_string, b_i64, "the sibling is a distinct property id");

    let event_id = proto::EventId::from_bytes([9u8; 32]);
    let buffer = foreign_id_state_buffer(b_i64, Value::I64(30), event_id.clone());
    assert_eq!(buffer[0], 0xA2, "seeded sibling state is 0xA2 (id-keyed)");
    let backend = LWWBackend::from_state_buffer(&buffer).unwrap();

    // Reading `title` as A, with B supplied as a same-name sibling, fails
    // visible (TypeSkew over B's real value) instead of fabricating a default:
    // A is absent, and the gate finds B holding data under the shared name.
    let err =
        lww_read_checked(&backend, a_string, "title", &[b_i64], |_| true).expect_err("read must fail visible over the retype sibling");
    match err {
        PropertyError::TypeSkew { name, a, b } => {
            assert_eq!(name, "title");
            assert_eq!(a, a_string.to_base64(), "a names the resolved (string) lineage");
            assert_eq!(b, b_i64.to_base64(), "b names the sibling (i64) lineage");
        }
        other => panic!("expected TypeSkew, got {other:?}"),
    }

    // Without the sibling in the supplied set (no catalog gate), the same absent
    // read is a clean `Ok(None)` -- the gate is exactly the supplied-siblings
    // input, not a property of the stored bytes. (`|_| true` also keeps the
    // foreign-data gate off; with `|_| false` B's un-nameable data would skew
    // via THAT gate instead, which is the bound-getter regime.)
    assert_eq!(lww_read_checked(&backend, a_string, "title", &[], |_| true).unwrap(), None, "absent id with no gate reads None, not skew");

    Ok(())
}

// ---------------------------------------------------------------------------
// (2) Required LWW absent, no sibling -> the View getter reads the type default.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn required_absent_i64_reads_type_default_zero() -> anyhow::Result<()> {
    // A bare entity with an empty LWW backend: no data, no binding, no sibling.
    // Reading a REQUIRED i64 projection reads the type default 0 (rule 3),
    // while a REQUIRED String reads "" and a REQUIRED bool reads false.
    let entity = Entity::create(EntityId::new(), "record".into());
    // Touch the LWW backend so it exists on the entity.
    let _ = entity.get_backend::<LWWBackend>().unwrap();

    let count: LWW<i64> = LWW::from_entity("count".into(), &entity);
    assert_eq!(count.get()?, 0, "absent required i64 reads the type default 0");

    let title: LWW<String> = LWW::from_entity("title".into(), &entity);
    assert_eq!(title.get()?, "", "absent required string reads the empty string (the #175 case)");

    let flag: LWW<bool> = LWW::from_entity("flag".into(), &entity);
    assert!(!flag.get()?, "absent required bool reads false");

    Ok(())
}

// ---------------------------------------------------------------------------
// (3) Optional absent -> None; IS NULL matches; a comparison does not.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn optional_absent_reads_none_and_is_null_matches() -> anyhow::Result<()> {
    // Projection half: an absent OPTIONAL reads None, never a fabricated inner
    // default (rule 2). Optionality short-circuits ahead of the required
    // default because the projected type is Option<i64>: its absent_default is
    // None, so the inner i64 default never fires.
    let entity = Entity::create(EntityId::new(), "record".into());
    let _ = entity.get_backend::<LWWBackend>().unwrap();
    let opt: LWW<Option<i64>> = LWW::from_entity("count".into(), &entity);
    assert_eq!(opt.get()?, None, "absent optional reads None");

    // Predicate half: through a real node so the identifier resolves. Register
    // an OPTIONAL `note` property on the record collection, then read a Record
    // that carries no `note` value -- so `note` resolves for the collection but
    // is absent on the entity.
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    // Warm the record model first (create+commit one record), then register the
    // optional `note` field against the same collection.
    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t".to_owned(), artist: "a".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    register_optional_note(&node).await?;
    let view = ctx.get::<RecordView>(rec_id).await?;

    // `note IS NULL` matches the entity that lacks note (absent -> NULL).
    let is_null = node.catalog.resolve_selection(&"record".into(), &ankql::parser::parse_selection("note IS NULL")?).unwrap();
    assert!(evaluate_predicate(view.entity(), &is_null.predicate)?, "absent optional matches IS NULL");

    // `note = 'x'` does NOT match (absent evaluates false, not an error).
    let eq = node.catalog.resolve_selection(&"record".into(), &ankql::parser::parse_selection("note = 'x'")?).unwrap();
    assert!(!evaluate_predicate(view.entity(), &eq.predicate)?, "comparison against absent optional is false");

    Ok(())
}

/// Register an OPTIONAL `note` LWW string property against the `record`
/// collection on a durable node, driving RFC 5.2 registration directly through
/// the durable execution path (a durable node registers itself; there is no
/// self peer connection to `request` over). Blocks until the catalog resolves
/// `note` so a later `resolve_selection` binds it.
async fn register_optional_note(node: &Node<SledStorageEngine, PermissiveAgent>) -> anyhow::Result<()> {
    node.execute_schema_registration(
        &DEFAULT_CONTEXT,
        vec![proto::ModelDescriptor { collection: "record".into(), name: "Record".into(), explicit_id: None }],
        vec![proto::PropertyDescriptor {
            minting_collection: "record".into(),
            name: "note".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "string".into(),
            target_collection: None,
            explicit_id: None,
        }],
        vec![proto::MembershipDescriptor {
            collection: "record".into(),
            property: proto::PropertyRef::Name("note".into()),
            optional: true,
        }],
    )
    .await?;
    node.catalog.wait_catalog_ready().await;
    for _ in 0..100 {
        if node.catalog.resolve("record", "note").is_some() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("catalog never reflected the optional `note` property");
}

// ---------------------------------------------------------------------------
// (4) Membership with no `optional` follow-up is treated optional (catalog).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn membership_without_optional_follow_up_is_treated_optional() -> anyhow::Result<()> {
    // RFC 5.4: a membership whose `optional` follow-up event has not arrived is
    // treated as optional (absent reads None), never defaulted. The catalog
    // datum this rule keys on is `MembershipDef.optional`, an `Option<bool>`
    // that is `None` until the follow-up lands (core/src/schema/catalog.rs,
    // `parse_membership`). This test pins that catalog datum for BOTH states.
    //
    // SCOPE (RFC 5.4 vs REN-ish strictness; see plan decision 14): predicate
    // evaluation does NOT consult per-membership optionality in Phase A -- the
    // filter's absent-as-NULL cut (test 3 above) unifies all three historical
    // missing-property behaviors, and required-defaults at the predicate level
    // are deliberately out of scope (they land only in the compiled View
    // getters, which obey their COMPILED optionality). This test pins the
    // catalog fact, not a predicate consequence.
    let node = durable_sled_setup().await?;
    let _ctx = node.context_async(DEFAULT_CONTEXT).await;

    // Register a widget model + label property + membership normally (directly
    // through the durable execution path). The registration protocol always
    // writes `optional`, so the applied membership reports Some(true): the
    // follow-up HAS arrived. The allocator returns the resolved ids.
    let (models, _properties, _memberships) = node
        .execute_schema_registration(
            &DEFAULT_CONTEXT,
            vec![proto::ModelDescriptor { collection: "widget".into(), name: "Widget".into(), explicit_id: None }],
            vec![proto::PropertyDescriptor {
                minting_collection: "widget".into(),
                name: "label".into(),
                renamed_from: None,
                backend: "lww".into(),
                value_type: "string".into(),
                target_collection: None,
                explicit_id: None,
            }],
            vec![proto::MembershipDescriptor {
                collection: "widget".into(),
                property: proto::PropertyRef::Name("label".into()),
                optional: true,
            }],
        )
        .await?;
    node.catalog.wait_catalog_ready().await;

    let model_id = models.iter().find(|m| m.collection == "widget").expect("widget model returned").id;
    let label_id = node.catalog.resolve("widget", "label").expect("label resolves after registration");
    let applied = wait_membership(&node, &model_id, &label_id).await.expect("membership present after registration");
    assert_eq!(applied.optional, Some(true), "an applied membership carries Some(optional) once the follow-up lands");

    // Now seed a GENESIS-ONLY membership: a `_ankurah_model_property` entity
    // carrying only the identity fields `model` + `property`, with NO
    // `optional` field. This is exactly the "follow-up not yet arrived" state.
    // The membership references a distinct (synthetic) property id so it does
    // not collide with the label membership above.
    let ghost_property = EntityId::new();

    // Commit the membership genesis (exactly the identity fields, no
    // `optional`) through commit_remote_transaction: the pipeline every
    // relayed catalog event takes -- it policy-checks, persists, applies, and
    // notifies the reactor, which is what feeds the catalog map's fetch-free
    // subscription. (A raw storage set_state would persist silently and the
    // map would never hear about it.)
    let genesis = membership_genesis(&model_id, &ghost_property);
    node.commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![proto::Attested::opt(genesis, None)]).await?;

    // Once the map applies it, `optional` is None -> treated optional.
    let ghost = wait_membership(&node, &model_id, &ghost_property).await.expect("genesis-only membership reflected in the catalog");
    assert_eq!(ghost.optional, None, "a membership whose `optional` follow-up has not arrived reports None (treated optional)");

    Ok(())
}

/// Build a genesis-only `_ankurah_model_property` event carrying just the
/// identity fields `model` + `property` (NO `optional` follow-up), the
/// "follow-up not yet arrived" catalog state. Mirrors the registration
/// executor's creation event: a name-keyed LWW backend (catalog bootstrap
/// exemption) with an empty parent clock. The genesis module was removed with
/// derivation (rev 4), so tests build this event directly.
fn membership_genesis(model: &EntityId, property: &EntityId) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::name("model"), Some(Value::EntityId(*model)));
    backend.set(PropertyKey::name("property"), Some(Value::EntityId(*property)));
    let operations = backend.to_operations().unwrap().unwrap();
    proto::Event {
        // #330: events carry a model id; the _ankurah_model_property catalog
        // collection has a well-known one.
        model: ankurah::core::schema::well_known_model_id(ankurah::core::schema::MODEL_PROPERTY_COLLECTION_ID)
            .expect("_ankurah_model_property has a well-known model id"),
        entity_id: EntityId::new(),
        operations: proto::OperationSet(BTreeMap::from([("lww".to_string(), operations)])),
        parent: proto::Clock::default(),
    }
}

/// Poll the catalog for a (model, property) membership, giving its wildcard
/// subscription time to apply a directly-seeded state.
async fn wait_membership(
    node: &Node<SledStorageEngine, PermissiveAgent>,
    model: &EntityId,
    property: &EntityId,
) -> Option<ankurah::core::schema::catalog::MembershipDef> {
    for _ in 0..100 {
        if let Some(m) = node.catalog.membership(model, property) {
            return Some(m);
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    node.catalog.membership(model, property)
}
