//! RFC 5.3 (specs/model-property-metadata/rfc.md): the resolution pass binds property references against the
//! catalog, producing resolved Identifiers and failing closed on unknowns.
//! It is wired at the query origin sites in the protocol v3 epoch; these
//! tests drive the pass directly for precise shape assertions.

mod common;
use ankql::ast::{Expr, OrderByItem, OrderKey, Predicate, PropertyId, Selection};
use ankurah::proto::EntityId;
use common::*;
use std::collections::BTreeMap;

/// A RegisterSchema request for `collection` with the given `(name, backend,
/// value_type)` properties, each a required membership.
fn register(collection: &str, props: &[(&str, &str, &str)]) -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: collection.into(), name: collection.into(), explicit_id: None }],
        properties: props
            .iter()
            .map(|(name, backend, value_type)| proto::PropertyDescriptor {
                minting_collection: collection.into(),
                name: (*name).into(),
                renamed_from: None,
                backend: (*backend).into(),
                value_type: (*value_type).into(),
                target_collection: None,
                explicit_id: None,
            })
            .collect(),
        memberships: props
            .iter()
            .map(|(name, _, _)| proto::MembershipDescriptor {
                collection: collection.into(),
                property: proto::PropertyRef::Name((*name).into()),
                optional: false,
            })
            .collect(),
    }
}

/// Send `register` and return the allocator-assigned property ids by display
/// name (sourced from the SchemaRegistered response).
async fn register_and_map(
    client: &Node<SledStorageEngine, PermissiveAgent>,
    server_id: EntityId,
    request: proto::NodeRequestBody,
) -> anyhow::Result<BTreeMap<String, EntityId>> {
    match client.request(server_id, &DEFAULT_CONTEXT, request).await? {
        proto::NodeResponseBody::SchemaRegistered { properties, .. } => Ok(properties.into_iter().map(|p| (p.name, p.id)).collect()),
        other => panic!("expected SchemaRegistered, got {other}"),
    }
}

fn first_expr(selection: &Selection) -> &Expr {
    match &selection.predicate {
        Predicate::Comparison { left, .. } => left,
        other => panic!("expected comparison, got {other:?}"),
    }
}

/// The resolved catalog property id an ORDER BY key carries, or `None` for an
/// unresolved path or the `id` pseudo-property. Mirrors the pre-column-layering
/// `OrderByItem::property` field the `OrderKey` enum replaced.
fn order_property_id(item: &OrderByItem) -> Option<ulid::Ulid> {
    match &item.key {
        OrderKey::Property(identifier) => match identifier.id_or_systemname() {
            PropertyId::EntityId(id) => Some(id),
            _ => None,
        },
        OrderKey::Path(_) => None,
    }
}

/// The display name an ORDER BY key renders to (the resolved-from label, or the
/// raw path's first step when unresolved). Mirrors the old
/// `OrderByItem::path.first()`.
fn order_display(item: &OrderByItem) -> String {
    match &item.key {
        OrderKey::Property(identifier) => identifier.to_string(),
        OrderKey::Path(path) => path.first().to_string(),
    }
}

#[tokio::test]
async fn resolution_binds_names_and_fails_closed() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    // Source the allocated property ids from the registration response.
    let ids = register_and_map(&client, server.id, register("album", &[("name", "yrs", "string"), ("payload", "lww", "json")])).await?;
    let name_id = ids["name"];
    let payload_id = ids["payload"];

    let collection = "album".into();

    // Simple reference resolves to the allocated property id.
    let resolved = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("name = 'x'")?).unwrap();
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!(ident.id_or_systemname(), PropertyId::EntityId(name_id.to_ulid()));
            assert_eq!(ident.to_string(), "name");
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // JSON subpath is preserved past the resolved property step.
    let resolved = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?).unwrap();
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!(ident.id_or_systemname(), PropertyId::EntityId(payload_id.to_ulid()));
            assert_eq!(ident.subpath, vec!["meta".to_string(), "genre".to_string()]);
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // The id pseudo-property resolves to its own identity (`PropertyId::Id`),
    // not a catalog property and not a raw Path (column-layering, #307:
    // resolve_expr binds `id` to `PropertyPath::id`).
    let resolved =
        server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("id = 'AQIDBAUGBwgJCgsMDQ4PEA'")?).unwrap();
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => assert_eq!(ident.id_or_systemname(), PropertyId::Id),
        other => panic!("expected the id pseudo-property to resolve to PropertyId::Id, got {other:?}"),
    }

    // The legacy collection-qualified form normalizes away.
    let resolved = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("album.name = 'x'")?).unwrap();
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!((ident.id_or_systemname(), ident.to_string().as_str()), (PropertyId::EntityId(name_id.to_ulid()), "name"))
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Unknown references fail closed, naming collection and property.
    let err = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("bogus = 1")?).unwrap_err();
    assert!(err.to_string().contains("bogus") && err.to_string().contains("album"), "got: {err}");

    // Idempotent: resolving an already-resolved selection is a no-op.
    let twice = server.catalog.resolve_selection(&collection, &resolved).unwrap();
    assert_eq!(format!("{:?}", twice), format!("{:?}", resolved));

    Ok(())
}

/// RFC 5.8: after a rename hint, the new display name resolves to the SAME
/// property id the old name resolved to before the rename.
#[tokio::test]
async fn resolution_follows_renames_to_the_same_id() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    register_and_map(&client, server.id, register("album", &[("name", "yrs", "string")])).await?;
    let collection = "album".into();

    let before = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("name = 'x' ORDER BY name")?).unwrap();
    let before_id = match first_expr(&before) {
        Expr::PropertyPath(i) => match i.id_or_systemname() {
            PropertyId::EntityId(id) => id,
            other => panic!("expected a registered property id, got {other:?}"),
        },
        other => panic!("expected Identifier, got {other:?}"),
    };
    assert_eq!(order_property_id(&before.order_by.as_ref().unwrap()[0]), Some(before_id));

    // Rename: hint renamed_from "name", display name "title".
    let rename = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "title".into(),
            renamed_from: Some("name".into()),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_collection: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    match client.request(server.id, &DEFAULT_CONTEXT, rename).await? {
        proto::NodeResponseBody::SchemaRegistered { .. } => {}
        other => panic!("expected SchemaRegistered, got {other}"),
    }

    let after = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("title = 'x'")?).unwrap();
    match first_expr(&after) {
        Expr::PropertyPath(i) => {
            assert_eq!(i.id_or_systemname(), PropertyId::EntityId(before_id), "rename must keep the property id")
        }
        other => panic!("expected Identifier, got {other:?}"),
    }
    let refreshed = server.catalog.resolve_selection(&collection, &before)?;
    let refreshed_order = &refreshed.order_by.as_ref().unwrap()[0];
    assert_eq!(order_property_id(refreshed_order), Some(before_id), "ORDER BY must retain stable identity");
    // Column-layering (#307): an already-resolved ORDER BY key is an idempotent
    // pass-through. Sorting addresses by id, so the Display label is a
    // resolved-at snapshot and is deliberately NOT refreshed on re-resolution
    // (core/src/schema/resolve.rs, `resolve_order_by`); it keeps the pre-rename
    // "name", while a FRESH resolve of the new "title" (above) carries the id.
    assert_eq!(order_display(refreshed_order), "name", "re-resolution keeps the resolved-at display label (not refreshed)");
    // The retired display name no longer resolves.
    assert!(server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("name = 'x'")?).is_err());

    Ok(())
}

/// ORDER BY keys resolve fail-closed under the same rules as predicate
/// references (codex review finding: sort keys previously bypassed the
/// resolution pass entirely).
#[tokio::test]
async fn order_by_resolves_fail_closed() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    register_and_map(&client, server.id, register("album", &[("name", "yrs", "string")])).await?;
    let collection = "album".into();

    // Known key resolves to a stable id; unknown key fails closed; id passes
    // through without pretending to be a catalog property.
    let known = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("true ORDER BY name")?)?;
    assert!(order_property_id(&known.order_by.as_ref().unwrap()[0]).is_some());
    let pseudo = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("true ORDER BY id")?)?;
    assert_eq!(order_property_id(&pseudo.order_by.as_ref().unwrap()[0]), None);
    let err = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("true ORDER BY bogus")?).unwrap_err();
    assert!(err.to_string().contains("bogus"), "got: {err}");

    // The legacy collection-qualified form normalizes away. The parser
    // does not produce dotted ORDER BY keys, so build the AST directly
    // (the form arrives from programmatic selections).
    let qualified = Selection {
        predicate: ankql::ast::Predicate::True,
        order_by: Some(vec![ankql::ast::OrderByItem {
            key: OrderKey::Path(ankql::ast::PathExpr { steps: vec!["album".into(), "name".into()] }),
            direction: ankql::ast::OrderDirection::Asc,
        }]),
        limit: None,
    };
    let resolved = server.catalog.resolve_selection(&collection, &qualified)?;
    let items = resolved.order_by.expect("order_by present");
    assert_eq!(order_display(&items[0]), "name");
    assert!(order_property_id(&items[0]).is_some());

    Ok(())
}

/// ORDER BY keys must name a WHOLE property. The parser already refuses
/// dotted ORDER BY; resolution now refuses the programmatically built form
/// too, because downstream support is partial (no planner index key
/// addresses a subpath, and a comparator falling back to the whole property
/// sorts the entire JSON value -- wrong rows under LIMIT). The `id`
/// pseudo-property refuses subpaths everywhere, predicates included: the
/// entity id has no subfields.
#[tokio::test]
async fn subpath_order_keys_and_id_subpaths_are_refused() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    register_and_map(&client, server.id, register("album", &[("payload", "lww", "json")])).await?;
    let collection = "album".into();

    let order = |steps: &[&str]| Selection {
        predicate: ankql::ast::Predicate::True,
        order_by: Some(vec![OrderByItem {
            key: OrderKey::Path(ankql::ast::PathExpr { steps: steps.iter().map(|s| s.to_string()).collect() }),
            direction: ankql::ast::OrderDirection::Asc,
        }]),
        limit: None,
    };

    // A subpath under a registered property, bare and collection-qualified,
    // and under the id pseudo-property.
    for steps in [&["payload", "meta"][..], &["album", "payload", "meta"][..], &["id", "x"][..], &["album", "id", "x"][..]] {
        let err = server.catalog.resolve_selection(&collection, &order(steps)).unwrap_err();
        assert!(err.to_string().contains("unsupported subpath"), "steps {steps:?}: {err}");
    }

    // The same restriction governs the frozen system/catalog collections.
    let catalog_collection = "_ankurah_model".into();
    let err = server.catalog.resolve_selection(&catalog_collection, &order(&["name", "x"])).unwrap_err();
    assert!(err.to_string().contains("not sortable"), "got: {err}");

    // Predicates: a JSON subpath under a registered property stays
    // supported...
    assert!(server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?).is_ok());
    // ...but the id pseudo-property has no subfields, bare or qualified, on
    // user and system collections alike.
    for query in ["id.foo = 'x'", "album.id.foo = 'x'"] {
        let err = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection(query)?).unwrap_err();
        assert!(err.to_string().contains("has no subfields"), "query {query:?}: {err}");
    }
    let err = server.catalog.resolve_selection(&catalog_collection, &ankql::parser::parse_selection("id.foo = 'x'")?).unwrap_err();
    assert!(err.to_string().contains("has no subfields"), "got: {err}");

    Ok(())
}

/// The systemize pass (frozen system/catalog collections) strips the legacy
/// collection qualifier exactly like the registered resolver does:
/// `_ankurah_model.name` addresses the property `name`, never a property
/// named `_ankurah_model` carrying `name` as a subpath.
#[tokio::test]
async fn systemize_strips_the_collection_qualifier() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let collection = "_ankurah_model".into();

    // Qualified predicate reference: the qualifier normalizes away.
    let resolved = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("_ankurah_model.name = 'x'")?)?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!(ident.id_or_systemname(), PropertyId::System { name: "name".into() });
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Qualified reference with a JSON subpath keeps the subpath.
    let resolved = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("_ankurah_model.name.x = 'y'")?)?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!(ident.id_or_systemname(), PropertyId::System { name: "name".into() });
            assert_eq!(ident.subpath, vec!["x".to_string()]);
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Qualified id resolves to the id pseudo-property; a subpath on it is
    // still refused through the qualified form.
    let resolved = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("_ankurah_model.id = 'x'")?)?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => assert_eq!(ident.id_or_systemname(), PropertyId::Id),
        other => panic!("expected Identifier, got {other:?}"),
    }
    let err = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("_ankurah_model.id.x = 'y'")?).unwrap_err();
    assert!(err.to_string().contains("has no subfields"), "got: {err}");

    // ORDER BY: the qualified whole-property key normalizes; a subpath left
    // after stripping is refused.
    let order = |steps: &[&str]| Selection {
        predicate: ankql::ast::Predicate::True,
        order_by: Some(vec![OrderByItem {
            key: OrderKey::Path(ankql::ast::PathExpr { steps: steps.iter().map(|s| s.to_string()).collect() }),
            direction: ankql::ast::OrderDirection::Asc,
        }]),
        limit: None,
    };
    let resolved = server.catalog.resolve_selection(&collection, &order(&["_ankurah_model", "name"]))?;
    let items = resolved.order_by.expect("order_by present");
    match &items[0].key {
        OrderKey::Property(ident) => {
            assert_eq!(ident.id_or_systemname(), PropertyId::System { name: "name".into() });
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected resolved order key, got {other:?}"),
    }
    let err = server.catalog.resolve_selection(&collection, &order(&["_ankurah_model", "name", "x"])).unwrap_err();
    assert!(err.to_string().contains("not sortable"), "got: {err}");

    Ok(())
}
