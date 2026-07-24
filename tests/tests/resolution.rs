//! RFC 5.3 (specs/model-property-metadata/rfc.md): the resolution pass binds property references against the
//! catalog, producing resolved PropertyPaths and failing closed on unknowns.
//! It is wired at the query origin sites in the version 1 (0.10.0) protocol epoch; these
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
fn order_property_id(item: &OrderByItem) -> Option<EntityId> {
    match &item.key {
        OrderKey::Property(identifier) => match identifier.id() {
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

    let collection = server.catalog.model_id_for("album").expect("album model registered");

    // Simple reference resolves to the allocated property id.
    let resolved = ankql::parser::parse_selection("name = 'x'")?.resolve_names(&collection, server.catalog.resolver())?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!(ident.id(), PropertyId::EntityId(name_id));
            assert_eq!(ident.to_string(), "name");
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // JSON subpath is preserved past the resolved property step.
    let resolved = ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?.resolve_names(&collection, server.catalog.resolver())?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!(ident.id(), PropertyId::EntityId(payload_id));
            assert_eq!(ident.subpath, vec!["meta".to_string(), "genre".to_string()]);
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // The id pseudo-property resolves to its own identity (`PropertyId::Id`),
    // not a catalog property and not a raw Path (column-layering, #307:
    // resolve_expr binds `id` to `PropertyPath::id`).
    let resolved =
        ankql::parser::parse_selection("id = 'AQIDBAUGBwgJCgsMDQ4PEA'")?.resolve_names(&collection, server.catalog.resolver())?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => assert_eq!(ident.id(), PropertyId::Id),
        other => panic!("expected the id pseudo-property to resolve to PropertyId::Id, got {other:?}"),
    }

    // The legacy collection-qualified form normalizes away.
    let resolved = ankql::parser::parse_selection("album.name = 'x'")?.resolve_names(&collection, server.catalog.resolver())?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!((ident.id(), ident.to_string().as_str()), (PropertyId::EntityId(name_id), "name"))
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Unknown references fail closed, naming the exact model identity and property.
    let err = ankql::parser::parse_selection("bogus = 1")?.resolve_names(&collection, server.catalog.resolver()).unwrap_err();
    assert!(err.to_string().contains("bogus") && err.to_string().contains(&collection.to_string()), "got: {err}");

    // Idempotent: resolving an already-resolved selection is a no-op.
    let twice = resolved.resolve_names(&collection, server.catalog.resolver())?;
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
    let collection = server.catalog.model_id_for("album").expect("album model registered");

    let before = ankql::parser::parse_selection("name = 'x' ORDER BY name")?.resolve_names(&collection, server.catalog.resolver())?;
    let before_id = match first_expr(&before) {
        Expr::PropertyPath(i) => match i.id() {
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

    let after = ankql::parser::parse_selection("title = 'x'")?.resolve_names(&collection, server.catalog.resolver())?;
    match first_expr(&after) {
        Expr::PropertyPath(i) => {
            assert_eq!(i.id(), PropertyId::EntityId(before_id), "rename must keep the property id")
        }
        other => panic!("expected Identifier, got {other:?}"),
    }
    let refreshed = before.resolve_names(&collection, server.catalog.resolver())?;
    let refreshed_order = &refreshed.order_by.as_ref().unwrap()[0];
    assert_eq!(order_property_id(refreshed_order), Some(before_id), "ORDER BY must retain stable identity");
    // Column-layering (#307): an already-resolved ORDER BY key is an idempotent
    // pass-through. Sorting addresses by id, so the Display label is a
    // resolved-at snapshot and is deliberately NOT refreshed on re-resolution;
    // it keeps the pre-rename
    // "name", while a FRESH resolve of the new "title" (above) carries the id.
    assert_eq!(order_display(refreshed_order), "name", "re-resolution keeps the resolved-at display label (not refreshed)");
    // The retired display name no longer resolves.
    assert!(ankql::parser::parse_selection("name = 'x'")?.resolve_names(&collection, server.catalog.resolver()).is_err());

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
    let collection = server.catalog.model_id_for("album").expect("album model registered");

    // Known key resolves to a stable id; unknown key fails closed; id passes
    // through without pretending to be a catalog property.
    let known = ankql::parser::parse_selection("true ORDER BY name")?.resolve_names(&collection, server.catalog.resolver())?;
    assert!(order_property_id(&known.order_by.as_ref().unwrap()[0]).is_some());
    let pseudo = ankql::parser::parse_selection("true ORDER BY id")?.resolve_names(&collection, server.catalog.resolver())?;
    assert_eq!(order_property_id(&pseudo.order_by.as_ref().unwrap()[0]), None);
    let err = ankql::parser::parse_selection("true ORDER BY bogus")?.resolve_names(&collection, server.catalog.resolver()).unwrap_err();
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
    let resolved = qualified.resolve_names(&collection, server.catalog.resolver())?;
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
    let collection = server.catalog.model_id_for("album").expect("album model registered");

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
        let err = order(steps).resolve_names(&collection, server.catalog.resolver()).unwrap_err();
        assert!(err.to_string().contains("unsupported subpath"), "steps {steps:?}: {err}");
    }

    // The same restriction governs the frozen system/catalog collections.
    let catalog_collection = ankurah::core::schema::model_collection();
    let err = order(&["name", "x"]).resolve_names(&catalog_collection, server.catalog.resolver()).unwrap_err();
    assert!(err.to_string().contains("not sortable"), "got: {err}");

    // Predicates: a JSON subpath under a registered property stays
    // supported...
    assert!(ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?.resolve_names(&collection, server.catalog.resolver()).is_ok());
    // ...but the id pseudo-property has no subfields, bare or qualified, on
    // user and system collections alike.
    for query in ["id.foo = 'x'", "album.id.foo = 'x'"] {
        let err = ankql::parser::parse_selection(query)?.resolve_names(&collection, server.catalog.resolver()).unwrap_err();
        assert!(err.to_string().contains("has no subfields"), "query {query:?}: {err}");
    }
    let err = ankql::parser::parse_selection("id.foo = 'x'")?.resolve_names(&catalog_collection, server.catalog.resolver()).unwrap_err();
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
    let collection = ankurah::core::schema::model_collection();

    // Qualified predicate reference: the qualifier normalizes away.
    let resolved = ankql::parser::parse_selection("_ankurah_model.name = 'x'")?.resolve_names(&collection, server.catalog.resolver())?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!(ident.id(), PropertyId::System(proto::SystemProperty::Name));
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Qualified reference with a JSON subpath keeps the subpath.
    let resolved = ankql::parser::parse_selection("_ankurah_model.name.x = 'y'")?.resolve_names(&collection, server.catalog.resolver())?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => {
            assert_eq!(ident.id(), PropertyId::System(proto::SystemProperty::Name));
            assert_eq!(ident.subpath, vec!["x".to_string()]);
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Qualified id resolves to the id pseudo-property; a subpath on it is
    // still refused through the qualified form.
    let resolved = ankql::parser::parse_selection("_ankurah_model.id = 'AQIDBAUGBwgJCgsMDQ4PEA'")?
        .resolve_names(&collection, server.catalog.resolver())?;
    match first_expr(&resolved) {
        Expr::PropertyPath(ident) => assert_eq!(ident.id(), PropertyId::Id),
        other => panic!("expected Identifier, got {other:?}"),
    }
    let err =
        ankql::parser::parse_selection("_ankurah_model.id.x = 'y'")?.resolve_names(&collection, server.catalog.resolver()).unwrap_err();
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
    let resolved = order(&["_ankurah_model", "name"]).resolve_names(&collection, server.catalog.resolver())?;
    let items = resolved.order_by.expect("order_by present");
    match &items[0].key {
        OrderKey::Property(ident) => {
            assert_eq!(ident.id(), PropertyId::System(proto::SystemProperty::Name));
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected resolved order key, got {other:?}"),
    }
    let err = order(&["_ankurah_model", "name", "x"]).resolve_names(&collection, server.catalog.resolver()).unwrap_err();
    assert!(err.to_string().contains("not sortable"), "got: {err}");

    Ok(())
}
