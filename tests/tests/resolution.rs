//! Property resolution binds references against the catalog, producing
//! resolved `PropertyPath`s and failing closed on unknowns. These tests drive
//! the pass directly for precise shape assertions.

mod common;
use ankql::ast::{Expr, OrderByItem, Parsed, PathExpr, Predicate, PropertyId, Resolved, Selection};
use ankurah::core::schema::resolver::resolve_selection;
use ankurah::proto::EntityId;
use common::*;
use std::collections::BTreeMap;

/// A RegisterSchema request for `collection` with the given `(name, backend,
/// value_type)` properties, each a required membership.
fn register(collection: &str, props: &[(&str, &str, &str)]) -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema {
        model: proto::RegisterModel {
            label: collection.into(),
            name: collection.into(),
            explicit_id: None,
            build_id: [0u8; 16],
            properties: props
                .iter()
                .map(|(name, backend, value_type)| proto::RegisterProperty {
                    name: (*name).into(),
                    renamed_from: None,
                    backend: (*backend).into(),
                    value_type: (*value_type).into(),
                    target_label: None,
                    explicit_id: None,
                    build_id: [0u8; 16],
                    optional: false,
                })
                .collect(),
        },
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
        proto::NodeResponseBody::SchemaRegistered { model } => {
            Ok(model.properties.into_iter().map(|property| (property.name, property.id)).collect())
        }
        other => panic!("expected SchemaRegistered, got {other}"),
    }
}

fn first_expr(selection: &Selection<Resolved>) -> &Expr<Resolved> {
    match &selection.predicate {
        Predicate::Comparison { left, .. } => left,
        other => panic!("expected comparison, got {other:?}"),
    }
}

fn entity_id_literal() -> String { EntityId::from_bytes([1; EntityId::BYTE_LEN]).to_base64() }

/// The catalog property id an ORDER BY key carries, or `None` for the `id`
/// pseudo-property (which carries its own identity, not a catalog one).
fn order_property_id(item: &OrderByItem<Resolved>) -> Option<EntityId> {
    match item.path.property_id() {
        PropertyId::EntityId(id) => Some(id),
        _ => None,
    }
}

/// The display name an ORDER BY key renders to: the resolved-from label.
fn order_display(item: &OrderByItem<Resolved>) -> String { item.path.to_string() }

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
    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("name = 'x'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!(ident.property_id(), PropertyId::EntityId(name_id));
            assert_eq!(ident.to_string(), "name");
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // JSON subpath is preserved past the resolved property step.
    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!(ident.property_id(), PropertyId::EntityId(payload_id));
            assert_eq!(ident.subpath, vec!["meta".to_string(), "genre".to_string()]);
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // The id pseudo-property resolves to its own identity (`PropertyId::Id`),
    // not a catalog property or raw path.
    let resolved =
        resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection(&format!("id = '{}'", entity_id_literal()))?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => assert_eq!(ident.property_id(), PropertyId::Id),
        other => panic!("expected the id pseudo-property to resolve to PropertyId::Id, got {other:?}"),
    }

    // The legacy collection-qualified form normalizes away.
    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("album.name = 'x'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!((ident.property_id(), ident.to_string().as_str()), (PropertyId::EntityId(name_id), "name"))
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Unknown references fail closed, naming the exact model identity and property.
    let err = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("bogus = 1")?).unwrap_err();
    assert!(err.to_string().contains("bogus") && err.to_string().contains(&collection.to_string()), "got: {err}");

    Ok(())
}

/// After a rename hint, the new display name resolves to the same property id.
#[tokio::test]
async fn resolution_follows_renames_to_the_same_id() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    register_and_map(&client, server.id, register("album", &[("name", "yrs", "string")])).await?;
    let collection = server.catalog.model_id_for("album").expect("album model registered");

    let before = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("name = 'x' ORDER BY name")?)?;
    let before_id = match first_expr(&before) {
        Expr::Path(i) => match i.property_id() {
            PropertyId::EntityId(id) => id,
            other => panic!("expected a registered property id, got {other:?}"),
        },
        other => panic!("expected Identifier, got {other:?}"),
    };
    assert_eq!(order_property_id(&before.order_by.as_ref().unwrap()[0]), Some(before_id));

    // Rename: hint renamed_from "name", display name "title".
    let rename = proto::NodeRequestBody::RegisterSchema {
        model: proto::RegisterModel {
            label: "album".into(),
            name: "album".into(),
            explicit_id: None,
            build_id: [0u8; 16],
            properties: vec![proto::RegisterProperty {
                name: "title".into(),
                renamed_from: Some("name".into()),
                backend: "yrs".into(),
                value_type: "string".into(),
                target_label: None,
                explicit_id: None,
                build_id: [0u8; 16],
                optional: false,
            }],
        },
    };
    match client.request(server.id, &DEFAULT_CONTEXT, rename).await? {
        proto::NodeResponseBody::SchemaRegistered { .. } => {}
        other => panic!("expected SchemaRegistered, got {other}"),
    }

    let after = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("title = 'x'")?)?;
    match first_expr(&after) {
        Expr::Path(i) => {
            assert_eq!(i.property_id(), PropertyId::EntityId(before_id), "rename must keep the property id")
        }
        other => panic!("expected Identifier, got {other:?}"),
    }
    // A selection bound before the rename still addresses the same property,
    // and keeps the label it was written under: sorting addresses by id, so
    // the label is a resolved-at snapshot that a later rename cannot move.
    let before_order = &before.order_by.as_ref().unwrap()[0];
    assert_eq!(order_property_id(before_order), Some(before_id), "ORDER BY must retain stable identity");
    assert_eq!(order_display(before_order), "name", "the label is the one the selection was written under");
    // The retired display name no longer resolves.
    assert!(resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("name = 'x'")?).is_err());

    Ok(())
}

/// ORDER BY keys resolve fail-closed under the same rules as predicate
/// references; sort keys must not bypass the
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
    let known = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("true ORDER BY name")?)?;
    assert!(order_property_id(&known.order_by.as_ref().unwrap()[0]).is_some());
    let pseudo = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("true ORDER BY id")?)?;
    assert_eq!(order_property_id(&pseudo.order_by.as_ref().unwrap()[0]), None);
    let err = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("true ORDER BY bogus")?).unwrap_err();
    assert!(err.to_string().contains("bogus"), "got: {err}");

    // The legacy collection-qualified form normalizes away. The parser
    // does not produce dotted ORDER BY keys, so build the AST directly
    // (the form arrives from programmatic selections).
    let qualified = Selection::<Parsed> {
        predicate: ankql::ast::Predicate::True,
        order_by: Some(vec![ankql::ast::OrderByItem {
            path: PathExpr { steps: vec!["album".into(), "name".into()] },
            direction: ankql::ast::OrderDirection::Asc,
        }]),
        limit: None,
    };
    let resolved = resolve_selection(&collection, &server.catalog, qualified)?;
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

    let order = |steps: &[&str]| Selection::<Parsed> {
        predicate: ankql::ast::Predicate::True,
        order_by: Some(vec![OrderByItem {
            path: PathExpr { steps: steps.iter().map(|s| s.to_string()).collect() },
            direction: ankql::ast::OrderDirection::Asc,
        }]),
        limit: None,
    };

    // A subpath under a registered property, bare and collection-qualified,
    // and under the id pseudo-property.
    for steps in [&["payload", "meta"][..], &["album", "payload", "meta"][..], &["id", "x"][..], &["album", "id", "x"][..]] {
        let err = resolve_selection(&collection, &server.catalog, order(steps)).unwrap_err();
        assert!(err.to_string().contains("unsupported subpath"), "steps {steps:?}: {err}");
    }

    // The same restriction governs the frozen system/catalog collections.
    let catalog_collection = ankurah::core::schema::model_collection();
    let err = resolve_selection(&catalog_collection, &server.catalog, order(&["name", "x"])).unwrap_err();
    assert!(err.to_string().contains("not sortable"), "got: {err}");

    // Predicates: a JSON subpath under a registered property stays
    // supported...
    assert!(resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?).is_ok());
    // ...but the id pseudo-property has no subfields, bare or qualified, on
    // user and system collections alike.
    for query in ["id.foo = 'x'", "album.id.foo = 'x'"] {
        let err = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection(query)?).unwrap_err();
        assert!(err.to_string().contains("has no subfields"), "query {query:?}: {err}");
    }
    let err = resolve_selection(&catalog_collection, &server.catalog, ankql::parser::parse_selection("id.foo = 'x'")?).unwrap_err();
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
    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("_ankurah_model.name = 'x'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!(ident.property_id(), PropertyId::System(proto::SystemProperty::Name));
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Qualified reference with a JSON subpath keeps the subpath.
    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("_ankurah_model.name.x = 'y'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!(ident.property_id(), PropertyId::System(proto::SystemProperty::Name));
            assert_eq!(ident.subpath, vec!["x".to_string()]);
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // Qualified id resolves to the id pseudo-property; a subpath on it is
    // still refused through the qualified form.
    let resolved = resolve_selection(
        &collection,
        &server.catalog,
        ankql::parser::parse_selection(&format!("_ankurah_model.id = '{}'", entity_id_literal()))?,
    )?;
    match first_expr(&resolved) {
        Expr::Path(ident) => assert_eq!(ident.property_id(), PropertyId::Id),
        other => panic!("expected Identifier, got {other:?}"),
    }
    let err = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("_ankurah_model.id.x = 'y'")?).unwrap_err();
    assert!(err.to_string().contains("has no subfields"), "got: {err}");

    // ORDER BY: the qualified whole-property key normalizes; a subpath left
    // after stripping is refused.
    let order = |steps: &[&str]| Selection::<Parsed> {
        predicate: ankql::ast::Predicate::True,
        order_by: Some(vec![OrderByItem {
            path: PathExpr { steps: steps.iter().map(|s| s.to_string()).collect() },
            direction: ankql::ast::OrderDirection::Asc,
        }]),
        limit: None,
    };
    let resolved = resolve_selection(&collection, &server.catalog, order(&["_ankurah_model", "name"]))?;
    let items = resolved.order_by.expect("order_by present");
    assert_eq!(items[0].path.property_id(), PropertyId::System(proto::SystemProperty::Name));
    assert!(items[0].path.subpath.is_empty());
    let err = resolve_selection(&collection, &server.catalog, order(&["_ankurah_model", "name", "x"])).unwrap_err();
    assert!(err.to_string().contains("not sortable"), "got: {err}");

    Ok(())
}
