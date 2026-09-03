//! Catalog-backed property resolution.

mod common;
use ankql::ast::{Expr, OrderByItem, Parsed, PathExpr, Predicate, PropertyId, Resolved, Selection};
use ankurah::core::schema::resolver::resolve_selection;
use ankurah::proto::EntityId;
use common::*;
use std::collections::BTreeMap;

/// A registration with required `(name, backend, value_type)` properties.
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

/// Register a schema and index its allocated property ids by name.
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

fn order_property_id(item: &OrderByItem<Resolved>) -> Option<EntityId> {
    match item.path.property_id() {
        PropertyId::EntityId(id) => Some(id),
        _ => None,
    }
}

fn order_display(item: &OrderByItem<Resolved>) -> String { item.path.to_string() }

#[tokio::test]
async fn resolution_binds_names_and_fails_closed() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let ids = register_and_map(&client, server.id, register("album", &[("name", "yrs", "string"), ("payload", "lww", "json")])).await?;
    let name_id = ids["name"];
    let payload_id = ids["payload"];

    let collection = server.catalog.model_id_for("album").expect("album model registered");

    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("name = 'x'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!(ident.property_id(), PropertyId::EntityId(name_id));
            assert_eq!(ident.to_string(), "name");
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!(ident.property_id(), PropertyId::EntityId(payload_id));
            assert_eq!(ident.subpath, vec!["meta".to_string(), "genre".to_string()]);
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    let resolved =
        resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection(&format!("id = '{}'", entity_id_literal()))?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => assert_eq!(ident.property_id(), PropertyId::Id),
        other => panic!("expected the id pseudo-property to resolve to PropertyId::Id, got {other:?}"),
    }

    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("album.name = 'x'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!((ident.property_id(), ident.to_string().as_str()), (PropertyId::EntityId(name_id), "name"))
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    let err = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("bogus = 1")?).unwrap_err();
    assert!(err.to_string().contains("bogus") && err.to_string().contains(&collection.to_string()), "got: {err}");

    Ok(())
}

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
    let before_order = &before.order_by.as_ref().unwrap()[0];
    assert_eq!(order_property_id(before_order), Some(before_id), "ORDER BY must retain stable identity");
    assert_eq!(order_display(before_order), "name", "the label is the one the selection was written under");
    assert!(resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("name = 'x'")?).is_err());

    Ok(())
}

#[tokio::test]
async fn order_by_resolves_fail_closed() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    register_and_map(&client, server.id, register("album", &[("name", "yrs", "string")])).await?;
    let collection = server.catalog.model_id_for("album").expect("album model registered");

    let known = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("true ORDER BY name")?)?;
    assert!(order_property_id(&known.order_by.as_ref().unwrap()[0]).is_some());
    let pseudo = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("true ORDER BY id")?)?;
    assert_eq!(order_property_id(&pseudo.order_by.as_ref().unwrap()[0]), None);
    let err = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("true ORDER BY bogus")?).unwrap_err();
    assert!(err.to_string().contains("bogus"), "got: {err}");

    // Programmatic selections may carry legacy collection-qualified keys.
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

    for steps in [&["payload", "meta"][..], &["album", "payload", "meta"][..], &["id", "x"][..], &["album", "id", "x"][..]] {
        let err = resolve_selection(&collection, &server.catalog, order(steps)).unwrap_err();
        assert!(err.to_string().contains("unsupported subpath"), "steps {steps:?}: {err}");
    }

    let catalog_collection = ankurah::core::schema::model_collection();
    let err = resolve_selection(&catalog_collection, &server.catalog, order(&["name", "x"])).unwrap_err();
    assert!(err.to_string().contains("not sortable"), "got: {err}");

    assert!(resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?).is_ok());
    for query in ["id.foo = 'x'", "album.id.foo = 'x'"] {
        let err = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection(query)?).unwrap_err();
        assert!(err.to_string().contains("has no subfields"), "query {query:?}: {err}");
    }
    let err = resolve_selection(&catalog_collection, &server.catalog, ankql::parser::parse_selection("id.foo = 'x'")?).unwrap_err();
    assert!(err.to_string().contains("has no subfields"), "got: {err}");

    Ok(())
}

#[tokio::test]
async fn systemize_strips_the_collection_qualifier() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let collection = ankurah::core::schema::model_collection();

    let resolved = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("_ankurah_model.name = 'x'")?)?;
    match first_expr(&resolved) {
        Expr::Path(ident) => {
            assert_eq!(ident.property_id(), PropertyId::System(proto::SystemProperty::Name));
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    let err = resolve_selection(&collection, &server.catalog, ankql::parser::parse_selection("_ankurah_model.name.x = 'y'")?).unwrap_err();
    assert!(err.to_string().contains("only JSON properties support subpaths"), "got: {err}");

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
