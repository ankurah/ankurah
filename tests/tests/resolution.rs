//! RFC 5.3 (specs/model-property-metadata/rfc.md): the resolution pass binds property references against the
//! catalog, producing resolved Identifiers and failing closed on unknowns.
//! It is wired at the query origin sites in the protocol v3 epoch; these
//! tests drive the pass directly for precise shape assertions.

mod common;
use ankql::ast::{Expr, Predicate, Selection};
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
        Expr::Identifier(ident) => {
            assert_eq!(ident.property, name_id.to_ulid());
            assert_eq!(ident.name, "name");
            assert!(ident.subpath.is_empty());
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // JSON subpath is preserved past the resolved property step.
    let resolved = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("payload.meta.genre = 'jazz'")?).unwrap();
    match first_expr(&resolved) {
        Expr::Identifier(ident) => {
            assert_eq!(ident.property, payload_id.to_ulid());
            assert_eq!(ident.subpath, vec!["meta".to_string(), "genre".to_string()]);
        }
        other => panic!("expected Identifier, got {other:?}"),
    }

    // The id pseudo-property stays a Path.
    let resolved =
        server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("id = 'AQIDBAUGBwgJCgsMDQ4PEA'")?).unwrap();
    assert!(matches!(first_expr(&resolved), Expr::Path(_)));

    // The legacy collection-qualified form normalizes away.
    let resolved = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("album.name = 'x'")?).unwrap();
    match first_expr(&resolved) {
        Expr::Identifier(ident) => assert_eq!((ident.property, ident.name.as_str()), (name_id.to_ulid(), "name")),
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
        Expr::Identifier(i) => i.property,
        other => panic!("expected Identifier, got {other:?}"),
    };
    assert_eq!(before.order_by.as_ref().unwrap()[0].property, Some(before_id));

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
        Expr::Identifier(i) => assert_eq!(i.property, before_id, "rename must keep the property id"),
        other => panic!("expected Identifier, got {other:?}"),
    }
    let refreshed = server.catalog.resolve_selection(&collection, &before)?;
    let refreshed_order = &refreshed.order_by.as_ref().unwrap()[0];
    assert_eq!(refreshed_order.property, Some(before_id), "ORDER BY must retain stable identity");
    assert_eq!(refreshed_order.path.first(), "title", "re-resolution refreshes the renamed display path");
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
    assert!(known.order_by.as_ref().unwrap()[0].property.is_some());
    let pseudo = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("true ORDER BY id")?)?;
    assert_eq!(pseudo.order_by.as_ref().unwrap()[0].property, None);
    let err = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("true ORDER BY bogus")?).unwrap_err();
    assert!(err.to_string().contains("bogus"), "got: {err}");

    // The legacy collection-qualified form normalizes away. The parser
    // does not produce dotted ORDER BY keys, so build the AST directly
    // (the form arrives from programmatic selections).
    let qualified = Selection {
        predicate: ankql::ast::Predicate::True,
        order_by: Some(vec![ankql::ast::OrderByItem {
            path: ankql::ast::PathExpr { steps: vec!["album".into(), "name".into()] },
            property: None,
            direction: ankql::ast::OrderDirection::Asc,
        }]),
        limit: None,
    };
    let resolved = server.catalog.resolve_selection(&collection, &qualified)?;
    let items = resolved.order_by.expect("order_by present");
    assert_eq!(items[0].path.steps, vec!["name".to_string()]);
    assert!(items[0].property.is_some());

    Ok(())
}
