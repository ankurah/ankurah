//! RFC 5.3: the resolution pass binds property references against the
//! catalog, producing resolved Identifiers and failing closed on unknowns
//! (AC4, AC5). Not yet wired into the query paths; invoked explicitly.

mod common;
use ankql::ast::{Expr, Predicate, Selection};
use ankurah::proto::schema_id;
use common::*;

fn register(collection: &str, props: &[(&str, &str, &str)]) -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: collection.into(), name: collection.into() }],
        properties: props
            .iter()
            .map(|(anchor, backend, value_type)| proto::PropertyDescriptor {
                minting_collection: collection.into(),
                anchor: (*anchor).into(),
                name: (*anchor).into(),
                backend: (*backend).into(),
                value_type: (*value_type).into(),
                target_model: None,
                explicit_id: None,
            })
            .collect(),
        memberships: props
            .iter()
            .map(|(anchor, _, _)| proto::MembershipDescriptor {
                collection: collection.into(),
                property: proto::PropertyRef::Anchor((*anchor).into()),
                optional: false,
            })
            .collect(),
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

    expect_success(
        client.request(server.id, &DEFAULT_CONTEXT, register("album", &[("name", "yrs", "string"), ("payload", "lww", "json")])).await?,
    );

    let root = server.system.root().unwrap().payload.entity_id;
    let album = schema_id::model_entity_id(&root, "album");
    let name_id = schema_id::property_entity_id(&root, &album, "name", "yrs", "string");
    let payload_id = schema_id::property_entity_id(&root, &album, "payload", "lww", "json");

    let collection = "album".into();

    // Simple reference resolves to the derived property id.
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

/// RFC 5.8: after an anchor rename, the new display name resolves to the
/// SAME property id the old name resolved to before the rename.
#[tokio::test]
async fn resolution_follows_renames_to_the_same_id() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, register("album", &[("name", "yrs", "string")])).await?);
    let collection = "album".into();

    let before = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("name = 'x'")?).unwrap();
    let before_id = match first_expr(&before) {
        Expr::Identifier(i) => i.property,
        other => panic!("expected Identifier, got {other:?}"),
    };

    // Rename: anchor "name", display name "title".
    let rename = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            name: "title".into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, rename).await?);

    let after = server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("title = 'x'")?).unwrap();
    match first_expr(&after) {
        Expr::Identifier(i) => assert_eq!(i.property, before_id, "rename must keep the property id"),
        other => panic!("expected Identifier, got {other:?}"),
    }
    // The retired display name no longer resolves.
    assert!(server.catalog.resolve_selection(&collection, &ankql::parser::parse_selection("name = 'x'")?).is_err());

    Ok(())
}

fn expect_success(resp: proto::NodeResponseBody) {
    match resp {
        proto::NodeResponseBody::Success => {}
        other => panic!("expected Success, got {other}"),
    }
}
