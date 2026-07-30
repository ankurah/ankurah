//! The three catalog collections read through ordinary typed Views
//! (core/src/schema/catalog/rows.rs) whose identity is a compile-time
//! SystemModel. No registration, no catalog lookup, no raw state-buffer
//! parsing on the typed read side.

mod common;
use ankurah::core::schema::catalog::rows::{SysModelPropertyRowView, SysModelRowView, SysPropertyRowView};
use ankurah::core::{property::backend::LWWBackend, property::backend::PropertyBackend, value::Value};
use ankurah::proto::SystemModel;
use common::*;
use std::collections::BTreeMap;

fn model_row_creation(label: Option<&str>, name: &str) -> anyhow::Result<proto::Event> {
    let backend = LWWBackend::new();
    if let Some(label) = label {
        backend.set("label".into(), Some(Value::String(label.into())));
    }
    backend.set("name".into(), Some(Value::String(name.into())));
    let backend_operations = backend.to_operations()?.expect("the row has fields");
    let model = proto::ModelId::System(SystemModel::Model);
    let mut operations = proto::OperationSet::from_backends(BTreeMap::from([("lww".into(), backend_operations)]));
    operations.push(proto::Operation::Membership(proto::Membership::Add(model)));
    Ok(proto::Event {
        collection: proto::CollectionId::fixed_name(ankurah::core::schema::MODEL_COLLECTION_ID),
        entity_id: EntityId::new(),
        operations,
        parent: proto::Clock::default(),
    })
}

/// Rows the registration executor wrote through generated Models and
/// Mutables read back through the generated Views, field for field: one
/// schema spelling serves both writes and reads.
#[tokio::test]
async fn executor_rows_read_through_typed_views() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let album_model = ctx.register_model::<Album>().await?;
    let album_entity = *album_model.as_entity_id().expect("registered app model has an entity id");

    // The model row, through the typed view.
    let models = ctx.fetch::<SysModelRowView>("label = 'album'").await?;
    assert_eq!(models.len(), 1, "the executor's model row is visible through the typed view");
    assert_eq!(models[0].id(), album_entity);
    assert_eq!(models[0].name()?, "Album");

    // The property rows, through the typed view: Album has name + year.
    let properties = ctx.fetch::<SysPropertyRowView>(format!("minted_for = '{album_entity}'").as_str()).await?;
    let mut names: Vec<String> = properties.iter().map(|p| p.name()).collect::<Result<_, _>>()?;
    names.sort();
    assert_eq!(names, ["name", "year"], "the executor's property rows read typed");
    for property in &properties {
        assert_eq!(property.backend()?, "yrs", "Album String fields resolve to the yrs backend");
        assert_eq!(property.value_type()?, "string");
        assert_eq!(property.minted_for()?, Some(album_entity));
    }

    // The membership rows, through the typed view.
    let memberships = ctx.fetch::<SysModelPropertyRowView>(format!("model = '{album_entity}'").as_str()).await?;
    assert_eq!(memberships.len(), 2, "one membership row per Album property");
    for membership in &memberships {
        assert!(!membership.optional()?, "Album's fields are required");
    }
    Ok(())
}

/// A test-injected catalog event committed outside `register_schema` appears
/// in the map through the typed LiveQuery feed. This pins the important
/// ownership boundary: the durable projection learns from catalog data, not
/// from an executor side-channel. `commit_remote_transaction` awaits
/// `notify_change`, so the map is current when it returns.
#[tokio::test]
async fn map_learns_from_the_feed_not_the_executor() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    node.catalog.wait_catalog_ready().await;

    let event = model_row_creation(Some("feed_only"), "FeedOnly")?;
    node.commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![proto::Attested::opt(event, None)]).await?;

    let model = node.catalog.model_by_label("feed_only").expect("the typed catalog feed delivered the row into the map");
    assert_eq!(model.name, "FeedOnly");
    Ok(())
}

/// A malformed row is noisy but local to that row: it contributes no map
/// entry and does not poison the subscription that delivers later valid
/// catalog changes.
#[tokio::test]
async fn malformed_feed_row_is_skipped_without_stopping_the_feed() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    node.catalog.wait_catalog_ready().await;
    let before = node.catalog.counts();

    let malformed = model_row_creation(None, "MissingLabel")?;
    node.commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![proto::Attested::opt(malformed, None)]).await?;
    assert_eq!(node.catalog.counts(), before, "a row missing its required label must not enter the map");

    let valid = model_row_creation(Some("after_malformed"), "AfterMalformed")?;
    node.commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![proto::Attested::opt(valid, None)]).await?;

    let model = node.catalog.model_by_label("after_malformed").expect("the feed must continue after a malformed row");
    assert_eq!(model.name, "AfterMalformed");
    assert_eq!(node.catalog.counts(), (before.0 + 1, before.1, before.2));
    Ok(())
}
