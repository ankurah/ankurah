//! The three catalog collections accessed through ordinary typed Models
//! (core/src/schema/catalog/rows.rs) whose identity is a compile-time
//! SystemModel. No registration, no catalog lookup, no raw state-buffer
//! parsing.

mod common;
use ankurah::core::schema::catalog::rows::{SysModelPropertyRowView, SysModelRow, SysModelRowView, SysPropertyRowView};
use ankurah::proto::SystemModel;
use common::*;

use ankurah::model::Mutable as _;

/// A typed create into a catalog collection travels the NORMAL entity path:
/// first-use ensure short-circuits to the closed system identity, the
/// staged membership asserts ModelId::System(Model), and commit-funnel
/// admissibility accepts it because the collection fact matches.
#[tokio::test]
async fn typed_create_into_catalog_collection() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let trx = ctx.begin();
    let row = trx.create(&SysModelRow { label: "typed_row".into(), name: "TypedRow".into() }).await?;
    let id = row.id();
    trx.commit().await?;

    let fetched = ctx.fetch::<SysModelRowView>("label = 'typed_row'").await?;
    assert_eq!(fetched.len(), 1, "typed fetch finds the typed create");
    assert_eq!(fetched[0].id(), id);
    assert_eq!(fetched[0].name()?, "TypedRow");
    use ankurah::model::View;
    assert!(
        fetched[0].entity().has_membership(&ankurah::proto::ModelId::System(SystemModel::Model)),
        "the row's membership is the closed system identity"
    );
    Ok(())
}

/// Rows the registration executor wrote as raw LWW events read back through
/// the typed Views, field for field: one schema spelling serves both the
/// executor's writes and typed reads.
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

/// A catalog row committed OUTSIDE the registration executor (a typed
/// create; `upsert_registered` never sees it) appears in the in-memory
/// catalog map, delivered by the policy-free reactor subscription.
/// `commit` awaits `notify_change`, and the feed listener folds inline on
/// the broadcast, so the map is current when commit returns.
#[tokio::test]
async fn map_learns_from_the_feed_not_the_executor() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;
    node.catalog.wait_catalog_ready().await;

    let trx = ctx.begin();
    trx.create(&SysModelRow { label: "feed_only".into(), name: "FeedOnly".into() }).await?;
    trx.commit().await?;

    let model = node.catalog.model_by_label("feed_only").expect("the reactor feed delivered the row into the map");
    assert_eq!(model.name, "FeedOnly");
    Ok(())
}
