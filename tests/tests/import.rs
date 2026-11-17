mod common;
use common::*;

async fn catalog_definition_events(node: &Node<SledStorageEngine, PermissiveAgent>) -> anyhow::Result<Vec<proto::Attested<proto::Event>>> {
    let mut events = Vec::new();
    for model in [proto::SystemModel::Model, proto::SystemModel::Property, proto::SystemModel::ModelProperty] {
        let selection = ankurah::ankql::ast::Selection {
            predicate: ankurah::ankql::ast::Predicate::MemberOf(proto::ModelId::System(model)),
            order_by: None,
            limit: None,
        };
        for state in node.storage.fetch_states(&selection).await? {
            events.extend(node.storage.dump_entity_events(state.payload.entity_id).await?);
        }
    }
    Ok(events)
}

#[tokio::test]
async fn imported_definitions_bind_another_nodes_struct() -> anyhow::Result<()> {
    let a = durable_sled_setup().await?;
    let b = durable_sled_setup().await?;
    let album_model = a.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<Album>().await?;
    b.import_events(&catalog_definition_events(&a).await?).await?;
    assert_eq!(b.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<Album>().await?, album_model);
    Ok(())
}
