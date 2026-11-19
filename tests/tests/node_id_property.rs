//! A model property can hold a node id, and a query can match it.

mod common;
use ankurah::{fetch, policy::DEFAULT_CONTEXT, Model, NodeId};
use anyhow::Result;
use common::durable_sled_setup;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Endorsement {
    #[active_type(LWW)]
    pub endorsed: NodeId,
}

#[tokio::test]
async fn storage_preserves_node_id_properties_and_equality_queries_match_them() -> Result<()> {
    let node = durable_sled_setup().await?;
    let context = node.context(DEFAULT_CONTEXT)?;
    let (this, other) = (node.wait_id().await?, NodeId::from_bytes([7; 32]));

    let trx = context.begin();
    let id = trx.create(&Endorsement { endorsed: this }).await?.id();
    trx.create(&Endorsement { endorsed: other }).await?;
    trx.commit().await?;

    assert_eq!(context.get::<EndorsementView>(id).await?.endorsed()?, this);
    let typed: Vec<EndorsementView> = fetch!(context, endorsed = { this }).await?;
    let text: Vec<EndorsementView> = context.fetch(format!("endorsed = '{}'", this.to_base64()).as_str()).await?;
    for matched in [typed, text] {
        assert_eq!(matched.iter().map(|view| view.id()).collect::<Vec<_>>(), [id]);
    }
    Ok(())
}
