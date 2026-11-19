//! Node id properties use NodeId objects in UniFFI input records, Mutable field wrappers and View getters.

mod common;

use std::sync::Arc;

use ankurah::NodeId;
use ankurah_tests_uniffi::{EndorsementInput, EndorsementOps};
use anyhow::Result;
use common::context;

#[tokio::test]
async fn node_id_fields_take_and_return_node_id_objects() -> Result<()> {
    let ctx = context().await?;
    let (first, second) = (NodeId::from_bytes([7; 32]), NodeId::from_bytes([8; 32]));
    let endorsement = EndorsementOps::new().create_one(&ctx, EndorsementInput { endorsed: Arc::new(first) }).await?;
    assert_eq!(*endorsement.uniffi_endorsed()?, first);

    let trx = ctx.begin();
    endorsement.uniffi_edit(&trx)?.uniffi_endorsed()?.set(Arc::new(second))?;
    trx.commit().await?;
    assert_eq!(*endorsement.uniffi_endorsed()?, second);
    Ok(())
}
