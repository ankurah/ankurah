mod common;

use anyhow::Result;
use common::*;
use serde::{Deserialize, Serialize};

mod before_rename {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Ledger {
        pub release_year: String,
    }
}

mod after_rename {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Ledger {
        #[property(renamed_from = "release_year")]
        pub year: String,
    }
}

#[tokio::test]
async fn property_rename_preserves_materialization_and_live_ordering() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await?;
    let trx = ctx.begin();
    let ten = trx.create(&before_rename::Ledger { release_year: "2010".into() }).await?.id();
    let two = trx.create(&before_rename::Ledger { release_year: "2002".into() }).await?.id();
    let three = trx.create(&before_rename::Ledger { release_year: "2003".into() }).await?.id();
    trx.commit().await?;

    let active = ctx.query_wait::<before_rename::LedgerView>("release_year > '2000' ORDER BY release_year ASC").await?;
    assert_eq!(active.ids(), vec![two, three, ten]);
    let selection = active.selection().peek().expect("initialized selection");
    let watcher = TestWatcher::changeset();
    let _guard = active.subscribe(&watcher);
    watcher.quiesce_drain().await;

    ctx.resolve_model_id::<after_rename::Ledger>().await?;
    assert_eq!(
        resolved_prop(&node, before_rename::Ledger::descriptor(), "release_year"),
        resolved_prop(&node, after_rename::Ledger::descriptor(), "year"),
        "a rename keeps the durable property id"
    );
    assert_eq!(active.selection().peek(), Some(selection), "an installed query retains its resolved identity");

    let stale = ctx.get::<before_rename::LedgerView>(ten).await?;
    let trx = ctx.begin();
    stale.edit(&trx)?.release_year()?.replace("2012")?;
    trx.commit().await?;
    assert_eq!(ctx.get::<after_rename::LedgerView>(ten).await?.year()?, "2012");

    let current = ctx.fetch::<after_rename::LedgerView>("year > '2000' ORDER BY year ASC").await?;
    assert_eq!(current.iter().map(|view| view.year().unwrap()).collect::<Vec<_>>(), vec!["2002", "2003", "2012"]);
    let old = ctx.fetch::<before_rename::LedgerView>("release_year > '2000' ORDER BY release_year ASC").await?;
    assert_eq!(old.iter().map(View::id).collect::<Vec<_>>(), vec![two, three, ten]);
    watcher.quiesce_drain().await;

    let trx = ctx.begin();
    let seven = trx.create(&after_rename::Ledger { year: "2007".into() }).await?.id();
    let one = trx.create(&after_rename::Ledger { year: "2001".into() }).await?.id();
    trx.commit().await?;
    assert!(watcher.wait().await);
    assert_eq!(active.ids(), vec![one, two, three, seven, ten]);
    Ok(())
}
