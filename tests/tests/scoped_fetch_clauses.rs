//! Repro attempt for an unexplained downstream observation: a relay client's
//! cached query with a multi-clause Ref-equality predicate (the shape the JWT
//! policy agent composes for self-scoped EndUser reads:
//! `user = 'X' AND account = 'A' AND domain = 'D' AND user = 'X'`) returned
//! empty initial fetches in the browser while a two-clause owner-shaped
//! predicate returned rows. All literals are strings, matching the original
//! observation (before typed EntityId arguments existed).

mod common;

use crate::common::*;
use ankurah::{Model, Ref};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct ScopeTarget {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct ScopeCred {
    pub user: Ref<ScopeTarget>,
    pub account: Ref<ScopeTarget>,
    pub domain: Ref<ScopeTarget>,
    pub label: String,
}

async fn eventually_count(lq: &ankurah::LiveQuery<ScopeCredView>, expected: usize) -> usize {
    for _ in 0..60 {
        let count = lq.ids().len();
        if count == expected {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    lq.ids().len()
}

#[tokio::test]
async fn relay_client_cached_fetch_with_composed_scope_clauses() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(DEFAULT_CONTEXT)?;
    let client_ctx = client.context(DEFAULT_CONTEXT)?;

    // Server-side records, mirroring signup-created credential state.
    let (user_id, account_id, domain_id) = {
        let trx = server_ctx.begin();
        let user = trx.create(&ScopeTarget { name: "user".into() }).await?;
        let account = trx.create(&ScopeTarget { name: "account".into() }).await?;
        let domain = trx.create(&ScopeTarget { name: "domain".into() }).await?;
        let ids = (user.id(), account.id(), domain.id());
        trx.commit().await?;
        let trx = server_ctx.begin();
        trx.create(&ScopeCred { user: ids.0.into(), account: ids.1.into(), domain: ids.2.into(), label: "the credential".into() }).await?;
        trx.commit().await?;
        ids
    };

    // Owner-shaped two-clause predicate (this worked downstream).
    let owner_q = format!("user = '{}' AND account = '{}'", user_id.to_base64(), account_id.to_base64());
    let owner_lq = client_ctx.query::<ScopeCredView>(owner_q.as_str())?;
    owner_lq.wait_initialized().await;
    assert_eq!(eventually_count(&owner_lq, 1).await, 1, "two-clause Ref predicate should fetch the credential");

    // EndUser-shaped composition: ((user AND account) AND domain) AND user.
    let enduser_q = format!(
        "user = '{}' AND account = '{}' AND domain = '{}' AND user = '{}'",
        user_id.to_base64(),
        account_id.to_base64(),
        domain_id.to_base64(),
        user_id.to_base64()
    );
    let enduser_lq = client_ctx.query::<ScopeCredView>(enduser_q.as_str())?;
    enduser_lq.wait_initialized().await;
    assert_eq!(
        eventually_count(&enduser_lq, 1).await,
        1,
        "four-clause composed Ref predicate (with duplicate user clause) should fetch the credential"
    );

    Ok(())
}
