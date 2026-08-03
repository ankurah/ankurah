//! Credential refresh re-permissions relayed subscriptions: updating a
//! Context's JwtContext re-sends its live queries to the durable peer,
//! which re-validates the new token and re-scopes the query under the new
//! claims. Visibility follows the credential, with no livequery teardown.
//!
//! The scoping is server-authoritative: the client agent grants reads but
//! carries no scope rules, so the durable node's `owner = $jwt.sub`
//! injection is the only visibility filter, exactly the deployment shape
//! where clients cannot be trusted to narrow their own queries.

mod common;
use common::*;

use ankurah::core::node::MatchArgs;
use ankurah::{Model, Node, Ref};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_jwt_auth::{JwtAgent, JwtContext, JwtKeys, PolicyConfig};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopeTarget {
    pub name: String,
}

#[derive(Model, Debug, serde::Serialize, serde::Deserialize)]
pub struct ScopeItem {
    pub owner: Ref<ScopeTarget>,
    pub label: String,
}

/// The durable node scopes scopeitem reads to the caller's own subject.
const SERVER_CONFIG: &str = r#"{
    "roles": {
        "Member": ["item:read", "target:read"]
    },
    "collections": {
        "scopeitem": {
            "read": "item:read",
            "scope": [
                { "filter": "owner = $jwt.sub" }
            ]
        },
        "scopetarget": {
            "read": "target:read"
        }
    }
}"#;

/// The client grants the same reads with NO scope rules.
const CLIENT_CONFIG: &str = r#"{
    "roles": {
        "Member": ["item:read", "target:read"]
    },
    "collections": {
        "scopeitem": {
            "read": "item:read"
        },
        "scopetarget": {
            "read": "target:read"
        }
    }
}"#;

/// Poll on IDENTITY, not count: a re-permission that swaps WHICH row
/// matches keeps the count at 1 throughout, so a count poll returns
/// before the swap and the follow-up id assert races it.
async fn eventually_ids(lq: &ankurah::LiveQuery<ScopeItemView>, expected: &[ankurah::proto::EntityId]) -> bool {
    for _ in 0..100 {
        if lq.ids() == expected {
            return true;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    lq.ids() == expected
}

async fn eventually_count(lq: &ankurah::LiveQuery<ScopeItemView>, expected: usize) -> usize {
    for _ in 0..100 {
        let count = lq.ids().len();
        if count == expected {
            return count;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    lq.ids().len()
}

async fn eventually(mut condition: impl FnMut() -> bool) -> bool {
    for _ in 0..100 {
        if condition() {
            return true;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    condition()
}

/// The deployed shape: jwt policy sync mirrors scope rules onto clients,
/// so the CLIENT also injects `owner = $jwt.sub` into the query. A
/// refresh must re-derive that client-side filter under the new claims
/// rather than re-sending the one baked at creation; a baked `owner =
/// alice` ANDed with the server's `owner = bob` would go permanently
/// dark. The local re-filter also drops alice's now-out-of-scope item
/// from the resultset.
#[tokio::test]
async fn refresh_rederives_client_scope_rules() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let server_agent = JwtAgent::new_ephemeral();
    server_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    server_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;

    // The client carries the SAME scoped config the policy sync would mirror.
    let client_agent = JwtAgent::new_ephemeral();
    client_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    client_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent);

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let root = server.context(JwtContext::system())?;
    let (alice_id, bob_id) = {
        let trx = root.begin();
        let alice = trx.create(&ScopeTarget { name: "alice".into() }).await?;
        let bob = trx.create(&ScopeTarget { name: "bob".into() }).await?;
        let ids = (alice.id(), bob.id());
        trx.commit().await?;
        ids
    };
    let (alice_item, bob_item) = {
        let trx = root.begin();
        let a = trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-1".into() }).await?.id();
        let b = trx.create(&ScopeItem { owner: bob_id.into(), label: "bob-1".into() }).await?.id();
        trx.commit().await?;
        (a, b)
    };

    let alice_claims = make_claims(&alice_id.to_base64(), &["Member"], "alice@example.com");
    let alice_token = sign_token(&keys, &alice_claims);
    let ctx = client.context(JwtContext::from_claims(alice_claims, alice_token))?;

    let args = MatchArgs { selection: "true".try_into()?, cached: false };
    let lq = ctx.query::<ScopeItemView>(args)?;
    lq.wait_initialized().await;
    assert_eq!(lq.ids(), vec![alice_item], "alice's credential sees exactly her item");

    let bob_claims = make_claims(&bob_id.to_base64(), &["Member"], "bob@example.com");
    let bob_token = sign_token(&keys, &bob_claims);
    ctx.update_cdata(JwtContext::from_claims(bob_claims, bob_token))?;
    lq.wait_initialized().await;

    // The client-side filter is now `owner = bob`: alice's item drops out
    // of the local resultset and bob's arrives through the re-subscribe.
    assert!(eventually_ids(&lq, &[bob_item]).await, "bob's item replaces alice's after re-permission, got {:?}", lq.ids());

    {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: bob_id.into(), label: "bob-2".into() }).await?;
        trx.commit().await?;
    }
    assert_eq!(eventually_count(&lq, 2).await, 2, "new in-scope commits flow under the re-derived filter");

    {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-2".into() }).await?;
        trx.commit().await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    assert_eq!(lq.ids().len(), 2, "old-credential commits must not arrive");
    Ok(())
}

/// One credential update re-permissions EVERY live query under the
/// context, each through its own version authority: a re-login with two
/// standing queries re-derives and re-subscribes both.
#[tokio::test]
async fn one_update_repermissions_every_query_under_the_context() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let server_agent = JwtAgent::new_ephemeral();
    server_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    server_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;

    let client_agent = JwtAgent::new_ephemeral();
    client_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    client_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent);

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let root = server.context(JwtContext::system())?;
    let (alice_id, bob_id) = {
        let trx = root.begin();
        let alice = trx.create(&ScopeTarget { name: "alice".into() }).await?;
        let bob = trx.create(&ScopeTarget { name: "bob".into() }).await?;
        let ids = (alice.id(), bob.id());
        trx.commit().await?;
        ids
    };
    let (alice_item, bob_item) = {
        let trx = root.begin();
        let a = trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-1".into() }).await?.id();
        let b = trx.create(&ScopeItem { owner: bob_id.into(), label: "bob-1".into() }).await?.id();
        trx.commit().await?;
        (a, b)
    };

    let alice_claims = make_claims(&alice_id.to_base64(), &["Member"], "alice@example.com");
    let alice_token = sign_token(&keys, &alice_claims);
    let ctx = client.context(JwtContext::from_claims(alice_claims, alice_token))?;

    let first = ctx.query::<ScopeItemView>(MatchArgs { selection: "true".try_into()?, cached: false })?;
    let second = ctx.query::<ScopeItemView>(MatchArgs { selection: "true".try_into()?, cached: false })?;
    first.wait_initialized().await;
    second.wait_initialized().await;
    assert_eq!(first.ids(), vec![alice_item], "first query sees alice's item");
    assert_eq!(second.ids(), vec![alice_item], "second query sees alice's item");
    assert_eq!((first.selection().value().1, second.selection().value().1), (1, 1));

    let bob_claims = make_claims(&bob_id.to_base64(), &["Member"], "bob@example.com");
    let bob_token = sign_token(&keys, &bob_claims);
    ctx.update_cdata(JwtContext::from_claims(bob_claims, bob_token))?;
    first.wait_initialized().await;
    second.wait_initialized().await;
    assert_eq!(first.selection().value().1, 2, "first query re-permissioned");
    assert_eq!(second.selection().value().1, 2, "second query re-permissioned");
    assert!(eventually_ids(&first, &[bob_item]).await, "first query re-scopes to bob, got {:?}", first.ids());
    assert!(eventually_ids(&second, &[bob_item]).await, "second query re-scopes to bob, got {:?}", second.ids());
    Ok(())
}

/// A DURABLE node's local livequery also re-permissions: `update_cdata`
/// re-derives the scope filter and re-activates the reactor registration,
/// so visibility follows the credential with no relay involved.
#[tokio::test]
async fn refresh_repermissions_durable_local_queries() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let agent = JwtAgent::new_ephemeral();
    agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    agent.set_keys(JwtKeys::Signing(keys.clone()));
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
    node.system.create().await?;

    let root = node.context(JwtContext::system())?;
    let (alice_id, bob_id) = {
        let trx = root.begin();
        let alice = trx.create(&ScopeTarget { name: "alice".into() }).await?;
        let bob = trx.create(&ScopeTarget { name: "bob".into() }).await?;
        let ids = (alice.id(), bob.id());
        trx.commit().await?;
        ids
    };
    let (alice_item, bob_item) = {
        let trx = root.begin();
        let a = trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-1".into() }).await?.id();
        let b = trx.create(&ScopeItem { owner: bob_id.into(), label: "bob-1".into() }).await?.id();
        trx.commit().await?;
        (a, b)
    };

    let alice_claims = make_claims(&alice_id.to_base64(), &["Member"], "alice@example.com");
    let alice_token = sign_token(&keys, &alice_claims);
    let ctx = node.context(JwtContext::from_claims(alice_claims, alice_token))?;

    let lq = ctx.query::<ScopeItemView>("true")?;
    lq.wait_initialized().await;
    assert_eq!(lq.ids(), vec![alice_item], "alice sees exactly her item");

    let bob_claims = make_claims(&bob_id.to_base64(), &["Member"], "bob@example.com");
    let bob_token = sign_token(&keys, &bob_claims);
    ctx.update_cdata(JwtContext::from_claims(bob_claims, bob_token))?;
    lq.wait_initialized().await;
    assert!(eventually_ids(&lq, &[bob_item]).await, "bob's item replaces alice's after the local re-filter, got {:?}", lq.ids());

    let bob_item_2 = {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-2".into() }).await?;
        let b2 = trx.create(&ScopeItem { owner: bob_id.into(), label: "bob-2".into() }).await?.id();
        trx.commit().await?;
        b2
    };
    assert_eq!(eventually_count(&lq, 2).await, 2, "one new item arrives");
    let mut expected = vec![bob_item, bob_item_2];
    expected.sort();
    let mut actual = lq.ids();
    actual.sort();
    assert_eq!(actual, expected, "bob's items exactly; alice's new item did not leak in");
    Ok(())
}

/// One livequery, two credentials: subscribed as alice it sees alice's
/// items; after `update_cdata` to bob's token the SAME subscription is
/// re-scoped by the server, bob's existing item arrives, new bob items
/// flow, and new alice items no longer do.
#[tokio::test]
async fn refresh_moves_subscription_visibility() -> anyhow::Result<()> {
    let keys = common::test_keys();

    let server_agent = JwtAgent::new_ephemeral();
    server_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    server_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;

    let client_agent = JwtAgent::new_ephemeral();
    client_agent.update_config(serde_json::from_str::<PolicyConfig>(CLIENT_CONFIG)?);
    client_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent);

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let root = server.context(JwtContext::system())?;
    let (alice_id, bob_id) = {
        let trx = root.begin();
        let alice = trx.create(&ScopeTarget { name: "alice".into() }).await?;
        let bob = trx.create(&ScopeTarget { name: "bob".into() }).await?;
        let ids = (alice.id(), bob.id());
        trx.commit().await?;
        ids
    };
    {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-1".into() }).await?;
        trx.create(&ScopeItem { owner: bob_id.into(), label: "bob-1".into() }).await?;
        trx.commit().await?;
    }

    let alice_claims = make_claims(&alice_id.to_base64(), &["Member"], "alice@example.com");
    let alice_token = sign_token(&keys, &alice_claims);
    let ctx = client.context(JwtContext::from_claims(alice_claims, alice_token))?;

    let args = MatchArgs { selection: "true".try_into()?, cached: false };
    let lq = ctx.query::<ScopeItemView>(args)?;
    lq.wait_initialized().await;
    assert_eq!(lq.ids().len(), 1, "alice's credential sees exactly her item");

    // The refresh: bob's token through the SAME context and subscription.
    let bob_claims = make_claims(&bob_id.to_base64(), &["Member"], "bob@example.com");
    let bob_token = sign_token(&keys, &bob_claims);
    ctx.update_cdata(JwtContext::from_claims(bob_claims, bob_token))?;
    lq.wait_initialized().await;

    // Bob's pre-existing item arrives through the re-subscribe deltas.
    // (Alice's item stays resident locally; revocation never claws back
    // rows the node already holds.)
    assert_eq!(eventually_count(&lq, 2).await, 2, "bob's existing item arrives after re-permission");

    // Forward visibility follows the new credential: bob's commits flow...
    {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: bob_id.into(), label: "bob-2".into() }).await?;
        trx.commit().await?;
    }
    assert_eq!(eventually_count(&lq, 3).await, 3, "new in-scope commits reach the re-permissioned subscription");

    // ...and alice's no longer do.
    {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-2".into() }).await?;
        trx.commit().await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    assert_eq!(lq.ids().len(), 3, "commits scoped to the old credential must not arrive");

    Ok(())
}

/// A credential change that REVOKES access does not freeze the query:
/// re-permission flips it to Denied, claws the rows back out of the live
/// resultset (locally persisted state is untouched), and a re-login
/// heals it, rows returning.
#[tokio::test]
async fn logout_claws_back_the_resultset() -> anyhow::Result<()> {
    use ankurah::core::livequery::LocalStatus;
    use ankurah::signals::Peek;

    let keys = common::test_keys();

    let server_agent = JwtAgent::new_ephemeral();
    server_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    server_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;

    let client_agent = JwtAgent::new_ephemeral();
    client_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    client_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent);

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let root = server.context(JwtContext::system())?;
    let (alice_id, alice_item) = {
        let trx = root.begin();
        let alice = trx.create(&ScopeTarget { name: "alice".into() }).await?;
        let alice_id = alice.id();
        let item = trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-1".into() }).await?.id();
        trx.commit().await?;
        (alice_id, item)
    };

    let alice_claims = make_claims(&alice_id.to_base64(), &["Member"], "alice@example.com");
    let alice_token = sign_token(&keys, &alice_claims);
    let ctx = client.context(JwtContext::from_claims(alice_claims.clone(), alice_token.clone()))?;

    let lq = ctx.query::<ScopeItemView>(MatchArgs { selection: "true".try_into()?, cached: false })?;
    lq.wait_initialized().await;
    assert_eq!(lq.ids(), vec![alice_item], "alice sees her item while logged in");

    // Logout: the anonymous credential cannot read the collection at all.
    ctx.update_cdata(JwtContext::NoUser)?;
    assert_eq!(eventually_count(&lq, 0).await, 0, "logout claws the rows back out of the live resultset");
    assert!(
        matches!(lq.status().peek().local, LocalStatus::Denied { .. }),
        "the query reports Denied rather than freezing, got {:?}",
        lq.status().peek().local
    );
    // The local denial above is INSTANT (the client's own policy agent
    // refuses synchronously), but the intent still ships upstream for the
    // server's own verdict. Wait for that verdict to land on the remote
    // leg before committing: the server's claw-back runs BEFORE it sends
    // the refusal, so the refusal's arrival is the observable point after
    // which the old registration provably cannot stream. Committing on
    // the local denial alone races the in-flight re-validation and the
    // absence assertion below becomes timing-dependent.
    assert!(
        eventually(|| matches!(lq.status().peek().remote, ankurah::core::livequery::RemoteStatus::Error { .. })).await,
        "the server's refusal lands on the remote leg, got {:?}",
        lq.status().peek().remote
    );

    // The server tore down its side too (the failed re-validation removed
    // the standing registration): a commit that alice WOULD see must not
    // stream to the denied subscription.
    let alice_item_2 = {
        let trx = root.begin();
        let item = trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-2".into() }).await?.id();
        trx.commit().await?;
        item
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        lq.ids(),
        Vec::<ankurah::proto::EntityId>::new(),
        "nothing streams into a denied query, even rows the old credential could see"
    );
    // The resultset alone cannot distinguish the server's teardown from
    // the client's own claw-back (the local registration is gone either
    // way). LOCAL STORAGE can: had the server kept streaming under the
    // old grant, the delta would have been persisted on arrival.
    let raw_scope_items = client.collections.get(&ankurah::proto::CollectionId::fixed_name("scopeitem")).await?;
    assert!(
        raw_scope_items.get_state(alice_item_2).await.is_err(),
        "the denied-window commit must never reach client storage: the server removed the registration"
    );

    // Re-login: the query heals, initialization completes, and BOTH rows
    // arrive (the one clawed back and the one committed while denied).
    ctx.update_cdata(JwtContext::from_claims(alice_claims, alice_token))?;
    tokio::time::timeout(std::time::Duration::from_secs(5), lq.wait_initialized())
        .await
        .expect("initialization completes after the re-login heals the query");
    assert_eq!(eventually_count(&lq, 2).await, 2, "the re-login restores visibility including the row committed while denied");
    let mut ids = lq.ids();
    ids.sort();
    let mut expected = vec![alice_item, alice_item_2];
    expected.sort();
    assert_eq!(ids, expected, "exactly alice's two items, no strays");
    // The positive arm self-validates the collection name above: after
    // the heal streams the missed row, the SAME raw read must find it.
    // (A wrong collection id would fail here, not pass vacuously there.)
    assert!(
        raw_scope_items.get_state(alice_item_2).await.is_ok(),
        "after the heal, the recovered row is in client storage under the asserted collection"
    );
    Ok(())
}

/// A reactor-notification subscriber may revoke the credential from
/// inside its callback. The callback runs synchronously inside the
/// reactor's send_update; the revocation's claw-back synchronously
/// removes the reactor registration, which takes the same subscription
/// state lock — this deadlocked while send_update broadcast under that
/// lock, and pins that it no longer does.
#[tokio::test(flavor = "multi_thread")]
async fn reactor_subscriber_may_revoke_reentrantly() -> anyhow::Result<()> {
    use ankurah::core::livequery::LocalStatus;
    use ankurah::signals::{Peek, Subscribe};

    let keys = common::test_keys();

    let server_agent = JwtAgent::new_ephemeral();
    server_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    server_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;

    let client_agent = JwtAgent::new_ephemeral();
    client_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    client_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent);

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let root = server.context(JwtContext::system())?;
    let alice_id = {
        let trx = root.begin();
        let alice = trx.create(&ScopeTarget { name: "alice".into() }).await?;
        let alice_id = alice.id();
        trx.create(&ScopeItem { owner: alice_id.into(), label: "alice-1".into() }).await?;
        trx.commit().await?;
        alice_id
    };

    let alice_claims = make_claims(&alice_id.to_base64(), &["Member"], "alice@example.com");
    let alice_token = sign_token(&keys, &alice_claims);
    let ctx = client.context(JwtContext::from_claims(alice_claims, alice_token))?;

    let lq = ctx.query::<ScopeItemView>(MatchArgs { selection: "true".try_into()?, cached: false })?;
    lq.wait_initialized().await;

    let revoked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _guard = {
        let ctx2 = ctx.clone();
        let revoked = revoked.clone();
        lq.subscribe(move |_changes: ankurah::changes::ChangeSet<ScopeItemView>| {
            if !revoked.swap(true, std::sync::atomic::Ordering::SeqCst) {
                ctx2.update_cdata(JwtContext::NoUser).expect("re-entrant revocation must not fail");
            }
        })
    };

    // Drive a reactor notification from an activation so the subscriber
    // fires inside send_update's dispatch: the narrowed selection drops
    // alice's row, and that membership change is the notified item (a
    // same-selection update produces no items and sends nothing).
    lq.update_selection("label = 'nothing-matches-this'")?;

    assert!(
        eventually_count(&lq, 0).await == 0 && matches!(lq.status().peek().local, LocalStatus::Denied { .. }),
        "the re-entrant revocation completes: rows clawed back and status Denied, got {:?} / {:?}",
        lq.ids(),
        lq.status().peek().local
    );
    assert!(revoked.load(std::sync::atomic::Ordering::SeqCst), "the subscriber must have fired and revoked");
    Ok(())
}

/// A RESULTSET-channel subscriber may revoke the credential from inside
/// its callback, and a multi-entity batch stays coherent through it.
/// Two pins in one: the deferred membership writes broadcast outside the
/// reactor state lock (revoking in-callback reaches remove_predicate and
/// that lock: pre-deferral this deadlocked), and each deferred write is
/// liveness-gated (the batch's second write lands AFTER the revocation
/// removed the query; ungated it would repopulate a denied resultset).
#[tokio::test(flavor = "multi_thread")]
async fn resultset_subscriber_may_revoke_and_the_batch_stays_clawed_back() -> anyhow::Result<()> {
    use ankurah::core::livequery::LocalStatus;
    use ankurah::signals::{Peek, Subscribe};

    let keys = common::test_keys();

    let server_agent = JwtAgent::new_ephemeral();
    server_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    server_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), server_agent);
    server.system.create().await?;

    let client_agent = JwtAgent::new_ephemeral();
    client_agent.update_config(serde_json::from_str::<PolicyConfig>(SERVER_CONFIG)?);
    client_agent.set_keys(JwtKeys::Signing(keys.clone()));
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent);

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let root = server.context(JwtContext::system())?;
    let alice_id = {
        let trx = root.begin();
        let alice = trx.create(&ScopeTarget { name: "alice".into() }).await?;
        let alice_id = alice.id();
        trx.commit().await?;
        alice_id
    };

    let alice_claims = make_claims(&alice_id.to_base64(), &["Member"], "alice@example.com");
    let alice_token = sign_token(&keys, &alice_claims);
    let ctx = client.context(JwtContext::from_claims(alice_claims, alice_token))?;

    let lq = ctx.query::<ScopeItemView>(MatchArgs { selection: "true".try_into()?, cached: false })?;
    lq.wait_initialized().await;

    let revoked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _guard = {
        let ctx2 = ctx.clone();
        let revoked = revoked.clone();
        lq.resultset().subscribe(move |_items: Vec<ScopeItemView>| {
            if !revoked.swap(true, std::sync::atomic::Ordering::SeqCst) {
                ctx2.update_cdata(JwtContext::NoUser).expect("re-entrant revocation must not fail");
            }
        })
    };
    // A ChangeSet observer alongside: the batch's ReactorUpdate is
    // dispatched AFTER the deferred writes, by which time the revocation
    // (synchronous inside the first write's broadcast) has already landed
    // as Denied, so the changeset channel must deliver NO membership
    // changes from the dead registration.
    let changeset_adds = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _cs_guard = {
        let changeset_adds = changeset_adds.clone();
        lq.subscribe(move |changes: ankurah::changes::ChangeSet<ScopeItemView>| {
            let adds = changes
                .changes
                .iter()
                .filter(|c| matches!(c, ankurah::changes::ItemChange::Add { .. } | ankurah::changes::ItemChange::Initial { .. }))
                .count();
            changeset_adds.fetch_add(adds, std::sync::atomic::Ordering::SeqCst);
        })
    };

    // One transaction, two rows: two deferred membership writes in one
    // reactor batch. The first write's broadcast revokes; the gate must
    // drop the second.
    {
        let trx = root.begin();
        trx.create(&ScopeItem { owner: alice_id.into(), label: "batch-1".into() }).await?;
        trx.create(&ScopeItem { owner: alice_id.into(), label: "batch-2".into() }).await?;
        trx.commit().await?;
    }

    // Order the waits: the query starts EMPTY, so a bare count==0 check
    // is trivially true before the batch even arrives. First the
    // subscriber proves delivery reached the resultset channel, then the
    // revocation lands as Denied, and only then is emptiness a claim
    // about the deferred-write gate rather than about nothing.
    assert!(
        eventually(|| revoked.load(std::sync::atomic::Ordering::SeqCst)).await,
        "the subscriber must fire on the batch delivery and revoke"
    );
    assert!(
        eventually(|| matches!(lq.status().peek().local, LocalStatus::Denied { .. })).await,
        "the re-entrant revocation lands as Denied, got {:?}",
        lq.status().peek().local
    );
    // A beat for an ungated second write to surface, then the claims:
    // nothing from the batch survives the claw-back, and the dead
    // registration's membership changes never reached the changeset
    // channel either.
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    assert!(lq.ids().is_empty(), "no batch row survives the revocation (the second deferred write is liveness-gated), got {:?}", lq.ids());
    assert_eq!(
        changeset_adds.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no membership change from the revoked batch reaches changeset subscribers"
    );
    Ok(())
}
