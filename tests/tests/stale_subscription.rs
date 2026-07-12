//! The strongest stale-SubscribeQuery regression from the identity review
//! handoff: a verified SubscribeQuery paused in async policy work
//! (`check_request`) across a hard reset PLUS a same-NodeId replacement
//! session must not install or update the replacement session's
//! subscription. The pause is a deterministic PolicyAgent hook, not a sleep.

mod common;

use ankql::ast::Predicate;
use ankurah::core::{
    entity::Entity,
    error::ValidationError,
    node::{Node as NodeAlias, NodeInner},
    policy::{AccessDenied, Admission, DefaultContext, PolicyAgent},
    storage::StorageEngine,
    util::Iterable,
};
use ankurah::policy::DEFAULT_CONTEXT;
use ankurah::proto::{self, Attested};
use ankurah::{Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use common::{Album, AlbumView};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;

const C: &DefaultContext = &DefaultContext {};

/// Permissive agent whose `check_request` parks exactly one armed
/// SubscribeQuery until the test releases it.
#[derive(Clone)]
struct PausingAgent {
    arm_subscribe_pause: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl PausingAgent {
    fn new() -> Self {
        Self { arm_subscribe_pause: Arc::new(AtomicBool::new(false)), entered: Arc::new(Notify::new()), release: Arc::new(Notify::new()) }
    }
}

#[async_trait]
impl PolicyAgent for PausingAgent {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C2>(
        &self,
        _node: &NodeInner<SE, Self>,
        cdata: &C2,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C2: Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &NodeAlias<SE, Self>,
        auth: &A,
        request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        if matches!(request.body, proto::NodeRequestBody::SubscribeQuery { .. })
            && self.arm_subscribe_pause.swap(false, Ordering::AcqRel)
        {
            // Register interest BEFORE signalling entry so the release cannot
            // be lost between the two.
            let released = self.release.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            self.entered.notify_waiters();
            released.await;
        }
        Ok(auth.iterable().map(|_| C).collect())
    }

    fn check_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &Entity,
        _entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Admission, AccessDenied> {
        Ok(Admission::Allow)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::NodeId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeAlias<SE, Self>, _state: &proto::EntityState) -> Admission { Admission::Allow }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::NodeId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C2>(&self, _data: &C2, _collection: &proto::CollectionId) -> Result<(), AccessDenied>
    where C2: Iterable<Self::ContextData> {
        Ok(())
    }

    fn filter_predicate<C2>(&self, _data: &C2, _collection: &proto::CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied>
    where C2: Iterable<Self::ContextData> {
        Ok(predicate)
    }

    fn check_read<C2>(
        &self,
        _data: &C2,
        _id: &proto::EntityId,
        _collection: &proto::CollectionId,
        _state: &proto::State,
        _resolver: Option<std::sync::Weak<dyn ankurah::core::schema::CatalogResolver>>,
    ) -> Result<(), AccessDenied>
    where
        C2: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C2>(
        &self,
        _data: &C2,
        _collection: &proto::CollectionId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>
    where
        C2: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _peer_id: &proto::NodeId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// The review standard applied to the SubscribeQuery seam: pause the verified
/// request immediately before its policy await resumes into a world where the
/// old system was hard reset AND the same client NodeId holds a replacement
/// session. The resumed request must produce a typed failure with no
/// subscription side effect on the replacement session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_subscribe_query_cannot_touch_replacement_session() -> Result<()> {
    let agent = PausingAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());

    let connection1 = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    let ctx = client.context_async(DEFAULT_CONTEXT).await;

    // Arm the pause, then issue the query whose SubscribeQuery will park
    // inside the server's check_request.
    agent.arm_subscribe_pause.store(true, Ordering::Release);
    let entered = agent.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    let stale_query = ctx.query::<AlbumView>("name = 'stale'")?;
    tokio::time::timeout(std::time::Duration::from_secs(5), entered).await.expect("the armed SubscribeQuery must reach check_request");

    // Drop the live query so the relay will NOT re-subscribe it after the
    // reconnect: the replacement session must end up with ZERO queries, so
    // any query the paused request installed would be visible.
    drop(stale_query);

    // Hard reset the server and recreate a replacement system, then connect
    // the SAME client NodeId over a fresh session. The old session's
    // SubscribeQuery is still parked in check_request throughout.
    server.system.hard_reset().await?;
    server.system.create().await?;
    // A joined ephemeral PINS its root and refuses a founder presenting a
    // different one; following the replacement system is an explicit local
    // reset, exactly as in a real deployment.
    client.system.hard_reset().await?;
    // connection1 stays ALIVE: the paused dispatch runs inside its receiver
    // task, and the whole point is to let it RESUME after the replacement
    // session exists (dropping it would cancel the paused request instead).
    let _connection2 = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while server.peer_subscription_query_count(&client.id).is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the replacement session must register");
    assert_eq!(server.peer_subscription_query_count(&client.id), Some(0), "the replacement session starts with no queries");

    // Release the paused request. It resumes bound to the OLD session and the
    // OLD generation; the hardened dispatch must refuse to look up (or touch)
    // the replacement session. Sample the count throughout the window: even a
    // TRANSIENT install (later rolled back by the generation guard) is a
    // subscription update of the replacement session and must never happen.
    agent.release.notify_waiters();
    let mut max_observed = 0usize;
    for _ in 0..400 {
        if let Some(count) = server.peer_subscription_query_count(&client.id) {
            max_observed = max_observed.max(count);
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        max_observed, 0,
        "a SubscribeQuery paused across reset plus same-NodeId replacement must not install into the replacement session, even transiently"
    );
    assert_eq!(server.peer_subscription_query_count(&client.id), Some(0));

    // The replacement session remains fully usable afterwards. The old
    // context belongs to the reset generation, so a fresh one drives the
    // proof query. Catalog warm-up rides the same relay, so assert growth
    // rather than an exact count.
    let fresh_ctx = client.context_async(DEFAULT_CONTEXT).await;
    let live = fresh_ctx.query_wait::<AlbumView>("name = 'fresh'").await?;
    let after = server.peer_subscription_query_count(&client.id).expect("replacement session still registered");
    assert!(after >= 1, "a genuine new query installs normally (saw {after})");
    drop(live);
    drop(connection1);
    Ok(())
}
