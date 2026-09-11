mod common;

use ankurah::core::{entity::Entity, error::ValidationError, node::NodeInner, storage::StorageEngine, util::Iterable};
use ankurah::policy::{AccessDenied, DefaultContext, PolicyAgent};
use ankurah::{error::RetrievalError, Mutable};
use anyhow::Result;
use common::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
};
use std::time::Duration;

#[derive(Clone)]
struct WriteProbe(Arc<dyn Fn() -> Result<(), AccessDenied> + Send + Sync>);

#[async_trait::async_trait]
impl PolicyAgent for WriteProbe {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        cdata: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &Node<SE, Self>,
        auth: &A,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    fn check_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _before: &Entity,
        _after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from: &EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &Node<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from: &EntityId,
        _state: &proto::Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C>(&self, _data: &C, _collection: &proto::CollectionId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    fn filter_predicate<C>(
        &self,
        _data: &C,
        _collection: &proto::CollectionId,
        predicate: ankql::ast::Predicate<ankql::ast::Resolved>,
    ) -> Result<ankql::ast::Predicate<ankql::ast::Resolved>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(predicate)
    }

    fn check_read<C>(
        &self,
        _data: &C,
        _id: &EntityId,
        _collection: &proto::CollectionId,
        _state: &proto::State,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(&self, _data: &C, _event: &proto::Attested<proto::Event>) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> {
        (self.0)()
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer: &EntityId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

async fn seed_album(context: &Context) -> Result<EntityId> {
    let trx = context.begin();
    let id = trx.create(&Album { name: "Initial".into(), year: "2024".into() }).await?.id();
    trx.commit().await?;
    Ok(id)
}

#[tokio::test]
async fn get_enforces_the_same_write_policy_as_edit() -> Result<()> {
    let allow_write = Arc::new(AtomicBool::new(true));
    let agent = WriteProbe(Arc::new({
        let allow_write = allow_write.clone();
        move || if allow_write.load(Ordering::SeqCst) { Ok(()) } else { Err(AccessDenied::ByPolicy("read-only test context")) }
    }));
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
    node.system.create().await?;
    let context = node.context(DEFAULT_CONTEXT)?;
    let id = seed_album(&context).await?;
    allow_write.store(false, Ordering::SeqCst);

    let view = context.get::<AlbumView>(id).await?;
    let trx = context.begin();
    assert!(matches!(view.edit(&trx), Err(RetrievalError::AccessDenied(AccessDenied::ByPolicy("read-only test context")))));
    assert!(matches!(trx.get::<Album>(&id).await, Err(RetrievalError::AccessDenied(AccessDenied::ByPolicy("read-only test context")))));

    allow_write.store(true, Ordering::SeqCst);
    let album = trx.get::<Album>(&id).await?;
    assert_eq!(album.entity(), view.edit(&trx)?.entity());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_get_and_edit_share_one_snapshot() -> Result<()> {
    for (left_get, right_get) in [(false, false), (true, false), (true, true)] {
        let armed = Arc::new(AtomicBool::new(false));
        let rendezvous = Arc::new((Mutex::new(0), Condvar::new()));
        let agent = WriteProbe(Arc::new({
            let armed = armed.clone();
            move || {
                if armed.load(Ordering::SeqCst) {
                    // Both callers must pass their first lookup before either can insert.
                    let (arrived, ready) = &*rendezvous;
                    let mut count = arrived.lock().unwrap();
                    *count += 1;
                    ready.notify_all();
                    let (count, _) = ready.wait_timeout_while(count, Duration::from_secs(2), |count| *count < 2).unwrap();
                    assert!(*count >= 2, "snapshot acquisition did not reach both write checks");
                }
                Ok(())
            }
        }));
        let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), agent);
        node.system.create().await?;
        let context = node.context(DEFAULT_CONTEXT)?;
        let id = seed_album(&context).await?;
        let view = context.get::<AlbumView>(id).await?;
        let trx = context.begin();
        let runtime = tokio::runtime::Handle::current();
        armed.store(true, Ordering::SeqCst);

        let acquire = |use_get| -> Result<Entity> {
            let album = if use_get { runtime.block_on(trx.get::<Album>(&id))? } else { view.edit(&trx)? };
            Ok(album.entity().clone())
        };
        let (left, right) = std::thread::scope(|scope| -> Result<_> {
            let left = scope.spawn(|| acquire(left_get));
            let right = scope.spawn(|| acquire(right_get));
            Ok((left.join().unwrap()?, right.join().unwrap()?))
        })?;
        assert_eq!(left, right, "both callers must receive the same transaction snapshot");
        assert_ne!(&left, view.entity(), "the snapshot must be detached from the resident entity");

        trx.get::<Album>(&id).await?.name()?.replace("Updated")?;
        assert_eq!(view.edit(&trx)?.name()?.value().as_deref(), Some("Updated"));
        assert_eq!(view.name()?, "Initial");
        let events = trx.commit_and_return_events().await?;
        assert_eq!(events.len(), 1);
        assert_eq!(view.name()?, "Updated");
    }
    Ok(())
}

#[tokio::test]
async fn get_reuses_an_uncommitted_created_entity() -> Result<()> {
    let context = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await.unwrap();
    let trx = context.begin();
    let created = trx.create(&Album { name: "Initial".into(), year: "2024".into() }).await?;
    created.name()?.replace("Updated")?;
    let fetched = trx.get::<Album>(&created.id()).await?;
    assert_eq!(created.entity(), fetched.entity());
    assert_eq!(fetched.name()?.value().as_deref(), Some("Updated"));
    trx.commit().await?;
    Ok(())
}
