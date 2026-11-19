use super::*;
use std::sync::atomic::AtomicUsize;
use tokio::sync::oneshot;

struct HeldRead {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    resume: Mutex<Option<oneshot::Receiver<()>>>,
    entities: Vec<TestEntity>,
}

impl HeldRead {
    fn new(entities: Vec<TestEntity>) -> (Arc<Self>, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, observed) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        (Arc::new(Self { entered: Mutex::new(Some(entered)), resume: Mutex::new(Some(resume)), entities }), observed, release)
    }

    async fn read(&self) -> Vec<TestEntity> {
        let resume = self.resume.lock().unwrap().take();
        if let Some(resume) = resume {
            self.entered.lock().unwrap().take().unwrap().send(()).unwrap();
            resume.await.unwrap();
        }
        self.entities.clone()
    }
}

#[async_trait::async_trait]
impl LocalEntitySource<TestEntity> for HeldRead {
    async fn fetch_entities_from_local(&self, _: &ankql::ast::Selection<Resolved>) -> Result<Vec<TestEntity>, RetrievalError> {
        Ok(self.read().await)
    }
}

#[async_trait::async_trait]
impl GapFetcher<TestEntity> for HeldRead {
    async fn fetch_gap(
        &self,
        _: &ankql::ast::Selection<Resolved>,
        _: Option<&TestEntity>,
        _: usize,
    ) -> Result<Vec<TestEntity>, RetrievalError> {
        Ok(self.read().await)
    }
}

struct CountPublications(Arc<AtomicUsize>);
impl PreNotifyHook for CountPublications {
    fn pre_notify(&self, _: u32) { self.0.fetch_add(1, Ordering::SeqCst); }
}

#[tokio::test]
async fn an_initial_read_completing_after_close_cannot_install_a_query() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let results = EntityResultSet::empty();
    let (source, entered, release) = HeldRead::new(vec![TestEntity::new("Old", "pending")]);
    let (listener, updates) = watcher();
    let _watch = subscription.subscribe(listener);
    let publications = Arc::new(AtomicUsize::new(0));
    let activation = reactor.upsert_query_and_notify(
        subscription.id(),
        QueryId::new(),
        sel("status = 'pending'"),
        source,
        results.clone(),
        Arc::new(MockGapFetcher::new()),
        1,
        CountPublications(publications.clone()),
    );
    tokio::pin!(activation);
    assert!(futures::poll!(&mut activation).is_pending());
    entered.await.unwrap();
    reactor.close().await;
    release.send(()).unwrap();
    assert!(activation.await.is_err());
    assert_eq!(results.len(), 0);
    assert!(!results.is_loaded());
    assert_eq!(publications.load(Ordering::SeqCst), 0);
    assert!(updates().is_empty());
    assert!(reactor.subscription(subscription.id()).is_none());
    let late = reactor.subscribe();
    assert!(reactor.subscription(late.id()).is_none(), "close also rejects later subscriptions");
}

#[tokio::test]
async fn a_delayed_first_fetch_cannot_overwrite_a_query_installed_while_it_waited() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let results = EntityResultSet::empty();
    let (old_source, entered, release) = HeldRead::new(vec![TestEntity::new("Old", "pending")]);
    let publications = Arc::new(AtomicUsize::new(0));
    let old = reactor.upsert_query_and_notify(
        subscription.id(),
        query,
        sel("status = 'pending'"),
        old_source,
        results.clone(),
        Arc::new(MockGapFetcher::new()),
        1,
        CountPublications(publications.clone()),
    );
    tokio::pin!(old);
    assert!(futures::poll!(&mut old).is_pending());
    entered.await.unwrap();
    let replacement = TestEntity::new("Current", "pending");
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            query,
            sel("status = 'pending'"),
            Arc::new(MockNode { entities: vec![replacement.clone()] }),
            results.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
        )
        .await
        .unwrap();
    let (listener, updates) = watcher();
    let _watch = subscription.subscribe(listener);
    release.send(()).unwrap();
    old.await.unwrap();
    assert_eq!(results.keys().collect::<Vec<_>>(), vec![replacement.id]);
    assert_eq!(publications.load(Ordering::SeqCst), 0);
    assert!(updates().is_empty());
}

#[tokio::test]
async fn a_gap_completing_after_close_cannot_refill_or_mark_loaded() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let selected = TestEntity::new("Selected", "pending");
    let source = Arc::new(MockNode { entities: vec![selected.clone()] });
    let results = EntityResultSet::empty();
    let (gap, entered, release) = HeldRead::new(vec![TestEntity::new("Late", "pending")]);
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            query,
            sel("status = 'pending' LIMIT 1"),
            source.clone(),
            results.clone(),
            gap.clone(),
            1,
            (),
        )
        .await
        .unwrap();
    selected.state.lock().unwrap().insert(prop("status"), "done".into());
    let publications = Arc::new(AtomicUsize::new(0));
    let activation = reactor.upsert_query_and_notify(
        subscription.id(),
        query,
        sel("status = 'pending' LIMIT 1"),
        source,
        results.clone(),
        gap,
        2,
        CountPublications(publications.clone()),
    );
    tokio::pin!(activation);
    assert!(futures::poll!(&mut activation).is_pending());
    entered.await.unwrap();
    reactor.close().await;
    release.send(()).unwrap();
    activation.await.unwrap();
    assert_eq!(results.len(), 0);
    assert!(!results.is_loaded());
    assert_eq!(publications.load(Ordering::SeqCst), 0);
    let watchers = reactor.0.watcher_set.lock().unwrap();
    let (index, wildcard, entities) = watchers.debug_data();
    assert!(index.is_empty() && wildcard.is_empty() && entities.is_empty());
}

#[tokio::test]
async fn a_removed_and_reinstalled_query_discards_its_old_gap_even_at_the_same_version() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let query = QueryId::new();
    let selected = TestEntity::new("Selected", "pending");
    let source = Arc::new(MockNode { entities: vec![selected.clone()] });
    let results = EntityResultSet::empty();
    let (gap, entered, release) = HeldRead::new(vec![TestEntity::new("Late", "pending")]);
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            query,
            sel("status = 'pending' LIMIT 1"),
            source.clone(),
            results.clone(),
            gap.clone(),
            1,
            (),
        )
        .await
        .unwrap();
    selected.state.lock().unwrap().insert(prop("status"), "done".into());
    let publications = Arc::new(AtomicUsize::new(0));
    let old = reactor.upsert_query_and_notify(
        subscription.id(),
        query,
        sel("status = 'pending' LIMIT 1"),
        source,
        results.clone(),
        gap,
        2,
        CountPublications(publications.clone()),
    );
    tokio::pin!(old);
    assert!(futures::poll!(&mut old).is_pending());
    entered.await.unwrap();
    subscription.remove_predicate(query).unwrap();
    let replacement = TestEntity::new("Current", "pending");
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            query,
            sel("status = 'pending' LIMIT 1"),
            Arc::new(MockNode { entities: vec![replacement.clone()] }),
            results.clone(),
            Arc::new(MockGapFetcher::new()),
            2,
            (),
        )
        .await
        .unwrap();
    let (listener, updates) = watcher();
    let _watch = subscription.subscribe(listener);
    release.send(()).unwrap();
    old.await.unwrap();
    assert_eq!(results.keys().collect::<Vec<_>>(), vec![replacement.id]);
    assert_eq!(publications.load(Ordering::SeqCst), 0);
    assert!(updates().is_empty());
}

#[tokio::test]
async fn close_waits_for_current_publication_and_is_idempotent() {
    let reactor = Reactor::<TestEntity, TestEvent>::new_test();
    let subscription = reactor.subscribe();
    let results = EntityResultSet::empty();
    reactor
        .upsert_query_and_notify(
            subscription.id(),
            QueryId::new(),
            sel("status = 'pending'"),
            Arc::new(MockNode { entities: vec![TestEntity::new("Old", "pending")] }),
            results.clone(),
            Arc::new(MockGapFetcher::new()),
            1,
            (),
        )
        .await
        .unwrap();
    let publication = reactor.0.notify_lock.lock().await;
    let close = reactor.close();
    tokio::pin!(close);
    assert!(futures::poll!(&mut close).is_pending());
    assert!(results.is_loaded());
    drop(publication);
    close.await;
    reactor.close().await;
    assert_eq!(results.len(), 0);
    assert!(!results.is_loaded());
}
