use super::*;
use crate::{
    error::RetrievalError,
    reactor::{
        tests::{prop, sel, watcher, MockGapFetcher, MockNode, TestChange, TestEntity, TestEvent},
        MembershipChange, ReactorUpdate,
    },
};
use ankurah_signals::Subscribe;
use proto::QueryId;

/// The source of a query whose membership the stream `.1` decides; it admits the ids in `.0`.
struct RemoteSource(Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>, QueryOrigin);
fn origin(query: QueryId) -> QueryOrigin { QueryOrigin { query, version: 1, request: proto::RequestId::default() } }
#[async_trait::async_trait]
impl LocalEntitySource<TestEntity> for RemoteSource {
    fn query_origin(&self) -> Option<&QueryOrigin> { Some(&self.1) }
    fn admits_query_entity(&self, entity: &TestEntity) -> bool { self.0.read().unwrap().contains(&entity.id) }
    async fn fetch_entities_from_local(&self, _: &ankql::ast::Selection<Resolved>) -> Result<Vec<TestEntity>, RetrievalError> { Ok(vec![]) }
}
/// A change the stream `origin` applied.
struct OwnChange {
    change: TestChange,
    origin: QueryOrigin,
}
impl std::fmt::Display for OwnChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.change.fmt(f) }
}
impl std::fmt::Debug for OwnChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.change.fmt(f) }
}
impl ChangeNotification for OwnChange {
    type Entity = TestEntity;
    type Event = TestEvent;
    fn into_parts(self) -> (TestEntity, Vec<TestEvent>) { self.change.into_parts() }
    fn entity(&self) -> &TestEntity { self.change.entity() }
    fn events(&self) -> &[TestEvent] { self.change.events() }
    fn query_origin(&self) -> Option<&QueryOrigin> { Some(&self.origin) }
}

mod answers;
mod notifications;
mod publications;
mod stream_changes;
