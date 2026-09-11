use crate::{context::SchemaResolver, error::RetrievalError, node::MatchArgs, schema::ModelStructDescriptor};
use ankql::ast::{Parsed, Resolved, Selection};
use ankurah_proto::CollectionId;

/// A synchronous resolution failure, retaining parsed input for a deferred retry.
#[derive(Debug)]
pub(crate) struct QueryResolutionError {
    pub error: RetrievalError,
    pub selection: Selection<Parsed>,
}

/// Resolve parsed input synchronously; already resolved input passes through unchanged.
pub(crate) trait ResolveQuery {
    fn resolve(
        self,
        resolver: &dyn SchemaResolver,
        schema: Option<&'static ModelStructDescriptor>,
        collection: &CollectionId,
    ) -> Result<MatchArgs<Resolved>, QueryResolutionError>;
}

impl ResolveQuery for MatchArgs<Parsed> {
    fn resolve(
        self,
        resolver: &dyn SchemaResolver,
        schema: Option<&'static ModelStructDescriptor>,
        collection: &CollectionId,
    ) -> Result<MatchArgs<Resolved>, QueryResolutionError> {
        let selection = resolver
            .resolve_query_selection(schema, collection, self.selection.clone())
            .map_err(|error| QueryResolutionError { error, selection: self.selection })?;
        Ok(MatchArgs { selection, cached: self.cached })
    }
}

impl ResolveQuery for MatchArgs<Resolved> {
    fn resolve(
        self,
        _resolver: &dyn SchemaResolver,
        _schema: Option<&'static ModelStructDescriptor>,
        _collection: &CollectionId,
    ) -> Result<MatchArgs<Resolved>, QueryResolutionError> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        context::Context, model::Model, policy::PermissiveAgent, schema::catalog::SysModelRow, session::SessionSet,
        test_utils::TestStorage, Node,
    };
    use ankql::ast::Predicate;
    use std::sync::Arc;

    #[tokio::test]
    async fn parsed_input_resolves_synchronously_and_preserves_errors() {
        let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let context = Context::new(node.clone(), SessionSet::new());
        let schema = Some(SysModelRow::descriptor());
        let collection = SysModelRow::collection();
        let input = || MatchArgs { selection: ankql::parser::parse_selection("true").unwrap(), cached: false };
        assert!(matches!(
            input().resolve(context.0.schema_resolver(), schema, &collection),
            Err(QueryResolutionError { error: RetrievalError::NodeNotReady, .. })
        ));
        node.system.create().await.unwrap();
        let resolved = input().resolve(context.0.schema_resolver(), schema, &collection).unwrap();
        assert!(!resolved.cached);
        assert!(matches!(resolved.selection.predicate, Predicate::True));
    }

    #[tokio::test]
    async fn resolved_input_is_a_noop() {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let context = Context::new(node, SessionSet::new());
        let input =
            MatchArgs { selection: Selection::<Resolved> { predicate: Predicate::True, order_by: None, limit: Some(7) }, cached: false };
        // The uninitialized resolver would reject this if the resolved arm called it.
        let resolved = input.resolve(context.0.schema_resolver(), None, &"test".into()).unwrap();
        assert_eq!(resolved.selection.limit, Some(7));
        assert!(!resolved.cached);
        assert!(matches!(resolved.selection.predicate, Predicate::True));
    }

    #[tokio::test]
    async fn deferred_resolution_returns_a_selection_without_a_livequery() {
        let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let context = Context::new(node.clone(), SessionSet::new());
        let mut resolution = context.0.schema_resolver().resolve_query_selection_when_ready(
            Some(SysModelRow::descriptor()),
            SysModelRow::collection(),
            ankql::parser::parse_selection("true").unwrap(),
        );
        drop(context);
        assert!(futures::poll!(&mut resolution).is_pending());
        node.system.create().await.unwrap();
        assert!(matches!(resolution.await.unwrap().predicate, Predicate::True));
    }
}
