use crate::internal::prelude::*;
use crate::schema::catalog::register::RegistrationAuth;
use crate::schema::registration::RegistrationError;
use ankql::ast::{Parsed, Resolved, Selection};
use async_trait::async_trait;
use futures::future::{ready, BoxFuture};

use super::{ContextAuth, ContextInner};

/// Context-scoped descriptor binding and selection resolution.
#[async_trait]
pub(crate) trait SchemaResolver: Send + Sync {
    /// Bind the descriptor's model and property IDs, registering missing declarations if needed.
    /// Return the model ID and the epoch the binding belongs to.
    async fn ensure_registered(&self, schema: &'static ModelStructDescriptor) -> Result<(proto::ModelId, SystemEpoch), RegistrationError>;

    /// Check entity ownership and bind the given descriptor locally, without registration or network access.
    /// Used by synchronous `Transaction::edit`.
    fn bind_descriptor_local(&self, schema: &'static ModelStructDescriptor, entity: &Entity) -> Result<(), RetrievalError>;

    /// Resolve names and literal types for `Context::fetch` after schema registration.
    /// Read-policy filtering is applied separately by `fetch_entities`.
    fn resolve_selection_with_descriptor(
        &self,
        schema: &'static ModelStructDescriptor,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError>;

    /// Resolve and policy-filter a livequery's initial or updated selection locally.
    /// Missing bindings/readiness return errors; the caller decides whether to resolve asynchronously.
    fn resolve_query_selection(
        &self,
        schema: Option<&'static ModelStructDescriptor>,
        collection_id: &CollectionId,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError>;

    /// Wait for the required catalog state, register missing declarations, and resolve with read policy.
    /// The returned future owns a weak context; the caller controls execution and cancellation.
    fn resolve_query_selection_when_ready(
        &self,
        schema: Option<&'static ModelStructDescriptor>,
        collection_id: CollectionId,
        selection: Selection<Parsed>,
    ) -> BoxFuture<'static, Result<Selection<Resolved>, RetrievalError>>;
}

#[async_trait]
impl<SE, PA> SchemaResolver for ContextInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Bind the descriptor's model and property IDs, registering missing declarations if needed.
    /// Return the model ID and the epoch the binding belongs to.
    async fn ensure_registered(&self, schema: &'static ModelStructDescriptor) -> Result<(proto::ModelId, SystemEpoch), RegistrationError> {
        let node = self.node.upgrade()?;
        node.system.check_not_halted()?;
        if let Some(system) = schema.system {
            return Ok((proto::ModelId::System(system), node.entities.system_epoch()));
        }
        let epoch = node.system.system_epoch().ok_or(RegistrationError::SystemNotReady)?;
        let mut registrant = schema.registrant(epoch);
        let auth = match &self.auth {
            ContextAuth::Sessions(sessions) => match sessions.write_credential() {
                Ok(credential) => RegistrationAuth::Credential(credential),
                Err(error) => RegistrationAuth::Unavailable(error),
            },
            ContextAuth::Privileged => RegistrationAuth::Privileged,
        };
        node.catalog.resolve_or_register(node.as_ref(), &mut registrant, auth).await?;
        let model = schema.resolved.get(epoch).ok_or_else(|| {
            RegistrationError::Retrieval(RetrievalError::Other(format!(
                "binding of '{}' did not retain its exact model identity",
                schema.label
            )))
        })?;
        Ok((model, epoch))
    }

    /// Check entity ownership and bind the given descriptor locally, without registration or network access.
    /// Used by synchronous `Transaction::edit`.
    fn bind_descriptor_local(&self, schema: &'static ModelStructDescriptor, entity: &Entity) -> Result<(), RetrievalError> {
        let node = self.node.upgrade()?;
        let epoch = node.entities.system_epoch();
        entity.check_epoch(epoch)?;
        if schema.system.is_some() || schema.resolved.get(epoch).is_some() {
            return Ok(());
        }
        let mut registrant = schema.registrant(epoch);
        node.catalog.resolve_local(&mut registrant).map_err(Into::into)
    }

    /// Resolve names and literal types for `Context::fetch` after schema registration.
    /// Read-policy filtering is applied separately by `fetch_entities`.
    fn resolve_selection_with_descriptor(
        &self,
        schema: &'static ModelStructDescriptor,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError> {
        let node = self.node.upgrade()?;
        schema.resolve_selection(&node.catalog, node.system.system_epoch(), selection)
    }

    /// Resolve and policy-filter a livequery's initial or updated selection locally.
    /// Missing bindings/readiness return errors; the caller decides whether to resolve asynchronously.
    fn resolve_query_selection(
        &self,
        schema: Option<&'static ModelStructDescriptor>,
        collection_id: &CollectionId,
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError> {
        let node = self.node.upgrade()?;
        node.system.check_not_halted()?;
        if let Some(schema) = schema {
            crate::schema::resolver::validate_selection_names(schema, &selection)?;
        }
        if node.system.system_epoch().is_none() {
            return Err(RetrievalError::NodeNotReady);
        }
        if !schema.is_some_and(|schema| schema.system.is_some()) {
            node.check_ready()?;
        }
        resolve_and_scope(self, schema, collection_id, selection)
    }

    /// Wait for the required catalog state, register missing declarations, and resolve with read policy.
    /// The returned future owns a weak context; the caller controls execution and cancellation.
    fn resolve_query_selection_when_ready(
        &self,
        schema: Option<&'static ModelStructDescriptor>,
        collection_id: CollectionId,
        selection: Selection<Parsed>,
    ) -> BoxFuture<'static, Result<Selection<Resolved>, RetrievalError>> {
        let sessions = match &self.auth {
            ContextAuth::Sessions(sessions) => sessions.clone(),
            ContextAuth::Privileged => {
                return Box::pin(ready(Err(RetrievalError::Other("the privileged context does not query".into()))));
            }
        };
        let node = match self.node.upgrade() {
            Ok(node) => node,
            Err(error) => return Box::pin(ready(Err(error.into()))),
        };
        let context = ContextInner { node: NodeHandle::Weak(node.weak()), auth: ContextAuth::Sessions(sessions) };
        let system = node.system.clone();
        let ready = node.wait_ready();

        Box::pin(async move {
            if system.system_epoch().is_none() {
                system.wait_system_ready().await?;
            }

            if let Some(schema) = schema {
                context.ensure_registered(schema).await?;
            } else {
                ready.await?;
            }
            resolve_and_scope(&context, schema, &collection_id, selection)
        })
    }
}

/// Resolve names and literal types from local schema state and enforce this context's read policy.
fn resolve_and_scope<SE, PA>(
    context: &ContextInner<SE, PA>,
    schema: Option<&'static ModelStructDescriptor>,
    collection_id: &CollectionId,
    selection: Selection<Parsed>,
) -> Result<Selection<Resolved>, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let node = context.node.upgrade()?;
    let sessions = match &context.auth {
        ContextAuth::Sessions(sessions) => sessions,
        ContextAuth::Privileged => return Err(RetrievalError::Other("the privileged context does not query".into())),
    };
    let credentials = sessions.current();
    let policy = crate::policy::ReadPolicy::new(&node.policy_agent, &credentials, collection_id);
    policy.check_collection()?;
    let mut selection = match schema {
        Some(schema) => schema.resolve_selection(&node.catalog, node.system.system_epoch(), selection)?,
        None => node.catalog.resolve_selection(collection_id, selection)?,
    };
    selection.predicate = policy.filter_predicate(selection.predicate)?;
    Ok(selection)
}
