use crate::internal::prelude::*;
use crate::schema::catalog::register::RegistrationAuth;
use crate::schema::registration::RegistrationError;
use ankql::ast::{Parsed, Resolved, Selection};
use ankurah_signals::Wait;
use async_trait::async_trait;
use futures::future::{ready, BoxFuture};

use super::{ContextAuth, ContextInner};

/// Context-scoped descriptor binding and selection resolution.
#[async_trait]
pub(crate) trait SchemaResolver: Send + Sync {
    /// Bind the descriptor's model and property IDs, registering missing declarations if needed.
    /// Preflight its policy, then return the model ID and the epoch the binding belongs to.
    async fn ensure_registered(&self, schema: &'static ModelStructDescriptor) -> Result<(proto::ModelId, NodeEpoch), RegistrationError>;

    /// Bind every component of a query as `ensure_registered` binds one, registering missing declarations.
    async fn ensure_components_registered(&self, components: &[&'static ModelStructDescriptor]) -> Result<(), RegistrationError> {
        for schema in components {
            self.ensure_registered(schema).await?;
        }
        Ok(())
    }

    /// Check entity ownership and bind the given descriptor locally, without registration or network access.
    /// Used by synchronous `Transaction::edit`.
    fn bind_descriptor_local(&self, schema: &'static ModelStructDescriptor, entity: &Entity) -> Result<ModelId, RetrievalError>;

    /// Resolve names and literal types against a query's components for `Context::fetch` after schema registration.
    /// Read-policy filtering is applied separately by `fetch_entities`.
    fn resolve_selection_with_descriptors(
        &self,
        components: &[&'static ModelStructDescriptor],
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError>;

    /// Resolve and policy-filter a livequery's initial or updated selection locally.
    /// Missing bindings/readiness return errors; the caller decides whether to resolve asynchronously.
    fn resolve_query_selection(
        &self,
        components: &[&'static ModelStructDescriptor],
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError>;

    /// Wait for the required catalog state, register missing declarations, and resolve with read policy.
    /// The returned future owns a weak context; the caller controls execution and cancellation.
    fn resolve_query_selection_when_ready(
        &self,
        components: Vec<&'static ModelStructDescriptor>,
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
    /// Preflight its policy, then return the model ID and the epoch the binding belongs to.
    async fn ensure_registered(&self, schema: &'static ModelStructDescriptor) -> Result<(proto::ModelId, NodeEpoch), RegistrationError> {
        let node = self.node.upgrade()?;
        node.lifecycle
            .while_running(async {
                if let Some(system) = schema.system {
                    return Ok((proto::ModelId::System(system), node.entities.node_epoch()));
                }
                let epoch = node.entities.node_epoch();
                if schema.resolved.get(epoch).is_err() {
                    let mut registrant = schema.registrant(epoch);
                    let auth = match &self.auth {
                        ContextAuth::Sessions(sessions) => match sessions.write_credential() {
                            Ok(credential) => RegistrationAuth::Credential(credential),
                            Err(error) => RegistrationAuth::Unavailable(error),
                        },
                        ContextAuth::Privileged => RegistrationAuth::Privileged,
                    };
                    node.catalog.resolve_or_register(node.as_ref(), &mut registrant, auth).await?;
                }
                let model = schema.resolved.get(epoch).map_err(|_| {
                    RegistrationError::Retrieval(RetrievalError::Other(format!(
                        "binding of '{}' did not retain its exact model identity",
                        schema.label
                    )))
                })?;
                if !matches!(&self.auth, ContextAuth::Privileged) {
                    node.policy_agent.preflight(node.as_ref(), model).await?;
                }
                Ok((model, epoch))
            })
            .await
    }

    /// Check entity ownership and bind the given descriptor locally, without registration or network access.
    /// Used by synchronous `Transaction::edit`.
    fn bind_descriptor_local(&self, schema: &'static ModelStructDescriptor, entity: &Entity) -> Result<ModelId, RetrievalError> {
        let node = self.node.upgrade()?;
        let epoch = node.entities.node_epoch();
        entity.check_epoch(epoch)?;
        let model = schema.bind_local(&node.catalog, epoch)?;
        if !entity.has_membership(&model) {
            return Err(RetrievalError::MissingComponent { entity_id: entity.id(), model_id: model });
        }
        Ok(model)
    }

    /// Resolve names and literal types against a query's components for `Context::fetch` after schema registration.
    /// Read-policy filtering is applied separately by `fetch_entities`.
    fn resolve_selection_with_descriptors(
        &self,
        components: &[&'static ModelStructDescriptor],
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError> {
        let node = self.node.upgrade()?;
        crate::schema::resolver::resolve_component_selection(components, &node.catalog, node.entities.node_epoch(), selection)
    }

    /// Resolve and policy-filter a livequery's initial or updated selection locally.
    /// Missing bindings/readiness return errors; the caller decides whether to resolve asynchronously.
    fn resolve_query_selection(
        &self,
        components: &[&'static ModelStructDescriptor],
        selection: Selection<Parsed>,
    ) -> Result<Selection<Resolved>, RetrievalError> {
        let node = self.node.upgrade()?;
        node.lifecycle.check_not_halted()?;
        crate::schema::resolver::validate_selection_names(components, &selection)?;
        if components.iter().any(|schema| schema.system.is_none()) {
            if !node.catalog.is_ready() {
                return Err(RetrievalError::NodeNotReady);
            }
        }
        resolve_and_scope(self, components, selection)
    }

    /// Wait for the required catalog state, register missing declarations, and resolve with read policy.
    /// The returned future owns a weak context; the caller controls execution and cancellation.
    fn resolve_query_selection_when_ready(
        &self,
        components: Vec<&'static ModelStructDescriptor>,
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
        let catalog_ready = node.catalog.wait_ready();
        let lifecycle = node.lifecycle.clone();
        let run = node.run.clone();

        Box::pin(async move {
            // The catalog may need a peer indefinitely. Hold only its readiness
            // signal until it is usable, then upgrade the weak context to register.
            // Builtin models bootstrap that catalog and must not wait for it.
            if components.iter().any(|schema| schema.system.is_none()) {
                tokio::select! {
                    biased;
                    _ = run.wait_value(false) => {
                        lifecycle.check_not_halted()?;
                        return Err(crate::error::NodeDropped.into());
                    }
                    () = catalog_ready => {}
                }
            }
            context.ensure_components_registered(&components).await?;
            resolve_and_scope(&context, &components, selection)
        })
    }
}

/// Resolve names and literal types from local schema state and enforce this context's read policy.
fn resolve_and_scope<SE, PA>(
    context: &ContextInner<SE, PA>,
    components: &[&'static ModelStructDescriptor],
    selection: Selection<Parsed>,
) -> Result<Selection<Resolved>, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let node = context.node.upgrade()?;
    if matches!(&context.auth, ContextAuth::Privileged) {
        return Err(RetrievalError::Other("the privileged context does not query".into()));
    }
    let mut selection = context.resolve_selection_with_descriptors(components, selection)?;
    let policy = crate::policy::ContextPolicy::new(&node.policy_agent, context.auth.clone());
    selection.predicate = policy.filter_predicate(selection.predicate)?;
    Ok(selection)
}
