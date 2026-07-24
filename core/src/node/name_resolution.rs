//! Node admission and asynchronous readiness around AnkQL name resolution.
//!
//! AnkQL owns traversal of `Selection`, `Predicate`, expressions, and order
//! keys. The node admits the exact compiled schema and, when necessary, waits
//! until absence from the catalog-backed resolver is authoritative.

use crate::policy::PolicyAgent;
use crate::property::PropertyError;
use crate::storage::StorageEngine;
use crate::{ModelId, Node};
use ankql::ast::Selection;

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Resolve one selection after admitting its optional compiled schema.
    ///
    /// AnkQL owns the traversal and reports `ResolverNotReady` only when it
    /// actually encounters an unresolved source name. That lets selections
    /// such as `true`, `id = ...`, and already-resolved wire ASTs proceed on a
    /// cold catalog without starting an otherwise-unnecessary catalog warm.
    pub(crate) async fn resolve_selection_names(
        &self,
        cdata: Option<&PA::ContextData>,
        model: &ModelId,
        schema: Option<&'static crate::schema::ModelSchema>,
        selection: &Selection,
    ) -> Result<Selection, PropertyError> {
        if let Some(schema) = schema {
            if let Some(cdata) = cdata {
                let admitted_model = self.catalog.ensure_schema_for_use(self, cdata, schema).await.map_err(|error| {
                    if self.catalog.model_by_label(schema.collection).is_none() {
                        PropertyError::UnregisteredCollection { collection: schema.collection.to_owned() }
                    } else {
                        PropertyError::RetrievalError(crate::error::RetrievalError::Other(error.to_string()))
                    }
                })?;
                if &admitted_model != model {
                    return Err(PropertyError::RetrievalError(crate::error::RetrievalError::Other(format!(
                        "compiled schema '{}' is bound to {}, not query model {}",
                        schema.name, admitted_model, model
                    ))));
                }
            } else if !self.catalog.has_schema_binding(schema) {
                return Err(PropertyError::UnregisteredCollection { collection: schema.collection.to_owned() });
            }
        }

        let resolver = self.catalog.resolver();
        match selection.resolve_names(model, resolver) {
            Ok(resolved) => return Ok(resolved),
            Err(ankql::NameResolutionError::ResolverNotReady { .. }) => {}
            Err(error) => return Err(error.into()),
        }

        if self.catalog.is_durable() {
            self.catalog.wait_catalog_ready().await;
        } else if let Some(cdata) = cdata {
            self.catalog.ensure_subscribed(cdata.clone(), self).await;
        } else {
            self.catalog.wait_catalog_ready().await;
        }

        selection.resolve_names(model, self.catalog.resolver()).map_err(Into::into)
    }
}
