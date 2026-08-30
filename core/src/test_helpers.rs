//! Test seams that bypass production machinery. Feature-gated; never
//! compiled into a normal build.

use crate::internal::prelude::*;
use crate::schema::registration::RegistrationError;

/// Bind `schema`'s cells from a hand-built definition under the node's
/// current epoch, bypassing registration and the catalog: deterministic
/// harnesses seed identical ids on every node this way.
pub fn seed_registered_schema<SE, PA>(
    node: &Node<SE, PA>,
    schema: &'static ModelStructDescriptor,
    model: &proto::RegisteredModel,
) -> Result<(), RegistrationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let epoch = node.system.schema_epoch().ok_or(RegistrationError::SystemNotReady)?;
    crate::schema::catalog::register::bind_registered(schema, model, epoch)
}
