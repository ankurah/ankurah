//! What the commit paths admit for emission into the attested event stream.

use crate::internal::prelude::*;
use ankurah_proto::EventStructureError;

/// An entity's first event must add exactly one membership, naming the model
/// its collection resolves to; later events may add no memberships at all.
pub(crate) fn check_membership<SE, PA>(
    node: &Node<SE, PA>,
    schema: Option<&'static ModelStructDescriptor>,
    event: &proto::Event,
) -> Result<(), EventStructureError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut memberships = event.operations().memberships().map(|proto::Membership::Add(model)| *model);

    if !event.is_entity_create() {
        return match memberships.next() {
            None => Ok(()),
            Some(_) => Err(EventStructureError::MembershipAfterGenesis),
        };
    }

    let (Some(model), None) = (memberships.next(), memberships.next()) else {
        return Err(EventStructureError::GenesisMembershipCount { found: event.operations().memberships().count() });
    };

    // Authority order: the built-in system mapping, then the declaration's
    // cell (bound by registration before the local catalog syncs), then the
    // catalog. Remote events carry no declaration.
    let expected = crate::schema::system_model_id(event.collection.as_str())
        .or_else(|| {
            let declared = schema.filter(|schema| schema.label == event.collection.as_str())?;
            declared.resolved.get(node.system.schema_epoch()?)
        })
        .or_else(|| node.catalog.model_id_for(event.collection.as_str()));

    match expected {
        Some(expected) if expected == model => Ok(()),
        Some(expected) => {
            Err(EventStructureError::MembershipModelMismatch { asserted: model, collection: event.collection.clone(), expected })
        }
        None => Err(EventStructureError::MembershipUnresolvedCollection { asserted: model, collection: event.collection.clone() }),
    }
}
