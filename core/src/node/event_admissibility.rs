//! What the commit paths admit for emission into the attested event stream.

use crate::internal::prelude::*;

/// An event refused by the commit paths' shared rules.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InadmissibleEvent {
    #[error("collection '{0}' is protected and only writable by the node's privileged context")]
    ProtectedCollection(CollectionId),
    #[error("membership changes after an entity's first event are not admissible")]
    MembershipAfterGenesis,
    #[error("an entity's first event must add exactly one membership, found {found}")]
    GenesisMembershipCount { found: usize },
    #[error("membership asserts model {asserted} but collection '{collection}' resolves to model {expected}")]
    MembershipModelMismatch { asserted: proto::ModelId, collection: CollectionId, expected: proto::ModelId },
    #[error("membership asserts model {asserted} but collection '{collection}' has no registered model")]
    MembershipUnresolvedCollection { asserted: proto::ModelId, collection: CollectionId },
    #[error("catalog lookup for collection '{collection}' failed: {message}")]
    CatalogLookup { collection: CollectionId, message: String },
}

/// Refuse writes to reserved collections from ordinary contexts and peers.
pub(crate) fn check_unprivileged_write(collection: &CollectionId) -> Result<(), InadmissibleEvent> {
    if crate::schema::is_reserved_collection(collection) {
        return Err(InadmissibleEvent::ProtectedCollection(collection.clone()));
    }
    Ok(())
}

/// An entity's first event must add exactly one membership, naming the model
/// its collection resolves to; later events may add no memberships at all.
pub(crate) fn check_membership<SE, PA>(
    node: &Node<SE, PA>,
    schema: Option<&'static ModelStructDescriptor>,
    event: &proto::Event,
) -> Result<(), InadmissibleEvent>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut memberships = event.operations().memberships().map(|proto::Membership::Add(model)| *model);

    if !event.is_entity_create() {
        return match memberships.next() {
            None => Ok(()),
            Some(_) => Err(InadmissibleEvent::MembershipAfterGenesis),
        };
    }

    let (Some(model), None) = (memberships.next(), memberships.next()) else {
        return Err(InadmissibleEvent::GenesisMembershipCount { found: event.operations().memberships().count() });
    };

    let expected = crate::schema::system_model_id(event.collection.as_str()).or_else(|| {
        let declared = schema.filter(|schema| schema.label == event.collection.as_str())?;
        declared.resolved.get(node.system.system_epoch()?)
    });
    let expected = match expected {
        Some(model) => Some(model),
        None => node
            .catalog
            .model_id_for(event.collection.as_str())
            .map_err(|error| InadmissibleEvent::CatalogLookup { collection: event.collection.clone(), message: error.to_string() })?,
    };

    match expected {
        Some(expected) if expected == model => Ok(()),
        Some(expected) => {
            Err(InadmissibleEvent::MembershipModelMismatch { asserted: model, collection: event.collection.clone(), expected })
        }
        None => Err(InadmissibleEvent::MembershipUnresolvedCollection { asserted: model, collection: event.collection.clone() }),
    }
}
