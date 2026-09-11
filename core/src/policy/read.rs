use super::{AccessDenied, PolicyAgent};
use crate::util::Iterable;
use ankql::ast::{Predicate, Resolved};
use ankurah_proto::{Attested, CollectionId, EntityId, Event, State};

/// Read checks for one collection, with catalog exemptions applied consistently.
pub(crate) struct ReadPolicy<'a, PA, C> {
    agent: Option<(&'a PA, &'a C)>,
    collection: &'a CollectionId,
}

impl<'a, PA: PolicyAgent, C: Iterable<PA::ContextData>> ReadPolicy<'a, PA, C> {
    pub(crate) fn new(agent: &'a PA, credentials: &'a C, collection: &'a CollectionId) -> Self {
        Self { agent: (!crate::schema::reads_bypass_policy(collection)).then_some((agent, credentials)), collection }
    }

    /// Local privileged contexts bypass read policy, independently of the collection.
    pub(crate) fn privileged(collection: &'a CollectionId) -> Self { Self { agent: None, collection } }

    pub(crate) fn check_collection(&self) -> Result<(), AccessDenied> {
        match self.agent {
            Some((agent, credentials)) => agent.can_access_collection(credentials, self.collection),
            None => Ok(()),
        }
    }

    pub(crate) fn filter_predicate(&self, predicate: Predicate<Resolved>) -> Result<Predicate<Resolved>, AccessDenied> {
        match self.agent {
            Some((agent, credentials)) => agent.filter_predicate(credentials, self.collection, predicate),
            None => Ok(predicate),
        }
    }

    pub(crate) fn check_read(&self, id: &EntityId, state: &State) -> Result<(), AccessDenied> {
        match self.agent {
            Some((agent, credentials)) => agent.check_read(credentials, id, self.collection, state),
            None => Ok(()),
        }
    }

    /// The collection's exemption cannot authorize an event from another collection.
    pub(crate) fn check_read_event(&self, event: &Attested<Event>) -> Result<(), AccessDenied> {
        if event.payload.collection != *self.collection {
            return Err(AccessDenied::CollectionDenied(event.payload.collection.clone()));
        }
        match self.agent {
            Some((agent, credentials)) => agent.check_read_event(credentials, event),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{PermissiveAgent, DEFAULT_CONTEXT};

    #[test]
    fn catalog_exemption_does_not_authorize_another_collections_event() {
        let collection = CollectionId::fixed_name(crate::schema::MODEL_COLLECTION_ID);
        let agent = PermissiveAgent::new();
        let policy = ReadPolicy::new(&agent, &DEFAULT_CONTEXT, &collection);
        let event = Event::genesis(collection.clone(), None, ankurah_proto::AuthorId::Unknown, Default::default());
        let mut event = Attested::opt(event, None);
        assert!(policy.check_read_event(&event).is_ok());
        event.payload.collection = "private".into();
        assert!(matches!(policy.check_read_event(&event), Err(AccessDenied::CollectionDenied(id)) if id.as_str() == "private"));
    }
}
