use super::{AccessDenied, PolicyAgent};
use crate::context::ContextAuth;
use crate::entity::{Entity, TemporaryEntity};
use crate::error::{InadmissibleEvent, MutationError};
use crate::node::Node;
use crate::selection::filter::evaluate_predicate;
use crate::storage::StorageEngine;
use crate::util::Iterable;
use ankql::ast::{Predicate, Resolved};
use ankurah_proto::{Attestation, Attested, EntityId, EntityState, Event, GetResult, Membership, ModelId, State};
use ankurah_signals::{ListenerGuard, Mut, Peek, Signal};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

/// The node's `PolicyAgent`, bound to one context's authority for one operation.
///
/// Core asks it every access question rather than calling the agent directly: which entities a query
/// may return (`filter_predicate`), whether a fetched or received entity or event may be read
/// (`check_read`, `check_read_event`), and whether a write or an admitted event is allowed
/// (`check_write`, `check_write_event`).
///
/// A sessions-backed context answers with its sessions' current credentials; a privileged context
/// bypasses policy. Build one per operation: its cached known-ID read predicate refreshes when the
/// sessions change, not when policy does.
pub struct ContextPolicy<'a, PA, C: Signal> {
    agent: &'a PA,
    auth: ContextAuth<C>,
    cache_revision: Arc<AtomicUsize>,
    retrieval: Mutex<Option<CachedPredicate>>,
    _session_listener: Option<ListenerGuard>,
}

struct CachedPredicate {
    revision: usize,
    predicate: Arc<Predicate<Resolved>>,
}

impl<'a, PA: PolicyAgent, C: Signal + Peek<Vec<PA::ContextData>>> ContextPolicy<'a, PA, C> {
    pub(crate) fn new(agent: &'a PA, auth: ContextAuth<C>) -> Self {
        let cache_revision = Arc::new(AtomicUsize::new(0));
        let listener = match &auth {
            ContextAuth::Sessions(sessions) => {
                let revision = cache_revision.clone();
                Some(sessions.listen(Arc::new(move |_| {
                    revision.fetch_add(1, Ordering::AcqRel);
                })))
            }
            ContextAuth::Privileged => None,
        };
        Self { agent, auth, cache_revision, retrieval: Mutex::new(None), _session_listener: listener }
    }

    /// Apply policy to an owned or borrowed session source, without granting privileged authority.
    pub fn for_sessions(agent: &'a PA, sessions: C) -> Self { Self::new(agent, ContextAuth::Sessions(sessions)) }

    /// Current session credentials for policy checks and outgoing requests.
    pub(crate) fn credentials(&self) -> Vec<PA::ContextData> {
        match &self.auth {
            ContextAuth::Sessions(sessions) => sessions.peek(),
            ContextAuth::Privileged => Vec::new(),
        }
    }

    /// Restrict a query to entities this context may discover.
    pub fn filter_predicate(&self, predicate: Predicate<Resolved>) -> Result<Predicate<Resolved>, AccessDenied> {
        if crate::schema::is_catalog_read(&predicate) {
            return Ok(predicate);
        }
        let allowed = match &self.auth {
            ContextAuth::Sessions(_) => self.agent.query_predicate(&self.credentials())?,
            ContextAuth::Privileged => Predicate::True,
        };
        Ok(if allowed == Predicate::True { predicate } else { Predicate::And(Box::new(predicate), Box::new(allowed)) })
    }

    /// Visibility for known-ID reads, including catalog bootstrap reads.
    /// Reuse the predicate until sessions change; a rejected policy contributes no grant.
    pub fn retrieval_predicate(&self) -> Arc<Predicate<Resolved>> {
        let mut cached = self.retrieval.lock().unwrap();
        loop {
            let revision = self.cache_revision.load(Ordering::Acquire);
            if let Some(cached) = cached.as_ref().filter(|cached| cached.revision == revision) {
                return cached.predicate.clone();
            }
            let allowed = match &self.auth {
                ContextAuth::Sessions(_) => self.agent.retrieval_predicate(&self.credentials()).unwrap_or(Predicate::False),
                ContextAuth::Privileged => Predicate::True,
            };
            let predicate = if allowed == Predicate::True {
                allowed
            } else {
                crate::schema::CATALOG_MODELS.into_iter().map(Predicate::MemberOf).fold(allowed, |allowed, catalog| {
                    if allowed == Predicate::False { catalog } else { Predicate::Or(Box::new(allowed), Box::new(catalog)) }
                })
            };
            // Do not cache a computation whose credentials changed while it ran.
            if self.cache_revision.load(Ordering::Acquire) != revision {
                continue;
            }
            let predicate = Arc::new(predicate);
            *cached = Some(CachedPredicate { revision, predicate: predicate.clone() });
            return predicate;
        }
    }

    /// Check a resident already admitted by the query predicate; privileged reads need no state inspection.
    pub(crate) fn can_read(&self, entity: &Entity) -> bool {
        matches!(&self.auth, ContextAuth::Privileged)
            || entity.to_state().ok().is_some_and(|state| self.check_read_state(&entity.id(), &state).is_ok())
    }

    /// Apply the retrieval predicate and additional read checks to an entity already available locally.
    pub fn check_read(&self, id: &EntityId, state: &State) -> Result<(), AccessDenied> {
        if state.memberships.iter().any(crate::schema::reads_bypass_policy) {
            return Ok(());
        }
        let predicate = self.retrieval_predicate();
        if *predicate != Predicate::True {
            let entity = TemporaryEntity::new(*id, state)
                .map_err(|_| AccessDenied::ByPolicy("Read predicate entity state could not be evaluated"))?;
            match evaluate_predicate(&entity, &predicate) {
                Ok(true) => {}
                Ok(false) => return Err(AccessDenied::ByPolicy("Entity is outside the retrieval predicate")),
                Err(_) => return Err(AccessDenied::ByPolicy("Read predicate could not be evaluated")),
            }
        }
        self.check_read_state(id, state)
    }

    /// Additional state checks after the query or retrieval predicate has passed.
    pub(crate) fn check_read_state(&self, id: &EntityId, state: &State) -> Result<(), AccessDenied> {
        if state.memberships.iter().any(crate::schema::reads_bypass_policy) {
            return Ok(());
        }
        if matches!(&self.auth, ContextAuth::Sessions(_)) {
            if let Some(error) = self.agent.check_reads(&self.credentials(), &[(id, state)]).remove(id) {
                return Err(error);
            }
        }
        Ok(())
    }

    /// Apply additional checks to a batch that has already passed the storage predicate.
    pub(crate) fn check_reads(&self, results: &mut [GetResult]) {
        if matches!(&self.auth, ContextAuth::Privileged) { return; }
        let states: Vec<_> = results.iter().filter_map(|result| {
            let GetResult::Found(state) = result else { return None };
            (!state.payload.state.memberships.iter().any(crate::schema::reads_bypass_policy))
                .then_some((&state.payload.entity_id, &state.payload.state))
        }).collect();
        let denied = self.agent.check_reads(&self.credentials(), &states);
        for result in results {
            if let GetResult::Found(state) = result {
                if denied.contains_key(&state.payload.entity_id)
                    && !state.payload.state.memberships.iter().any(crate::schema::reads_bypass_policy)
                {
                    *result = GetResult::AccessDenied(state.payload.entity_id);
                }
            }
        }
    }

    /// Require entity visibility and any additional event-specific permission.
    pub fn check_read_event(&self, event: &Attested<Event>, state: &State) -> Result<(), AccessDenied> {
        if state.memberships.iter().any(crate::schema::reads_bypass_policy) {
            return Ok(());
        }
        self.check_read(&event.payload.entity_id, state)?;
        match &self.auth {
            ContextAuth::Sessions(_) => self.agent.check_read_event(&self.credentials(), event),
            ContextAuth::Privileged => Ok(()),
        }
    }

    fn check_models(&self, models: impl IntoIterator<Item = ModelId>) -> Result<(), InadmissibleEvent> {
        if matches!(&self.auth, ContextAuth::Sessions(_)) {
            for model in models {
                if crate::schema::is_reserved_model(&model) {
                    return Err(InadmissibleEvent::ProtectedModel(model));
                }
            }
        }
        Ok(())
    }

    fn write_credential(&self) -> Result<PA::ContextData, AccessDenied> {
        let mut credentials = self.credentials().into_iter();
        match (credentials.next(), credentials.next()) {
            (Some(credential), None) => Ok(credential),
            (None, _) => Err(AccessDenied::ByPolicy("write operations require a session; this context's source has none")),
            _ => Err(AccessDenied::ByPolicy("write operations act as one principal; this context's source has several sessions")),
        }
    }

    /// Authorize creating or editing an entity, before transaction commit.
    pub(crate) fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> {
        self.check_models(entity.memberships())
            .map_err(|_| AccessDenied::ByPolicy("reserved models accept writes only from the node's privileged context"))?;
        match &self.auth {
            ContextAuth::Sessions(_) => self.agent.check_write(&self.write_credential()?, entity, None),
            ContextAuth::Privileged => Ok(()),
        }
    }

    /// Authorize an event together with the original and resulting entity states.
    pub(crate) fn check_write_event<SE: StorageEngine>(
        &self,
        node: &Node<SE, PA>,
        before: &Entity,
        after: &Entity,
        event: &Event,
    ) -> Result<Option<Attestation>, MutationError> {
        self.check_models(event.operations().memberships().map(|Membership::Add(model)| *model))?;
        self.check_models(after.memberships())?;
        match &self.auth {
            ContextAuth::Sessions(_) => Ok(self.agent.check_write_event(node, &self.write_credential()?, before, after, event)?),
            ContextAuth::Privileged => Ok(None),
        }
    }

    pub(crate) fn attest_state<SE: StorageEngine>(&self, node: &Node<SE, PA>, state: &EntityState) -> Option<Attestation> {
        match &self.auth {
            ContextAuth::Sessions(_) => self.agent.attest_state(node, state),
            ContextAuth::Privileged => None,
        }
    }
}

impl<'a, PA: PolicyAgent> ContextPolicy<'a, PA, Mut<Vec<PA::ContextData>>> {
    /// Apply policy to fixed credentials, such as those authenticated from an incoming request.
    pub fn from_credentials(agent: &'a PA, credentials: &impl Iterable<PA::ContextData>) -> Self {
        Self::for_sessions(agent, Mut::new(credentials.iterable().cloned().collect()))
    }
}
