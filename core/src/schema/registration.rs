//! Typed failures and policy-visible plans for schema registration.
//! Registration upserts one model and its nested properties; canonical
//! `(backend, value_type)` pairs are immutable.

use crate::internal::prelude::*;
use ankurah_proto::EntityId;

use crate::property::PropertyError;
use crate::schema::catalog::{SysModelRow, SysPropertyRow};

/// A durable schema-registration request could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    /// Registration was attempted on a node without durable storage.
    #[error("registration executes on durable nodes; this node is ephemeral")]
    NotDurable,
    /// The node has not established a system root or catalog epoch.
    #[error("system is not ready")]
    SystemNotReady,
    /// Strict offline registration could not consult an allocator.
    #[error(
        "collection '{0}' has never been registered on this node and no durable peer is connected; \
         connect to the system once first"
    )]
    NoDurablePeer(
        /// The source-level collection label that could not be registered.
        String,
    ),
    /// A known model's complete compiled shape could not be confirmed offline.
    #[error(
        "collection '{0}' is registered, but this binary's schema for it is unconfirmed \
         and no durable peer is connected; connect to the system once first"
    )]
    UnconfirmedSchema(
        /// The source-level collection label whose binding is unconfirmed.
        String,
    ),
    /// A descriptor attempted to claim the protected system namespace.
    #[error(
        "collection '{0}' starts with the reserved prefix '{prefix}'; the system and catalog collections are never registered through this executor",
        prefix = crate::schema::RESERVED_COLLECTION_PREFIX
    )]
    ReservedCollection(
        /// The rejected source-level collection label.
        String,
    ),
    /// An explicit property binding named an unknown catalog entity.
    #[error("explicit property id {property} does not exist in the catalog; explicit binding never mints")]
    ExplicitIdNotFound {
        /// The requested property entity.
        property: EntityId,
    },
    /// An explicit model binding named an unknown catalog entity.
    #[error("explicit model id {model} does not exist in the catalog; explicit binding never mints")]
    ExplicitModelIdNotFound {
        /// The requested model entity.
        model: EntityId,
    },
    /// An explicit model binding conflicts with the catalog's model label.
    #[error("explicit model id {model} is bound to label '{found_label}'; binder declares '{label}'")]
    ExplicitModelIdMismatch {
        /// The explicitly bound model entity.
        model: EntityId,
        /// The model label already stored in the catalog.
        found_label: String,
        /// The model label declared by the requester.
        label: String,
    },
    /// A declaration conflicts with an existing property's canonical backend or type.
    #[error(
        "property '{name}' in '{collection}' is canonically ({found_backend}, {found_value_type}); this binary declares ({backend}, {value_type}). Backend and value type must match exactly; changing either requires a migration"
    )]
    IncompatibleProperty {
        /// The property's minting-model collection label.
        collection: String,
        /// The property's registered name.
        name: String,
        /// The canonical backend already stored in the catalog.
        found_backend: String,
        /// The canonical value type already stored in the catalog.
        found_value_type: String,
        /// The backend declared by the requester.
        backend: String,
        /// The value type declared by the requester.
        value_type: String,
    },
    /// Two entries in one request assign different semantics to one name.
    #[error("property '{name}' is declared more than once with conflicting metadata in '{collection}'")]
    ConflictingDuplicateProperty {
        /// The containing model label.
        collection: String,
        /// The duplicated property name.
        name: String,
    },
    #[error("property '{name}' is already bound to another identity in '{collection}'")]
    PropertyNameTaken { collection: String, name: String },
    /// Policy denied the plan, or the context had no single write principal.
    #[error("registration of model '{collection}' was refused by policy: {source}")]
    PolicyDenied {
        /// The source-level model label being registered.
        collection: String,
        /// The policy or credential-source refusal.
        source: AccessDenied,
    },
    /// Committing the catalog transaction failed.
    #[error(transparent)]
    Mutation(
        /// The durable mutation failure.
        #[from]
        MutationError,
    ),
    /// Reading catalog or storage state failed.
    #[error(transparent)]
    Retrieval(
        /// The underlying catalog or storage read failure.
        #[from]
        RetrievalError,
    ),
}

impl From<crate::error::NodeHaltReason> for RegistrationError {
    fn from(reason: crate::error::NodeHaltReason) -> Self { Self::Retrieval(reason.into()) }
}

impl From<crate::error::NodeDropped> for RegistrationError {
    fn from(error: crate::error::NodeDropped) -> Self { Self::Retrieval(error.into()) }
}

impl From<crate::error::NodeReadinessError> for RegistrationError {
    fn from(error: crate::error::NodeReadinessError) -> Self { Self::Retrieval(error.into()) }
}

/// Preserve an underlying retrieval failure without double-wrapping it.
impl From<RegistrationError> for crate::error::RetrievalError {
    fn from(error: RegistrationError) -> Self {
        match error {
            RegistrationError::Retrieval(inner) => inner,
            other => crate::error::RetrievalError::Other(other.to_string()),
        }
    }
}

/// Preserve registration failures as mutation sources on write paths.
impl From<RegistrationError> for crate::error::MutationError {
    fn from(error: RegistrationError) -> Self { crate::error::MutationError::General(Box::new(error)) }
}

/// A typed accessor's failure is a failure to write the catalog row.
impl From<PropertyError> for RegistrationError {
    fn from(error: PropertyError) -> Self { RegistrationError::Mutation(error.into()) }
}

/// The concrete writes a policy agent evaluates before registration commits.
#[derive(Debug, Default)]
pub struct RegistrationPlan {
    /// Model entities this request will CREATE, with their allocated ids.
    pub creates_models: Vec<(EntityId, SysModelRow)>,
    /// Property entities this request will CREATE, with their allocated ids.
    pub creates_properties: Vec<(EntityId, SysPropertyRow)>,
    /// Model-property memberships this request will CREATE, fully resolved.
    pub creates_memberships: Vec<PlannedModelPropertyMembership>,
    /// Metadata changes to existing catalog entities.
    pub updates: Vec<PlannedUpdate>,
    /// Definitions that resolved to existing entities with no changes:
    /// pure no-ops, listed for context.
    pub existing: Vec<EntityId>,
}

impl RegistrationPlan {
    /// Whether registration can return without consulting policy or committing.
    pub fn is_noop(&self) -> bool {
        self.creates_models.is_empty()
            && self.creates_properties.is_empty()
            && self.creates_memberships.is_empty()
            && self.updates.is_empty()
    }
}

/// A model-property membership creation in a [`RegistrationPlan`], resolved
/// to ids.
#[derive(Debug, Clone)]
pub struct PlannedModelPropertyMembership {
    /// The durable identity assigned to the membership entity.
    pub id: EntityId,
    /// The model receiving the property.
    pub model: EntityId,
    /// The property admitted to the model.
    pub property: EntityId,
    /// Whether entities of the model may omit the property.
    pub optional: bool,
}

/// A metadata follow-up on an existing catalog entity, in a
/// [`RegistrationPlan`].
#[derive(Debug, Clone)]
pub struct PlannedUpdate {
    /// Which catalog collection the entity lives in.
    pub collection: crate::ModelId,
    /// The durable catalog entity to update.
    pub entity: EntityId,
    /// The system field whose metadata value changes.
    pub field: String,
    /// The current catalog value, when one exists.
    pub from: Option<crate::value::Value>,
    /// The requested catalog value. `None` means the field will be cleared.
    pub to: Option<crate::value::Value>,
}
