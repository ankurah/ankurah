//! Open an exact Selection and reconcile its tree before serving it.

use super::{DigestTree, TreeManager};
use crate::entity::TemporaryEntity;
use crate::error::RetrievalError;
use crate::schema::resolver::system_property_value_type;
use crate::selection::filter::Filterable;
use crate::storage::StorageEngine;
use crate::sync::{EngineAdapter, EntityHead, EntityMemberId, TreeProperty};
use ankql::ast::{OrderDirection, Resolved, Selection};
use ankurah_core_types::key_spec::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah_core_types::{PropertyId, Value, ValueType};
use ankurah_digest_tree::{DigestGroup, DigestTreeView};
use ankurah_proto::{ModelId, SystemModel, SystemProperty};

impl<SE: StorageEngine + 'static, G: DigestGroup> TreeManager<SE, G> {
    /// Open the exact Selection, including its LIMIT membership, and reconcile
    /// all canonical-state changes since its last completed read boundary.
    pub async fn open(&self, selection: Selection<Resolved>) -> Result<DigestTree<SE, G>, RetrievalError> {
        let _publication = self.publication.lock().await;
        self.open_locked(selection, false).await
    }

    /// Rebuild membership from canonical states. Existing views retain the
    /// shared adapter and observe the completed replacement after publication.
    pub async fn refresh(&self, selection: Selection<Resolved>) -> Result<DigestTree<SE, G>, RetrievalError> {
        let _publication = self.publication.lock().await;
        self.open_locked(selection, true).await
    }

    /// The digest crate's live view; ownership keeps this tree maintained.
    pub async fn view(
        &self,
        selection: Selection<Resolved>,
    ) -> Result<DigestTreeView<TreeProperty, EntityMemberId, EntityHead, G, EngineAdapter<SE, TreeProperty, EntityHead, G>>, RetrievalError>
    {
        Ok(self.open(selection).await?.view())
    }

    pub(super) async fn key_spec(&self, selection: &Selection<Resolved>) -> Result<KeySpec<TreeProperty>, RetrievalError> {
        let mut parts = Vec::new();
        for order in selection.order_by.as_deref().unwrap_or_default() {
            let property = order.path.property_id();
            let sub_path = (!order.path.subpath.is_empty()).then(|| order.path.subpath.clone());
            let value_type = if sub_path.is_some() { ValueType::Json } else { self.value_type(property).await? };
            parts.push(IndexKeyPart {
                key: TreeProperty(property),
                sub_path,
                direction: match order.direction {
                    OrderDirection::Asc => IndexDirection::Asc,
                    OrderDirection::Desc => IndexDirection::Desc,
                },
                value_type,
                nulls: None,
                collation: None,
            });
        }
        Ok(KeySpec::new(parts))
    }

    async fn value_type(&self, property: PropertyId) -> Result<ValueType, RetrievalError> {
        match property {
            PropertyId::Id => Ok(ValueType::EntityId),
            PropertyId::System(property) => Ok(system_property_value_type(property)),
            PropertyId::EntityId(id) => {
                let state = match self.storage.get_state(id).await {
                    Ok(state) => state,
                    Err(RetrievalError::EntityNotFound(_)) => {
                        return self
                            .catalog
                            .upgrade()
                            .and_then(|catalog| catalog.bound_value_type(&id))
                            .ok_or(RetrievalError::EntityNotFound(id));
                    }
                    Err(error) => return Err(error),
                };
                let model_id = ModelId::System(SystemModel::Property);
                if !state.payload.state.memberships.contains(&model_id) {
                    return Err(RetrievalError::MissingComponent { entity_id: id, model_id });
                }
                let entity = TemporaryEntity::new(id, &state.payload.state)?;
                let value = entity.value(&PropertyId::System(SystemProperty::ValueType));
                match value {
                    Some(Value::String(value)) => ValueType::from_property_str(&value),
                    _ => None,
                }
                .ok_or_else(|| RetrievalError::InvalidState(format!("property {id} has no valid value_type")))
            }
        }
    }
}
