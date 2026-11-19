//! Open an exact Selection and reconcile its tree before serving it.

use super::{DigestTree, TreeManager};
use crate::entity::TemporaryEntity;
use crate::error::RetrievalError;
use crate::selection::filter::Filterable;
use crate::storage::{StateSnapshot, StorageEngine};
use crate::sync::{EngineAdapter, EntityHead, EntityMemberId, TreeProperty};
use ankql::ast::{OrderDirection, Resolved, Selection};
use ankurah_core_types::key_spec::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah_core_types::{PropertyId, ValueType};
use ankurah_digest_tree::{DigestGroup, DigestTreeView};

impl<SE: StorageEngine + 'static, G: DigestGroup> TreeManager<SE, G> {
    /// Canonical catalog writes can bypass ordinary immutable registration.
    /// Never publish members encoded under a type different from this snapshot.
    pub(super) async fn validate_order_types(
        &self,
        tree: &DigestTree<SE, G>,
        snapshot: &mut dyn StateSnapshot,
    ) -> Result<(), RetrievalError> {
        use ankurah_proto::{ModelId, SystemModel, SystemProperty};
        let parts: Vec<_> = tree
            .key_spec()
            .keyparts
            .iter()
            // Subpaths always encode their extracted JSON value. The base
            // property may hold JSON as text or bytes without changing that key.
            .filter(|part| part.sub_path.is_none())
            .filter_map(|part| match part.key.0 {
                PropertyId::EntityId(id) => Some((id, part)),
                _ => None,
            })
            .collect();
        if parts.is_empty() {
            return Ok(());
        }
        let states = snapshot.get_states(&parts.iter().map(|(id, _)| *id).collect::<Vec<_>>()).await?;
        if states.len() != parts.len() {
            return Err(crate::sync::Error::AnswerMisfit.into());
        }
        for ((id, part), state) in parts.into_iter().zip(states) {
            // A local descriptor may be bound before its catalog row is stored.
            let Some(state) = state else { continue };
            let entity = TemporaryEntity::new(id, &state.payload.state)?;
            let value_type = entity.value(&PropertyId::System(SystemProperty::ValueType));
            let valid = state.payload.state.memberships.contains(&ModelId::System(SystemModel::Property))
                && matches!(value_type, Some(crate::value::Value::String(name)) if ValueType::from_property_str(&name) == Some(part.value_type));
            if !valid {
                return Err(RetrievalError::InvalidState(format!(
                    "ordering property {id} changed type; its existing tree cannot be continued"
                )));
            }
        }
        Ok(())
    }

    /// Order an independent fixture through the production member/key mapping.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) async fn fixture_order(
        &self,
        selection: &Selection<Resolved>,
        states: &[ankurah_proto::Attested<ankurah_proto::EntityState>],
    ) -> Result<Vec<ankurah_proto::EntityId>, RetrievalError> {
        use ankurah_digest_tree::MemoryStore;
        use futures::TryStreamExt;
        let entities = states
            .iter()
            .map(|state| TemporaryEntity::new(state.payload.entity_id, &state.payload.state))
            .collect::<Result<Vec<_>, _>>()?;
        let tree = ankurah_digest_tree::DigestTree::<_, _, _, G, _>::create(
            MemoryStore::new(),
            self.key_spec(selection).await?,
            entities.iter().map(crate::sync::member::EntityMember),
        )
        .await
        .map_err(crate::sync::error::open_failure)?;
        tree.view()
            .range(..)
            .map_err(crate::sync::error::open_failure)?
            .map_ok(|leaf| leaf.id.0)
            .try_collect()
            .await
            .map_err(crate::sync::error::open_failure)
    }

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
        if let Some(value_type) = crate::selection::ordering::property_value_type(&*self.storage, property).await? {
            return Ok(value_type);
        }
        let PropertyId::EntityId(id) = property else { unreachable!("system property types are known") };
        self.catalog.upgrade().and_then(|catalog| catalog.bound_value_type(&id)).ok_or(RetrievalError::EntityNotFound(id))
    }
}
