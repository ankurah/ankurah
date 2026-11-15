//! Find a live tree or rebuild its persisted contents from canonical entities.

use ankql::ast::{OrderDirection, Resolved, Selection};
use ankurah_core_types::key_spec::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah_core_types::{PropertyId, Value, ValueType};
use ankurah_digest_tree::{DigestGroup, DigestTreeView};
use ankurah_proto::{ModelId, SystemModel, SystemProperty};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use super::{selection_key, DigestTree, TreeManager};
use crate::entity::TemporaryEntity;
use crate::error::RetrievalError;
use crate::schema::resolver::system_property_value_type;
use crate::selection::filter::Filterable;
use crate::storage::{StorageEngine, StorageTransaction};
use crate::sync::member::EntityMember;
use crate::sync::{storage_adapter, EngineAdapter, EntityHead, EntityMemberId, TreeId, TreeProperty};

impl<SE: StorageEngine + 'static, G: DigestGroup> TreeManager<SE, G> {
    /// Return the tree for this exact resolved Selection, creating it if needed.
    /// Full predicate membership is maintained; LIMIT belongs to the query.
    ///
    /// A live tree is reused. After its last owner goes away, or after restart,
    /// its persisted contents may have missed entity changes, so they are rebuilt
    /// before the tree is returned. The stable backing id reuses its storage.
    ///
    /// The supplied publication lock covers the entity scan through registration.
    /// All entity writers must coordinate with it as described on [`TreeManager`].
    pub async fn open(&self, selection: Selection<Resolved>) -> Result<DigestTree<SE, G>, RetrievalError> {
        let key = selection_key(&selection)?;
        let _publication = self.publication.lock().await;
        if let Some(tree) = self.tree(&key) {
            return Ok(tree);
        }

        self.rebuild(selection, key, None).await
    }

    /// Rebuild this exact Selection's tree from current canonical entities,
    /// even when it is already resident. This explicitly restores a tree after
    /// a committed maintenance refusal or a storage reset; `open` only reuses
    /// a resident handle and does not detect that it needs recovery.
    ///
    /// An existing incarnation keeps its original shared handle, so its views
    /// and transactions continue to keep it maintained. After a reset or drop,
    /// the new tree replaces it; handles to the old incarnation remain retired.
    /// A failed or cancelled rebuild leaves the resident registration unchanged.
    /// Damaged rows and incompatible stored key specs still fail the rebuild.
    ///
    /// The supplied publication lock covers the scan through registration, as
    /// for `open`. Do not call this while holding that lock or while an entity
    /// transaction whose outcome it should reflect remains uncommitted.
    pub async fn refresh(&self, selection: Selection<Resolved>) -> Result<DigestTree<SE, G>, RetrievalError> {
        let key = selection_key(&selection)?;
        let _publication = self.publication.lock().await;
        let resident = self.tree(&key);
        self.rebuild(selection, key, resident).await
    }

    /// Rebuild with the publication lock held, keeping the resident's shared
    /// lifetime when storage still holds its incarnation.
    async fn rebuild(
        &self,
        selection: Selection<Resolved>,
        key: Vec<u8>,
        resident: Option<DigestTree<SE, G>>,
    ) -> Result<DigestTree<SE, G>, RetrievalError> {
        let key_spec = self.key_spec(&selection).await?;
        let members = self.storage.fetch_states(&Selection { predicate: selection.predicate.clone(), order_by: None, limit: None }).await?;
        let entities = members
            .iter()
            .map(|state| TemporaryEntity::new(state.payload.entity_id, &state.payload.state))
            .collect::<Result<Vec<_>, _>>()?;

        // Version the naming scheme independently of tree generations. Selection
        // identity includes LIMIT, but not display labels; group IDs separate encodings.
        let id = TreeId::new(format!("selection-v1-{}-{}", G::ROW_VERSION, URL_SAFE_NO_PAD.encode(Sha256::digest(&key))));
        let mut trx = self.storage.transaction();
        let created = storage_adapter::rebuild(
            Arc::clone(&self.storage),
            Arc::clone(&self.cache),
            &mut trx,
            id,
            key_spec,
            entities.iter().map(EntityMember),
        )
        .await?;
        trx.commit().await?.committed()?;
        // No await after commit: cancellation cannot leave a returned tree
        // outside the maintenance registry.
        let rebuilt = created.landed();
        let tree = match resident {
            Some(resident) if resident.store().same_tree(rebuilt.store()) => resident,
            _ => rebuilt,
        };
        self.remember(selection, key, tree.clone());
        Ok(tree)
    }

    /// Open the Selection's tree and return the digest crate's read-only view.
    /// The view keeps the tree alive and maintained, including after LRU eviction.
    pub async fn view(
        &self,
        selection: Selection<Resolved>,
    ) -> Result<DigestTreeView<TreeProperty, EntityMemberId, EntityHead, G, EngineAdapter<SE, TreeProperty, EntityHead, G>>, RetrievalError>
    {
        Ok(self.open(selection).await?.view())
    }

    async fn key_spec(&self, selection: &Selection<Resolved>) -> Result<KeySpec<TreeProperty>, RetrievalError> {
        let mut parts = Vec::new();
        for order in selection.order_by.as_deref().unwrap_or_default() {
            if !order.path.subpath.is_empty() {
                return Err(RetrievalError::Other("ORDER BY keys name whole properties; JSON subpaths are not sortable".into()));
            }
            let property = order.path.property_id();
            parts.push(IndexKeyPart {
                key: TreeProperty(property),
                sub_path: None,
                direction: match order.direction {
                    OrderDirection::Asc => IndexDirection::Asc,
                    OrderDirection::Desc => IndexDirection::Desc,
                },
                value_type: self.value_type(property).await?,
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
                let state = self.storage.get_state(id).await?;
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
