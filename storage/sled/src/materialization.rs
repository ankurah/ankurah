use ankql::ast::PropertyId;
use ankurah_core::selection::filter::Filterable;
use ankurah_proto::EntityId;
use ankurah_storage_common::filtering::HasEntityId;

/// Projected property values for one entity, used for filtering and sorting.
/// Contains pre-materialized values extracted from a model tree, keyed by
/// durable [`PropertyId`] (not a physical column) so the in-memory post-filter
/// and sort read by resolved identity exactly like the entity's own evaluation.
#[derive(Debug)]
pub struct ProjectedEntity {
    pub(crate) id: EntityId,
    pub(crate) map: std::collections::BTreeMap<PropertyId, ankurah_core::value::Value>,
}

impl Filterable for ProjectedEntity {
    fn collection(&self) -> &str { "" }

    /// Read by name: the `id` pseudo-property and system properties. A registered
    /// property is addressed by id via [`Filterable::value_by_id`], not here.
    fn value(&self, name: &str) -> Option<ankurah_core::value::Value> {
        if name == "id" {
            return Some(ankurah_core::value::Value::EntityId(self.id));
        }
        let property = ankql::ast::SystemProperty::from_name(name)?;
        self.map.get(&PropertyId::System(property)).cloned()
    }

    /// Read a registered property by its stable id -- the identity form a
    /// resolved `Expr::PropertyPath` evaluates through. A miss is absent
    /// (NULL); there is no fallback to a name.
    fn value_by_id(&self, property_id: EntityId) -> Option<ankurah_core::value::Value> {
        self.map.get(&PropertyId::EntityId(property_id)).cloned()
    }
}

impl HasEntityId for ProjectedEntity {
    fn entity_id(&self) -> EntityId { self.id }
}
