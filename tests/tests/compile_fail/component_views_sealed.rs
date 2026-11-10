//! Only core implements `ComponentViews`: a view, or a tuple of views. A caller's own
//! type cannot be what a typed query returns.

use ankurah::core::schema::ModelStructDescriptor;
use ankurah::entity::Entity;
use ankurah::error::RetrievalError;
use ankurah::model::ComponentViews;

pub struct NotAView;

impl ComponentViews for NotAView {
    fn components() -> Vec<&'static ModelStructDescriptor> { Vec::new() }

    fn views_of(_: Entity) -> Result<Self, RetrievalError> { Ok(NotAView) }
}

fn main() {}
