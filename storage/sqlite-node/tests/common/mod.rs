#[allow(unused)]
pub use ankurah::{policy::DEFAULT_CONTEXT, Context, EntityId, Model, Node, PermissiveAgent};
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}
