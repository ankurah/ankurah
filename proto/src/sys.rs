use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Item {
    /// The genesis clock for the system - this serves as the root of the clock tree for all entities in the system
    SysRoot,
    Collection {
        name: String,
    },
    #[serde(other)]
    Other,
}
