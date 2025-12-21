use ankurah::Model;
use serde::{Deserialize, Serialize};

// liaison id=model
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    #[active_type(YrsString)]
    pub name: String,
    pub artist: String,
    pub year: i32,
}
// liaison end
