use ankurah_core::property::value::YrsString;
use ankurah_core::Model;
use serde::{Deserialize, Serialize};

pub struct Album {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Session {
    #[active_value(YrsString)]
    pub date_connected: String,
    #[active_value(YrsString)]
    pub ip_address: String,
    #[active_value(YrsString)]
    pub node_id: String,
}
