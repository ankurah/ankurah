// #[cfg(feature = "react")]
use ankurah_core::WasmSignal;
use wasm_bindgen::prelude::*;

use ankurah_proto as proto;

// #[cfg_attr(feature = "react", derive(WasmSignal))]
#[derive(Debug, Clone, PartialEq, strum::Display)]
pub enum ConnectionState {
    None,
    Connecting { url: String },
    Connected { url: String, presence: proto::Presence },
    Closed,
    Error { message: String },
}

// TODO make a WasmEnum macro to generate this:

impl From<ConnectionState> for ConnectionStateEnum {
    fn from(val: ConnectionState) -> Self {
        ConnectionStateEnum(val)
    }
}
impl std::ops::Deref for ConnectionStateEnum {
    type Target = ConnectionState;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[wasm_bindgen]
#[derive(WasmSignal, Debug, Clone, PartialEq)]
pub struct ConnectionStateEnum(ConnectionState);

#[wasm_bindgen]
impl ConnectionStateEnum {
    pub fn value(&self) -> String {
        match self.0 {
            ConnectionState::None { .. } => "None",
            ConnectionState::Connecting { .. } => "Connecting",
            ConnectionState::Connected { .. } => "Connected",
            ConnectionState::Closed => "Closed",
            ConnectionState::Error { .. } => "Error",
        }
        .to_string()
    }
    //     // pub fn url(&self) -> Option<String> {
    //     //     match &self.0 {
    //     //         ConnectionState::Connecting { url } => Some(url.clone()),
    //     //         ConnectionState::Connected { url, .. } => Some(url.clone()),
    //     //         _ => None,
    //     //     }
    //     // }
    //     // pub fn node_id(&self) -> Option<proto::NodeId> {
    //     //     match &self.0 {
    //     //         ConnectionState::Connected { node_id, .. } => Some(node_id.clone()),
    //     //         _ => None,
    //     //     }
    //     // }
}
