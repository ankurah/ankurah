// #[cfg(feature = "react")]
use ankurah::WasmSignal;
use wasm_bindgen::prelude::*;

use ankurah_proto as proto;

// #[cfg_attr(feature = "react", derive(WasmSignal))]
#[derive(Debug, Clone, PartialEq, strum::Display)]
pub enum ConnectionState {
    None,
    Connecting { url: String },
    Connected { url: String, server_presence: proto::Presence },
    Closed,
    Error { message: String },
}

// TODO make a WasmEnum macro to generate this:

impl From<ConnectionState> for ConnectionStateEnum {
    fn from(val: ConnectionState) -> Self { ConnectionStateEnum(val) }
}
impl std::ops::Deref for ConnectionStateEnum {
    type Target = ConnectionState;
    fn deref(&self) -> &Self::Target { &self.0 }
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
}
