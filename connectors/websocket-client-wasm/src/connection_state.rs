use ankurah::WasmSignal;
use wasm_bindgen::prelude::*;

use ankurah_proto as proto;

#[derive(Debug, Clone, PartialEq, strum::Display)]
pub enum ConnectionState {
    None,
    Connecting { url: String },
    Connected { url: String, server_presence: proto::Presence },
    Refused { url: String, reason: String },
    Closed,
    Error { message: String, cause: Option<ankurah_core::connector::PeerConnectionError> },
}

// TODO make a WasmEnum macro to generate this:

impl From<&ConnectionState> for ConnectionStateEnum {
    fn from(val: &ConnectionState) -> Self { ConnectionStateEnum(val.clone()) }
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
    #[wasm_bindgen(getter)]
    pub fn error_message(&self) -> Option<String> {
        match &self.0 {
            ConnectionState::Error { message, .. } => Some(message.clone()),
            _ => None,
        }
    }

    /// The reason the server gave for refusing this node, in the `Refused` state.
    #[wasm_bindgen(getter)]
    pub fn refusal_reason(&self) -> Option<String> {
        match &self.0 {
            ConnectionState::Refused { reason, .. } => Some(reason.clone()),
            _ => None,
        }
    }

    #[wasm_bindgen(getter)]
    pub fn error_kind(&self) -> Option<String> {
        use ankurah_core::connector::PeerConnectionError;
        let ConnectionState::Error { cause, .. } = &self.0 else { return None };
        Some(
            match cause {
                Some(PeerConnectionError::NoCommonVersion { .. }) => "NoCommonVersion",
                Some(PeerConnectionError::UnreadableOpening(_)) => "UnreadableOpening",
                Some(PeerConnectionError::RepeatedOpening) => "RepeatedOpening",
                Some(PeerConnectionError::NoOpening) => "NoOpening",
                Some(PeerConnectionError::ChallengeMismatch(_)) => "ChallengeMismatch",
                Some(PeerConnectionError::InvalidSignature(_)) => "InvalidSignature",
                Some(PeerConnectionError::RelayedChallenge { .. }) => "RelayedChallenge",
                Some(PeerConnectionError::SelfConnection(_)) => "SelfConnection",
                Some(PeerConnectionError::OfferMismatch(_)) => "OfferMismatch",
                Some(PeerConnectionError::Refused(_)) => "Refused",
                Some(PeerConnectionError::AnnouncementFailed(_)) => "AnnouncementFailed",
                Some(PeerConnectionError::NodeHalted(_)) => "NodeHalted",
                None => "Transport",
            }
            .into(),
        )
    }

    pub fn value(&self) -> String {
        match self.0 {
            ConnectionState::None => "None",
            ConnectionState::Connecting { .. } => "Connecting",
            ConnectionState::Connected { .. } => "Connected",
            ConnectionState::Refused { .. } => "Refused",
            ConnectionState::Closed => "Closed",
            ConnectionState::Error { .. } => "Error",
        }
        .to_string()
    }
}
