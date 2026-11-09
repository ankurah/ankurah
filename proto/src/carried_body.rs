//! The body of a request, a response or an update, as its sender encoded it.

use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::encoding::decode_exact;

/// A body of kind `T` as its sender encoded it: length-prefixed bytes, so the frame that carries
/// it, with its ids and addresses, decodes even when this build cannot read the body.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct CarriedBody<T> {
    bytes: Vec<u8>,
    #[serde(skip)]
    kind: PhantomData<fn() -> T>,
}

/// A body this build cannot decode: one of a kind a later build added, or a malformed encoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a body this build cannot decode: {0}")]
pub struct UnreadableBody(pub String);

impl<T: Serialize + DeserializeOwned> CarriedBody<T> {
    /// `body`, encoded once to be carried.
    pub fn encode(body: &T) -> Self {
        // Cannot fail: bincode encodes every request, response and update body.
        Self { bytes: bincode::serialize(body).expect("bodies encode"), kind: PhantomData }
    }

    /// The body these bytes encode; bytes left over after it make it unreadable.
    pub fn decode(&self) -> Result<T, UnreadableBody> { decode_exact(&self.bytes).map_err(|error| UnreadableBody(error.to_string())) }
}

impl<T> Clone for CarriedBody<T> {
    fn clone(&self) -> Self { Self { bytes: self.bytes.clone(), kind: PhantomData } }
}

impl<T: std::fmt::Debug + Serialize + DeserializeOwned> std::fmt::Debug for CarriedBody<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.decode() {
            Ok(body) => body.fmt(f),
            Err(error) => write!(f, "CarriedBody({} bytes, {error})", self.bytes.len()),
        }
    }
}

impl<T: std::fmt::Display + Serialize + DeserializeOwned> std::fmt::Display for CarriedBody<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.decode() {
            Ok(body) => body.fmt(f),
            Err(error) => write!(f, "{} bytes, {error}", self.bytes.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{identity::Identity, request::RequestId, Message, NodeMessage, NodeRequest, NodeRequestBody};

    /// A body that is variant 99 of its kind, which no build defines yet.
    fn unknown_kind<T>() -> CarriedBody<T> { bincode::deserialize(&bincode::serialize(&vec![99u8, 0, 0, 0]).unwrap()).unwrap() }

    #[test]
    fn a_carried_body_decodes_to_the_body_it_encodes() {
        let body = NodeRequestBody::Get { ids: vec![crate::EntityId::from_bytes([1; 32])] };
        let carried = CarriedBody::encode(&body);
        assert_eq!(bincode::serialize(&carried.decode().unwrap()).unwrap(), bincode::serialize(&body).unwrap());
        assert_eq!(&bincode::serialize(&carried).unwrap()[8..], &bincode::serialize(&body).unwrap()[..], "a length, then the body");
    }

    #[test]
    fn a_body_of_an_unknown_kind_or_with_bytes_left_over_is_unreadable() {
        assert!(unknown_kind::<NodeRequestBody>().decode().is_err());
        let mut bytes = bincode::serialize(&NodeRequestBody::Get { ids: vec![] }).unwrap();
        bytes.push(0);
        let trailing: CarriedBody<NodeRequestBody> = bincode::deserialize(&bincode::serialize(&bytes).unwrap()).unwrap();
        assert!(trailing.decode().is_err());
    }

    /// The ids and addresses a node needs to answer a request sit outside its body.
    #[test]
    fn a_frame_whose_body_this_build_cannot_read_still_decodes() {
        let (from, to) = (Identity::Ed25519([1; 32]).into(), Identity::Ed25519([2; 32]).into());
        let request = NodeRequest { id: RequestId::new(), to, from, body: unknown_kind() };
        let frame = bincode::serialize(&Message::PeerMessage(NodeMessage::Request { auth: vec![], request })).unwrap();
        let Message::PeerMessage(NodeMessage::Request { request: decoded, .. }) = bincode::deserialize(&frame).unwrap() else {
            panic!("the frame decodes as the request it carries")
        };
        assert_eq!((decoded.from, decoded.to), (from, to));
        assert!(decoded.body.decode().is_err());
    }
}
