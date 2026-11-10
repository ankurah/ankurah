use std::collections::BTreeMap;

use ankurah_proto as proto;

use super::PeerConnectionError;

/// What this node knows about one connection's handshake: the opening it sends, then the peer's
/// opening and the version the two chose. [`Node::register_peer`](crate::Node::register_peer)
/// spends it with the peer's presence, so a presence registers only the connection it answers.
#[derive(Debug)]
pub struct Handshake {
    ours: proto::Opening,
    peer: Option<PeerOpening>,
}

/// The peer's opening, and the version chosen from the two offers.
#[derive(Debug)]
struct PeerOpening {
    opening: proto::Opening,
    version: u32,
}

impl Handshake {
    /// `node`'s side of a new connection: it offers the versions this build speaks, under a fresh challenge.
    pub fn new(node: proto::NodeId) -> Self {
        let ours = proto::Opening { versions: proto::ProtocolVersions::SPOKEN, challenge: proto::HandshakeChallenge::new(node) };
        Self { ours, peer: None }
    }

    /// The opening to send as this connection's first frame.
    pub fn opening(&self) -> proto::Opening { self.ours }

    /// Whether the peer's opening has been received.
    pub fn opened(&self) -> bool { self.peer.is_some() }

    /// Read the peer's first frame as its opening, as [`proto::Message::read_opening`] reads it.
    pub fn read_opening(&mut self, frame: &[u8]) -> Result<(), PeerConnectionError> {
        let opening = proto::Message::read_opening(frame, self.ours.versions).map_err(|error| match error {
            proto::OpeningError::NoCommonVersion(theirs) => PeerConnectionError::NoCommonVersion { ours: self.ours.versions, theirs },
            proto::OpeningError::Undecodable(reason) => PeerConnectionError::UnreadableOpening(reason),
        })?;
        self.receive_opening(opening)
    }

    /// Take the peer's opening from a connector that carries it without encoding it.
    pub fn receive_opening(&mut self, opening: proto::Opening) -> Result<(), PeerConnectionError> {
        if self.opened() {
            return Err(PeerConnectionError::RepeatedOpening);
        }
        let ours = self.ours.versions;
        let version =
            ours.highest_shared(opening.versions).ok_or(PeerConnectionError::NoCommonVersion { ours, theirs: opening.versions })?;
        self.peer = Some(PeerOpening { opening, version });
        Ok(())
    }

    /// The claims `node_id`'s presence makes on this connection: it answers the peer's challenge,
    /// and records the versions the two sides offered and the version chosen. They name no
    /// feature, since this build assigns none, and carry no announcement until a node adds its
    /// plugins' ([`Node::presence`](crate::Node::presence)).
    pub fn presence_claims(&self, node_id: proto::NodeId, durable: bool) -> Result<proto::PresenceClaims, PeerConnectionError> {
        let peer = self.peer.as_ref().ok_or(PeerConnectionError::NoOpening)?;
        Ok(proto::PresenceClaims {
            node_id,
            durable,
            challenge: peer.opening.challenge,
            timestamp: proto::time::unix_ms_now(),
            negotiation: proto::VersionNegotiation {
                offered: self.ours.versions,
                peer_offered: peer.opening.versions,
                chosen: peer.version,
            },
            features: Vec::new(),
            announcements: BTreeMap::new(),
        })
    }

    /// Spend this handshake on `presence`: check that it answers the challenge `receiver` issued
    /// here under a valid signature from a node other than `receiver`, and that it repeats the
    /// versions each side offered on this connection and the version chosen, so a party between
    /// the two cannot change the offers unnoticed.
    pub(crate) fn authenticate(self, receiver: proto::NodeId, presence: &proto::Presence) -> Result<(), PeerConnectionError> {
        let claims = &presence.claims;
        let peer_id = claims.node_id;
        let peer = self.peer.ok_or(PeerConnectionError::NoOpening)?;
        if self.ours.challenge.issuer() != receiver || claims.challenge != self.ours.challenge {
            return Err(PeerConnectionError::ChallengeMismatch(peer_id));
        }
        if !presence.signature_is_valid() {
            return Err(PeerConnectionError::InvalidSignature(peer_id));
        }
        if peer_id == receiver {
            return Err(PeerConnectionError::SelfConnection(peer_id));
        }
        let seen_by_peer =
            proto::VersionNegotiation { offered: peer.opening.versions, peer_offered: self.ours.versions, chosen: peer.version };
        if claims.negotiation != seen_by_peer {
            return Err(PeerConnectionError::OfferMismatch(peer_id));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeKey;

    fn versions(lowest: u32, highest: u32) -> proto::ProtocolVersions { proto::ProtocolVersions { lowest, highest } }

    fn opening(versions: proto::ProtocolVersions) -> proto::Opening {
        proto::Opening { versions, challenge: proto::HandshakeChallenge::new(NodeKey::generate().node_id()) }
    }

    #[test]
    fn an_opening_offering_a_wider_range_settles_on_the_version_this_build_speaks() {
        let mut handshake = Handshake::new(NodeKey::generate().node_id());
        let peer = opening(versions(1, 3));
        handshake.receive_opening(peer).unwrap();
        let claims = handshake.presence_claims(NodeKey::generate().node_id(), false).unwrap();
        assert_eq!(claims.challenge, peer.challenge, "the presence answers the peer's challenge");
        assert_eq!(claims.negotiation, proto::VersionNegotiation { offered: versions(1, 1), peer_offered: versions(1, 3), chosen: 1 });
        assert!(claims.features.is_empty() && claims.announcements.is_empty());
    }

    #[test]
    fn an_opening_sharing_no_version_is_refused_with_both_ranges() {
        let mut handshake = Handshake::new(NodeKey::generate().node_id());
        assert_eq!(
            handshake.receive_opening(opening(versions(2, 3))),
            Err(PeerConnectionError::NoCommonVersion { ours: versions(1, 1), theirs: versions(2, 3) })
        );
        assert!(!handshake.opened());

        let mut frame = bincode::serialize(&proto::Message::Open(opening(versions(2, 3)))).unwrap();
        frame.extend_from_slice(b"bytes a version-1 reader rejects");
        assert_eq!(
            handshake.read_opening(&frame),
            Err(PeerConnectionError::NoCommonVersion { ours: versions(1, 1), theirs: versions(2, 3) })
        );
    }

    #[test]
    fn a_second_opening_is_refused_and_no_presence_answers_before_the_first() {
        let mut handshake = Handshake::new(NodeKey::generate().node_id());
        let node = NodeKey::generate().node_id();
        assert_eq!(handshake.presence_claims(node, false), Err(PeerConnectionError::NoOpening));
        handshake.read_opening(&bincode::serialize(&proto::Message::Open(opening(versions(1, 1)))).unwrap()).unwrap();
        assert!(handshake.opened());
        assert_eq!(handshake.receive_opening(opening(versions(1, 1))), Err(PeerConnectionError::RepeatedOpening));
    }

    #[test]
    fn an_unreadable_opening_is_refused() {
        let mut handshake = Handshake::new(NodeKey::generate().node_id());
        assert!(matches!(handshake.read_opening(&[0; 3]), Err(PeerConnectionError::UnreadableOpening(_))));
        assert!(!handshake.opened());
    }
}
