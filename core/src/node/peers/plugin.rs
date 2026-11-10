use std::collections::{BTreeMap, BTreeSet};

use ankurah_proto as proto;
use async_trait::async_trait;

use crate::connector::PeerProvenance;
use crate::node::Node;
use crate::policy::{PeerTrust, PolicyAgent};
use crate::storage::StorageEngine;

/// Behaviour an application attaches to a node from outside core. Plugins attach when the node is
/// built (see [`Node::builder`]), so each one sees every peer registration of its node.
#[async_trait]
pub trait NodePlugin<SE, PA>: Send + Sync + 'static
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Names this plugin's entry in a presence.
    fn name(&self) -> &'static str;

    /// Bytes this node announces to every peer under `name`, inside its signed presence. `Err`
    /// fails the presence, and with it the connection.
    async fn announce(&self, _node: &Node<SE, PA>) -> Result<Option<Vec<u8>>, AnnouncementFailure> { Ok(None) }

    /// Asked once per registration, after the presence verifies and trust is decided and
    /// before the peer is registered. `Err` refuses the peer. No message from the peer is read
    /// while this runs, so a plugin decides from what the handshake carried. The hook can be
    /// dropped at any await: when its node halts or the connector shuts down, when the connection
    /// closes, or when an accepting connector's handshake deadline passes; the peer is then not
    /// registered.
    async fn register_peer(&self, _node: &Node<SE, PA>, _peer: &PeerRegistration<'_>) -> Result<(), PeerRefusal> { Ok(()) }
}

/// One peer's registration as a plugin sees it.
#[derive(Debug, Clone, Copy)]
pub struct PeerRegistration<'a> {
    pub node_id: proto::NodeId,
    pub provenance: PeerProvenance,
    pub trust: PeerTrust,
    /// What the peer announced under the asking plugin's name, if anything.
    pub announcement: Option<&'a [u8]>,
}

/// A plugin's refusal of a peer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("plugin {plugin} refused the peer: {reason}")]
pub struct PeerRefusal {
    pub plugin: &'static str,
    pub reason: String,
}

/// A plugin's failure to make its announcement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("plugin {plugin} could not make its announcement: {reason}")]
pub struct AnnouncementFailure {
    pub plugin: &'static str,
    pub reason: String,
}

/// Two plugins given to one node under the same name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("two plugins are named {0}")]
pub struct DuplicatePluginName(pub &'static str);

/// A node's plugins in the order plugged, no two under one name.
pub(crate) struct NodePlugins<SE, PA>(Vec<Box<dyn NodePlugin<SE, PA>>>);

impl<SE, PA> NodePlugins<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn none() -> Self { Self(Vec::new()) }

    pub(crate) fn new(plugins: Vec<Box<dyn NodePlugin<SE, PA>>>) -> Result<Self, DuplicatePluginName> {
        let mut names = BTreeSet::new();
        match plugins.iter().find(|plugin| !names.insert(plugin.name())) {
            Some(duplicate) => Err(DuplicatePluginName(duplicate.name())),
            None => Ok(Self(plugins)),
        }
    }

    /// Each plugin's announcement, asked in the order plugged, under the plugin's name; the first
    /// failure stops the rest.
    pub(crate) async fn announcements(&self, node: &Node<SE, PA>) -> Result<BTreeMap<String, Vec<u8>>, AnnouncementFailure> {
        let mut announcements = BTreeMap::new();
        for plugin in &self.0 {
            if let Some(announcement) = plugin.announce(node).await? {
                announcements.insert(plugin.name().to_owned(), announcement);
            }
        }
        Ok(announcements)
    }

    /// Ask each plugin, in the order plugged, about registering the peer whose presence makes
    /// `claims`; the first refusal stops the rest.
    pub(crate) async fn register_peer(
        &self,
        node: &Node<SE, PA>,
        claims: &proto::PresenceClaims,
        provenance: PeerProvenance,
        trust: PeerTrust,
    ) -> Result<(), PeerRefusal> {
        for plugin in &self.0 {
            let announcement = claims.announcements.get(plugin.name()).map(Vec::as_slice);
            plugin.register_peer(node, &PeerRegistration { node_id: claims.node_id, provenance, trust, announcement }).await?;
        }
        Ok(())
    }
}
