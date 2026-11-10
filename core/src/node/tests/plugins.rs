use super::{register_keyed_peer, RecordingSender};
use crate::connector::{PeerConnectionError, PeerProvenance};
use crate::error::NodeHaltReason;
use crate::node::{AnnouncementFailure, DuplicatePluginName, Node, NodeKey, NodePlugin, NodeState, PeerRefusal, PeerRegistration};
use crate::policy::{PeerTrust, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT};
use crate::storage::StorageEngine;
use crate::test_helpers::opened_handshakes;
use crate::test_utils::TestStorage;
use ankurah_proto as proto;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Mutex};

/// What a probe answers when asked about a registration.
#[derive(Clone, Copy)]
enum Answer {
    Accept,
    Refuse,
    ResetThenRefuse,
}

/// One registration a probe was asked about, as it saw it.
#[derive(Debug, PartialEq)]
struct Asked {
    plugin: &'static str,
    node_id: proto::NodeId,
    provenance: PeerProvenance,
    trust: PeerTrust,
    announcement: Option<Vec<u8>>,
}

/// The registrations every probe of one node was asked about, in the order asked.
#[derive(Clone, Default)]
struct AskedLog(Arc<Mutex<Vec<Asked>>>);

impl AskedLog {
    fn plugins(&self) -> Vec<&'static str> { self.0.lock().unwrap().iter().map(|asked| asked.plugin).collect() }
}

/// A plugin that announces `announcement`, records each registration it is asked about, and answers `answer`.
struct Probe {
    name: &'static str,
    announcement: Option<Vec<u8>>,
    answer: Answer,
    asked: AskedLog,
}

fn probe(name: &'static str, answer: Answer, asked: &AskedLog) -> Probe { Probe { name, announcement: None, answer, asked: asked.clone() } }

#[async_trait]
impl<SE, PA> NodePlugin<SE, PA> for Probe
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn name(&self) -> &'static str { self.name }

    async fn announce(&self, _node: &Node<SE, PA>) -> Result<Option<Vec<u8>>, AnnouncementFailure> { Ok(self.announcement.clone()) }

    async fn register_peer(&self, node: &Node<SE, PA>, peer: &PeerRegistration<'_>) -> Result<(), PeerRefusal> {
        self.asked.0.lock().unwrap().push(Asked {
            plugin: self.name,
            node_id: peer.node_id,
            provenance: peer.provenance,
            trust: peer.trust,
            announcement: peer.announcement.map(<[u8]>::to_vec),
        });
        let refusal = PeerRefusal { plugin: self.name, reason: "the probe refuses".into() };
        match self.answer {
            Answer::Accept => Ok(()),
            Answer::Refuse => Err(refusal),
            Answer::ResetThenRefuse => {
                node.reset().await.unwrap();
                Err(refusal)
            }
        }
    }
}

type TestNode = Node<TestStorage, PermissiveAgent>;

fn node_with(probes: impl IntoIterator<Item = Probe>) -> TestNode {
    let builder = Node::builder(Arc::new(TestStorage::default()), PermissiveAgent::new());
    probes.into_iter().fold(builder, |builder, probe| builder.plug(probe)).build().unwrap()
}

#[tokio::test]
async fn plugins_are_asked_in_the_order_plugged_and_the_first_refusal_stops_the_rest() {
    let asked = AskedLog::default();
    let node = node_with([
        probe("first", Answer::Accept, &asked),
        probe("second", Answer::Refuse, &asked),
        probe("third", Answer::Accept, &asked),
    ]);
    let key = NodeKey::generate();
    let (sent, _) = mpsc::channel();
    let refused = register_keyed_peer(&node, &key, true, PeerProvenance::Configured, Box::new(RecordingSender(sent))).await;
    let refusal = PeerRefusal { plugin: "second", reason: "the probe refuses".into() };
    assert_eq!(refused, Err(PeerConnectionError::Refused(refusal)));
    assert_eq!(asked.plugins(), ["first", "second"]);
}

#[tokio::test]
async fn a_refused_peer_is_in_no_peer_map_and_receives_nothing() {
    let node = node_with([probe("refuser", Answer::Refuse, &AskedLog::default())]);
    let key = NodeKey::generate();
    let peer = key.node_id();
    let (sent, messages) = mpsc::channel();
    let refused = register_keyed_peer(&node, &key, true, PeerProvenance::Configured, Box::new(RecordingSender(sent))).await;
    assert!(matches!(refused, Err(PeerConnectionError::Refused(_))));

    assert_eq!(node.granted_trust(&peer), None);
    assert!(node.get_durable_peers().is_empty());
    assert!(!node.subscription_relay.as_ref().unwrap().is_connected(&peer));
    let commit = crate::node::durable_peer::relay_to_required_peers(&node, &DEFAULT_CONTEXT, proto::TransactionId::new(), &[]);
    tokio::pin!(commit);
    assert!(matches!(futures::poll!(&mut commit), std::task::Poll::Ready(Ok(()))), "no durable peer is there to relay to");
    assert!(matches!(
        node.request(peer, &DEFAULT_CONTEXT, proto::NodeRequestBody::Get { ids: vec![] }).await,
        Err(crate::error::RequestError::PeerNotConnected)
    ));
    assert_eq!(messages.try_iter().count(), 0, "nothing was sent to the refused peer");
}

#[tokio::test]
async fn a_peer_every_plugin_accepts_is_registered_and_each_plugin_sees_its_registration() {
    let asked = AskedLog::default();
    let node = node_with([probe("first", Answer::Accept, &asked), probe("second", Answer::Accept, &asked)]);
    let key = NodeKey::generate();
    let peer = key.node_id();
    let (sent, _) = mpsc::channel();
    register_keyed_peer(&node, &key, true, PeerProvenance::Configured, Box::new(RecordingSender(sent))).await.unwrap();

    assert_eq!(node.granted_trust(&peer), Some(PeerTrust { durable: true }));
    assert_eq!(node.get_durable_peers(), vec![peer]);
    assert!(node.subscription_relay.as_ref().unwrap().is_connected(&peer));
    let seen = |plugin| Asked {
        plugin,
        node_id: peer,
        provenance: PeerProvenance::Configured,
        trust: PeerTrust { durable: true },
        announcement: None,
    };
    assert_eq!(*asked.0.lock().unwrap(), [seen("first"), seen("second")]);
}

#[tokio::test]
async fn an_announcement_reaches_the_peers_plugin_of_the_same_name_inside_the_signed_presence() {
    let announcer = node_with([
        Probe { name: "shared", announcement: Some(vec![1, 2, 3]), answer: Answer::Accept, asked: AskedLog::default() },
        Probe { name: "only-on-the-announcer", announcement: Some(vec![9]), answer: Answer::Accept, asked: AskedLog::default() },
        Probe { name: "silent", announcement: None, answer: Answer::Accept, asked: AskedLog::default() },
    ]);
    let asked = AskedLog::default();
    let receiver = node_with([probe("shared", Answer::Accept, &asked), probe("only-on-the-receiver", Answer::Accept, &asked)]);

    let (mut ours, theirs) = opened_handshakes(receiver.id, PeerProvenance::Inbound, announcer.id);
    let presence = announcer.presence(&theirs).await.unwrap();
    let announced = BTreeMap::from([("only-on-the-announcer".to_owned(), vec![9]), ("shared".to_owned(), vec![1, 2, 3])]);
    assert_eq!(presence.claims.announcements, announced, "a plugin that announces nothing has no entry");
    assert!(presence.signature_is_valid());

    let (sent, _) = mpsc::channel();
    receiver.register_peer(presence, &mut ours, Box::new(RecordingSender(sent))).await.unwrap();
    let announcements: Vec<_> = asked.0.lock().unwrap().iter().map(|asked| (asked.plugin, asked.announcement.clone())).collect();
    assert_eq!(announcements, [("shared", Some(vec![1, 2, 3])), ("only-on-the-receiver", None)]);
}

#[tokio::test]
async fn an_announcement_changed_after_signing_fails_the_presence_signature_before_any_plugin_is_asked() {
    let announcer =
        node_with([Probe { name: "shared", announcement: Some(vec![1, 2, 3]), answer: Answer::Accept, asked: AskedLog::default() }]);
    let asked = AskedLog::default();
    let receiver = node_with([probe("shared", Answer::Accept, &asked)]);

    let (mut ours, theirs) = opened_handshakes(receiver.id, PeerProvenance::Inbound, announcer.id);
    let mut presence = announcer.presence(&theirs).await.unwrap();
    presence.claims.announcements.insert("shared".into(), vec![4]);
    let (sent, _) = mpsc::channel();
    let refused = receiver.register_peer(presence, &mut ours, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::InvalidSignature(announcer.id)));
    assert!(asked.plugins().is_empty());
}

/// A plugin may reset its node from the hook; the registration still fails with the plugin's
/// refusal, not with the halt the reset caused.
#[tokio::test]
async fn a_plugin_that_resets_its_node_and_refuses_returns_its_refusal() {
    let node = node_with([probe("resetter", Answer::ResetThenRefuse, &AskedLog::default())]);
    let key = NodeKey::generate();
    let (sent, _) = mpsc::channel();
    let refused = register_keyed_peer(&node, &key, true, PeerProvenance::Configured, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::Refused(PeerRefusal { plugin: "resetter", reason: "the probe refuses".into() })));
    assert_eq!(node.state().value(), NodeState::Halted(NodeHaltReason::Reset));
    assert_eq!(node.granted_trust(&key.node_id()), None);
}

/// A plugin that cannot make its announcement.
struct FailingAnnouncer;

#[async_trait]
impl<SE, PA> NodePlugin<SE, PA> for FailingAnnouncer
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn name(&self) -> &'static str { "failing" }

    async fn announce(&self, _node: &Node<SE, PA>) -> Result<Option<Vec<u8>>, AnnouncementFailure> {
        Err(AnnouncementFailure { plugin: "failing", reason: "the probe cannot read its data".into() })
    }
}

#[tokio::test]
async fn a_plugin_that_cannot_announce_fails_the_presence() {
    let node = Node::builder(Arc::new(TestStorage::default()), PermissiveAgent::new()).plug(FailingAnnouncer).build().unwrap();
    let (handshake, _) = opened_handshakes(node.id, PeerProvenance::Configured, NodeKey::generate().node_id());
    let failure = AnnouncementFailure { plugin: "failing", reason: "the probe cannot read its data".into() };
    assert_eq!(node.presence(&handshake).await, Err(PeerConnectionError::AnnouncementFailed(failure)));
}

/// A plugin that halts its node with a reset, then accepts the peer.
struct ResetThenAccept;

#[async_trait]
impl<SE, PA> NodePlugin<SE, PA> for ResetThenAccept
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn name(&self) -> &'static str { "reset-then-accept" }

    async fn register_peer(&self, node: &Node<SE, PA>, _: &PeerRegistration<'_>) -> Result<(), PeerRefusal> {
        node.reset().await.unwrap();
        Ok(())
    }
}

/// A halted node registers no peer: not one presented after the halt, and not one whose
/// plugins halted the node while it was asked about.
#[tokio::test]
async fn a_peer_is_not_registered_on_a_node_halted_before_or_while_its_plugins_are_asked() {
    let asked = AskedLog::default();
    let halted = node_with([probe("probe", Answer::Accept, &asked)]);
    let reason = halted.lifecycle.halt(NodeHaltReason::CatalogLoad("fixture".into()));
    let key = NodeKey::generate();
    let (sent, _) = mpsc::channel();
    let refused = register_keyed_peer(&halted, &key, true, PeerProvenance::Configured, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::NodeHalted(reason)));
    assert!(asked.plugins().is_empty(), "no plugin is asked on a halted node");

    let resetting = Node::builder(Arc::new(TestStorage::default()), PermissiveAgent::new()).plug(ResetThenAccept).build().unwrap();
    let (sent, messages) = mpsc::channel();
    let refused = register_keyed_peer(&resetting, &key, true, PeerProvenance::Configured, Box::new(RecordingSender(sent))).await;
    assert_eq!(refused, Err(PeerConnectionError::NodeHalted(NodeHaltReason::Reset)));
    assert_eq!(resetting.granted_trust(&key.node_id()), None);
    assert_eq!(messages.try_iter().count(), 0, "nothing was sent to the peer");
}

#[test]
fn two_plugins_under_one_name_fail_build() {
    let asked = AskedLog::default();
    let built = Node::builder(Arc::new(TestStorage::default()), PermissiveAgent::new())
        .plug(probe("same", Answer::Accept, &asked))
        .plug(probe("other", Answer::Accept, &asked))
        .plug(probe("same", Answer::Accept, &asked))
        .build();
    assert!(matches!(built, Err(DuplicatePluginName("same"))));
}
