use super::*;
use ankurah::signals::Signal;
use ankurah_core::{connector::PeerSender, error::NodeHaltReason};
use ankurah_proto as proto;
use async_trait::async_trait;
use futures::channel::oneshot;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen(inline_js = r#"
let original;
let sockets = [];
export function mock_websockets() {
    original = globalThis.WebSocket;
    globalThis.WebSocket = class {
        static OPEN = 1;
        constructor() { this.readyState = 1; this.sent = []; sockets.push(this); }
        send(data) { this.sent.push(data); }
        close() { this.readyState = 3; }
    };
}
export function restore_websockets() { globalThis.WebSocket = original; sockets = []; }
export function deliver(index, bytes) {
    sockets[index].onmessage(new MessageEvent("message", { data: bytes.slice().buffer }));
}
export function close_from_server(index) {
    sockets[index].readyState = 3;
    sockets[index].onclose(new CloseEvent("close", { code: 1000 }));
}
export function sent_count(index) { return sockets[index].sent.length; }
export function socket_count() { return sockets.length; }
export function is_closed(index) { return sockets[index].readyState === 3; }
"#)]
extern "C" {
    fn mock_websockets();
    fn restore_websockets();
    fn deliver(index: usize, bytes: &[u8]);
    fn close_from_server(index: usize);
    fn sent_count(index: usize) -> usize;
    fn socket_count() -> usize;
    fn is_closed(index: usize) -> bool;
}

struct RestoreWebSockets;
impl Drop for RestoreWebSockets {
    fn drop(&mut self) { restore_websockets(); }
}

#[derive(Clone)]
struct HeldAdmission(Arc<AdmissionState>);

struct AdmissionState {
    started: Mut<bool>,
    published: Mut<bool>,
    handled: Mut<usize>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
    presence_started: Mut<bool>,
    release_presence: Mutex<Option<oneshot::Receiver<()>>>,
    node_state: Mut<NodeState>,
}

#[async_trait]
impl NodeComms for HeldAdmission {
    fn id(&self) -> proto::EntityId { proto::EntityId::from_bytes([1; 32]) }
    async fn presence(&self) -> Result<proto::Presence, NodeHaltReason> {
        let release = self.0.release_presence.lock().unwrap().take().unwrap();
        self.0.presence_started.set(true);
        release.await.map_err(|_| NodeHaltReason::SystemLoad("presence cancelled".into()))?;
        Ok(proto::Presence { node_id: self.id(), durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION })
    }
    async fn register_peer(&self, _: proto::Presence, _: Box<dyn PeerSender>) -> Result<(), PeerConnectionError> {
        let release = self.0.release.lock().unwrap().take().unwrap();
        self.0.started.set(true);
        release.await.map_err(|_| PeerConnectionError::MissingSystem)?;
        self.0.published.set(true);
        Ok(())
    }
    fn state(&self) -> Read<NodeState> { self.0.node_state.read() }
    fn deregister_peer(&self, _: proto::EntityId) { self.0.published.set(false); }
    async fn handle_message(&self, _: proto::NodeMessage) -> anyhow::Result<()> {
        self.0.handled.set(self.0.handled.value() + 1);
        Ok(())
    }
    fn cloned(&self) -> Box<dyn NodeComms> { Box::new(self.clone()) }
}

#[wasm_bindgen_test]
async fn close_and_halt_cancel_admission_and_queued_frames() {
    enum Stop {
        Close,
        Halt,
        Drop,
    }
    mock_websockets();
    let _restore = RestoreWebSockets;
    let run = async {
        for (socket, (close_before_admission, close_before_presence, stop)) in [
            (true, false, Stop::Close),
            (false, true, Stop::Close),
            (false, false, Stop::Close),
            (true, false, Stop::Halt),
            (false, true, Stop::Halt),
            (false, false, Stop::Halt),
            (true, false, Stop::Drop),
            (false, true, Stop::Drop),
            (false, false, Stop::Drop),
        ]
        .into_iter()
        .enumerate()
        {
            let halt = matches!(stop, Stop::Halt);
            let (release, receiver) = oneshot::channel();
            let (release_presence, presence_receiver) = oneshot::channel();
            let node = HeldAdmission(Arc::new(AdmissionState {
                started: Mut::new(false),
                published: Mut::new(false),
                handled: Mut::new(0),
                release: Mutex::new(Some(receiver)),
                presence_started: Mut::new(false),
                release_presence: Mutex::new(Some(presence_receiver)),
                node_state: Mut::new(NodeState::Uninitialized),
            }));
            let client = Arc::new(ClientInner {
                server_url: "ws://mock.invalid".into(),
                connection: RefCell::new(None),
                state: Mut::new(ConnectionState::None),
                node: Box::new(node.clone()),
                run: Calculated::new({
                    let state = node.state();
                    move || !matches!(state.get(), NodeState::Halted(_))
                }),
                reconnect_delay: RefCell::new(0),
            });
            assert!(client.run.get());
            node.0.node_state.set(NodeState::Startup);
            assert!(client.run.get());
            node.0.node_state.set(NodeState::Running);
            let handle = WebsocketClient { inner: client.clone() };
            let mut ready = Box::pin(handle.ready());
            assert!(futures::poll!(&mut ready).is_pending());
            let connected_notifications = Arc::new(AtomicUsize::new(0));
            let state = client.state.read();
            let _listener = state.listen(Arc::new({
                let state = state.clone();
                let count = connected_notifications.clone();
                move |_| {
                    if matches!(state.value(), ConnectionState::Connected { .. }) {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
            client.connect().unwrap();
            client.state.set(ConnectionState::Error { message: "transient transport error".into(), cause: None });
            assert!(futures::poll!(&mut ready).is_pending());
            let peer = proto::EntityId::from_bytes([2; 32]);
            deliver(
                socket,
                &bincode::serialize(&proto::Message::Presence(proto::Presence {
                    node_id: peer,
                    durable: true,
                    system_root: None,
                    protocol_version: proto::PROTOCOL_VERSION,
                }))
                .unwrap(),
            );
            deliver(
                socket,
                &bincode::serialize(&proto::Message::PeerMessage(proto::NodeMessage::Response(proto::NodeResponse {
                    request_id: proto::RequestId::new(),
                    from: peer,
                    to: node.id(),
                    body: proto::NodeResponseBody::Success,
                })))
                .unwrap(),
            );
            node.0.started.read().wait_for(|started| *started).await;
            assert!(!node.0.published.value());
            assert_eq!(node.0.handled.value(), 0, "the following frame must wait for admission");
            assert_eq!(sent_count(socket), 0, "client presence must wait for admission");

            let release = if close_before_admission {
                release
            } else {
                release.send(()).unwrap();
                node.0.presence_started.read().wait_for(|started| *started).await;
                assert!(node.0.published.value());
                assert_eq!(node.0.handled.value(), 0, "the following frame must wait for presence");
                assert_eq!(sent_count(socket), 0);
                release_presence
            };

            if close_before_admission || close_before_presence {
                match stop {
                    Stop::Halt => node.0.node_state.set(NodeState::Halted(NodeHaltReason::SystemLoad("fixture".into()))),
                    Stop::Close => close_from_server(socket),
                    Stop::Drop => {
                        let connection = client.connection.borrow_mut().take();
                        drop(connection);
                    }
                }
                sleep(Duration::ZERO).await;
                assert!(release.send(()).is_err(), "closing must drop the pending handshake future");
                sleep(Duration::ZERO).await;
                assert!(!node.0.published.value());
                assert_eq!(node.0.handled.value(), 0);
                assert_eq!(sent_count(socket), 0);
                assert_eq!(connected_notifications.load(Ordering::Relaxed), 0, "the old connection must never publish Connected");
                if matches!(stop, Stop::Drop) {
                    assert!(is_closed(socket), "dropping the connection owner must close the socket");
                    continue;
                }
                if halt {
                    assert!(matches!(client.state.value(), ConnectionState::Error { cause: Some(PeerConnectionError::NodeHalted(_)), .. }));
                } else {
                    assert_eq!(client.state.value(), ConnectionState::Closed);
                }
                if !halt {
                    assert!(futures::poll!(&mut ready).is_pending(), "ready must wait through a transport error");
                    if close_before_admission {
                        let cause = PeerConnectionError::MissingSystem;
                        client.state.set(ConnectionState::Error { message: cause.to_string(), cause: Some(cause) });
                    } else {
                        node.0.node_state.set(NodeState::Halted(NodeHaltReason::SystemLoad("fixture".into())));
                        state
                            .wait_for(|state| {
                                matches!(state, ConnectionState::Error { cause: Some(PeerConnectionError::NodeHalted(_)), .. })
                            })
                            .await;
                        sleep(Duration::from_millis(550)).await;
                        assert_eq!(socket_count(), socket + 1, "halt must cancel a pending retry");
                    }
                }
                assert!(matches!(futures::poll!(&mut ready), std::task::Poll::Ready(Err(_))));
            } else {
                release.send(()).unwrap();
                state.wait_for(|state| matches!(state, ConnectionState::Connected { .. })).await;
                node.0.handled.read().wait_for(|count| *count == 1).await;
                assert!(node.0.published.value());
                assert_eq!(sent_count(socket), 1);
                assert_eq!(connected_notifications.load(Ordering::Relaxed), 1);
                assert!(matches!(futures::poll!(&mut ready), std::task::Poll::Ready(Ok(()))));
                if matches!(stop, Stop::Drop) {
                    let connection = client.connection.borrow_mut().take();
                    drop(connection);
                    assert!(is_closed(socket));
                    assert!(!node.0.published.value(), "dropping the connection owner must deregister an idle peer");
                }
                if halt {
                    node.0.node_state.set(NodeState::Halted(NodeHaltReason::SystemLoad("fixture".into())));
                    state
                        .wait_for(|state| matches!(state, ConnectionState::Error { cause: Some(PeerConnectionError::NodeHalted(_)), .. }))
                        .await;
                    assert!(!node.0.published.value(), "halt must deregister an idle peer");
                    assert!(handle.ready().await.is_err());
                }
            }
            if matches!(node.0.node_state.value(), NodeState::Halted(_)) {
                assert!(!client.run.get());
                assert!(is_closed(socket));
                client.connect().unwrap();
                assert_eq!(socket_count(), socket + 1, "a halted client must not reconnect");
            }
            drop(ready);
            drop(handle);
            drop(client);
            assert!(!node.0.published.value(), "dropping the successful connection also deregisters its peer");
        }
    };
    futures::pin_mut!(run);
    let outcome = futures::future::select(run, Box::pin(sleep(Duration::from_secs(5)))).await;
    assert!(matches!(outcome, futures::future::Either::Left(_)), "admission test exceeded five seconds");
}
