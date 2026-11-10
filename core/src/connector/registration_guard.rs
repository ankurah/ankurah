use super::NodeComms;
use crate::node::ConnectionRegistration;

/// A connector's hold on one peer registration. Dropping it deregisters that registration, so a
/// handshake or a connection that ends any way, cancelled or dropped included, leaves nothing
/// registered.
#[must_use = "dropping the guard deregisters the peer"]
pub struct RegistrationGuard {
    registration: ConnectionRegistration,
    /// The node that issued the registration; `None` once the registration is handed back.
    node: Option<Box<dyn NodeComms>>,
}

impl RegistrationGuard {
    /// Hold `registration`, which `node` issued.
    pub fn new(node: Box<dyn NodeComms>, registration: ConnectionRegistration) -> Self { Self { registration, node: Some(node) } }

    /// The registration held, which the peer's frames arrive on.
    pub fn registration(&self) -> ConnectionRegistration { self.registration }

    /// Stop holding the registration and hand it back to a caller that deregisters it itself.
    pub fn release(mut self) -> ConnectionRegistration {
        self.node = None;
        self.registration
    }
}

impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        if let Some(node) = self.node.take() {
            node.deregister_peer(self.registration);
        }
    }
}
