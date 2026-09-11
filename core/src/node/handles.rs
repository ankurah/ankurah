use ankurah_proto as proto;

use super::{Node, WeakNode};
use crate::error::NodeDropped;
use crate::policy::PolicyAgent;

pub enum NodeHandle<SE, PA>
where PA: PolicyAgent
{
    Weak(WeakNode<SE, PA>),
    Strong(Node<SE, PA>),
}

impl<SE, PA> NodeHandle<SE, PA>
where PA: PolicyAgent
{
    /// Access the node, returning an error if a weak handle has expired.
    pub fn upgrade(&self) -> Result<NodeRef<'_, SE, PA>, NodeDropped> {
        match self {
            Self::Weak(node) => node.upgrade().map(NodeRef::Owned).ok_or(NodeDropped),
            Self::Strong(node) => Ok(NodeRef::Ref(node)),
        }
    }

    pub fn node_id(&self) -> proto::EntityId {
        match self {
            Self::Weak(node) => node.node_id(),
            Self::Strong(node) => node.id,
        }
    }
}

pub enum NodeRef<'a, SE, PA>
where PA: PolicyAgent
{
    Ref(&'a Node<SE, PA>),
    Owned(Node<SE, PA>),
}

impl<'a, SE, PA> std::ops::Deref for NodeRef<'a, SE, PA>
where PA: PolicyAgent
{
    type Target = Node<SE, PA>;
    fn deref(&self) -> &Node<SE, PA> {
        match self {
            Self::Ref(node) => node,
            Self::Owned(node) => node,
        }
    }
}

impl<'a, SE, PA> AsRef<Node<SE, PA>> for NodeRef<'a, SE, PA>
where PA: PolicyAgent
{
    fn as_ref(&self) -> &Node<SE, PA> {
        match self {
            Self::Ref(node) => node,
            Self::Owned(node) => node,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{MutationError, RetrievalError};
    use crate::policy::{AccessDenied, PermissiveAgent};
    use crate::test_utils::TestStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn upgrade_borrows_strong_handles_and_retains_live_weak_handles() {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        node.system.wait_loaded().await.unwrap();
        let weak = NodeHandle::Weak(node.weak());
        let strong = NodeHandle::Strong(node);
        let borrowed = strong.upgrade().unwrap();
        assert!(matches!(borrowed, NodeRef::Ref(_)));
        let owned = weak.upgrade().unwrap();
        assert!(matches!(owned, NodeRef::Owned(_)));
        assert!(Arc::ptr_eq(&borrowed.0, &owned.0));
        drop(borrowed);
        drop(strong);
        assert!(weak.upgrade().is_ok());
        drop(owned);

        let error = weak.upgrade().err().expect("the last strong reference was dropped");
        assert_eq!(error, NodeDropped);
        assert!(matches!(RetrievalError::from(error), RetrievalError::NodeDropped(NodeDropped)));
        assert!(matches!(MutationError::from(error), MutationError::NodeDropped(NodeDropped)));
        assert!(matches!(MutationError::from(RetrievalError::from(error)), MutationError::NodeDropped(NodeDropped)));
        assert!(matches!(AccessDenied::from(error), AccessDenied::NodeDropped));
    }
}
