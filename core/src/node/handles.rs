use ankurah_proto as proto;

use super::{Node, WeakNode};
use crate::policy::PolicyAgent;

pub enum NodeType<SE, PA>
where PA: PolicyAgent
{
    Weak(WeakNode<SE, PA>),
    Strong(Node<SE, PA>),
}

impl<SE, PA> NodeType<SE, PA>
where PA: PolicyAgent
{
    pub fn upgrade(&self) -> Option<NodeRef<'_, SE, PA>> {
        match self {
            Self::Weak(node) => node.upgrade().map(NodeRef::Owned),
            Self::Strong(node) => Some(NodeRef::Ref(node)),
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
