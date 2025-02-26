use crate::*;

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "N{}", &id_str[20..])
    }
}

impl From<NodeId> for String {
    fn from(node_id: NodeId) -> Self { node_id.0.to_string() }
}
impl From<&str> for CollectionId {
    fn from(val: &str) -> Self { CollectionId(val.to_string()) }
}
impl PartialEq<str> for CollectionId {
    fn eq(&self, other: &str) -> bool { self.0 == other }
}

impl From<CollectionId> for String {
    fn from(collection_id: CollectionId) -> Self { collection_id.0 }
}
impl AsRef<str> for CollectionId {
    fn as_ref(&self) -> &str { &self.0 }
}

impl CollectionId {
    pub fn as_str(&self) -> &str { &self.0 }
}

impl std::fmt::Display for CollectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "R{}", &id_str[20..])
    }
}

impl std::fmt::Display for NotificationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id_str = self.0.to_string();
        write!(f, "N{}", &id_str[20..])
    }
}

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "S-{}", self.0.to_string()) }
}

impl Default for NodeId {
    fn default() -> Self { Self::new() }
}

impl NodeId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl Default for RequestId {
    fn default() -> Self { Self::new() }
}

impl RequestId {
    pub fn new() -> Self { Self(Ulid::new()) }
}
impl NotificationId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl Default for SubscriptionId {
    fn default() -> Self { Self::new() }
}

impl SubscriptionId {
    pub fn new() -> Self { Self(Ulid::new()) }

    /// To be used only for testing
    pub fn test(id: u64) -> Self { Self(Ulid::from_parts(id, 0)) }
}

impl std::fmt::Display for NodeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Request {} from {}->{}: {}", self.id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for Notification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Notification {} from {}->{}: {}", self.id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for NodeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Response({}) {}->{} {}", self.request_id, self.from, self.to, self.body)
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event({} {}/{} {} {})",
            self.id,
            self.collection,
            self.entity_id,
            self.parent,
            self.operations
                .iter()
                .map(|(backend, ops)| format!("{} => {}b", backend, ops.iter().map(|op| op.diff.len()).sum::<usize>()))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "State(clock {} buffers {})",
            self.head,
            self.state_buffers.iter().map(|(backend, buf)| format!("{} => {}b", backend, buf.len())).collect::<Vec<_>>().join(" ")
        )
    }
}

impl std::fmt::Display for NodeRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRequestBody::CommitEvents { events } => {
                write!(f, "CommitEvents [{}]", events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", "))
            }
            NodeRequestBody::Fetch { collection, predicate } => {
                write!(f, "Fetch {collection} {predicate}")
            }
            NodeRequestBody::Subscribe { subscription_id, collection, predicate } => {
                write!(f, "Subscribe {subscription_id} {collection} {predicate}")
            }
        }
    }
}

impl std::fmt::Display for NotificationBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotificationBody::SubscriptionUpdate { subscription_id, events } => {
                write!(
                    f,
                    "SubscriptionUpdate {subscription_id} [{}]",
                    events.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ")
                )
            }
            NotificationBody::Unsubscribe { subscription_id } => {
                write!(f, "Unsubscribe {subscription_id}")
            }
        }
    }
}

impl std::fmt::Display for NodeResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeResponseBody::CommitComplete => write!(f, "CommitComplete"),
            NodeResponseBody::Fetch(tuples) => {
                write!(f, "Fetch [{}]", tuples.iter().map(|(id, _)| id.to_string()).collect::<Vec<_>>().join(", "))
            }
            NodeResponseBody::Subscribe { initial, subscription_id } => write!(
                f,
                "Subscribe {} initial [{}]",
                subscription_id,
                initial.iter().map(|(id, state)| format!("{} {}", id, state)).collect::<Vec<_>>().join(", ")
            ),
            NodeResponseBody::Success => write!(f, "Success"),
            NodeResponseBody::Error(e) => write!(f, "Error: {e}"),
        }
    }
}
