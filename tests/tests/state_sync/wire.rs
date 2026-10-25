//! What a test observes of the messages between a client and its server:
//! whether a message pushes an entity or answers a subscribe, the updates and
//! responses that reached the client, the offers the client sent, and what an
//! account lists.

use super::*;

/// Whether `message` is an update that pushes `entity`.
pub(super) fn pushes(message: &proto::NodeMessage, entity: EntityId) -> bool {
    match message {
        proto::NodeMessage::Update(update) => {
            let proto::NodeUpdateBody::SubscriptionUpdate { items } = &update.body;
            items.iter().any(|item| item.entity_id == entity)
        }
        _ => false,
    }
}

/// Whether `message` answers a subscribe.
pub(super) fn answers_subscribe(message: &proto::NodeMessage) -> bool {
    matches!(message, proto::NodeMessage::Response(response) if matches!(response.body, proto::NodeResponseBody::QuerySubscribed { .. }))
}

/// What the server sent the client, as a gate saw it: each update's items
/// and each response's body, in arrival order.
#[derive(Clone, Default)]
pub(super) struct Received {
    updates: Arc<Mutex<Vec<Vec<proto::SubscriptionUpdateItem>>>>,
    responses: Arc<Mutex<Vec<proto::NodeResponseBody>>>,
}

impl Received {
    pub(super) fn record(&self, message: &proto::NodeMessage) {
        match message {
            proto::NodeMessage::Update(update) => {
                let proto::NodeUpdateBody::SubscriptionUpdate { items } = &update.body;
                self.updates.lock().unwrap().push(items.clone());
            }
            proto::NodeMessage::Response(response) => self.responses.lock().unwrap().push(response.body.clone()),
            _ => {}
        }
    }

    /// Every update's items.
    pub(super) fn updates(&self) -> Vec<Vec<proto::SubscriptionUpdateItem>> { self.updates.lock().unwrap().clone() }

    /// Every item pushed for one of `entities`.
    pub(super) fn items_for(&self, entities: &[EntityId]) -> Vec<proto::SubscriptionUpdateItem> {
        self.updates().into_iter().flatten().filter(|item| entities.contains(&item.entity_id)).collect()
    }

    pub(super) fn responses(&self) -> Vec<proto::NodeResponseBody> { self.responses.lock().unwrap().clone() }

    pub(super) fn clear(&self) {
        self.updates.lock().unwrap().clear();
        self.responses.lock().unwrap().clear();
    }
}

/// Each entity `account` lists with its head, or none when it describes a
/// span by a digest.
pub(super) fn listed(account: &proto::KnownState) -> Option<BTreeMap<EntityId, proto::Clock>> {
    let mut listed = BTreeMap::new();
    for span in &account.spans {
        let proto::SpanContent::Heads(inventory) = span else { return None };
        listed.extend(inventory.heads.to_vec().into_iter().map(|head| (head.entity.entity_id, head.entity.head)));
    }
    Some(listed)
}

/// The offer each of a client's Fetches (no query id) and subscribes carried.
pub(super) type Offers = Arc<Mutex<Vec<(Option<proto::QueryId>, proto::KnownState)>>>;

/// Connect `client` to `server` through a gate that records the client's
/// offers. Tests connect once the server holds their data, so that the
/// client's catalog types each property before its first query: a query
/// whose keys it cannot type falls back to a tree keyed by entity id.
pub(super) async fn connect_recording_offers(client: &Sled, server: &Sled) -> anyhow::Result<(GatedConnection, Offers)> {
    let offers = Offers::default();
    let (connection, _gate) = {
        let offers = offers.clone();
        GatedConnection::new(client, server, move |message| {
            if let proto::NodeMessage::Request { request, .. } = message {
                match &request.body {
                    proto::NodeRequestBody::Fetch { known_state, .. } => offers.lock().unwrap().push((None, known_state.clone())),
                    proto::NodeRequestBody::SubscribeQuery { query_id, known_state, .. } => {
                        offers.lock().unwrap().push((Some(*query_id), known_state.clone()))
                    }
                    _ => {}
                }
            }
            false
        })
        .await
    };
    client.wait_ready().await?;
    Ok((connection, offers))
}

/// The tree of each offer `query` carried (none for a Fetch), in order.
pub(super) fn specs(offers: &Offers, query: Option<proto::QueryId>) -> Vec<proto::TreeSpec> {
    offers.lock().unwrap().iter().filter(|(id, _)| *id == query).map(|(_, known)| known.spec.clone()).collect()
}
