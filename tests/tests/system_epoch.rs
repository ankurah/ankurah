mod common;

use ankurah::core::{
    connector::PeerConnectionError,
    error::{NodeHaltReason, RetrievalError},
};
use common::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

#[tokio::test]
async fn system_replacement_does_not_wait_for_an_inflight_commit() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = durable_sled_setup().await?;
        let client = ephemeral_sled_setup().await?;
        let response_held = Arc::new(Notify::new());
        let (_connection, gate) = {
            let response_held = response_held.clone();
            GatedConnection::new(&server, &client, move |message| {
                if matches!(
                    message,
                    proto::NodeMessage::Response(proto::NodeResponse { body: proto::NodeResponseBody::CommitComplete { .. }, .. })
                ) {
                    response_held.notify_one();
                    true
                } else {
                    false
                }
            })
            .await
        };
        let context = client.context_async(DEFAULT_CONTEXT).await?;
        let root = client.system.root().expect("original root");
        let transaction = context.begin();
        transaction.create(&Album { name: "in flight".into(), year: "2026".into() }).await?;
        let commit = tokio::spawn(transaction.commit());
        tokio::time::timeout(Duration::from_secs(2), response_held.notified()).await?;

        let other = durable_sled_setup().await?;
        let proposed = other.system.root().expect("replacement root");
        let halt_reason = NodeHaltReason::SystemReplacement { current: root.payload.entity_id, proposed: proposed.payload.entity_id };
        client.set_allow_system_replacement(true);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), client.system.adopt_system(proposed)).await?,
            Err(PeerConnectionError::NodeHalted(halt_reason.clone()))
        );
        commit.abort();
        assert!(matches!(
            context.begin().commit().await,
            Err(MutationError::NodeHalted(error) | MutationError::RetrievalError(RetrievalError::NodeHalted(error))) if error == halt_reason
        ));

        gate.release_held(&client).await;
        assert_eq!(client.state().peek(), NodeState::Halted(halt_reason.clone()));
        assert!(client.system.system_epoch().is_none());
        assert!(client.system.root().is_none());
        assert_eq!(client.system.wait_system_ready().await, Err(halt_reason));
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn halted_node_stops_queries_and_new_operations_but_retained_views_remain_readable() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = durable_sled_setup().await?;
        let server_context = server.context(DEFAULT_CONTEXT)?;
        let transaction = server_context.begin();
        let id = transaction.create(&Album { name: "retained".into(), year: "2026".into() }).await?.id();
        transaction.commit().await?;

        let client = ephemeral_sled_setup().await?;
        let _connection = LocalProcessConnection::new(&server, &client).await?;
        let context = client.context_async(DEFAULT_CONTEXT).await?;
        let query = context.query_wait::<AlbumView>("true").await?;
        query.wait_durable_answered().await?;
        let retained = context.get::<AlbumView>(id).await?;
        let epoch = client.system.system_epoch();
        let root = client.system.root().expect("original root");
        let transaction = context.begin();
        retained.edit(&transaction)?.name()?.replace("must not commit")?;
        assert_eq!(query.ids(), vec![id]);
        assert!(query.loaded());

        let other = durable_sled_setup().await?;
        let proposed = other.system.root().expect("replacement root");
        let halt_reason = NodeHaltReason::SystemReplacement { current: root.payload.entity_id, proposed: proposed.payload.entity_id };
        client.set_allow_system_replacement(true);
        assert_eq!(client.system.adopt_system(proposed).await, Err(PeerConnectionError::NodeHalted(halt_reason.clone())));

        assert_eq!(client.state().peek(), NodeState::Halted(halt_reason.clone()));
        assert!(client.system.system_epoch().is_none());
        assert_eq!(retained.name()?, "retained");
        assert_eq!(Some(retained.entity().system_epoch()), epoch);
        tokio::time::timeout(Duration::from_secs(2), async {
            while query.loaded() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(query.ids().is_empty());
        assert!(!query.loaded());
        assert!(matches!(query.update_selection("name = 'retained'"), Err(RetrievalError::NodeHalted(error)) if error == halt_reason));
        assert!(matches!(context.get::<AlbumView>(id).await, Err(RetrievalError::NodeHalted(error)) if error == halt_reason));
        assert!(matches!(context.get_cached::<AlbumView>(id).await, Err(RetrievalError::NodeHalted(error)) if error == halt_reason));
        assert!(matches!(context.fetch::<AlbumView>("true").await, Err(RetrievalError::NodeHalted(error)) if error == halt_reason));
        assert!(matches!(context.query::<AlbumView>("true"), Err(RetrievalError::NodeHalted(error)) if error == halt_reason));
        assert_eq!(client.context_async(DEFAULT_CONTEXT).await.err(), Some(halt_reason.clone()));
        let context_error = client.context(DEFAULT_CONTEXT).err().expect("a halted node cannot issue a new context");
        assert_eq!(context_error.downcast_ref::<NodeHaltReason>(), Some(&halt_reason));

        let new_transaction = context.begin();
        retained.edit(&new_transaction)?.name()?.replace("local only")?;
        assert_eq!(retained.name()?, "retained", "editing only changes the transaction's snapshot");
        let create_error = new_transaction
            .create(&Album { name: "must not create".into(), year: "2026".into() })
            .await
            .err()
            .expect("schema registration still requires a functioning node");
        assert!(create_error.to_string().contains(&halt_reason.to_string()), "{create_error}");
        assert!(matches!(
            new_transaction.commit().await,
            Err(MutationError::NodeHalted(error) | MutationError::RetrievalError(RetrievalError::NodeHalted(error))) if error == halt_reason
        ));
        assert!(matches!(
            transaction.commit().await,
            Err(MutationError::NodeHalted(error) | MutationError::RetrievalError(RetrievalError::NodeHalted(error))) if error == halt_reason
        ));
        assert_eq!(server_context.get::<AlbumView>(id).await?.name()?, "retained");
        assert_eq!(server_context.fetch::<AlbumView>("true").await?.len(), 1);
        assert!(query.ids().is_empty());
        assert!(!query.loaded());
        assert_eq!(retained.name()?, "retained");
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn retained_views_keep_their_bindings_when_another_node_registers_the_same_model() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let first = durable_sled_setup().await?;
        let context = first.context(DEFAULT_CONTEXT)?;
        let transaction = context.begin();
        let id = transaction.create(&Album { name: "first node".into(), year: "2026".into() }).await?.id();
        transaction.commit().await?;
        let retained = context.get::<AlbumView>(id).await?;
        let name_field = Album::descriptor().field_by_name("name").unwrap();
        let old_property = name_field.resolved.get(retained.entity().system_epoch()).unwrap();

        let second = durable_sled_setup().await?;
        let context = second.context(DEFAULT_CONTEXT)?;
        let transaction = context.begin();
        let id = transaction.create(&Album { name: "second node".into(), year: "2026".into() }).await?.id();
        transaction.commit().await?;
        let fresh = context.get::<AlbumView>(id).await?;

        assert_ne!(fresh.entity().system_epoch(), retained.entity().system_epoch());
        assert_ne!(name_field.resolved.get(fresh.entity().system_epoch()).unwrap(), old_property);
        assert_eq!(retained.name()?, "first node");
        assert_eq!(fresh.name()?, "second node");
        let transaction = context.begin();
        assert!(retained.edit(&transaction).is_err(), "a view from another Node cannot enter this transaction");
        let model = first.catalog.model_by_label("album").unwrap().unwrap().0;
        let catalog_row = first.context(DEFAULT_CONTEXT)?.get::<ankurah::core::schema::catalog::SysModelRowView>(model).await?;
        let error = catalog_row.edit(&transaction).err().expect("pinned property IDs do not exempt catalog entities");
        assert!(matches!(error, RetrievalError::MutationError(error) if matches!(*error, MutationError::ForeignEntity)));
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn held_remote_commit_allows_an_unrelated_commit_and_inbound_update() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = durable_sled_setup().await?;
        let server_context = server.context(DEFAULT_CONTEXT)?;
        let transaction = server_context.begin();
        let held_id = transaction.create(&Album { name: "held original".into(), year: "2026".into() }).await?.id();
        let independent_id = transaction.create(&Album { name: "independent original".into(), year: "2026".into() }).await?.id();
        let incoming_id = transaction.create(&Album { name: "incoming original".into(), year: "2026".into() }).await?.id();
        transaction.commit().await?;

        let client = ephemeral_sled_setup().await?;
        let request_held = Arc::new(Notify::new());
        let (_connection, gate) = {
            let request_held = request_held.clone();
            GatedConnection::new(&client, &server, move |message| {
                if let proto::NodeMessage::Request {
                    request: proto::NodeRequest { body: proto::NodeRequestBody::CommitTransaction { events, .. }, .. },
                    ..
                } = message
                {
                    if events.iter().any(|event| event.payload.entity_id == held_id) {
                        request_held.notify_one();
                        return true;
                    }
                }
                false
            })
            .await
        };
        let context = client.context_async(DEFAULT_CONTEXT).await?;
        let query = context.query_wait::<AlbumView>("true").await?;
        query.wait_durable_answered().await?;
        assert_eq!(query.ids().len(), 3);
        let held_view = context.get::<AlbumView>(held_id).await?;
        let independent_view = context.get::<AlbumView>(independent_id).await?;

        let transaction = context.begin();
        held_view.edit(&transaction)?.name()?.replace("held complete")?;
        let held_commit = tokio::spawn(transaction.commit());
        tokio::time::timeout(Duration::from_secs(2), request_held.notified()).await?;
        assert!(!held_commit.is_finished());
        assert_eq!(server_context.get::<AlbumView>(held_id).await?.name()?, "held original");

        let transaction = context.begin();
        independent_view.edit(&transaction)?.name()?.replace("independent complete")?;
        tokio::time::timeout(Duration::from_secs(2), transaction.commit()).await??;
        assert_eq!(server_context.get::<AlbumView>(independent_id).await?.name()?, "independent complete");
        assert!(!held_commit.is_finished(), "only the unrelated commit may complete before gate release");

        let incoming_observed = Arc::new(Notify::new());
        let _updates = {
            let incoming_observed = incoming_observed.clone();
            query.subscribe(move |changes: ChangeSet<AlbumView>| {
                if changes.resultset.by_id(&incoming_id).is_some_and(|view| matches!(view.name(), Ok(name) if name == "incoming complete"))
                {
                    incoming_observed.notify_one();
                }
            })
        };
        let incoming_view = server_context.get::<AlbumView>(incoming_id).await?;
        let transaction = server_context.begin();
        incoming_view.edit(&transaction)?.name()?.replace("incoming complete")?;
        transaction.commit().await?;
        tokio::time::timeout(Duration::from_secs(2), incoming_observed.notified()).await?;
        assert_eq!(query.resultset().by_id(&incoming_id).expect("updated row remains selected").name()?, "incoming complete");
        assert!(!held_commit.is_finished(), "the inbound update must apply while the first request remains held");
        assert_eq!(server_context.get::<AlbumView>(held_id).await?.name()?, "held original");
        assert!(client.state().peek().halt_reason().is_none());

        gate.release_held(&server).await;
        tokio::time::timeout(Duration::from_secs(2), held_commit).await???;
        assert_eq!(server_context.get::<AlbumView>(held_id).await?.name()?, "held complete");
        assert_eq!(independent_view.name()?, "independent complete");
        assert_eq!(query.resultset().by_id(&incoming_id).unwrap().name()?, "incoming complete");
        Ok(())
    })
    .await?
}
