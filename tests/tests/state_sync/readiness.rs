//! When a live query becomes ready: not before the server's account of its
//! result arrives, however many of its pushes came first, and only for the
//! version of the query that asked.

use super::*;
use futures_util::FutureExt;
use std::sync::atomic::{AtomicBool, Ordering};

fn subscribed(message: &proto::NodeMessage) -> bool {
    matches!(message, proto::NodeMessage::Response(response) if matches!(response.body, proto::NodeResponseBody::QuerySubscribed { .. }))
}

#[tokio::test]
async fn pushes_that_arrive_before_the_account_in_any_order_do_not_make_the_query_ready() -> anyhow::Result<()> {
    // Once armed, the gate holds the subscribe's answer and every push of a pet.
    let pets: Arc<Mutex<Vec<EntityId>>> = Arc::default();
    let armed = Arc::new(AtomicBool::new(false));
    let received = Received::default();
    let (server, client_node, _connection, gate) = {
        let (pets, armed, received) = (pets.clone(), armed.clone(), received.clone());
        gated(move |message| {
            let hold = armed.load(Ordering::SeqCst) && (subscribed(message) || pets.lock().unwrap().iter().any(|id| pushes(message, *id)));
            if hold {
                received.record(message);
            }
            hold
        })
        .await?
    };
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    // More members than one update carries, so the push spans several updates.
    let ids = create_pets(&server, "1", (0..300).map(|n| format!("pet {n}"))).await?;
    *pets.lock().unwrap() = ids.clone();
    armed.store(true, Ordering::SeqCst);

    let query = client.query::<PetView>(nocache("age = '1'")?)?;
    wait_until("the answer and every push are held", || received.responses().len() == 1 && received.items_for(&ids).len() == ids.len())
        .await;
    let (answer, pushed): (Vec<_>, Vec<_>) = gate.take_held().into_iter().partition(subscribed);
    assert!(pushed.len() > 1, "the push spans several updates");

    for update in pushed.into_iter().rev() {
        client_node.handle_message(update).await?;
    }
    for id in &ids {
        client_node.storage.get_state(*id).await?;
    }
    assert!(!query.loaded(), "the copy holds every member, but without the account nothing says that is all of them");

    for message in answer {
        client_node.handle_message(message).await?;
    }
    tokio::time::timeout(Duration::from_secs(5), query.wait_initialized()).await??;
    assert_eq!(sorted(query.ids()), sorted(ids));
    Ok(())
}

#[tokio::test]
async fn an_older_versions_answer_neither_makes_the_newer_version_ready_nor_disturbs_it() -> anyhow::Result<()> {
    // Once armed, the gate holds every subscribe's answer.
    let armed = Arc::new(AtomicBool::new(false));
    let (server, client_node, _connection, gate) = {
        let armed = armed.clone();
        gated(move |message| armed.load(Ordering::SeqCst) && subscribed(message)).await?
    };
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", ["old".to_string(), "new".to_string()]).await?;
    armed.store(true, Ordering::SeqCst);

    let query = client.query::<PetView>(nocache("name = 'old'")?)?;
    wait_until("the first version's answer is held", || gate.held() == 1).await;
    query.update_selection("name = 'new'")?;
    wait_until("the second version's answer is held", || gate.held() == 2).await;

    // The first version's answer arrives first, and its pushes have arrived:
    // its copy is ready for it.
    gate.release_first(&client_node, 1).await;
    // A round trip of the client's own lets whatever that answer set off run.
    assert_eq!(client.fetch::<PetView>("name = 'old'").await?.len(), 1);
    assert!(query.wait_initialized().now_or_never().is_none(), "the older version's answer did not make the newer version ready");

    gate.release_held(&client_node).await;
    tokio::time::timeout(Duration::from_secs(5), query.wait_initialized()).await??;
    assert_eq!(query.ids(), [ids[1]]);
    // The newer version stays established, following its own result.
    let trx = server.begin();
    trx.get::<Pet>(&ids[1]).await?.age()?.replace("2")?;
    trx.commit().await?;
    wait_until("the newer version follows its member's change", || query.peek().first().is_some_and(|pet| pet.age().unwrap() == "2")).await;
    assert_eq!(query.ids(), [ids[1]], "the older version's answer installed nothing");
    Ok(())
}
