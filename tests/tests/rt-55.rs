use ankurah::{changes::ChangeKind, Model, PermissiveAgent};
use ankurah::{changes::ChangeSet, Node};
use ankurah_storage_sled::SledStorageEngine;

#[derive(Model, Clone)]
pub struct User {
    pub name: String,
    pub status: String,
}

mod common;

#[tokio::test]
async fn rt_55() -> anyhow::Result<()> {
    let storage_engine = SledStorageEngine::new_test()?;
    let node = Node::new_durable(std::sync::Arc::new(storage_engine), PermissiveAgent::new()).context(ankurah::policy::DEFAULT_CONTEXT);

    let tx1 = node.begin();
    let alice_id = tx1.create(&User { name: "alice".to_owned(), status: "active".to_owned() }).await.id();
    tx1.commit().await?;

    let (watcher, check_changes) = common::changeset_watcher::<UserView>();
    node.subscribe("status = 'active'", move |changeset: ChangeSet<UserView>| watcher(changeset)).await?;

    assert_eq!(check_changes(), vec![vec![(alice_id.clone(), ChangeKind::Initial)]]);

    let tx2 = node.begin();
    let bob_id = tx2.create(&User { name: "bob".to_owned(), status: "active".to_owned() }).await.id();
    tx2.commit().await?;

    assert_eq!(check_changes(), vec![vec![(bob_id.clone(), ChangeKind::Add)]]);

    Ok(())
}
// println!("User jjones created");

// // Update a user to make it so they no longer match the subscription.
// // This should trigger the remove event
// println!("Removing tfox from subscription");
// let tfox: UserView = node.get(tfox_id).await?;
// let tx3 = node.begin();
// let mutable = tfox.edit(&tx3).await?;
// mutable.status.replace("disabled");
// tx3.commit().await?;

// // Update a use without effecting their match status.
// // This should trigger the update event
// println!("Updating jjones ");
// let jjones: UserView = node.get(jjones_id).await?;
// let tx4 = node.begin();
// let mutable = jjones.edit(&tx4).await?;
// mutable.email.replace("updated@example.org");
// tx4.commit().await?;

// println!("CTRL-C to exit.");

// Wait for a CTRL-C
// tokio::signal::ctrl_c().await.ok();
// sleep for 2 seconds
// tokio::time::sleep(std::time::Duration::from_secs(2)).await;

// println!("Example complete");

//     Ok(())
// }
