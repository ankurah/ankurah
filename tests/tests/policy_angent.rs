use ankurah::{
    changes::{ChangeKind, ChangeSet},
    core::{context::Context, node::ContextData},
    policy::{PolicyAgent, DEFAULT_CONTEXT as c},
    Model, Mutable, Node, PermissiveAgent, ResultSet,
};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::{Arc, Mutex};

mod common;
use common::{Album, AlbumView, Pet, PetView};

#[derive(Debug, Clone, Model)]
pub struct User {
    pub username: String,
    pub role: String, // "admin", "manager", "employee"
    pub department: String,
}

#[derive(Debug, Clone, Model)]
pub struct Doc {
    pub title: String,
    pub access: String, // "public", "internal", "confidential", "restricted"
    pub dept: String,
    pub owner: String, // username of creator
}

pub enum MyContextData {
    Root,
    User(UserView),
}

impl std::fmt::Debug for MyContextData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyContextData::Root => write!(f, "Root"),
            MyContextData::User(user) => write!(f, "User({})", user.username()?),
        }
    }
}
impl ContextData for MyContextData {}

#[tokio::test]
async fn local_access_control() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), MyAccessControlAgent {});
    let root_context = node.context(MyContextData::Root);
    // Create our test dataset
    create_test_dataset(root_context.clone()).await?;

    let alice = root_context.fetch_one::<UserView>("username = 'alice'").await?.unwrap();
    let bob = root_context.fetch_one::<UserView>("username = 'bob'").await?.unwrap();
    let charlie = root_context.fetch_one::<UserView>("username = 'charlie'").await?.unwrap();

    let alice_context = node.context(MyContextData::User(alice));
    let bob_context = node.context(MyContextData::User(bob));
    let charlie_context = node.context(MyContextData::User(charlie));

    Ok(())
}

#[derive(Debug, Clone)]
pub struct MyAccessControlAgent {
    // maybe I have a clone of the Node in here
    // or maybe I have my own state machine stuff
    // or maybe I'm totally stateless
}
impl PolicyAgent for MyAccessControlAgent {
    type ContextData = MyContextData;

    fn can_access_collection(&self, data: &Self::ContextData, collection: &ankurah::proto::CollectionId) -> ankurah::policy::AccessResult {
        todo!()
    }

    fn can_read_entity(
        &self,
        data: &Self::ContextData,
        collection: &ankurah::proto::CollectionId,
        id: &ankurah::ID,
    ) -> ankurah::policy::AccessResult {
        todo!()
    }

    fn can_modify_entity(
        &self,
        data: &Self::ContextData,
        collection: &ankurah::proto::CollectionId,
        id: &ankurah::ID,
    ) -> ankurah::policy::AccessResult {
        todo!()
    }

    fn can_create_in_collection(
        &self,
        data: &Self::ContextData,
        collection: &ankurah::proto::CollectionId,
    ) -> ankurah::policy::AccessResult {
        todo!()
    }

    fn can_subscribe(
        &self,
        data: &Self::ContextData,
        collection: &ankurah::proto::CollectionId,
        predicate: &ankql::ast::Predicate,
    ) -> ankurah::policy::AccessResult {
        todo!()
    }

    fn can_communicate_with_node(&self, data: &Self::ContextData, node_id: &ankurah::proto::NodeId) -> ankurah::policy::AccessResult {
        todo!()
    }
}

async fn create_test_dataset(context: Context) -> Result<()> {
    let trx = context.begin();

    // Create test users with different roles
    trx.create(&User { username: "alice".into(), role: "admin".into(), department: "IT".into() }).await;
    trx.create(&User { username: "bob".into(), role: "manager".into(), department: "HR".into() }).await;
    trx.create(&User { username: "charlie".into(), role: "employee".into(), department: "Finance".into() }).await;

    // Create test documents with different sensitivity levels
    trx.create(&Doc { title: "Corp Strategy".into(), access: "restricted".into(), dept: "Executive".into(), owner: "alice".into() }).await;
    trx.create(&Doc { title: "HR Policies".into(), access: "internal".into(), dept: "HR".into(), owner: "bob".into() }).await;
    trx.create(&Doc { title: "Financials".into(), access: "confidential".into(), dept: "Finance".into(), owner: "charlie".into() }).await;
    trx.create(&Doc { title: "Press Release".into(), access: "public".into(), dept: "Marketing".into(), owner: "bob".into() }).await;
    trx.commit().await?;
    Ok(())
}
