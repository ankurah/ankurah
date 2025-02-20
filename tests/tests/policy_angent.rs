use ankql::ast::Predicate;
use ankurah::{
    changes::{ChangeKind, ChangeSet},
    core::{context::Context, node::ContextData},
    policy::{AccessResult, PolicyAgent, DEFAULT_CONTEXT as c},
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
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), TestAgent {});
    let root_context = node.context(MyContextData::Root);
    create_test_dataset(root_context.clone()).await?;

    let alice = root_context.fetch_one::<UserView>("username = 'alice'").await?.unwrap();
    let bob = root_context.fetch_one::<UserView>("username = 'bob'").await?.unwrap();
    let charlie = root_context.fetch_one::<UserView>("username = 'charlie'").await?.unwrap();

    let ctx_a = node.context(MyContextData::User(alice));
    let ctx_b = node.context(MyContextData::User(bob));
    let ctx_c = node.context(MyContextData::User(charlie));

    // Scenarios we need to cover in this test:

    // ALICE

    // 1. Alice is allowed to fetch all users (this will just work because we're not actually checking anything yet)
    let all_users = ctx_a.fetch::<UserView>(Predicate::True).await?;
    assert!(all_users.len() == 3);

    // 2. Alice is allowed to create a new user
    let trx = ctx_a.begin();
    trx.create(&User { username: "dave".into(), role: "employee".into(), department: "IT".into() }).await;
    trx.commit().await?;

    // CHARLIE

    // 3. Charlie does a wildcard fetch for docs, but only Press Release and HR Policies are returned
    let charlie_docs = ctx_c.fetch::<DocView>(Predicate::True).await?;
    let mut titles: Vec<_> = charlie_docs.iter().map(|d| d.title().unwrap()).collect();
    titles.sort();
    assert_eq!(titles, vec!["HR Policies", "Press Release"]);

    // 4. Charlie is allowed to fetch all users
    let charlie_users = ctx_c.fetch::<UserView>(Predicate::True).await?;
    assert!(charlie_users.len() == 4); // Including newly created Dave

    // 5. Charlie is sneaky and tries to edit his own role, but is blocked
    let user1 = all_users.items.iter().find(|u| u.username().unwrap() == "charlie").unwrap();
    let trx = ctx_c.begin();
    user1.edit(&trx).await?.role.replace("admin")?;
    trx.commit().await?;

    // 6. Charlie tries to create a user (will be restricted later)
    let trx = ctx_c.begin();
    trx.create(&User { username: "eve".into(), role: "employee".into(), department: "Finance".into() }).await;
    trx.commit().await?;

    Ok(())
}

// TODO cover fetch,subscribe,create,modify,delete

#[derive(Debug, Clone)]
pub struct TestAgent {
    // maybe I have a clone of the Node in here
    // or maybe I have my own state machine stuff
    // or maybe I'm totally stateless
}
impl PolicyAgent for TestAgent {
    type ContextData = MyContextData;

    fn can_access_collection(&self, data: &Self::ContextData, collection: &ankurah::proto::CollectionId) -> ankurah::policy::AccessResult {
        match data {
            MyContextData::Root => AccessResult::Allow,    // Root can access everything
            MyContextData::User(_) => AccessResult::Allow, // All users can access collections, but individual entities will be filtered
        }
    }

    fn can_read_entity(
        &self,
        data: &Self::ContextData,
        collection: &ankurah::proto::CollectionId,
        id: &ankurah::ID,
    ) -> ankurah::policy::AccessResult {
        match data {
            MyContextData::Root => AccessResult::Allow, // Root can read everything
            MyContextData::User(user) => {
                let role = user.role()?;
                match collection.as_str() {
                    "doc" => {
                        // We'll need to fetch the doc to check its access level
                        // For now return Ok - we'll filter in the fetch predicate
                        Ok(())
                    }
                    "user" => Ok(()), // All users can read user info
                    _ => Ok(()),
                }
            }
        }
    }

    fn can_modify_entity(
        &self,
        data: &Self::ContextData,
        collection: &ankurah::proto::CollectionId,
        id: &ankurah::ID,
    ) -> ankurah::policy::AccessResult {
        // TODO Entity -> View downcasting
        match data {
            MyContextData::Root => AccessResult::Allow, // Root can modify everything
            MyContextData::User(user) => {
                let role = user.role().unwrap();
                match role.as_str() {
                    "admin" => AccessResult::Allow, // Admins can modify anything
                    "manager" => {
                        match collection.as_str() {
                            "user" => AccessResult::Deny,
                            "doc" => AccessResult::Allow, // Managers can modify docs
                            _ => AccessResult::Allow,
                        }
                    }
                    _ => AccessResult::Deny,
                }
            }
        }
    }

    fn can_create_in_collection(
        &self,
        data: &Self::ContextData,
        collection: &ankurah::proto::CollectionId,
    ) -> ankurah::policy::AccessResult {
        match data {
            MyContextData::Root => AccessResult::Allow, // Root can create anything
            MyContextData::User(user) => {
                let role = user.role().unwrap();
                match role.as_str() {
                    "admin" => AccessResult::Allow, // Admins can create anything
                    "manager" => {
                        match collection.as_str() {
                            "user" => AccessResult::Deny,
                            "doc" => AccessResult::Allow, // Managers can create docs
                            _ => AccessResult::Allow,
                        }
                    }
                    _ => AccessResult::Deny,
                }
            }
        }
    }

    fn can_subscribe(
        &self,
        data: &Self::ContextData,
        collection: &ankurah::proto::CollectionId,
        predicate: &ankql::ast::Predicate,
    ) -> ankurah::policy::AccessResult {
        // For now, use same rules as can_access_collection
        self.can_access_collection(data, collection)
    }

    fn can_communicate_with_node(&self, data: &Self::ContextData, node_id: &ankurah::proto::NodeId) -> ankurah::policy::AccessResult {
        // For this test, allow all node communication
        Ok(())
    }
}

async fn create_test_dataset(context: Context) -> Result<()> {
    let trx = context.begin();
    trx.create(&User { username: "alice".into(), role: "admin".into(), department: "IT".into() }).await;
    trx.create(&User { username: "bob".into(), role: "manager".into(), department: "HR".into() }).await;
    trx.create(&User { username: "charlie".into(), role: "employee".into(), department: "Finance".into() }).await;
    trx.create(&Doc { title: "Corp Strategy".into(), access: "restricted".into(), dept: "Executive".into(), owner: "alice".into() }).await;
    trx.create(&Doc { title: "HR Policies".into(), access: "internal".into(), dept: "HR".into(), owner: "bob".into() }).await;
    trx.create(&Doc { title: "Financials".into(), access: "confidential".into(), dept: "Finance".into(), owner: "charlie".into() }).await;
    trx.create(&Doc { title: "Press Release".into(), access: "public".into(), dept: "Marketing".into(), owner: "bob".into() }).await;
    trx.commit().await?;
    Ok(())
}
