use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
use ankql::parser::parse_selection;
use ankurah::error::{MutationError, RetrievalError, ValidationError};
use ankurah::model::Entity;
use ankurah::property::PropertyError;
use ankurah::{
    changes::{ChangeKind, ChangeSet},
    core::{context::Context, node::ContextData},
    policy::{AccessDenied, PolicyAgent},
    proto::{self, CollectionId},
    Model, Mutable, Node, PermissiveAgent, ResultSet,
};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
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
    User { username: String, role: String, department: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyContextProto {
    pub username: String,
    pub role: String,
    pub department: String,
    pub evil_bit: bool,
}

impl std::fmt::Debug for MyContextData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyContextData::Root => write!(f, "Root"),
            MyContextData::User { username, role, department } => write!(f, "User({} {} {})", username, role, department),
        }
    }
}
#[async_trait]
impl ContextData for MyContextData {
    async fn validate(context: proto::BearerContext) -> Result<Self, ValidationError> {
        let proto: MyContextProto = serde_json::from_slice(&context.0).map_err(|e| ValidationError::Deserialization(Box::new(e)))?;
        if proto.evil_bit {
            Err(ValidationError::Rejected("Evil bit is set"))
        } else {
            let username = proto.username;
            if username == "root" {
                Ok(MyContextData::Root)
            } else {
                let role = proto.role;
                let department = proto.department;
                Ok(MyContextData::User { username, role, department })
            }
        }
    }

    fn proto(&self) -> proto::BearerContext {
        match self {
            MyContextData::Root => {
                let proto = MyContextProto { username: "root".into(), role: "root".into(), department: "root".into(), evil_bit: false };
                proto::BearerContext(serde_json::to_vec(&proto).map_err(|e| ValidationError::Serialization(e.to_string()))?)
            }
            MyContextData::User { username, role, department } => {
                let proto =
                    MyContextProto { username: username.clone(), role: role.clone(), department: department.clone(), evil_bit: false };
                proto::BearerContext(serde_json::to_vec(&proto).map_err(|e| ValidationError::Serialization(e.to_string()))?)
            }
        }
    }
}
impl TryFrom<UserView> for MyContextData {
    type Error = PropertyError;
    fn try_from(user: UserView) -> Result<Self, Self::Error> {
        Ok(MyContextData::User { username: user.username()?, role: user.role()?, department: user.department()? })
    }
}

#[tokio::test]
async fn local_access_control() -> Result<(), Box<dyn std::error::Error>> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), TestAgent {});
    let root_context = node.context(MyContextData::Root);
    create_test_dataset(root_context.clone()).await?;

    let alice = root_context.fetch_one::<UserView>("username = 'alice'").await?.unwrap();
    let bob = root_context.fetch_one::<UserView>("username = 'bob'").await?.unwrap();
    let charlie = root_context.fetch_one::<UserView>("username = 'charlie'").await?.unwrap();

    let c_alice = node.context(alice.try_into()?);
    let _c_bob = node.context(bob.try_into()?);
    let c_charlie = node.context(charlie.try_into()?);

    // Scenarios we need to cover in this test:

    // ALICE

    // 1. Alice is allowed to fetch all users (this will just work because we're not actually checking anything yet)
    let all_users = c_alice.fetch::<UserView>(Predicate::True).await?;
    assert!(all_users.len() == 3);

    // 2. Alice is allowed to create a new user
    let trx = c_alice.begin();
    trx.create(&User { username: "dave".into(), role: "employee".into(), department: "IT".into() }).await?;
    trx.commit().await?;

    // 3. Charlie does a wildcard fetch for docs, but only Press Release and HR Policies are returned
    let charlie_docs = c_charlie.fetch::<DocView>(Predicate::True).await?;
    let mut titles: Vec<_> = charlie_docs.iter().map(|d| d.title().unwrap()).collect();
    titles.sort();
    assert_eq!(titles, vec!["HR Policies", "Press Release"]);
    // 4. Charlie is allowed to fetch all users
    let charlie_users = c_charlie.fetch::<UserView>(Predicate::True).await?;
    assert!(charlie_users.len() == 4); // Including newly created Dave

    // 5. Charlie is sneaky and tries to edit his own role, but is blocked
    let user1 = all_users.items.iter().find(|u| u.username().unwrap() == "charlie").unwrap();
    let trx = c_charlie.begin();
    // assert!(matches!(user1.edit(&trx).await?.role.replace("admin"), Err(MutationError::AccessDenied(_))));
    assert!(matches!(user1.edit(&trx).await, Err(MutationError::AccessDenied(_))));
    trx.commit().await?;

    // 6. Charlie tries to create a user - but is blocked
    let trx = c_charlie.begin();
    assert!(matches!(
        trx.create(&User { username: "eve".into(), role: "employee".into(), department: "Finance".into() }).await,
        Err(MutationError::AccessDenied(_))
    ));
    trx.commit().await?;

    Ok(())
}

#[tokio::test]
async fn keeping_peers_honest() -> Result<(), Box<dyn std::error::Error>> {
    // Create a server with TestAgent (enforcing policies) and a "dishonest" client with PermissiveAgent (no restrictions)
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), TestAgent {});
    let dishonest_client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), DishonestTestAgent {});

    // Connect the client to the server
    let _conn = LocalProcessConnection::new(&dishonest_client, &server).await?;

    // Set up the initial dataset on the server with proper permissions
    let server_context = server.context(MyContextData::Root);
    create_test_dataset(server_context.clone()).await?;

    // Get user references from server
    let alice = server_context.fetch_one::<UserView>("username = 'alice'").await?.unwrap();
    let charlie = server_context.fetch_one::<UserView>("username = 'charlie'").await?.unwrap();

    // Create contexts for both honest and dishonest operations
    let c_alice = dishonest_client.context(alice.try_into()?);
    let c_charlie = dishonest_client.context(charlie.try_into()?);

    // 1. Alice is allowed to fetch all users (this will just work because we're not actually checking anything yet)
    let all_users = c_alice.fetch::<UserView>(Predicate::True).await?;
    assert!(all_users.len() == 3);

    // 2. Alice is allowed to create a new user
    let trx = c_alice.begin();
    trx.create(&User { username: "dave".into(), role: "employee".into(), department: "IT".into() }).await?;
    trx.commit().await?;

    // 3. Charlie does a wildcard fetch for docs, but only Press Release and HR Policies are returned
    let charlie_docs = c_charlie.fetch::<DocView>(Predicate::True).await?;
    let mut titles: Vec<_> = charlie_docs.iter().map(|d| d.title().unwrap()).collect();
    titles.sort();
    assert_eq!(titles, vec!["HR Policies", "Press Release"]);
    // 4. Charlie is allowed to fetch all users
    let charlie_users = c_charlie.fetch::<UserView>(Predicate::True).await?;
    assert!(charlie_users.len() == 4); // Including newly created Dave

    // 5. Charlie is sneaky and tries to edit his own role, but is blocked
    let user1 = all_users.items.iter().find(|u| u.username().unwrap() == "charlie").unwrap();
    let trx = c_charlie.begin();
    // assert!(matches!(user1.edit(&trx).await?.role.replace("admin"), Err(MutationError::AccessDenied(_))));
    assert!(matches!(user1.edit(&trx).await, Err(MutationError::AccessDenied(_))));
    trx.commit().await?;

    // 6. Charlie tries to create a user - but is blocked
    let trx = c_charlie.begin();
    assert!(matches!(
        trx.create(&User { username: "eve".into(), role: "employee".into(), department: "Finance".into() }).await,
        Err(MutationError::AccessDenied(_))
    ));
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

#[derive(Debug, Clone)]
pub struct DishonestTestAgent {}

/// Everything is allowed
impl PolicyAgent for DishonestTestAgent {
    type ContextData = MyContextData;
    fn pre_create(&self, cdata: &MyContextData, entity: &Entity) -> Result<(), AccessDenied> { Ok(()) }
    fn pre_edit(&self, cdata: &MyContextData, entity: &Entity) -> Result<(), AccessDenied> { Ok(()) }
    fn can_access_collection(&self, context: &MyContextData, _collection: &CollectionId) -> Result<(), AccessDenied> { Ok(()) }
    fn filter_predicate(&self, cdata: &MyContextData, collection: &CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied> {
        Ok(predicate)
    }
}

/// Actually enforces policies
impl PolicyAgent for TestAgent {
    type ContextData = MyContextData;
    fn pre_create(&self, cdata: &MyContextData, entity: &Entity) -> Result<(), AccessDenied> {
        println!("pre_create: {:?}", cdata);
        match cdata {
            MyContextData::Root => Ok(()),
            MyContextData::User { username, role, department } => {
                let collection = entity.collection();
                println!("collection: {} role: {} username: {}", collection, role, username);
                match collection.as_str() {
                    "user" => {
                        if role == "admin" {
                            Ok(())
                        } else {
                            Err(AccessDenied::ByPolicy("Only admins can create users"))
                        }
                    }
                    "doc" => {
                        // everybody can create documents
                        Ok(())
                    }
                    _ => Err(AccessDenied::ByPolicy("Unknown collection")),
                }
            }
        }
    }
    fn pre_edit(&self, cdata: &MyContextData, entity: &Entity) -> Result<(), AccessDenied> {
        match cdata {
            MyContextData::Root => Ok(()),
            MyContextData::User { username, role, department } => {
                // same as create
                let collection = entity.collection();
                println!("collection: {} role: {} username: {}", collection, role, username);
                match collection.as_str() {
                    "user" => {
                        if role == "admin" {
                            Ok(())
                        } else {
                            Err(AccessDenied::ByPolicy("Only admins can edit users"))
                        }
                    }
                    "doc" => {
                        // everybody can edit documents
                        Ok(())
                    }
                    _ => Err(AccessDenied::ByPolicy("Unknown collection")),
                }
            }
        }
    }
    fn can_access_collection(&self, context: &MyContextData, _collection: &CollectionId) -> Result<(), AccessDenied> { Ok(()) }
    fn filter_predicate(&self, cdata: &MyContextData, collection: &CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied> {
        match cdata {
            MyContextData::Root => Ok(predicate),
            MyContextData::User { role, .. } => {
                if role == "admin" {
                    Ok(predicate)
                } else if collection == "doc" {
                    // Define access filter based on role
                    let access_filter = match role.as_str() {
                        "manager" => parse_selection("access IN ('restricted', 'internal', 'public')")?,
                        "employee" => parse_selection("access IN ('internal', 'public')")?,
                        _ => return Err(AccessDenied::CollectionDenied(collection.clone())),
                    };

                    Ok(Predicate::And(Box::new(access_filter), Box::new(predicate)))
                } else {
                    Ok(predicate)
                }
            }
        }
    }
}

async fn create_test_dataset(context: Context) -> Result<()> {
    let trx = context.begin();
    trx.create(&User { username: "alice".into(), role: "admin".into(), department: "IT".into() }).await?;
    trx.create(&User { username: "bob".into(), role: "manager".into(), department: "HR".into() }).await?;
    trx.create(&User { username: "charlie".into(), role: "employee".into(), department: "Finance".into() }).await?;
    trx.create(&Doc { title: "Corp Strategy".into(), access: "restricted".into(), dept: "Executive".into(), owner: "alice".into() })
        .await?;
    trx.create(&Doc { title: "HR Policies".into(), access: "internal".into(), dept: "HR".into(), owner: "bob".into() }).await?;
    trx.create(&Doc { title: "Financials".into(), access: "confidential".into(), dept: "Finance".into(), owner: "charlie".into() }).await?;
    trx.create(&Doc { title: "Press Release".into(), access: "public".into(), dept: "Marketing".into(), owner: "bob".into() }).await?;
    trx.commit().await?;
    Ok(())
}
