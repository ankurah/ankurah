use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
use ankql::parser::parse_selection;
use ankurah::error::MutationError;
use ankurah::model::Entity;
use ankurah::{
    changes::{ChangeKind, ChangeSet},
    core::{context::Context, node::ContextData},
    policy::{AccessDenied, PolicyAgent, DEFAULT_CONTEXT as c},
    proto::CollectionId,
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
async fn local_access_control() -> Result<(), Box<dyn std::error::Error>> {
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
    trx.create(&User { username: "dave".into(), role: "employee".into(), department: "IT".into() }).await?;
    trx.commit().await?;
    println!("passed 2");
    // CHARLIE

    // 3. Charlie does a wildcard fetch for docs, but only Press Release and HR Policies are returned
    let charlie_docs = ctx_c.fetch::<DocView>(Predicate::True).await?;
    let mut titles: Vec<_> = charlie_docs.iter().map(|d| d.title().unwrap()).collect();
    titles.sort();
    assert_eq!(titles, vec!["HR Policies", "Press Release"]);
    println!("passed 3");
    // 4. Charlie is allowed to fetch all users
    let charlie_users = ctx_c.fetch::<UserView>(Predicate::True).await?;
    assert!(charlie_users.len() == 4); // Including newly created Dave
    println!("passed 4");

    // 5. Charlie is sneaky and tries to edit his own role, but is blocked
    let user1 = all_users.items.iter().find(|u| u.username().unwrap() == "charlie").unwrap();
    let trx = ctx_c.begin();
    // assert!(matches!(user1.edit(&trx).await?.role.replace("admin"), Err(MutationError::AccessDenied(_))));
    assert!(matches!(user1.edit(&trx).await, Err(MutationError::AccessDenied(_))));
    trx.commit().await?;
    println!("passed 5");

    // 6. Charlie tries to create a user - but is blocked
    let trx = ctx_c.begin();
    assert!(matches!(
        trx.create(&User { username: "eve".into(), role: "employee".into(), department: "Finance".into() }).await,
        Err(MutationError::AccessDenied(_))
    ));
    trx.commit().await?;
    println!("passed 6");

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
    fn pre_create(&self, cdata: &MyContextData, entity: &Entity) -> Result<(), AccessDenied> {
        println!("pre_create: {:?}", cdata);
        match cdata {
            MyContextData::Root => Ok(()),
            MyContextData::User(user) => {
                let username: String = user.username()?;
                let role: String = user.role()?;
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
            MyContextData::User(user) => {
                // same as create
                let username: String = user.username()?;
                let role: String = user.role()?;
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
            MyContextData::User(user) => {
                let role: String = user.role()?;

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
