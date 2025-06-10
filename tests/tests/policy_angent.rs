// use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};
// use ankql::parser::parse_selection;
// use ankurah::error::{MutationError, RetrievalError, ValidationError};
// use ankurah::model::Entity;
// use ankurah::property::PropertyError;
// use ankurah::{
//     changes::{ChangeKind, ChangeSet},
//     core::{context::Context, node::ContextData},
//     policy::{AccessDenied, PolicyAgent},
//     proto::{self, CollectionId},
//     Model, Mutable, Node, PermissiveAgent, ResultSet,
// };
// use ankurah_connector_local_process::LocalProcessConnection;
// use ankurah_storage_sled::SledStorageEngine;
// use anyhow::Result;
// use async_trait::async_trait;
// use serde::{Deserialize, Serialize};
// use std::sync::{Arc, Mutex};

// mod common;
// use common::{Album, AlbumView, Pet, PetView};

// #[derive(Debug, Clone, Model)]
// pub struct User {
//     pub username: String,
//     pub max_level: u8,
// }

// #[derive(Debug, Clone, Model)]
// pub struct Doc {
//     pub title: String,
//     pub level: u8,
//     pub owner: String, // username of creator
// }

// pub enum MyContextData {
//     Root,
//     User { username: String, max_level: u8 },
// }

// impl std::fmt::Debug for MyContextData {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             MyContextData::Root => write!(f, "Root"),
//             MyContextData::User { username, max_level } => {
//                 write!(f, "User({} {} {})", username, max_level)
//             }
//         }
//     }
// }
// #[async_trait]
// impl ContextData for MyContextData {
//     // async fn validate(context: proto::AuthData) -> Result<Self, ValidationError> {
//     //     let proto: MyContextProto = serde_json::from_slice(&context.0).map_err(|e| ValidationError::Deserialization(Box::new(e)))?;
//     //     if proto.evil_bit {
//     //         Err(ValidationError::Rejected("Evil bit is set"))
//     //     } else {
//     //         let username = proto.username;
//     //         if username == "root" {
//     //             Ok(MyContextData::Root)
//     //         } else {
//     //             let role = proto.role;
//     //             let department = proto.department;
//     //             Ok(MyContextData::User { username, role, department })
//     //         }
//     //     }
//     // }

//     // fn proto(&self) -> proto::AuthData {
//     //     match self {
//     //         MyContextData::Root => {
//     //             let proto = MyContextProto { username: "root".into(), role: "root".into(), department: "root".into(), evil_bit: false };
//     //             proto::AuthData(serde_json::to_vec(&proto).map_err(|e| ValidationError::Serialization(e.to_string()))?)
//     //         }
//     //         MyContextData::User { username, role, department } => {
//     //             let proto =
//     //                 MyContextProto { username: username.clone(), role: role.clone(), department: department.clone(), evil_bit: false };
//     //             proto::AuthData(serde_json::to_vec(&proto).map_err(|e| ValidationError::Serialization(e.to_string()))?)
//     //         }
//     //     }
//     // }
// }
// impl TryFrom<UserView> for MyContextData {
//     type Error = PropertyError;
//     fn try_from(user: UserView) -> Result<Self, Self::Error> {
//         Ok(MyContextData::User { username: user.username()?, max_level: user.max_access_level()? })
//     }
// }

// #[tokio::test]
// async fn local_access_control() -> Result<(), Box<dyn std::error::Error>> {
//     let node = NodeBuilder::new(Arc::new(SledStorageEngine::new_test().build_durable().unwrap()), TestAgent {});
//     let root_context = node.context(MyContextData::Root);
//     create_test_dataset(root_context.clone()).await?;

//     let alice = root_context.fetch_one::<UserView>("username = 'alice'").await?.unwrap();
//     let bob = root_context.fetch_one::<UserView>("username = 'bob'").await?.unwrap();
//     let charlie = root_context.fetch_one::<UserView>("username = 'charlie'").await?.unwrap();

//     let c_alice = node.context(alice.try_into()?);
//     let _c_bob = node.context(bob.try_into()?);
//     let c_charlie = node.context(charlie.try_into()?);

//     // Scenarios we need to cover in this test:

//     // ALICE

//     // 1. Alice is allowed to fetch all users (this will just work because we're not actually checking anything yet)
//     let all_users = c_alice.fetch::<UserView>(Predicate::True).await?;
//     assert!(all_users.len() == 3);

//     // 2. Alice is allowed to create a new user
//     let trx = c_alice.begin();
//     trx.create(&User { username: "dave".into(), max_level: "employee".into() }).await?;
//     trx.commit().await?;

//     // 3. Charlie does a wildcard fetch for docs, but only Press Release and HR Policies are returned
//     let charlie_docs = c_charlie.fetch::<DocView>(Predicate::True).await?;
//     let mut titles: Vec<_> = charlie_docs.iter().map(|d| d.title().unwrap()).collect();
//     titles.sort();
//     assert_eq!(titles, vec!["HR Policies", "Press Release"]);
//     // 4. Charlie is allowed to fetch all users
//     let charlie_users = c_charlie.fetch::<UserView>(Predicate::True).await?;
//     assert!(charlie_users.len() == 4); // Including newly created Dave

//     // 5. Charlie is sneaky and tries to edit his own role, but is blocked
//     let user1 = all_users.items.iter().find(|u| u.username().unwrap() == "charlie").unwrap();
//     let trx = c_charlie.begin();
//     // assert!(matches!(user1.edit(&trx)?.role.replace("admin"), Err(MutationError::AccessDenied(_))));
//     assert!(matches!(user1.edit(&trx), Err(MutationError::AccessDenied(_))));
//     trx.commit().await?;

//     // 6. Charlie tries to create a user - but is blocked
//     let trx = c_charlie.begin();
//     assert!(matches!(
//         trx.create(&User { username: "eve".into(), max_level: "employee".into() }).await,
//         Err(MutationError::AccessDenied(_))
//     ));
//     trx.commit().await?;

//     Ok(())
// }

// #[tokio::test]
// async fn keeping_peers_honest() -> Result<(), Box<dyn std::error::Error>> {
//     // Create a server with TestAgent (enforcing policies) and a "dishonest" client with PermissiveAgent (no restrictions)
//     let server = NodeBuilder::new(Arc::new(SledStorageEngine::new_test().build_durable().unwrap()), TestAgent {});
//     let dishonest_client = NodeBuilder::new(Arc::new(SledStorageEngine::new_test().build_ephemeral().unwrap()), DishonestTestAgent {});

//     // Connect the client to the server
//     let _conn = LocalProcessConnection::new(&dishonest_client, &server).await?;

//     // Set up the initial dataset on the server with proper permissions
//     let server_context = server.context(MyContextData::Root);
//     create_test_dataset(server_context.clone()).await?;

//     // Get user references from server
//     let alice = server_context.fetch_one::<UserView>("username = 'alice'").await?.unwrap();
//     let charlie = server_context.fetch_one::<UserView>("username = 'charlie'").await?.unwrap();

//     // Create contexts for both honest and dishonest operations
//     let c_alice = dishonest_client.context(alice.try_into()?);
//     let c_charlie = dishonest_client.context(charlie.try_into()?);

//     // 1. Alice is allowed to fetch all users (this will just work because we're not actually checking anything yet)
//     let all_users = c_alice.fetch::<UserView>(Predicate::True).await?;
//     assert!(all_users.len() == 3);

//     // 2. Alice is allowed to create a new user
//     let trx = c_alice.begin();
//     trx.create(&User { username: "dave".into(), max_level: "employee".into() }).await?;
//     trx.commit().await?;

//     // 3. Charlie does a wildcard fetch for docs, but only Press Release and HR Policies are returned
//     let charlie_docs = c_charlie.fetch::<DocView>(Predicate::True).await?;
//     let mut titles: Vec<_> = charlie_docs.iter().map(|d| d.title().unwrap()).collect();
//     titles.sort();
//     assert_eq!(titles, vec!["HR Policies", "Press Release"]);
//     // 4. Charlie is allowed to fetch all users
//     let charlie_users = c_charlie.fetch::<UserView>(Predicate::True).await?;
//     assert!(charlie_users.len() == 4); // Including newly created Dave

//     // 5. Charlie is sneaky and tries to edit his own role, but is blocked
//     let user1 = all_users.items.iter().find(|u| u.username().unwrap() == "charlie").unwrap();
//     let trx = c_charlie.begin();
//     // assert!(matches!(user1.edit(&trx)?.role.replace("admin"), Err(MutationError::AccessDenied(_))));
//     assert!(matches!(user1.edit(&trx), Err(MutationError::AccessDenied(_))));
//     trx.commit().await?;

//     // 6. Charlie tries to create a user - but is blocked
//     let trx = c_charlie.begin();
//     assert!(matches!(
//         trx.create(&User { username: "eve".into(), max_level: "employee".into() }).await,
//         Err(MutationError::AccessDenied(_))
//     ));
//     trx.commit().await?;

//     Ok(())
// }

// // TODO cover fetch,subscribe,create,modify,delete

// #[derive(Debug, Clone)]
// pub struct TestAgent {
//     // maybe I have a clone of the Node in here
//     // or maybe I have my own state machine stuff
//     // or maybe I'm totally stateless
// }

// #[derive(Debug, Clone)]
// pub struct DishonestTestAgent {}

// /// Everything is allowed
// #[async_trait]
// impl PolicyAgent for DishonestTestAgent {
//     type ContextData = MyContextData;

//     fn check_read(&self, cdata: &MyContextData, entity: &Entity) -> Result<(), AccessDenied> { Ok(()) }
//     fn check_write(&self, cdata: &MyContextData, entity: &Entity, event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

//     fn can_access_collection(&self, _context: &MyContextData, _collection: &CollectionId) -> Result<(), AccessDenied> { Ok(()) }
//     fn filter_predicate(
//         &self,
//         _cdata: &MyContextData,
//         _collection: &CollectionId,
//         predicate: Predicate,
//     ) -> Result<Predicate, AccessDenied> {
//         Ok(predicate)
//     }

//     fn sign_request<SE: StorageEngine>(
//         &self,
//         node: &NodeInner<SE, Self>,
//         cdata: &Self::ContextData,
//         request: &proto::Request,
//     ) -> proto::AuthData {
//         proto::AuthData(vec![])
//     }

//     async fn check_request<SE: StorageEngine>(
//         &self,
//         node: &Node<SE, Self>,
//         auth: &proto::AuthData,
//         request: &proto::Request,
//     ) -> Result<Self::ContextData, ValidationError> {
//         Ok(MyContextData::Root)
//     }
//     fn check_event<SE: StorageEngine>(
//         &self,
//         node: &Node<SE, Self>,
//         cdata: &Self::ContextData,
//         event: &proto::Event,
//     ) -> Result<Option<proto::Attestation>, AccessDenied> {
//         Ok(None)
//     }

//     fn validate_received_event<SE: StorageEngine>(
//         &self,
//         node: &Node<SE, Self>,
//         received_from_node: &proto::NodeId,
//         event: &Attested<proto::Event>,
//     ) -> Result<(), AccessDenied> {
//         Ok(())
//     }
// }

// pub struct MyAuthData {
//     pub user_id: String,
//     pub fake_signed: bool,
// }
// pub struct MyAttestation {
//     pub node_id: String,
//     pub signature: String,
// }

// /// Actually enforces policies
// impl PolicyAgent for TestAgent {
//     type ContextData = MyContextData;
//     type ContextData: ContextData;

//     fn sign_request<SE: StorageEngine>(
//         &self,
//         node: &NodeInner<SE, Self>,
//         cdata: &MyContextData,
//         request: &proto::Request,
//     ) -> proto::AuthData {
//         // Pretending this is a JWT or something like that
//         proto::AuthData(serde_json::to_vec(&MyAuthData { user_id: cdata.username.clone(), fake_signed: true }).unwrap())
//     }

//     async fn check_request<SE: StorageEngine>(
//         &self,
//         node: &Node<SE, Self>,
//         auth: &proto::AuthData,
//         request: &proto::Request,
//     ) -> Result<Self::ContextData, ValidationError>
//     where
//         Self: Sized,
//     {
//         let auth_data: MyAuthData = serde_json::from_slice(&auth.0).map_err(|e| ValidationError::Deserialization(Box::new(e)))?;
//         // "Validate" the JWT
//         if auth_data.fake_signed {
//             Ok(MyContextData::User { username: auth_data.user_id, max_level: 0 })
//         } else {
//             Err(ValidationError::InvalidAuthData)
//         }
//     }

//     fn check_event<SE: StorageEngine>(
//         &self,
//         node: &Node<SE, Self>,
//         cdata: &Self::ContextData,
//         entity: &Entity,
//         event: &proto::Event,
//     ) -> Result<Option<proto::Attestation>, AccessDenied> {
//         self.check_write(cdata, entity, Some(event))?;
//         Ok(Some(proto::Attestation(
//             serde_json::to_vec(&MyAttestation { node_id: node.id.to_base64(), signature: "shibboleet".to_string() }).unwrap(),
//         )))
//     }

//     /// Validate an event attestation
//     fn validate_received_event<SE: StorageEngine>(
//         &self,
//         node: &Node<SE, Self>,
//         received_from_node: &proto::NodeId,
//         event: &Attested<proto::Event>,
//     ) -> Result<(), AccessDenied> {
//         // iterate over all the attestions and check if the node_id is in the list of trusted nodes (durable peers)
//         let trusted_nodes = node.get_durable_peers();
//         for attestation in event.attestations.iter() {
//             let attestation: MyAttestation = serde_json::from_slice(&attestation.0).map_err(|e| AccessDenied::InvalidAttestation)?;
//             if trusted_nodes.contains(&attestation.node_id) && attestation.signature == "shibboleet" {
//                 return Ok(());
//             }
//         }
//         Err(AccessDenied::InsufficientAttestation)
//     }

//     fn can_access_collection(&self, data: &Self::ContextData, collection: &proto::CollectionId) -> Result<(), AccessDenied>;

//     fn filter_predicate(
//         &self,
//         cdata: &MyContextData,
//         collection: &CollectionId,
//         mut predicate: Predicate,
//     ) -> Result<Predicate, AccessDenied> {
//         match cdata {
//             // Root can access everything
//             MyContextData::Root => Ok(predicate),
//             MyContextData::User { max_level, username } => {
//                 // We only want to show docs that are owned by the user or have sufficient access
//                 if (collection == "doc") {
//                     let addendum = parse_selection(&format!("username = '{}' OR level <= {}", username, max_level))?;
//                     predicate = Predicate::And(Box::new(predicate), Box::new(addendum));
//                 }

//                 Ok(predicate)
//             }
//         }
//     }

//     /// Check if a context can read an entity
//     fn check_read(&self, cdata: &MyContextData, entity: &Entity) -> Result<(), AccessDenied> {
//         // do the same same as filter predicate on the entity
//         let collection = entity.collection();

//         if entity.collection() == "doc" {
//             if cdata.max_level <= cdata.max_level {
//                 return Ok(());
//             } else {
//                 Err(AccessDenied::ByPolicy("Only access level 3 or higher can read users"))
//             }
//         }

//         Ok(())
//     }

//     fn check_write(&self, cdata: &MyContextData, entity: &Entity) -> Result<(), AccessDenied> {
//         match cdata {
//             MyContextData::Root => Ok(()),
//             MyContextData::User { username, max_level, .. } => {
//                 // same as create
//                 let collection = entity.collection();
//                 println!("collection: {} max_level: {} username: {}", collection, max_level, username);
//                 match collection.as_str() {
//                     "user" => {
//                         if max_level >= 3 {
//                             Ok(())
//                         } else {
//                             Err(AccessDenied::ByPolicy("Only access level 3 or higher can edit users"))
//                         }
//                     }
//                     "doc" => {
//                         // everybody can edit documents
//                         Ok(())
//                     }
//                     _ => Err(AccessDenied::ByPolicy("Unknown collection")),
//                 }
//             }
//         }
//     }
// }

// async fn create_test_dataset(context: Context) -> Result<()> {
//     let trx = context.begin();
//     trx.create(&User { username: "alice".into(), max_level: 3 }).await?; // CFO
//     trx.create(&User { username: "bob".into(), max_level: 2 }).await?; // Manager
//     trx.create(&User { username: "charlie".into(), max_level: 1 }).await?; // Employee

//     trx.create(&Doc { title: "Press Release".into(), level: 0, owner: "bob".into() }).await?; // Anyone can read
//     trx.create(&Doc { title: "HR Policies".into(), level: 1, owner: "bob".into() }).await?; // employees can read
//     trx.create(&Doc { title: "Corp Strategy".into(), level: 2, owner: "alice".into() }).await?; // managers can read
//     trx.create(&Doc { title: "Financials".into(), level: 3, owner: "charlie".into() }).await?; // leadership can read
//     trx.commit().await?;
//     Ok(())
// }
