// mod common;
// use ankurah::{
//     policy::DEFAULT_CONTEXT as c,
//     property::{value::LWW, PropertyError, PropertyValue, YrsString},
//     Model, Mutable, Node, PermissiveAgent, Property, ID,
// };
// use ankurah_storage_sled::SledStorageEngine;
// use anyhow::Result;

// use common::{Album, AlbumView};
// use serde::{Deserialize, Serialize};
// use std::marker::PhantomData;
// use std::sync::Arc;

// pub struct ErasedEdge {
//     //pub relationship_name: String, // [Model::name(), EdgeModel::name()]
//     pub edge_id: ID,
//     pub a: ID,
//     pub b: ID,
// }

// #[derive(Model)]
// pub struct NoneModel;

// pub struct Edge<BaseModel: Model, EdgeModel: Model = NoneModel> {
//     phantom: PhantomData<(BaseModel, EdgeModel)>,
// }

// impl<BaseModel: Model, EdgeModel: Model> std::fmt::Debug for Edge<BaseModel, EdgeModel> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_struct("Edge").finish() }
// }

// #[derive(Model)]
// pub struct Parent;
// #[derive(Model)]
// pub struct Child;
// #[derive(Model)]
// pub struct Sibling;

// #[derive(Model, Debug)]
// pub struct Person {
//     pub name: String,

//     #[ephemeral]
//     pub parents: Edge<Person, Parent>, // many-to-one

//     #[ephemeral]
//     pub children: Edge<Person, Child>, // one-to-many

//     #[ephemeral]
//     pub siblings: Edge<Person, Sibling>, // many-to-many

//     #[ephemeral]
//     pub residence: Edge<Residence>, // one-to-one
// }

// #[derive(Model, Debug)]
// pub struct Residence {
//     pub name: String,

//     #[ephemeral]
//     pub occupants: Edge<Person>,
// }

// #[tokio::test]
// async fn property_backends() -> Result<()> {
//     let client = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new()).context(c);

//     let trx = client.begin();
//     let cat_video = trx.create(&Person { name: "Blah".to_owned(), ..default() }).await;

//     trx.commit().await?;

//     Ok(())
// }

// #[cfg(feature = "postgres")]
// mod pg_common;

// #[cfg(feature = "postgres")]
// #[tokio::test]
// async fn pg_property_backends() -> Result<()> {
//     use ankurah::PermissiveAgent;

//     let (_container, storage_engine) = pg_common::create_postgres_container().await?;
//     let client = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new()).context(c);

//     let trx = client.begin();
//     let cat_video = trx.create(&Person { name: "Blah".to_owned(), ..Default::default() }).await;

//     trx.commit().await?;

//     Ok(())
// }
