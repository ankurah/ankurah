use ankurah_core::node::Node;
use ankurah_core::storage::SledStorageEngine;
use ankurah_derive::Model;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{info, Level};

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    // Gradually uncomment this example as we add functionality
    // let server = Node::new();
    let client = Node::new(SledStorageEngine::new().unwrap());

    // client.local_connect(&server);

    // let client_albums = client.collection::<Album>("album");
    // let server_albums = server.collection::<Album>("album");

    // Lets get signals working after we have the basics
    // tokio::spawn(server_albums.signal().for_each(|changeset| {
    //     println!("Album recordset changed on server: {}", changeset.operation);
    // }));

    // tokio::spawn(client_albums.signal().for_each(|changeset| {
    //     println!("Album recordset changed on client: {}", changeset.operation);
    // }));

    // TODO add macro for this
    // let album = create_album! {
    //     client,
    //     name: "The Dark Sid of the Moon",
    // };
    let album = {
        let trx = client.begin()?;
        let album = AlbumRecord::new(
            &client,
            Album {
                name: "The Dark Sid of the Moon".to_string(),
            },
        );
        // This should create the materialized storage-record with the TypeModuleStates and the intial storage-event with the initial TypeModuleOps

        // traits we need
        // TypeValue -- we need an concrete type accessor for the impl of this each field on the "instance object"
        // TypeEngine -- implement the state management for one or more property TypeValues of the same record

        info!("Album created: {:?}", album);
        // LEFT OFF HERE - need to get derive(Model) working again
        // This is an "edit", that issues an "operation" for the underlying TypeModule that the type is associated
        // with. And the "event" contains a serialized vec of all (1) operations that occurred during that transaction.
        // and the "state" of the materialized record is updated to include ALL TypeModuleStates for all fields in the model.
        // for now 1 edit = 1 operation = 1 transaction aka EVENT. Later we'll batch them up
        // ALSO - for now, lets assume that we can instantiate one TypeModule instance per each field, but later we will likely
        // want to update them to support multiple fields per modules

        // I don't particularly care if `album` is Record<Album> or AlbumRecord
        // as long as the field accesors (album.name, or album.name()) return a concrete TypeValue impl
        // and we can access the id property for the instance.
        album.name().insert(12, "e");
        // trxguard commits the transaction on drop
        album
    };

    assert_eq!(album.name.value(), "The Dark Side of the Moon");

    use ankurah_core::types::traits::StateSync;
    let update = album.name().get_pending_update();
    println!("Update length: {}", update.unwrap().len());

    // should immediately have two operations - one for the initial insert, and one for the edit
    // assert_eq!(album.operation_count(), 2);
    // assert_eq!(client_albums.operation_count(), 2);
    // assert_eq!(client_albums.record_count(), 1);

    // Both client and server signals should trigger
    Ok(())
}
