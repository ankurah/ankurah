use ankurah_core::node::Node;
use ankurah_core::storage::SledStorageEngine;
use ankurah_derive::Model;
use serde::{Deserialize, Serialize};
use tracing::{info, Level};

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    name: String,
}

#[tokio::main]
async fn main() {
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

    // let album = create_album! {
    //     client,
    //     name: "The Dark Sid of the Moon",
    // };
    let album = AlbumRecord::new(
        &client,
        Album {
            name: "The Dark Sid of the Moon".to_string(),
        },
    );

    info!("Album created: {:?}", album);
    // album.name.insert(12, "e"); // Whoops typo
    // assert_eq!(album.name.value(), "The Dark Side of the Moon");

    // should immediately have two operations - one for the initial insert, and one for the edit
    // assert_eq!(album.operation_count(), 2);
    // assert_eq!(client_albums.operation_count(), 2);
    // assert_eq!(client_albums.record_count(), 1);

    // Both client and server signals should trigger
}
