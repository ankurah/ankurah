use ankurah_model_derive::Model;
use model::node::Node;

#[tokio::main]
async fn main() {
    // Gradually uncomment this example as we add functionality

    // let server = Node::new();
    let client = Node::new();

    // client.local_connect(&server);

    let client_albums = client.collection::<Album>("album");
    // let server_albums = server.collection::<Album>("album");

    // Lets get signals working after we have the basics
    // tokio::spawn(server_albums.signal().for_each(|changeset| {
    //     println!("Album recordset changed on server: {}", changeset.operation);
    // }));

    // tokio::spawn(client_albums.signal().for_each(|changeset| {
    //     println!("Album recordset changed on client: {}", changeset.operation);
    // }));

    let album = create_album! {
        client,
        name: "The Dark Sid of the Moon",
        status: Status::OnSale,
    };

    album.name.insert(12, "e"); // Whoops typo
    assert_eq!(album.name.value(), "The Dark Side of the Moon");

    // should immediately have two operations - one for the initial insert, and one for the edit
    assert_eq!(album.operation_count(), 2);
    assert_eq!(client_albums.operation_count(), 2);
    assert_eq!(client_albums.record_count(), 1);

    // Both client and server signals should trigger
}

use std::sync::mpsc;

#[derive(Model)] // This line now uses the Model derive macro
pub struct Album {
    id: ID,
    name: String,
}
