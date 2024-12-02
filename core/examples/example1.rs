use std::sync::Arc;

use ankurah_core::property::value::YrsString;
use ankurah_core::storage::SledStorageEngine;
use ankurah_core::{model::ScopedRecord, node::Node};
use ankurah_derive::Model;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{info, Level};

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    // DECISION: The model always contains projected types, which will initally be just the native types.
    // We will leave the door open to backend specific projected newtypes in the future, but we will not have the active value types in the model.
    // Implication: we will still need to have a native type to active type lookup in the Model macro for now.
    // We have the option of adding override attributes to switch backends in the future.
    // We will initially only use Model structs for initial construction of the record (or a property group thereof) but we may later consider
    // using them for per-property group retrieval binding, but preferably only via an immutable borrow.
    #[active_value(YrsString)]
    pub name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    // POINT 2 - registration? - agreed. not needed.
    // POINT 3 - rollbacks?
    // let rec = fetch(...);

    // <Button onClick={
    //     let trx = asdfasdfasdf;
    //     rec.name.set("new name");
    //     rec.edit(trx).set(whatever)
    // } />

    // let album : ReadRecord = fetch(...);
    // let album2 = album.clone();
    // query(track).subscribe(move || {
    //     let trx = client.begin();
    //     // Either: find a way to bind the edit of the existing record to the new trx
    //     // OR: get a new editable form of the record that is bound to the new trx ()
    //     let w : WritableRecord = album2.edit(trx);
    //     w.status.set("new name");
    // })

    // POINT 4 - explicit transactions for write/read? (how does this affect subscribers?)
    // POINT 5 - trx record vs UOW registration?
    // POINT 6 - backend instantiation and setup - who has a copy of what and how is it instantiated from a wayfinding perspective?
    // POINT 7 - State and Event DAG construction and happens before determination (operation id/precursor in state and event data)

    // Gradually uncomment this example as we add functionality
    // let server = Node::new();
    let client = Arc::new(Node::new(Box::new(SledStorageEngine::new().unwrap())));

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
        // let album = AlbumRecord::build(&client).with(Album {
        //     name: "The Dark Sid of the Moon".to_string(),
        // }).insert();

        // Conducive to macro syntax like create_album! { name: "", other: "" }
        // AND conducive to property graph usage in the future
        // let record = AlbumRecord::build().add<StringValue>("name","The Dark Sid of the Moon").add<StringValue>("other", "whatever").insert(&client);
        // record.name()

        // let model : &Artist = record.getcolumnfamily::<Artist>();
        // We should do this, but it's not a priority short term
        // record.get<StringValue>("name")

        // let record = fetchwhatever()? // if missing a field, but record is otherwise found, it probably shouldn't fail here
        // record.name.get()?; // it should fail here

        // What does this buy us?
        // * Plays nice with the future column family idea
        // * Decouples models from records a little bit more (which is a good thing for property graph future stuff)
        // * allows for individual field construction in the event that we want to direclyt construct value objects without injecting the record inner after the fact
        // Downsides:
        // *

        let trx = client.begin();
        let album = trx.create(&Album {
            name: "The Dark Sid of the Moon".into(),
        });

        info!("Album created: {:?}", album);

        //let test = client.fetch_record(album.id(), Album::bucket_name()).unwrap();

        album.name().insert(12, "e");
        //let record_event = album.get_record_event();
        //println!("Record event: {:?}", record_event);
        assert_eq!(album.name().value(), "The Dark Side of the Moon");

        let album_id = album.id();
        let from_scoped_album = trx.edit::<Album>(album).unwrap();
        let from_id = trx.edit::<Album>(album_id).unwrap();

        trx.commit().unwrap();

        let trx = client.begin();
        let album = trx.edit::<Album>(album_id).unwrap();
        println!("{:?}", album.name().value());
        let album = album.to_erased_record();
        trx.rollback();

        album
    };

    //assert_eq!(album.name(), "The Dark Side of the Moon");

    // should immediately have two operations - one for the initial insert, and one for the edit
    // assert_eq!(album.operation_count(), 2);
    // assert_eq!(client_albums.operation_count(), 2);
    // assert_eq!(client_albums.record_count(), 1);

    // Both client and server signals should trigger
    std::thread::sleep(std::time::Duration::from_millis(500));
    Ok(())
}
