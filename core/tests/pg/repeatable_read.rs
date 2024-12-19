use super::*;

#[test]
fn test_repeatable_read() -> Result<()> {
    let (_container, postgres) = create_postgres_container()?;
    let client = Arc::new(Node::new(Box::new(postgres)));

    let id;
    {
        let trx = client.begin();
        let album_rw = trx.create(&Album {
            name: "I love cats".into(),
        });
        assert_eq!(album_rw.name().value(), "I love cats");
        id = album_rw.id();
        trx.commit()?;
    }

    // TODO: implement ScopedRecord.read() -> Record
    let album_ro: AlbumRecord = client.get_record(id).unwrap();

    let trx2 = client.begin();
    let album_rw2 = album_ro.edit(&trx2)?;

    let trx3 = client.begin();
    let album_rw3 = album_ro.edit(&trx3)?;

    // tx2 cats -> tofu
    album_rw2.name().delete(7, 4);
    album_rw2.name().insert(7, "tofu");
    assert_eq!(album_rw2.name().value(), "I love tofu");

    // tx3 love -> devour
    album_rw3.name().delete(2, 4);
    album_rw3.name().insert(2, "devour");
    // a modest proposal
    assert_eq!(album_rw3.name().value(), "I devour cats");

    // trx2 and 3 are uncommited, so the value should not be updated
    assert_eq!(album_ro.name(), "I love cats");
    trx2.commit()?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name(), "I love tofu");

    trx3.commit()?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name(), "I devour tofu");

    Ok(())
}
