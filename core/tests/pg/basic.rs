use super::*;

#[test]
fn test_postgres() -> Result<()> {
    let (_container, storage_engine) = create_postgres_container()?;
    let node = Arc::new(Node::new(Box::new(storage_engine)));

    let trx = node.begin();
    let _album = trx.create(&Album {
        name: "The rest of the owl".to_owned(),
    });

    trx.commit()?;

    Ok(())
}
