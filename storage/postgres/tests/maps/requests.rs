//! The requests a transaction's map writes make, counted at a proxy between
//! a node and its database: however long a commit's writes take to send,
//! they go with its `COMMIT` in one round trip.

use ankurah::core::storage::{MapId, MapWrite, StorageEngine, StorageTransaction};

use crate::database::Database;

/// A put of `$bytes` bytes under a key of the map `$map`.
macro_rules! put {
    ($map:expr, $bytes:expr) => {
        MapWrite::Put { map: MapId::new($map), key: b"k".to_vec(), value: vec![1; $bytes] }
    };
}

/// A commit whose writes take long to send, a large one behind a small one
/// the database answers at once, still sends them with its `COMMIT` in one
/// round trip.
#[tokio::test]
async fn a_commit_whose_writes_take_long_to_send_still_sends_them_with_its_commit_in_one_round_trip() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let proxy = database.proxy().await?;
    let engine = database.node_through(&proxy).await?;

    // The engine's first map call opens the connection the next one finds.
    let mut first = engine.transaction();
    first.map_write(vec![put!("a", 0)]).await?;
    first.commit().await?.committed()?;

    proxy.expect_round_trips(&[1, 3]);
    let before = proxy.round_trips();
    let mut trx = engine.transaction();
    trx.map_write(vec![put!("a", 1), put!("b", 8 << 20)]).await?;
    trx.commit().await?.committed()?;
    let made = proxy.round_trips() - before;

    assert_eq!(made, 2, "the transaction's beginning, then the two maps' puts and the COMMIT");
    Ok(())
}
