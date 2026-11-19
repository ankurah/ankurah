//! The maps' table keeps entries of a few kilobytes in line, and every
//! statement a connection prepares finds entries by the primary key, one key
//! at a time, so that none reads a whole map, or every map's entries, to find
//! a few: shown by the plans PostgreSQL makes for each, over a table that
//! holds a map of 100,000 entries and a thousand small ones beside one
//! written moments ago, before the table's statistics are gathered and
//! after, and for as many executions of a prepared statement as it takes
//! PostgreSQL to settle on a generic plan.

use ankurah_core::storage::{MapId, MapWrite, StorageEngine, StorageTransaction};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};

use super::{clear, create_table, delete, get, put, range_sqls};
use crate::test_server::{config, Server};
use crate::{Postgres, MAP_TABLE};

#[tokio::test]
async fn an_entry_of_a_few_kilobytes_stays_in_the_table() -> anyhow::Result<()> {
    let server = Server::start().await?;
    let engine = Postgres::new(server.pool().await?).await?;

    // Bytes that don't compress, as a digest's don't: a xorshift generator's.
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let value: Vec<u8> = (0..2_100)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let mut trx = engine.transaction();
    trx.map_write(vec![MapWrite::Put { map: MapId::new("m"), key: b"a".to_vec(), value }]).await?;
    trx.commit().await?.committed()?;

    let client = connect(&server).await?;
    let toast = format!("SELECT pg_relation_size(reltoastrelid) FROM pg_class WHERE relname = '{MAP_TABLE}'");
    let moved_out: i64 = client.query_one(&toast, &[]).await?.get(0);

    assert_eq!(moved_out, 0, "no entry went to the table's TOAST table");
    Ok(())
}

#[tokio::test]
async fn no_statement_reads_the_table_whole_to_find_a_maps_entries() -> anyhow::Result<()> {
    let server = Server::start().await?;
    let mut client = connect(&server).await?;
    create_table(&mut client).await?;
    fill(&client).await?;

    // Clearing a map reads all its entries, which for the large map is
    // reading the table whole; clearing a small one must read none of the
    // large one's.
    let keyed: Vec<String> = [get(), put(), delete()].into_iter().chain(range_sqls()).collect();
    let mut plans: Vec<(&str, &str)> = keyed.iter().flat_map(|sql| [(sql.as_str(), "large"), (sql.as_str(), "fresh")]).collect();
    let clear = clear();
    plans.push((&clear, "fresh"));

    for analyzed in [false, true] {
        if analyzed {
            client.batch_execute("ANALYZE").await?;
        }

        for &(sql, map) in &plans {
            for plan in plans_over_executions(&client, sql, map).await? {
                let whole = plan.contains(&format!("Seq Scan on {MAP_TABLE}"));
                let filters_entries = plan.lines().any(|line| line.trim_start().starts_with("Filter:"));

                assert!(!whole, "for map {map:?}, analyzed: {analyzed}, the statement\n{sql}\nis planned as\n{plan}");
                assert!(
                    !filters_entries,
                    "for map {map:?}, analyzed: {analyzed}, the statement\n{sql}\nreads entries to drop them:\n{plan}"
                );
            }
        }
    }

    Ok(())
}

/// Fill the table: a map of 100,000 entries, a thousand maps of ten, and a
/// map written moments ago, of two.
async fn fill(client: &Client) -> anyhow::Result<()> {
    let keys: Vec<Vec<u8>> = (0..100_000u64).map(key).collect();
    client
        .execute(
            &format!(
                r#"INSERT INTO "{MAP_TABLE}" ("map", "key", "value") SELECT 'large', key, $2 FROM unnest($1::bytea[]) AS entries (key)"#
            ),
            &[&keys, &vec![0u8; 100]],
        )
        .await?;

    client
        .batch_execute(&format!(
            r#"INSERT INTO "{MAP_TABLE}" ("map", "key", "value")
               SELECT convert_to('small ' || map, 'UTF8'), int8send(key), '\x00'::bytea
               FROM generate_series(1, 1000) AS map, generate_series(1, 10) AS key;
               INSERT INTO "{MAP_TABLE}" ("map", "key", "value") VALUES ('fresh', '\x01', '\x00'), ('fresh', '\x02', '\x00');"#
        ))
        .await?;

    Ok(())
}

/// The plans PostgreSQL makes for `sql` on the map `map` over a prepared
/// statement's first executions: custom plans for the first five, then
/// whichever it settles on, custom or generic. The first parameter is the
/// map, and every other gets a value of its type: a key, a few keys or as
/// many values, or a limit.
async fn plans_over_executions(client: &Client, sql: &str, map: &str) -> anyhow::Result<Vec<String>> {
    let types = client.prepare(sql).await?.params().to_vec();
    client.batch_execute(&format!("PREPARE checked AS {sql}")).await?;

    let bytes = |key: Vec<u8>| format!("'\\x{}'::bytea", key.iter().map(|byte| format!("{byte:02x}")).collect::<String>());
    let values: Vec<String> = types
        .iter()
        .enumerate()
        .map(|(place, parameter)| match *parameter {
            Type::BYTEA if place == 0 => format!("'{map}'::bytea"),
            Type::BYTEA => bytes(key(50_000)),
            Type::BYTEA_ARRAY => format!("ARRAY[{}]", [1, 50_000, 99_999].map(|n| bytes(key(n))).join(", ")),
            Type::INT8 => "64".to_owned(),
            ref other => panic!("no example of a parameter of type {other}"),
        })
        .collect();

    let execute = format!("EXPLAIN EXECUTE checked({})", values.join(", "));
    let mut plans = Vec::new();
    for _ in 0..8 {
        let rows = client.query(&execute, &[]).await?;
        plans.push(rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>().join("\n"));
    }

    client.batch_execute("DEALLOCATE checked").await?;
    Ok(plans)
}

/// The key of the `n`th entry.
fn key(n: u64) -> Vec<u8> { n.to_be_bytes().to_vec() }

/// A connection of the test's own to `server`'s database.
async fn connect(server: &Server) -> anyhow::Result<Client> {
    let (client, connection) = config(&server.host, server.port).connect(NoTls).await?;
    tokio::spawn(connection);

    Ok(client)
}
