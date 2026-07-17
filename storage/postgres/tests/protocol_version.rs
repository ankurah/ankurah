//! Protocol-version record semantics: a fresh store is marked with
//! `ankurah_proto::PROTOCOL_VERSION` on engine construction and checked on
//! every open; a store carrying a different (or missing) record refuses to
//! open, advising a development-database reset.

use ankurah_core::storage::StorageEngine;
use ankurah_proto::PROTOCOL_VERSION;
use ankurah_storage_postgres::Postgres;
use anyhow::Result;
use bb8_postgres::PostgresConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

/// A fresh postgres container plus a pool onto it (no engine yet: these tests
/// exercise engine construction itself).
async fn fresh_pool() -> Result<(ContainerAsync<postgres::Postgres>, bb8::Pool<PostgresConnectionManager<tokio_postgres::NoTls>>)> {
    let container: ContainerAsync<postgres::Postgres> =
        postgres::Postgres::default().with_db_name("ankurah").with_user("postgres").with_password("postgres").start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let manager = PostgresConnectionManager::new_from_stringlike(
        format!("host={host} port={port} user=postgres password=postgres dbname=ankurah"),
        tokio_postgres::NoTls,
    )?;
    let pool = bb8::Pool::builder().build(manager).await?;
    Ok((container, pool))
}

#[tokio::test]
async fn fresh_store_records_version_and_reopens() -> Result<()> {
    let (_container, pool) = fresh_pool().await?;

    // A fresh store records the version at engine construction.
    let engine = Postgres::new(pool.clone()).await?;
    {
        let client = pool.get().await?;
        let value: String = client.query_one(r#"SELECT "value" FROM "ankurah_meta" WHERE "key" = 'protocol_version'"#, &[]).await?.get(0);
        assert_eq!(value, PROTOCOL_VERSION.to_string());
    }

    // Reopening a store with the matching record proceeds, with or without data.
    engine.collection(&"albums".into()).await?;
    let reopened = Postgres::new(pool.clone()).await?;

    // A collection wipe preserves the record: the meta table is not a
    // collection, and the wiped store still reopens.
    assert!(reopened.delete_all_collections().await?, "wiping existing collections reports true");
    assert!(!reopened.delete_all_collections().await?, "nothing left to wipe reports false");
    let _rereopened = Postgres::new(pool).await?;
    Ok(())
}

#[tokio::test]
async fn mismatched_or_missing_version_refuses() -> Result<()> {
    let (_container, pool) = fresh_pool().await?;

    // Record the version and give the store some data.
    let engine = Postgres::new(pool.clone()).await?;
    engine.collection(&"albums".into()).await?;

    // Rewrite the stored version out of band.
    {
        let client = pool.get().await?;
        client.execute(r#"UPDATE "ankurah_meta" SET "value" = '999' WHERE "key" = 'protocol_version'"#, &[]).await?;
    }
    let err = match Postgres::new(pool.clone()).await {
        Ok(_) => panic!("expected the mismatched version to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("999") && err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the found and expected versions: {err}");

    // Remove the record while ankurah tables remain: the store now reads as an
    // unversioned store.
    {
        let client = pool.get().await?;
        client.execute(r#"DROP TABLE "ankurah_meta""#, &[]).await?;
    }
    let err = match Postgres::new(pool).await {
        Ok(_) => panic!("expected the unversioned store with existing data to refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains(&PROTOCOL_VERSION.to_string()), "refusal must name the expected version: {err}");
    Ok(())
}
