//! Store format record semantics: a fresh store is marked with this engine's
//! `STORE_FORMAT_VERSION` on engine construction and checked on every open; a
//! store carrying a different (or missing) record refuses to open with a
//! `StoreFormatError`.

use ankurah_core::storage::{StorageEngine, StoreFormatError};
use ankurah_proto::{ModelId, SystemModel};
use ankurah_storage_postgres::{Postgres, STORE_FORMAT_VERSION};
use anyhow::Result;
use bb8_postgres::PostgresConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

/// Create the model catalog's materialization, so the store holds Ankurah data beside its version record.
async fn touch_catalog_materialization(engine: &Postgres) -> Result<()> {
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    engine.fetch_states(&selection.clone().and_member_of(ModelId::System(SystemModel::Model))).await?;
    Ok(())
}

/// The refusal constructing an engine over `pool` meets, as the typed store format error.
async fn refusal(pool: &bb8::Pool<PostgresConnectionManager<tokio_postgres::NoTls>>) -> StoreFormatError {
    let error = Postgres::new(pool.clone()).await.err().expect("the store is refused");
    error.downcast_ref::<StoreFormatError>().cloned().unwrap_or_else(|| panic!("not a store format refusal: {error}"))
}

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
async fn a_fresh_store_records_its_format_version_and_reopens() -> Result<()> {
    let (_container, pool) = fresh_pool().await?;

    // A fresh store records the version at engine construction.
    let engine = Postgres::new(pool.clone()).await?;
    {
        let client = pool.get().await?;
        let value: String =
            client.query_one(r#"SELECT "value" FROM "_ankurah_meta" WHERE "key" = 'store_format_version'"#, &[]).await?.get(0);
        assert_eq!(value, STORE_FORMAT_VERSION.to_string());
    }

    // Reopening a store with the matching record proceeds, with or without data.
    touch_catalog_materialization(&engine).await?;
    let reopened = Postgres::new(pool.clone()).await?;

    // A collection wipe preserves the record: the meta table is not a
    // collection, and the wiped store still reopens.
    assert!(reopened.delete_all().await?, "wiping existing storage reports true");
    assert!(!reopened.delete_all().await?, "nothing left to wipe reports false");
    let _rereopened = Postgres::new(pool).await?;
    Ok(())
}

#[tokio::test]
async fn unrelated_tables_do_not_block_initialization_or_get_dropped() -> Result<()> {
    let (_container, pool) = fresh_pool().await?;
    {
        let client = pool.get().await?;
        client.execute(r#"CREATE TABLE "application_data" ("value" TEXT NOT NULL)"#, &[]).await?;
        client.execute(r#"INSERT INTO "application_data" ("value") VALUES ('keep me')"#, &[]).await?;
    }

    let engine = Postgres::new(pool.clone()).await?;
    touch_catalog_materialization(&engine).await?;
    assert!(engine.delete_all().await?, "Ankurah-owned storage was deleted");
    assert!(!engine.delete_all().await?, "unrelated tables do not count as Ankurah storage");

    let client = pool.get().await?;
    let value: String = client.query_one(r#"SELECT "value" FROM "application_data""#, &[]).await?.get(0);
    assert_eq!(value, "keep me");
    Ok(())
}

#[tokio::test]
async fn a_store_of_another_format_or_without_one_is_refused() -> Result<()> {
    let (_container, pool) = fresh_pool().await?;

    // Record the version and give the store some data.
    let engine = Postgres::new(pool.clone()).await?;
    touch_catalog_materialization(&engine).await?;

    // Rewrite the stored version out of band.
    {
        let client = pool.get().await?;
        client.execute(r#"UPDATE "_ankurah_meta" SET "value" = '999' WHERE "key" = 'store_format_version'"#, &[]).await?;
    }
    assert_eq!(refusal(&pool).await, StoreFormatError::Mismatch { found: 999, expected: STORE_FORMAT_VERSION });

    // Remove the record while ankurah tables remain: the store now reads as an
    // unversioned store.
    {
        let client = pool.get().await?;
        client.execute(r#"DROP TABLE "_ankurah_meta""#, &[]).await?;
    }
    assert_eq!(refusal(&pool).await, StoreFormatError::Unversioned { expected: STORE_FORMAT_VERSION });
    Ok(())
}
