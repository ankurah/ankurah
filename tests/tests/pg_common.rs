#![cfg(feature = "postgres")]

use ankurah_storage_postgres::Postgres;
use anyhow::Result;
use bb8_postgres::PostgresConnectionManager;
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

pub async fn create_postgres_container() -> Result<(ContainerAsync<postgres::Postgres>, ankurah_storage_postgres::Postgres)> {
    let container: ContainerAsync<postgres::Postgres> = postgres::Postgres::default()
        .with_db_name("ankurah")
        .with_user("postgres")
        .with_password("postgres")
        .with_init_sql(include_str!("pg_init.sql").to_string().into_bytes())
        // if you want to inspect the container
        // .with_container_name("ankurah_pg")
        // .with_reuse(testcontainers::ReuseDirective::Always)
        .start()
        .await
        .unwrap();

    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let manager = PostgresConnectionManager::new_from_stringlike(
        format!("host={host} port={port} user=postgres password=postgres dbname=ankurah"),
        tokio_postgres::NoTls,
    )?;
    let pool = bb8::Pool::builder().build(manager).await?;

    let storage_engine = Postgres::new(pool)?;

    Ok((container, storage_engine))
}
