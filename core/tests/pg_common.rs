#![cfg(feature = "postgres")]

use anyhow::Result;
// use postgres::NoTls;
use ankurah_core::storage::Postgres;
use r2d2_postgres::PostgresConnectionManager;
use std::sync::Arc;
use testcontainers::Container;
use testcontainers_modules::{postgres, testcontainers::runners::SyncRunner};

pub fn create_postgres_container() -> Result<(
    Container<postgres::Postgres>,
    ankurah_core::storage::Postgres,
)> {
    let container: Container<postgres::Postgres> = postgres::Postgres::default()
        .with_db_name("ankurah")
        .with_user("postgres")
        .with_password("postgres")
        .with_init_sql(include_str!("pg_init.sql").to_string().into_bytes())
        .start()
        .unwrap();

    let host = container.get_host()?;
    let port = container.get_host_port_ipv4(5432)?;
    let manager = PostgresConnectionManager::new(
        format!("host={host} port={port} user=postgres password=postgres dbname=ankurah")
            .parse()
            .unwrap(),
        ::postgres::NoTls,
    );
    let pool = r2d2::Pool::new(manager).unwrap();

    let storage_engine = Postgres::new(pool)?;

    Ok((container, storage_engine))
}
