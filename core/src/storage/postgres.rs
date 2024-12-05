use crate::{
    model::ID,
    storage::{RecordState, StorageBucket, StorageEngine},
};

use tokio_postgres::Client;

pub struct PostgresStorageEngine {
    // TODO: the rest of the owl
    //client: Client,
}

impl PostgresStorageEngine {
    pub fn new(client: Client) -> anyhow::Result<Self> {
        //Client::connect(params, tls_mode)
        //let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
        todo!()
    }
}

impl StorageEngine for PostgresStorageEngine {
    fn bucket(&self, name: &str) -> anyhow::Result<std::sync::Arc<dyn StorageBucket>> {
        todo!()
    }
}
