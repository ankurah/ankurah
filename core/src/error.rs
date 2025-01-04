use ankurah_proto::ID;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RetrievalError {
    #[error("ID {0:?} not found")]
    NotFound(ID),
    #[error("Storage error: {0}")]
    StorageError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Update failed: {0}")]
    FailedUpdate(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Deserialization error: {0}")]
    DeserializationError(bincode::Error),
    #[error("No durable peers available for fetch operation")]
    NoDurablePeers,
    #[error("Other error: {0}")]
    Other(String),
    #[error("bucket name must only contain valid characters")]
    InvalidBucketName,
    #[error("ankql filter: {0}")]
    AnkqlFilter(ankql::selection::filter::Error),
    #[error("Future join: {0}")]
    FutureJoin(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    Anyhow(anyhow::Error),
}

impl RetrievalError {
    pub fn storage(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        RetrievalError::StorageError(Box::new(err))
    }

    pub fn future_join(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        RetrievalError::FutureJoin(Box::new(err))
    }
}

impl From<bincode::Error> for RetrievalError {
    fn from(e: bincode::Error) -> Self {
        RetrievalError::DeserializationError(e)
    }
}

impl From<ankql::selection::filter::Error> for RetrievalError {
    fn from(err: ankql::selection::filter::Error) -> Self {
        RetrievalError::AnkqlFilter(err)
    }
}

impl From<anyhow::Error> for RetrievalError {
    fn from(err: anyhow::Error) -> Self {
        RetrievalError::Anyhow(err)
    }
}
